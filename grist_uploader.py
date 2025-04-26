import requests
import csv
import json
import os
import traceback
import shutil
import logging
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- GristUploader Class (unchanged) ---
class GristUploader:
    def __init__(self, doc_id, api_key=None, server_url=None):
        """Initialize the GristUploader with necessary parameters."""
        self.doc_id = doc_id
        self.api_key = api_key or os.getenv('GRIST_API_KEY')
        self.server_url = server_url or os.getenv('GRIST_SERVER_URL', 'https://docs.getgrist.com')

        if not self.api_key:
            raise ValueError("API key is required. Provide as parameter or set GRIST_API_KEY environment variable.")
        if not self.doc_id:
             raise ValueError("Grist Doc ID is required. Set GRIST_DOC_ID environment variable.")

        # Set up headers for API requests
        self.headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json',
        }

    def get_tables(self):
        """Get list of tables in the document."""
        url = f"{self.server_url}/api/docs/{self.doc_id}/tables"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status() # Raises HTTPError for bad responses (4xx or 5xx)
        return response.json()

    def get_table_data(self, table_id):
        """Get data from a specific table."""
        url = f"{self.server_url}/api/docs/{self.doc_id}/tables/{table_id}/records"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json()

    def get_table_columns(self, table_id):
        """Get column information for a specific table."""
        url = f"{self.server_url}/api/docs/{self.doc_id}/tables/{table_id}/columns"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        data = response.json()
        # print(f"Raw column data for {table_id}: {json.dumps(data, indent=2)}") # Debug print
        return data

    def add_records(self, table_id, records):
        """Add records to a specific table."""
        if not records:
            print(f"No records provided to add to table {table_id}.")
            return None # Nothing to add

        url = f"{self.server_url}/api/docs/{self.doc_id}/tables/{table_id}/records"
        data = {"records": records}

        # print(f"Sending request to: {url}") # Debug print
        # print(f"First record sample: {json.dumps(records[0], indent=2)}") # Debug print

        response = requests.post(url, headers=self.headers, json=data)
        response.raise_for_status() # Let exceptions propagate
        return response.json()

# --- Helper Functions (mostly unchanged) ---
def create_column_mapping_from_grist(columns_data, csv_headers):
    """Create a mapping from CSV headers to Grist column IDs"""
    mapping = {}
    grist_columns = []
    if "columns" in columns_data and isinstance(columns_data["columns"], list):
        grist_columns = {col["fields"]["label"]: col["id"] for col in columns_data["columns"] if "id" in col and "fields" in col and "label" in col["fields"]}
        grist_col_ids = {col["id"] for col in columns_data["columns"] if "id" in col} # Keep track of raw IDs too

    # print(f"Grist Labels->IDs: {grist_columns}") # Debug
    # print(f"Grist IDs: {grist_col_ids}") # Debug

    for csv_header in csv_headers:
        clean_csv_header = csv_header.strip()
        # 1. Try exact label match
        if clean_csv_header in grist_columns:
            mapping[csv_header] = grist_columns[clean_csv_header]
            continue
        # 2. Try exact ID match (if CSV header happens to be the ID)
        if clean_csv_header in grist_col_ids:
             mapping[csv_header] = clean_csv_header
             continue
        # 3. Try label match ignoring case and replacing space with underscore
        normalized_csv_header = clean_csv_header.lower().replace(" ", "_")
        found_match = False
        for label, col_id in grist_columns.items():
             if label.lower().replace(" ", "_") == normalized_csv_header:
                 mapping[csv_header] = col_id
                 found_match = True
                 break
        if found_match:
            continue
        # 4. Try ID match ignoring case and replacing space with underscore
        for col_id in grist_col_ids:
             if col_id.lower().replace(" ", "_") == normalized_csv_header:
                 mapping[csv_header] = col_id
                 found_match = True
                 break
        # if found_match: # Already handled by continue
        #     continue

        # If no match found, maybe log a warning or skip
        # print(f"Warning: No Grist column found for CSV header '{csv_header}'")

    # print(f"Generated column mapping: {json.dumps(mapping, indent=2)}") # Debug
    return mapping

def read_csv_to_records(csv_file_path, column_mapping):
    """Read a CSV file and convert it to records for Grist using the mapping."""
    records = []
    try:
        with open(csv_file_path, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            if not reader.fieldnames: # Check if headers exist
                 print(f"Warning: CSV file seems empty or has no headers: {csv_file_path}")
                 return []
            for row in reader:
                record = {"fields": {}}
                valid_row = False
                for csv_col, grist_field in column_mapping.items():
                    if csv_col in row and row[csv_col] is not None: # Check if column exists in row and has value
                        record["fields"][grist_field] = row[csv_col]
                        valid_row = True # Mark row as having at least one mapped value
                if valid_row: # Only add records that have at least one mapped field
                    records.append(record)
    except FileNotFoundError:
        print(f"Error: File not found while reading: {csv_file_path}")
        raise # Re-raise to be caught by the processing logic
    except Exception as e:
        print(f"Error reading CSV file {csv_file_path}: {e}")
        raise # Re-raise
    return records

# --- New/Modified Functions ---

def setup_logging(log_file):
    """Sets up logging to file."""
    logging.basicConfig(
        level=logging.ERROR,
        format='%(asctime)s - %(levelname)s - %(message)s',
        filename=log_file,
        filemode='a' # Append to the log file
    )
    # Also add a handler to print errors to console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.ERROR)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    logging.getLogger('').addHandler(console_handler)


def upload_csv_to_grist(csv_file_path, table_id, uploader, grist_columns_data):
    """Handles reading, mapping, and uploading a single CSV to a Grist table."""
    print(f"Attempting to upload: {os.path.basename(csv_file_path)} to table {table_id}")

    # Extract CSV headers
    try:
        with open(csv_file_path, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            try:
                csv_headers = next(reader)
                # Check if there's at least one data row by trying to read it
                first_row = next(reader, None)
                if first_row is None and not csv_headers: # Completely empty file
                     print(f"Skipping empty file (no headers, no data): {os.path.basename(csv_file_path)}")
                     return True # Treat as success (nothing to upload)
                if first_row is None:
                     print(f"Skipping file with only headers: {os.path.basename(csv_file_path)}")
                     return True # Treat as success (nothing to upload)
                # print(f"CSV Headers for {os.path.basename(csv_file_path)}: {csv_headers}") # Debug
            except StopIteration:
                print(f"Skipping empty file (StopIteration): {os.path.basename(csv_file_path)}")
                return True # Treat as success
    except FileNotFoundError:
         print(f"Error: File not found before reading headers: {csv_file_path}")
         raise # Propagate error
    except Exception as e:
         print(f"Error reading headers from {csv_file_path}: {e}")
         raise # Propagate error


    # Generate mapping
    column_mapping = create_column_mapping_from_grist(grist_columns_data, csv_headers)
    if not column_mapping:
         print(f"Warning: No column mapping generated for {os.path.basename(csv_file_path)}. Check CSV headers and Grist columns.")
         # Decide if this is an error or just a skip. Let's treat as skippable success for now.
         return True

    # Read CSV data
    records = read_csv_to_records(csv_file_path, column_mapping)
    if not records:
        print(f"No data records found or mapped in {os.path.basename(csv_file_path)}.")
        return True # Treat as success (nothing to upload)

    # Upload records in batches
    batch_size = 100 # Consider making this configurable via .env if needed
    total_records_in_file = len(records)
    print(f"Uploading {total_records_in_file} records from {os.path.basename(csv_file_path)} in batches of {batch_size}...")

    for i in range(0, total_records_in_file, batch_size):
        batch = records[i:i+batch_size]
        try:
            result = uploader.add_records(table_id, batch)
            # print(f"Uploaded batch {i // batch_size + 1}/{(total_records_in_file + batch_size - 1) // batch_size} for {os.path.basename(csv_file_path)}") # Debug
        except Exception as e:
            # Error during batch upload - log and signal failure for the whole file
            print(f"Error uploading batch for {os.path.basename(csv_file_path)}: {e}")
            # Log detailed error if possible (e.g., response content from HTTPError)
            if isinstance(e, requests.exceptions.HTTPError) and e.response is not None:
                 logging.error(f"Grist upload failed for {os.path.basename(csv_file_path)} (Table: {table_id}). Status: {e.response.status_code}. Response: {e.response.text}")
            else:
                 logging.error(f"Grist upload failed for {os.path.basename(csv_file_path)} (Table: {table_id}). Error: {e}\n{traceback.format_exc()}")
            return False # Indicate failure for this file

    print(f"Successfully uploaded {total_records_in_file} records from {os.path.basename(csv_file_path)}.")
    return True # Indicate success for this file


def move_and_rename_file(source_path, success_dir):
    """Moves a file to the success directory and renames its extension."""
    if not os.path.exists(source_path):
        print(f"Warning: Source file not found for moving: {source_path}")
        return False # Indicate failure

    base_filename = os.path.basename(source_path)
    name_part, _ = os.path.splitext(base_filename)
    dest_filename = f"{name_part}.success"
    dest_path = os.path.join(success_dir, dest_filename)

    try:
        # Ensure success directory exists
        os.makedirs(success_dir, exist_ok=True)
        shutil.move(source_path, dest_path)
        print(f"Successfully moved and renamed {base_filename} to {dest_filename} in {success_dir}")
        return True
    except Exception as e:
        logging.error(f"Failed to move/rename {base_filename} to {dest_path}. Error: {e}\n{traceback.format_exc()}")
        return False


def main():
    # --- Configuration ---
    log_file_name = os.getenv('LOG_FILE_NAME', 'upload_errors.log')
    setup_logging(log_file_name)

    doc_id = os.getenv('GRIST_DOC_ID')
    header_table_id = os.getenv('GRIST_TABLE_ID')
    items_table_id = os.getenv('GRIST_ITEMS_TABLE_ID')
    csv_directory_path = os.getenv('CSV_DIRECTORY_PATH')
    success_folder_name = os.getenv('SUCCESS_FOLDER_NAME', 'Success')

    # --- Validate Configuration ---
    if not all([doc_id, header_table_id, items_table_id, csv_directory_path]):
        logging.error("Missing critical environment variables (GRIST_DOC_ID, GRIST_TABLE_ID, GRIST_ITEMS_TABLE_ID, CSV_DIRECTORY_PATH). Exiting.")
        return

    if not os.path.isdir(csv_directory_path):
        logging.error(f"CSV directory path '{csv_directory_path}' not found or is not a directory. Exiting.")
        return

    success_dir_path = os.path.join(csv_directory_path, success_folder_name)
    try:
        os.makedirs(success_dir_path, exist_ok=True)
    except OSError as e:
         logging.error(f"Could not create success directory '{success_dir_path}'. Error: {e}. Exiting.")
         return

    print(f"Processing CSV files from directory: {csv_directory_path}")
    print(f"Header Table ID: {header_table_id}")
    print(f"Items Table ID: {items_table_id}")
    print(f"Success Folder: {success_dir_path}")
    print(f"Log File: {log_file_name}")

    # --- Initialize Grist Uploader ---
    try:
        uploader = GristUploader(doc_id)
    except ValueError as e:
        logging.error(f"Failed to initialize Grist Uploader: {e}. Exiting.")
        return
    except Exception as e:
         logging.error(f"An unexpected error occurred during Grist Uploader initialization: {e}\n{traceback.format_exc()}. Exiting.")
         return

    # --- Pre-fetch Grist Column Info (reduces API calls) ---
    try:
        print("Getting Grist column info for Header table...")
        header_columns_data = uploader.get_table_columns(header_table_id)
        print("Getting Grist column info for Items table...")
        items_columns_data = uploader.get_table_columns(items_table_id)
    except Exception as e:
        logging.error(f"Failed to get Grist column information. Check API key, Doc ID, Table IDs, and network connection. Error: {e}\n{traceback.format_exc()}. Exiting.")
        return

    # --- Find and Pair Files ---
    files_in_dir = os.listdir(csv_directory_path)
    file_prefixes = set()
    potential_files = {}

    for f in files_in_dir:
        if f.endswith('_Header.csv'):
            prefix = f[:-len('_Header.csv')]
            file_prefixes.add(prefix)
            potential_files[f] = os.path.join(csv_directory_path, f)
        elif f.endswith('_Items.csv'):
            prefix = f[:-len('_Items.csv')]
            file_prefixes.add(prefix)
            potential_files[f] = os.path.join(csv_directory_path, f)

    print(f"Found {len(file_prefixes)} unique file prefixes.")

    # --- Process Paired Files ---
    processed_count = 0
    success_count = 0
    fail_count = 0

    for prefix in sorted(list(file_prefixes)): # Sort for consistent processing order
        header_filename = f"{prefix}_Header.csv"
        items_filename = f"{prefix}_Items.csv"

        header_path = potential_files.get(header_filename)
        items_path = potential_files.get(items_filename)

        # Check if BOTH files for the prefix exist in the source directory
        if header_path and items_path and os.path.exists(header_path) and os.path.exists(items_path):
            print(f"\n--- Processing Pair: {prefix} ---")
            processed_count += 1
            header_success = False
            items_success = False

            # 1. Process Header File
            try:
                header_success = upload_csv_to_grist(header_path, header_table_id, uploader, header_columns_data)
            except Exception as e:
                # Catch unexpected errors during header processing (already logged in upload_csv_to_grist if it's an upload error)
                if not isinstance(e, (requests.exceptions.RequestException, FileNotFoundError)): # Avoid double logging known errors
                     logging.error(f"Unexpected error processing header file {header_filename}: {e}\n{traceback.format_exc()}")
                header_success = False # Ensure it's marked as failed

            # 2. Process Items File (only if header was successful)
            if header_success:
                try:
                    items_success = upload_csv_to_grist(items_path, items_table_id, uploader, items_columns_data)
                except Exception as e:
                    if not isinstance(e, (requests.exceptions.RequestException, FileNotFoundError)):
                         logging.error(f"Unexpected error processing items file {items_filename}: {e}\n{traceback.format_exc()}")
                    items_success = False
            else:
                 print(f"Skipping items file {items_filename} because header processing failed.")
                 items_success = False # Mark items as failed if header failed

            # 3. Move files if BOTH succeeded
            if header_success and items_success:
                print(f"Both uploads successful for prefix '{prefix}'. Moving files...")
                moved_header = move_and_rename_file(header_path, success_dir_path)
                moved_items = move_and_rename_file(items_path, success_dir_path)
                if moved_header and moved_items:
                    success_count += 1
                    print(f"Successfully processed and moved pair: {prefix}")
                else:
                    fail_count += 1
                    logging.error(f"Uploads succeeded for '{prefix}', but failed to move one or both files. Please check manually.")
            else:
                fail_count += 1
                print(f"Processing failed for pair '{prefix}'. Files will not be moved. Check log '{log_file_name}' for details.")

        elif header_path and not items_path:
             # Only header exists, maybe log this? Or ignore if it's expected.
             # print(f"Found header file '{header_filename}' but no matching items file.")
             pass
        elif not header_path and items_path:
             # Only items exists
             # print(f"Found items file '{items_filename}' but no matching header file.")
             pass
        # else: both don't exist (or one/both already moved) - do nothing


    # --- Final Summary ---
    print("\n--- Upload Summary ---")
    print(f"Total unique prefixes found: {len(file_prefixes)}")
    print(f"Pairs attempted processing (both files existed): {processed_count}")
    print(f"Pairs successfully processed and moved: {success_count}")
    print(f"Pairs failed (upload or move error): {fail_count}")
    print(f"Check '{log_file_name}' for any error details.")
    print("Processing complete.")


if __name__ == "__main__":
    main()
