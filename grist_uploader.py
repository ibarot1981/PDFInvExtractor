import requests
import csv
import json
import os
import traceback
import shutil
import logging
import time # Added for potential retries or delays
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


# --- Invoice Log Functions ---

PROCESSED_INVOICES_LOG_FILENAME = "processed_invoices.log"
INVOICE_NUMBER_COLUMN_LABEL = "Invoice Number" # Assumed label in Grist and CSV Header

def get_grist_column_id_by_label(columns_data, label):
    """Finds the Grist column ID for a given label."""
    if "columns" in columns_data and isinstance(columns_data["columns"], list):
        for col in columns_data["columns"]:
            if "fields" in col and col["fields"].get("label") == label and "id" in col:
                return col["id"]
    return None

def fetch_and_populate_log(log_path, uploader: GristUploader, header_table_id, invoice_col_label):
    """Fetches existing invoice numbers from Grist and populates the log file."""
    print(f"Log file '{log_path}' not found. Fetching existing invoice numbers from Grist table '{header_table_id}'...")
    processed_invoices = set()
    try:
        # 1. Get column ID for Invoice Number
        header_columns = uploader.get_table_columns(header_table_id)
        invoice_col_id = get_grist_column_id_by_label(header_columns, invoice_col_label)
        if not invoice_col_id:
            logging.error(f"Could not find column with label '{invoice_col_label}' in Grist table '{header_table_id}'. Cannot create initial log.")
            # Decide behaviour: raise error or return empty set? Let's return empty for now.
            print(f"Error: Column '{invoice_col_label}' not found in Grist. Initial log file will be empty.")
            # Create an empty file to prevent re-fetching attempts
            with open(log_path, 'w', encoding='utf-8') as f:
                pass # Create empty file
            return processed_invoices # Return empty set

        # 2. Get all records from the header table
        print(f"Fetching all records from Grist table '{header_table_id}' to get invoice numbers...")
        # Note: This might be slow for very large tables. Grist API might have pagination.
        # For simplicity, assuming get_table_data fetches all for now.
        # TODO: Implement pagination if needed for very large tables.
        all_records_data = uploader.get_table_data(header_table_id)
        records = all_records_data.get("records", [])
        print(f"Found {len(records)} records in Grist table '{header_table_id}'.")

        # 3. Extract invoice numbers
        for record in records:
            fields = record.get("fields", {})
            invoice_num = fields.get(invoice_col_id)
            if invoice_num: # Check if not None or empty string
                processed_invoices.add(str(invoice_num).strip()) # Ensure string and strip whitespace

        # 4. Write to log file
        print(f"Writing {len(processed_invoices)} unique invoice numbers to '{log_path}'...")
        with open(log_path, 'w', encoding='utf-8') as f:
            for inv_num in sorted(list(processed_invoices)): # Sort for readability
                f.write(inv_num + '\n')
        print("Log file created successfully.")

    except requests.exceptions.RequestException as e:
        logging.error(f"Network error fetching data from Grist for initial log: {e}")
        print(f"Error: Network error connecting to Grist. Could not create initial log file '{log_path}'.")
        # Don't create the file, so it tries again next time.
        # Return empty set, processing will likely fail later anyway.
        return set()
    except Exception as e:
        logging.error(f"Error fetching or writing initial invoice log '{log_path}': {e}\n{traceback.format_exc()}")
        print(f"Error: Failed to create initial log file '{log_path}'. See error log.")
        # Don't create the file. Return empty set.
        return set()

    return processed_invoices

def load_or_create_invoice_log(log_path, uploader, header_table_id, invoice_col_label):
    """Loads processed invoices from the log file, or creates it by fetching from Grist if it doesn't exist."""
    processed_invoices = set()
    if os.path.exists(log_path):
        print(f"Loading processed invoices from '{log_path}'...")
        try:
            with open(log_path, 'r', encoding='utf-8') as f:
                for line in f:
                    invoice_num = line.strip()
                    if invoice_num: # Avoid adding empty lines
                        processed_invoices.add(invoice_num)
            print(f"Loaded {len(processed_invoices)} processed invoice numbers.")
        except Exception as e:
            logging.error(f"Error reading invoice log file '{log_path}': {e}. Treating as empty.")
            print(f"Warning: Could not read existing log file '{log_path}'. Assuming no invoices processed yet.")
            # Proceed as if the file didn't exist, but log the error.
            # Optionally, could try to fetch from Grist again here, but might overwrite a corrupted log.
            # Let's return empty for safety.
            return set()
    else:
        # File doesn't exist, fetch from Grist and create it
        processed_invoices = fetch_and_populate_log(log_path, uploader, header_table_id, invoice_col_label)

    return processed_invoices

def append_invoice_to_log(log_path, invoice_number):
    """Appends a successfully processed invoice number to the log file."""
    try:
        with open(log_path, 'a', encoding='utf-8') as f:
            f.write(str(invoice_number).strip() + '\n')
    except Exception as e:
        logging.error(f"Failed to append invoice number '{invoice_number}' to log file '{log_path}': {e}")
        # This is problematic, as the invoice is processed but not logged. Manual check might be needed.
        print(f"CRITICAL WARNING: Failed to log processed invoice '{invoice_number}' to '{log_path}'. Duplicate check may fail next time.")


def get_invoice_number_from_csv(csv_path, invoice_col_label):
    """Reads the first data row of a CSV and returns the value from the 'Invoice Number' column."""
    try:
        with open(csv_path, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            headers = next(reader, None) # Read header row
            if not headers:
                # print(f"Warning: CSV file has no headers: {csv_path}")
                return None # Or raise error? Let's return None.

            try:
                invoice_col_index = headers.index(invoice_col_label)
            except ValueError:
                # print(f"Warning: Column '{invoice_col_label}' not found in CSV headers: {csv_path}")
                logging.warning(f"Column '{invoice_col_label}' not found in CSV headers: {os.path.basename(csv_path)}")
                return None # Column doesn't exist

            first_data_row = next(reader, None) # Read first data row
            if not first_data_row:
                # print(f"Warning: CSV file has headers but no data rows: {csv_path}")
                return None # No data to get invoice number from

            if invoice_col_index < len(first_data_row):
                invoice_num = first_data_row[invoice_col_index].strip()
                return invoice_num if invoice_num else None # Return None if empty string
            else:
                # print(f"Warning: Data row is shorter than expected (missing invoice column data?): {csv_path}")
                logging.warning(f"Data row shorter than expected in {os.path.basename(csv_path)}. Cannot get invoice number.")
                return None

    except FileNotFoundError:
        # This shouldn't happen if called within the main loop's check, but handle defensively.
        logging.error(f"File not found when trying to read invoice number: {csv_path}")
        return None
    except Exception as e:
        logging.error(f"Error reading invoice number from {os.path.basename(csv_path)}: {e}")
        return None


# --- Processing Functions ---

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


def move_and_rename_file(source_path, base_success_dir):
    """
    Moves a file to a Month-Year subdirectory within the base success directory
    and renames its extension to .success.
    """
    if not os.path.exists(source_path):
        print(f"Warning: Source file not found for moving: {source_path}")
        return False # Indicate failure

    try:
        # 1. Determine Month-Year subdirectory
        now = datetime.now()
        month_year_str = now.strftime("%b-%y") # e.g., Apr-25
        month_year_dir = os.path.join(base_success_dir, month_year_str)

        # 2. Ensure Month-Year subdirectory exists
        os.makedirs(month_year_dir, exist_ok=True)

        # 3. Define destination filename and path
        base_filename = os.path.basename(source_path)
        name_part, _ = os.path.splitext(base_filename)
        dest_filename = f"{name_part}.success"
        dest_path = os.path.join(month_year_dir, dest_filename) # Path inside Month-Year dir

        # 4. Move the file
        shutil.move(source_path, dest_path)
        print(f"Successfully moved and renamed {base_filename} to {dest_filename} in {month_year_dir}")
        return True
    except Exception as e:
        # Log error with the intended destination path for clarity
        # Construct intended path again for logging, in case error happened before dest_path was set
        intended_dest_dir = os.path.join(base_success_dir, datetime.now().strftime("%b-%y"))
        intended_dest_filename = f"{os.path.splitext(os.path.basename(source_path))[0]}.success"
        intended_dest_path = os.path.join(intended_dest_dir, intended_dest_filename)
        logging.error(f"Failed to move/rename {os.path.basename(source_path)} to {intended_dest_path}. Error: {e}\n{traceback.format_exc()}")
        return False

def move_and_rename_duplicate(source_path, rejected_dir):
    """Moves a file to the rejected directory and renames it with a _duplicate suffix."""
    if not os.path.exists(source_path):
        print(f"Warning: Source file not found for moving to rejected: {source_path}")
        return False # Indicate failure

    base_filename = os.path.basename(source_path)
    name_part, ext = os.path.splitext(base_filename)
    # Ensure we handle potential double extensions like .tar.gz if needed, but .csv is simple
    dest_filename = f"{name_part}_duplicate{ext}"
    dest_path = os.path.join(rejected_dir, dest_filename)

    try:
        # Ensure rejected directory exists
        os.makedirs(rejected_dir, exist_ok=True)
        shutil.move(source_path, dest_path)
        print(f"Moved duplicate file {base_filename} to {dest_filename} in {rejected_dir}")
        return True
    except Exception as e:
        logging.error(f"Failed to move/rename duplicate file {base_filename} to {dest_path}. Error: {e}\n{traceback.format_exc()}")
        print(f"Error: Failed to move duplicate file {base_filename} to rejected folder.")
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
    rejected_folder_name = os.getenv('REJECTED_FOLDER_NAME', 'Rejected') # New config

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

    rejected_dir_path = os.path.join(csv_directory_path, rejected_folder_name)
    try:
        os.makedirs(rejected_dir_path, exist_ok=True) # Ensure rejected folder exists too
    except OSError as e:
         # Log error but maybe don't exit? Depends on desired behaviour if rejected folder fails.
         # Let's log and continue, duplicate handling will fail later if dir doesn't exist.
         logging.error(f"Could not create rejected directory '{rejected_dir_path}'. Error: {e}. Duplicate files might not be moved.")


    print(f"Processing CSV files from directory: {csv_directory_path}")
    print(f"Header Table ID: {header_table_id}")
    print(f"Items Table ID: {items_table_id}")
    print(f"Success Folder: {success_dir_path}")
    print(f"Rejected Folder: {rejected_dir_path}") # Print new folder path
    print(f"Log File: {log_file_name}")
    processed_log_path = os.path.join(csv_directory_path, PROCESSED_INVOICES_LOG_FILENAME)
    print(f"Processed Invoice Log: {processed_log_path}")


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

    # --- Load or Create Processed Invoice Log ---
    try:
        processed_invoice_numbers = load_or_create_invoice_log(
            processed_log_path, uploader, header_table_id, INVOICE_NUMBER_COLUMN_LABEL
        )
    except Exception as e:
        # Errors during log loading/creation are logged within the functions
        logging.critical(f"Failed to load or create the processed invoice log '{processed_log_path}'. Cannot proceed safely with duplicate checks. Exiting. Error: {e}")
        print(f"CRITICAL: Failed to initialize invoice log. Exiting.")
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
    duplicate_count = 0 # Add counter for duplicates

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
            invoice_number = None # Initialize

            # 0. Check for Duplicates using the log file
            try:
                invoice_number = get_invoice_number_from_csv(header_path, INVOICE_NUMBER_COLUMN_LABEL)
                if invoice_number is None:
                    print(f"Warning: Could not read Invoice Number from {header_filename}. Skipping this pair.")
                    logging.warning(f"Could not read Invoice Number from {header_filename} for prefix '{prefix}'. Skipping.")
                    fail_count += 1
                    continue # Skip to next prefix

                if invoice_number in processed_invoice_numbers:
                    print(f"Duplicate detected: Invoice Number '{invoice_number}' from file {header_filename} already processed. Moving to Rejected folder.")
                    duplicate_count += 1
                    # Move both files to Rejected folder with _duplicate suffix
                    moved_header_dup = move_and_rename_duplicate(header_path, rejected_dir_path)
                    moved_items_dup = move_and_rename_duplicate(items_path, rejected_dir_path)
                    if not moved_header_dup or not moved_items_dup:
                         # Log that the move failed, files might remain in source
                         logging.error(f"Failed to move one or both duplicate files for prefix '{prefix}' (Invoice: {invoice_number}) to Rejected folder.")
                    # No need to increment fail_count here unless move fails? Let's count separately.
                    continue # Skip to next prefix

            except Exception as e:
                 print(f"Error checking for duplicate invoice number in {header_filename}: {e}. Skipping this pair.")
                 logging.error(f"Error checking duplicate for prefix '{prefix}' ({header_filename}): {e}\n{traceback.format_exc()}")
                 fail_count += 1
                 continue # Skip to next prefix


            # --- If not a duplicate, proceed with upload ---
            print(f"Invoice Number '{invoice_number}' not found in log. Proceeding with upload...")

            # 1. Process Header File
            try:
                header_success = upload_csv_to_grist(header_path, header_table_id, uploader, header_columns_data)
            except Exception as e:
                if not isinstance(e, (requests.exceptions.RequestException, FileNotFoundError)):
                     logging.error(f"Unexpected error processing header file {header_filename}: {e}\n{traceback.format_exc()}")
                header_success = False

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
                 items_success = False

            # 3. Move files and Update Log if BOTH succeeded
            if header_success and items_success:
                print(f"Both uploads successful for prefix '{prefix}'. Moving files...")
                moved_header = move_and_rename_file(header_path, success_dir_path)
                moved_items = move_and_rename_file(items_path, success_dir_path)
                if moved_header and moved_items:
                    success_count += 1
                    # Add to log file and in-memory set
                    append_invoice_to_log(processed_log_path, invoice_number)
                    processed_invoice_numbers.add(invoice_number) # Update in-memory set
                    print(f"Successfully processed, moved, and logged pair: {prefix} (Invoice: {invoice_number})")
                else:
                    fail_count += 1
                    # CRITICAL: Uploads succeeded but move failed. Invoice is NOT logged.
                    logging.error(f"Uploads succeeded for '{prefix}' (Invoice: {invoice_number}), but failed to move one or both files. INVOICE NOT LOGGED AS PROCESSED. Please check manually.")
                    print(f"CRITICAL WARNING: Uploads succeeded for '{prefix}' but move failed. Invoice '{invoice_number}' was NOT logged. Manual check required.")
            else:
                fail_count += 1
                print(f"Processing failed for pair '{prefix}'. Files will not be moved. Invoice '{invoice_number}' not logged. Check '{log_file_name}' for details.")

        elif header_path and not items_path:
             # Only header exists
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
    print(f"Pairs detected as duplicates and moved to Rejected: {duplicate_count}")
    print(f"Pairs failed (upload, move, or other error): {fail_count}")
    print(f"Check '{log_file_name}' for any error details.")
    print("Processing complete.")


if __name__ == "__main__":
    main()
