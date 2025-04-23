import requests
import csv
import json
import os
import traceback
from dotenv import load_dotenv

# Load environment variables from .env file (for API key and other credentials)
load_dotenv()

class GristUploader:
    def __init__(self, doc_id, api_key=None, server_url=None):
        """Initialize the GristUploader with necessary parameters."""
        self.doc_id = doc_id
        self.api_key = api_key or os.getenv('GRIST_API_KEY')
        self.server_url = server_url or os.getenv('GRIST_SERVER_URL', 'https://docs.getgrist.com')
        
        if not self.api_key:
            raise ValueError("API key is required. Provide as parameter or set GRIST_API_KEY environment variable.")
        
        # Set up headers for API requests
        self.headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json',
        }
    
    def get_tables(self):
        """Get list of tables in the document."""
        url = f"{self.server_url}/api/docs/{self.doc_id}/tables"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
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
        print(f"Raw column data: {json.dumps(data, indent=2)}")  # Debug print
        return data
    
    def add_records(self, table_id, records):
        """Add records to a specific table."""
        url = f"{self.server_url}/api/docs/{self.doc_id}/tables/{table_id}/records"
        data = {"records": records}
        
        print(f"Sending request to: {url}")
        print(f"First record sample: {json.dumps(records[0] if records else {}, indent=2)}")
        
        try:
            response = requests.post(url, headers=self.headers, json=data)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            print(f"HTTP Error: {e}")
            print(f"Response content: {e.response.text if hasattr(e.response, 'text') else 'No response text'}")
            raise
        except Exception as e:
            print(f"Unexpected error: {str(e)}")
            raise
    
    def clear_table(self, table_id):
        """Clear all records from a table."""
        try:
            # First get all records to determine their IDs
            table_data = self.get_table_data(table_id)
            if not table_data.get('records'):
                print("Table is already empty")
                return None  # Table is already empty
            
            # Delete all records
            record_ids = [record['id'] for record in table_data['records']]
            url = f"{self.server_url}/api/docs/{self.doc_id}/tables/{table_id}/records"
            data = {"records": [{"id": rec_id, "manualSort": 0} for rec_id in record_ids]}
            response = requests.delete(url, headers=self.headers, json=data)
            response.raise_for_status()
            print(f"Successfully cleared {len(record_ids)} records from table")
            return response.json()
        except Exception as e:
            print(f"Error clearing table: {e}")
            traceback.print_exc()
            return None

def create_column_mapping_from_grist(columns_data, csv_headers):
    """Create a mapping from CSV headers to Grist column IDs"""
    mapping = {}
    
    # Extract column IDs from Grist response
    grist_columns = []
    if "columns" in columns_data and isinstance(columns_data["columns"], list):
        grist_columns = [col["id"] for col in columns_data["columns"] if "id" in col]
    
    print(f"Found Grist columns: {grist_columns}")
    
    # Create mapping by matching each CSV header to the most similar Grist column
    for csv_header in csv_headers:
        # Try exact match first
        if csv_header in grist_columns:
            mapping[csv_header] = csv_header
            continue
        
        # Try replacing spaces with underscores
        underscore_version = csv_header.replace(" ", "_")
        if underscore_version in grist_columns:
            mapping[csv_header] = underscore_version
            continue
        
        # Try case-insensitive match
        for grist_col in grist_columns:
            if csv_header.lower().replace(" ", "_") == grist_col.lower():
                mapping[csv_header] = grist_col
                break
    
    print(f"Generated column mapping: {json.dumps(mapping, indent=2)}")
    return mapping

def read_csv_to_records(csv_file_path, column_mapping=None):
    """
    Read a CSV file and convert it to records for Grist.
    
    Args:
        csv_file_path (str): Path to the CSV file
        column_mapping (dict, optional): Mapping of CSV columns to Grist fields
                                        If None, uses CSV headers as field names
    
    Returns:
        List of dictionaries representing records
    """
    records = []
    
    with open(csv_file_path, 'r', newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        
        for row in reader:
            if column_mapping:
                # Map CSV columns to Grist fields
                record = {"fields": {}}
                for csv_col, grist_field in column_mapping.items():
                    if csv_col in row:
                        record["fields"][grist_field] = row[csv_col]
            else:
                # Use CSV headers directly
                record = {"fields": row}
            
            records.append(record)
    
    return records

def main():
    # Configuration
    doc_id = os.getenv('GRIST_DOC_ID')
    table_id = os.getenv('GRIST_TABLE_ID')
    csv_file_path = os.getenv('CSV_FILE_PATH', 'data.csv')
    
    # Initialize the uploader
    uploader = GristUploader(doc_id)
    
    try:
        # Get table columns to see exact field names
        print("Getting table columns...")
        columns_data = uploader.get_table_columns(table_id)
        
        # Extract CSV headers
        print(f"Reading CSV headers from {csv_file_path}...")
        with open(csv_file_path, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            csv_headers = next(reader)
            first_row = next(reader, None)
            print(f"CSV Headers: {csv_headers}")
            print(f"First row data: {first_row}")
        
        # Generate mapping from CSV headers to Grist column IDs
        column_mapping = create_column_mapping_from_grist(columns_data, csv_headers)
        
        # Optional: Clear existing data
        print(f"Clearing existing data from table {table_id}...")
        uploader.clear_table(table_id)
        
        # Read CSV data with the generated mapping
        print(f"Reading data from {csv_file_path} with column mapping...")
        records = read_csv_to_records(csv_file_path, column_mapping)
        
        # Upload records in batches
        batch_size = 100
        total_records = len(records)
        print(f"Uploading {total_records} records in batches of {batch_size}...")
        
        for i in range(0, total_records, batch_size):
            batch = records[i:i+batch_size]
            result = uploader.add_records(table_id, batch)
            print(f"Uploaded batch {i // batch_size + 1}/{(total_records + batch_size - 1) // batch_size}")
        
        print("Upload completed successfully!")
    
    except Exception as e:
        print(f"Error: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main()