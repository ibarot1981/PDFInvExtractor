#!/usr/bin/env python3
"""
Script to extract unique values from a specified field in a Grist database
and save them to a CSV file (one value per line).

usage : 
python extract_unique_values.py --server docs.getgrist.com --doc YOUR_DOC_ID --table YOUR_TABLE --field YOUR_FIELD --api-key YOUR_API_KEY --output unique_values.csv

eg. :
python grist_extract_tmp.py --server http://safcost.duckdns.org:8484 --doc gi1sPNycQAHoMTekxE6QN3 --table InvoiceItems --field Item --api-key 7a377f3f2d53207dc917c86030503e7cd3311686 --output unique_items.csv

"""

import sys
import os
import argparse

# Try to import required packages, install if missing
try:
    import requests
except ImportError:
    print("Installing required package: requests")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "requests"])
    import requests

try:
    import pandas as pd
except ImportError:
    print("Installing required package: pandas")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pandas"])
    import pandas as pd


def setup_argparse():
    """Set up command line argument parsing."""
    parser = argparse.ArgumentParser(
        description='Extract unique values from a Grist database field and save to CSV.'
    )
    parser.add_argument(
        '--server', '-s',
        required=True,
        help='Grist server URL (e.g., https://docs.getgrist.com)'
    )
    parser.add_argument(
        '--doc', '-d',
        required=True,
        help='Document ID'
    )
    parser.add_argument(
        '--table', '-t',
        required=True,
        help='Table name in the Grist document'
    )
    parser.add_argument(
        '--field', '-f',
        required=True,
        help='Field name to extract unique values from'
    )
    parser.add_argument(
        '--api-key', '-k',
        help='API key for Grist access',
        default=os.environ.get('GRIST_API_KEY', '')
    )
    parser.add_argument(
        '--output', '-o',
        default='unique_values.csv',
        help='Output CSV filename (default: unique_values.csv)'
    )
    return parser


def get_table_data(server_url, doc_id, table_name, api_key):
    """Fetch data from a table in a Grist document."""
    # Construct API endpoint URL
    api_url = f"{server_url}/api/docs/{doc_id}/tables/{table_name}/records"
    
    # Set up headers with API key if provided
    headers = {}
    if api_key:
        headers['Authorization'] = f'Bearer {api_key}'
    
    # Make the API request
    response = requests.get(api_url, headers=headers)
    
    # Check if request was successful
    if response.status_code != 200:
        print(f"Error: API request failed with status code {response.status_code}")
        print(f"Response: {response.text}")
        sys.exit(1)
    
    return response.json()


def extract_unique_values(data, field_name):
    """Extract unique values from a specific field in the data."""
    # Extract records from the API response
    records = data.get('records', [])
    
    if not records:
        print("Warning: No records found in the response")
        return []
    
    # Check if the field exists in the first record
    if not records[0].get('fields', {}).get(field_name):
        print(f"Error: Field '{field_name}' not found in the records")
        # Show available fields
        print("Available fields:", list(records[0].get('fields', {}).keys()))
        sys.exit(1)
    
    # Extract values from the specified field
    values = [record.get('fields', {}).get(field_name) for record in records]
    
    # Filter out None values and convert to strings
    values = [str(val) for val in values if val is not None]
    
    # Get unique values
    unique_values = list(set(values))
    
    # Sort for consistent output
    unique_values.sort()
    
    return unique_values


def save_to_csv(values, output_file):
    """Save values to a CSV file, one value per line."""
    # Create a DataFrame with a single column
    df = pd.DataFrame({
        'value': values
    })
    
    # Save to CSV without index and header
    df.to_csv(output_file, index=False, header=False)
    print(f"Saved {len(values)} unique values to {output_file}")


def main():
    # Parse command line arguments
    parser = setup_argparse()
    args = parser.parse_args()
    
    # Ensure server URL has the correct format
    server_url = args.server
    if not server_url.startswith(('http://', 'https://')):
        server_url = 'https://' + server_url
    
    print(f"Connecting to Grist server: {server_url}")
    print(f"Document: {args.doc}")
    print(f"Table: {args.table}")
    print(f"Field: {args.field}")
    
    # Get data from Grist
    data = get_table_data(server_url, args.doc, args.table, args.api_key)
    
    # Extract unique values
    unique_values = extract_unique_values(data, args.field)
    
    # Save to CSV
    save_to_csv(unique_values, args.output)


if __name__ == "__main__":
    main()