import os
import csv
import re
import traceback
from datetime import datetime
import pdfplumber
import shutil

# === CONFIGURATION ===
INPUT_DIR = 'files/input'
ARCHIVE_DIR = 'files/archive'
ERROR_DIR = 'files/error'
OUTPUT_DIR = 'files/output'

# === PDF HEADER EXTRACTION FUNCTION ===
def extract_header_from_pdf(file_path):
    """
    Extract header information from PDF invoice and return as dictionary
    """
    with pdfplumber.open(file_path) as pdf:
        # We only process the first page for header information
        text = pdf.pages[0].extract_text()
        lines = text.splitlines()
    
    header_data = {
        'invoice_number': '',
        'invoice_date': '',
        'consignee_name': '',
        'consignee_address': '',
        'consignee_gstin': '',
        'consignee_state': '',
        'buyer_name': '',
        'buyer_address': '',
        'buyer_gstin': '',
        'buyer_state': '',
        'place_of_supply': '',
        'destination': ''
    }
    
    # Find invoice number
    for idx, line in enumerate(lines):
        if 'SC' in line and re.search(r'SC\d{5}-\d{2}-\d{2}', line):
            match = re.search(r'(SC\d{5}-\d{2}-\d{2})', line)
            if match:
                header_data['invoice_number'] = match.group(1)
                break
    
    # Find invoice date
    for idx, line in enumerate(lines):
        if 'Dated' in line:
            date_match = re.search(r'Dated\s+(\d{1,2}-[A-Za-z]{3}-\d{2})', line)
            if date_match:
                header_data['invoice_date'] = date_match.group(1)
                break
    
    # Find destination
    for idx, line in enumerate(lines):
        if 'Destination' in line:
            dest_match = re.search(r'Destination\s+(.+?)$', line)
            if dest_match:
                header_data['destination'] = dest_match.group(1).strip()
            # If not on the same line, it might be on next line
            elif idx + 1 < len(lines) and not lines[idx+1].strip().startswith('Motor Vehicle'):
                header_data['destination'] = lines[idx+1].strip()
            break
    
    # Find place of supply
    for idx, line in enumerate(lines):
        if 'Place of Supply' in line:
            match = re.search(r'Place of Supply\s*:\s*(.+?)$', line)
            if match:
                header_data['place_of_supply'] = match.group(1).strip()
            break
    
    # Process Consignee and Buyer sections
    consignee_start = None
    consignee_end = None
    buyer_start = None
    buyer_end = None
    
    # First locate section boundaries
    for idx, line in enumerate(lines):
        if 'Consignee (Ship to)' in line:
            consignee_start = idx + 1
        elif consignee_start and 'Buyer (Bill to)' in line:
            consignee_end = idx
            buyer_start = idx + 1
        elif buyer_start and 'Place of Supply' in line:
            buyer_end = idx
            break
    
    # Extract Consignee information
    if consignee_start and consignee_end:
        # Extract name (should be first line after "Consignee (Ship to)")
        header_data['consignee_name'] = lines[consignee_start].strip()
        
        # Extract address (lines between name and GSTIN)
        address_lines = []
        for idx in range(consignee_start + 1, consignee_end):
            line = lines[idx].strip()
            if 'GSTIN/UIN' in line:
                break
            address_lines.append(line)
        header_data['consignee_address'] = ", ".join(address_lines)
        
        # Extract GSTIN and State
        for idx in range(consignee_start, consignee_end):
            line = lines[idx].strip()
            if 'GSTIN/UIN' in line:
                gstin_match = re.search(r'GSTIN/UIN\s*:\s*([A-Z0-9]+)', line)
                if gstin_match:
                    header_data['consignee_gstin'] = gstin_match.group(1)
            if 'State Name' in line:
                state_match = re.search(r'State Name\s*:\s*([^,]+)', line)
                if state_match:
                    header_data['consignee_state'] = state_match.group(1).strip()
    
    # Extract Buyer information
    if buyer_start and buyer_end:
        # Extract name (should be first line after "Buyer (Bill to)")
        header_data['buyer_name'] = lines[buyer_start].strip()
        
        # Extract address (lines between name and GSTIN)
        address_lines = []
        for idx in range(buyer_start + 1, buyer_end):
            line = lines[idx].strip()
            if 'GSTIN/UIN' in line:
                break
            address_lines.append(line)
        header_data['buyer_address'] = ", ".join(address_lines)
        
        # Extract GSTIN and State
        for idx in range(buyer_start, buyer_end):
            line = lines[idx].strip()
            if 'GSTIN/UIN' in line:
                gstin_match = re.search(r'GSTIN/UIN\s*:\s*([A-Z0-9]+)', line)
                if gstin_match:
                    header_data['buyer_gstin'] = gstin_match.group(1)
            if 'State Name' in line:
                state_match = re.search(r'State Name\s*:\s*([^,]+)', line)
                if state_match:
                    header_data['buyer_state'] = state_match.group(1).strip()
    
    # Clean up any extra data in fields
    # Sometimes PDFs have layout issues that cause text to merge across columns
    for key in header_data:
        if isinstance(header_data[key], str):
            # Remove common field names that might be extracted with the values
            for term in ['Dispatch Doc No.', 'Delivery Note Date', 'Dispatched through', 'Destination', 
                         'By Tempo', 'West Mumbai', 'Bill of Lading/LR-RR No.', 'Motor Vehicle No.']:
                if term in header_data[key]:
                    header_data[key] = header_data[key].replace(term, '')
            
            # Clean up any multiple spaces, leading/trailing spaces
            header_data[key] = re.sub(r'\s+', ' ', header_data[key]).strip()
    
    # Debug output
    print("\n--- Extracted Header Data ---")
    for key, value in header_data.items():
        print(f"{key}: {value}")
    print("----------------------------\n")
    
    return header_data

# === PROCESS PDF AND WRITE TO CSV ===
def process_pdf(file_path):
    try:
        header_data = extract_header_from_pdf(file_path)
        
        # Format the date for the output filename
        try:
            invoice_date_obj = datetime.strptime(header_data['invoice_date'], "%d-%b-%y")
            month_file = invoice_date_obj.strftime("%b%y")
        except ValueError:
            print(f"Warning: Invalid invoice date: '{header_data['invoice_date']}'")
            # Use current month/year as fallback
            month_file = datetime.now().strftime("%b%y")
        
        output_csv = os.path.join(OUTPUT_DIR, f"{month_file}Headers.csv")
        
        # Create output directory if it doesn't exist
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        # Check if the file exists to determine if we need to write headers
        file_exists = os.path.isfile(output_csv)
        
        with open(output_csv, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # Write headers if file is new
            if not file_exists:
                writer.writerow([
                    'Invoice Number', 'Invoice Date',
                    'Consignee Name', 'Consignee Address', 'Consignee GSTIN', 'Consignee State',
                    'Buyer Name', 'Buyer Address', 'Buyer GSTIN', 'Buyer State',
                    'Place of Supply', 'Destination'
                ])
            
            # Write data row
            writer.writerow([
                header_data['invoice_number'],
                header_data['invoice_date'],
                header_data['consignee_name'],
                header_data['consignee_address'],
                header_data['consignee_gstin'],
                header_data['consignee_state'],
                header_data['buyer_name'],
                header_data['buyer_address'],
                header_data['buyer_gstin'],
                header_data['buyer_state'],
                header_data['place_of_supply'],
                header_data['destination']
            ])
        
        print(f"✅ Extracted header data from {os.path.basename(file_path)} and appended to {output_csv}")
        return True
    
    except Exception as e:
        print(f"❌ Error processing {file_path}: {e}")
        traceback.print_exc()
        return False

# === FILE MOVING WITH TIMESTAMP ===
def move_file(file_path, target_dir):
    os.makedirs(target_dir, exist_ok=True)
    base_name = os.path.basename(file_path)
    name, ext = os.path.splitext(base_name)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    new_name = f"{name}_{timestamp}{ext}"
    shutil.move(file_path, os.path.join(target_dir, new_name))

# === HANDLE FILE ===
def handle_file(file_path):
    success = process_pdf(file_path)
    if success:
        move_file(file_path, ARCHIVE_DIR)
        print(f"📦 Archived: {file_path}")
    else:
        move_file(file_path, ERROR_DIR)
        print(f"⚠️ Moved to error folder: {file_path}")

# === PROCESS EXISTING FILES ===
def process_existing_files():
    print("🔍 Checking for existing files...")
    if not os.path.exists(INPUT_DIR):
        print(f"📁 Input directory '{INPUT_DIR}' does not exist. Creating...")
        os.makedirs(INPUT_DIR)
        return
        
    file_count = 0
    for filename in os.listdir(INPUT_DIR):
        if filename.lower().endswith('.pdf'):
            file_path = os.path.join(INPUT_DIR, filename)
            print(f"📄 Processing: {filename}")
            handle_file(file_path)
            file_count += 1
    
    if file_count > 0:
        print(f"✅ Processed {file_count} existing PDF files")
    else:
        print("📭 No PDF files found in input directory")

# === MAIN ===
if __name__ == "__main__":
    # Ensure directories exist
    for directory in [INPUT_DIR, ARCHIVE_DIR, ERROR_DIR, OUTPUT_DIR]:
        os.makedirs(directory, exist_ok=True)
    
    print("🚀 PDF Invoice Header Extractor")
    print(f"📁 Input Directory: {INPUT_DIR}")
    print(f"📁 Output Directory: {OUTPUT_DIR}")
    print(f"📁 Archive Directory: {ARCHIVE_DIR}")
    print(f"📁 Error Directory: {ERROR_DIR}")
    
    process_existing_files()
    
    print("✨ Processing complete!")