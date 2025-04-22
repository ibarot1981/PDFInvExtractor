import os
import csv
import re
import traceback
import time
import signal
import sys
from datetime import datetime
import pdfplumber
import shutil
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

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
    file_name = os.path.basename(file_path)
    
    with pdfplumber.open(file_path) as pdf:
        # We only process the first page for header information
        text = pdf.pages[0].extract_text()
        lines = text.splitlines()
    
    header_data = {
        'file_name': file_name,  # Added filename as the first field
        'invoice_number': '',
        'invoice_date': '',
        'consignee_name': '',
        'consignee_address': '',
        'consignee_gstin': '',
        'consignee_state': '',
        'consignee_contact': '',
        'consignee_email': '',
        'buyer_name': '',
        'buyer_address': '',
        'buyer_gstin': '',
        'buyer_state': '',
        'buyer_contact': '',
        'buyer_email': '',
        'place_of_supply': '',
        'destination': ''
    }
    
    # Print all extracted lines for debugging
    print("\n--- Raw PDF Content ---")
    for i, line in enumerate(lines):
        print(f"Line {i}: {line}")
    print("----------------------\n")
    
    # Find invoice number
    invoice_line_idx = None
    for idx, line in enumerate(lines):
        if 'SC' in line and re.search(r'SC\d{5}-\d{2}-\d{2}', line):
            match = re.search(r'(SC\d{5}-\d{2}-\d{2})', line)
            if match:
                header_data['invoice_number'] = match.group(1)
                invoice_line_idx = idx
                break
    
    # Improved date extraction - first look near invoice number
    # New approach: Look for date near invoice number line
    if invoice_line_idx is not None:
        # Check the invoice line and surrounding lines (+/- 3 lines)
        for i in range(max(0, invoice_line_idx - 3), min(len(lines), invoice_line_idx + 4)):
            date_match = re.search(r'(\d{1,2}-[A-Za-z]{3}-\d{2})', lines[i])
            if date_match and 'Ack Date' not in lines[i]:
                header_data['invoice_date'] = date_match.group(1)
                print(f"Found date near invoice number: {header_data['invoice_date']}")
                break
    
    # If date not found near invoice, try other approaches
    # Approach 1: Look for "Dated" followed by date
    if not header_data['invoice_date']:
        for idx, line in enumerate(lines):
            if 'Dated' in line:
                date_match = re.search(r'Dated\s+(\d{1,2}-[A-Za-z]{3}-\d{2})', line)
                if date_match:
                    header_data['invoice_date'] = date_match.group(1)
                    print(f"Found date via 'Dated' pattern: {header_data['invoice_date']}")
                    break
    
    # Approach 2: If above fails, look for date pattern anywhere near relevant fields
    if not header_data['invoice_date']:
        for idx, line in enumerate(lines):
            # Look for date pattern in the line
            date_match = re.search(r'(\d{1,2}-[A-Za-z]{3}-\d{2})', line)
            if date_match and not line.startswith('Ack Date'):  # Avoid Ack Date
                # Make sure it's not part of the acknowledgment date
                if 'Ack Date' not in line:
                    header_data['invoice_date'] = date_match.group(1)
                    print(f"Found date via general pattern: {header_data['invoice_date']}")
                    break
    
    # Approach 3: Look specifically near bill of lading
    if not header_data['invoice_date']:
        for idx, line in enumerate(lines):
            if 'Bill of Lading' in line or 'LR-RR No' in line:
                # Check this line and next few lines
                for i in range(idx, min(idx+5, len(lines))):
                    date_match = re.search(r'(\d{1,2}-[A-Za-z]{3}-\d{2})', lines[i])
                    if date_match:
                        header_data['invoice_date'] = date_match.group(1)
                        print(f"Found date near bill of lading: {header_data['invoice_date']}")
                        break
                if header_data['invoice_date']:
                    break
    
    # Enhanced approach for finding destination - multiple methods
    destination_found = False
    
    # Method 1: Look for explicit "Destination:" or "Destination "
    for idx, line in enumerate(lines):
        if re.search(r'Destination\s*[:]\s*(.+?)(?:$|Motor Vehicle|Dispatched)', line, re.IGNORECASE):
            dest_match = re.search(r'Destination\s*[:]\s*(.+?)(?:$|Motor Vehicle|Dispatched)', line, re.IGNORECASE)
            if dest_match:
                header_data['destination'] = dest_match.group(1).strip()
                destination_found = True
                print(f"Found destination via pattern 1: {header_data['destination']}")
                break
    
    # Method 2: Look for "Destination" word and extract the next part
    if not destination_found:
        for idx, line in enumerate(lines):
            if re.search(r'\bDestination\b', line, re.IGNORECASE):
                # Check if destination is on the same line
                parts = re.split(r'\bDestination\b[\s:]*', line, flags=re.IGNORECASE)
                if len(parts) > 1 and parts[1].strip():
                    # Get everything after "Destination" on the same line
                    header_data['destination'] = parts[1].strip()
                    # If there are other fields on the same line, trim at those points
                    for stop_point in ['Motor Vehicle', 'Dispatched through', 'Terms of Delivery']:
                        if stop_point in header_data['destination']:
                            header_data['destination'] = header_data['destination'].split(stop_point)[0].strip()
                    
                    destination_found = True
                    print(f"Found destination via pattern 2: {header_data['destination']}")
                    break
                
                # If not on same line, check the next line
                elif idx + 1 < len(lines) and not any(x in lines[idx+1].lower() for x in ['motor vehicle', 'dispatched']):
                    next_line = lines[idx+1].strip()
                    # Make sure it's not the start of another section
                    if not any(next_line.startswith(x) for x in ['Motor', 'Dispatched', 'Terms']):
                        header_data['destination'] = next_line
                        # If there are other fields on this line, trim at those points
                        for stop_point in ['Motor Vehicle', 'Dispatched through', 'Terms of Delivery']:
                            if stop_point in header_data['destination']:
                                header_data['destination'] = header_data['destination'].split(stop_point)[0].strip()
                        
                        destination_found = True
                        print(f"Found destination via pattern 3: {header_data['destination']}")
                        break
    
    # Method 3: Look specifically between "Destination" and "Motor Vehicle"
    if not destination_found:
        for idx, line in enumerate(lines):
            if 'Destination' in line:
                # Find the index range for the lines between Destination and Motor Vehicle
                dest_idx = idx
                motor_idx = None
                
                for i in range(idx, min(len(lines), idx + 5)):
                    if 'Motor Vehicle' in lines[i]:
                        motor_idx = i
                        break
                
                if dest_idx is not None and motor_idx is not None:
                    # Extract text between these points
                    if dest_idx == motor_idx:
                        # Destination and Motor Vehicle on same line
                        parts = line.split('Destination')[1].split('Motor Vehicle')[0].strip()
                        if parts:
                            header_data['destination'] = parts.strip(':').strip()
                            destination_found = True
                            print(f"Found destination via pattern 4: {header_data['destination']}")
                    else:
                        # Destination and Motor Vehicle on different lines
                        dest_text = lines[dest_idx].split('Destination')[1].strip(':').strip()
                        if dest_text:
                            header_data['destination'] = dest_text
                            destination_found = True
                            print(f"Found destination via pattern 5: {header_data['destination']}")
    
    # Method 4: Look for common destination patterns like "Destination: Mumbai"
    if not destination_found:
        for idx, line in enumerate(lines):
            dest_pattern = re.search(r'Destination\s*[:-]\s*([A-Za-z\s]+)(?:\s|$)', line, re.IGNORECASE)
            if dest_pattern:
                header_data['destination'] = dest_pattern.group(1).strip()
                destination_found = True
                print(f"Found destination via pattern 6: {header_data['destination']}")
                break
    
    # Find place of supply
    for idx, line in enumerate(lines):
        if 'Place of Supply' in line:
            match = re.search(r'Place of Supply\s*:\s*(.+?)(?:$|State Code)', line)
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
        consignee_full_text = ""  # For searching contact and email
        
        for idx in range(consignee_start + 1, consignee_end):
            line = lines[idx].strip()
            if 'GSTIN/UIN' in line:
                break
            address_lines.append(line)
            consignee_full_text += line + " "
        
        header_data['consignee_address'] = ", ".join(address_lines)
        
        # Extract contact number
        phone_patterns = [
            r'(?:Phone|Ph|Tel|T|Contact|Mobile|Mob)[:\s.\-]+(\+?\d[\d\s\-]{8,})',
            r'(?<!\S)(\+?\d{10,12})(?!\S)',  # Standalone 10-12 digit number
            r'(?<!\S)(\d{3,5}[\s\-]\d{6,8})(?!\S)'  # Format like 022-12345678
        ]
        
        for pattern in phone_patterns:
            phone_match = re.search(pattern, consignee_full_text, re.IGNORECASE)
            if phone_match:
                header_data['consignee_contact'] = phone_match.group(1).strip()
                # Remove contact from address
                header_data['consignee_address'] = re.sub(pattern, '', header_data['consignee_address'], flags=re.IGNORECASE)
                break
        
        # Extract email
        email_match = re.search(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}', consignee_full_text)
        if email_match:
            header_data['consignee_email'] = email_match.group(0).strip()
            # Remove email from address
            header_data['consignee_address'] = header_data['consignee_address'].replace(header_data['consignee_email'], '')
        
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
        buyer_full_text = ""  # For searching contact and email
        
        for idx in range(buyer_start + 1, buyer_end):
            line = lines[idx].strip()
            if 'GSTIN/UIN' in line:
                break
            address_lines.append(line)
            buyer_full_text += line + " "
        
        header_data['buyer_address'] = ", ".join(address_lines)
        
        # Extract contact number
        phone_patterns = [
            r'(?:Phone|Ph|Tel|T|Contact|Mobile|Mob)[:\s.\-]+(\+?\d[\d\s\-]{8,})',
            r'(?<!\S)(\+?\d{10,12})(?!\S)',  # Standalone 10-12 digit number
            r'(?<!\S)(\d{3,5}[\s\-]\d{6,8})(?!\S)'  # Format like 022-12345678
        ]
        
        for pattern in phone_patterns:
            phone_match = re.search(pattern, buyer_full_text, re.IGNORECASE)
            if phone_match:
                header_data['buyer_contact'] = phone_match.group(1).strip()
                # Remove contact from address
                header_data['buyer_address'] = re.sub(pattern, '', header_data['buyer_address'], flags=re.IGNORECASE)
                break
        
        # Extract email
        email_match = re.search(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}', buyer_full_text)
        if email_match:
            header_data['buyer_email'] = email_match.group(0).strip()
            # Remove email from address  
            header_data['buyer_address'] = header_data['buyer_address'].replace(header_data['buyer_email'], '')
        
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
            
            # Remove any leading colons or similar punctuation
            header_data[key] = re.sub(r'^[:\s]+', '', header_data[key])
            
            # Clean up common punctuation issues in addresses after removing contacts/emails
            if 'address' in key:
                header_data[key] = re.sub(r'\s*,\s*,\s*', ', ', header_data[key])
                header_data[key] = re.sub(r'\s*,\s*$', '', header_data[key])
    
    # Debug output
    print("\n--- Extracted Header Data ---")
    for key, value in header_data.items():
        print(f"{key}: {value}")
    print("----------------------------\n")
    
    return header_data

# === ITEM DETAILS EXTRACTION FUNCTION - MODIFIED ===
def extract_items_from_pdf(file_path):
    """
    Extract item details from PDF invoice and return as a list of dictionaries
    """
    file_name = os.path.basename(file_path)
    invoice_number = ""
    items = []
    processed_item_numbers = set()  # Track already processed item numbers
    
    with pdfplumber.open(file_path) as pdf:
        # First, get the invoice number from the first page
        first_page_text = pdf.pages[0].extract_text()
        first_page_lines = first_page_text.splitlines()
        
        for line in first_page_lines:
            if 'SC' in line and re.search(r'SC\d{5}-\d{2}-\d{2}', line):
                match = re.search(r'(SC\d{5}-\d{2}-\d{2})', line)
                if match:
                    invoice_number = match.group(1)
                    break
        
        # Now process each page separately to avoid duplicates
        for page_num, page in enumerate(pdf.pages):
            page_text = page.extract_text()
            lines = page_text.splitlines()
            
            print(f"\nProcessing page {page_num + 1}")
            
            # Find the start and end of item table for this page
            item_start_idx = None
            item_end_idx = None
            
            for idx, line in enumerate(lines):
                # Look for table headers
                if (re.search(r'Sl\s+Description', line) and 
                    ('Quantity' in line or 'HSN/SAC' in line)) or \
                   (re.search(r'No\.\s+Goods and Services', line)):
                    item_start_idx = idx + 1
                    break
            
            if item_start_idx:
                # Look for end markers
                for idx in range(item_start_idx, len(lines)):
                    if ("Amount Chargeable" in lines[idx] or 
                        "Total" in lines[idx] or 
                        "continued to page" in lines[idx] or
                        "SUBJECT TO" in lines[idx]):
                        item_end_idx = idx
                        break
                
                # If no end marker found, process until end of page
                if not item_end_idx:
                    item_end_idx = len(lines)
                
                # Process item lines for this page
                for idx in range(item_start_idx, item_end_idx):
                    line = lines[idx].strip()
                    
                    # Skip empty lines or table headers
                    if not line or line.startswith("Sl") or line.startswith("No."):
                        continue
                    
                    # Check if line starts with a number (potential item number)
                    item_num_match = re.match(r'^(\d+)\s+', line)
                    if not item_num_match:
                        continue
                    
                    item_no = item_num_match.group(1).strip()
                    
                    # Skip if we've already processed this item number
                    if item_no in processed_item_numbers:
                        continue
                    
                    # Print for debugging
                    print(f"Processing line: {line}")
                    
                    # Find decimal numbers for amount and rate
                    decimal_numbers = re.findall(r'([\d,.]+\.\d{2})', line)
                    
                    # Find HSN code (typically an 8-digit number at end of line)
                    hsn_match = re.search(r'(\d{6,8})$', line)
                    hsn = hsn_match.group(1) if hsn_match else ""
                    
                    # Find quantity and unit
                    qty_match = re.search(r'(\d+)\s+NOS', line)
                    qty_value = qty_match.group(1) if qty_match else ""
                    qty_unit = "NOS" if qty_match else ""
                    
                    # Handle service items (like "Interstate Repairs")
                    if "Interstate Repairs" in line or len(decimal_numbers) == 1:
                        # Service items might only have one decimal number (the amount)
                        amount = decimal_numbers[0] if decimal_numbers else ""
                        rate = ""
                        
                        # Extract description (remove item number from beginning)
                        description = line[len(item_no):].strip()
                        
                        # If we have an amount, remove it from description
                        if amount:
                            description = description.replace(amount, "").strip()
                        
                        # If we have HSN, remove it from description
                        if hsn:
                            description = re.sub(rf'\s*{hsn}\s*$', '', description)
                        
                        items.append({
                            'file_name': file_name,
                            'invoice_number': invoice_number,
                            'item_no': item_no,
                            'description': description.strip(),
                            'qty_value': qty_value,
                            'qty_unit': qty_unit,
                            'rate': "",
                            'amount': amount,
                            'hsn_sac': hsn
                        })
                        processed_item_numbers.add(item_no)
                        continue
                    
                    # Regular items with amount and rate
                    if len(decimal_numbers) >= 2:
                        amount = decimal_numbers[0]  # First number is amount
                        rate = decimal_numbers[1]    # Second number is rate
                        
                        # Extract the raw description (everything after item number)
                        raw_description = line[len(item_no):].strip()
                        
                        # Extract the position of the first decimal number (amount)
                        amount_pos = raw_description.find(amount)
                        if amount_pos > 0:
                            # Get text from start to before first decimal number
                            description = raw_description[:amount_pos].strip()
                        else:
                            # Fallback: just get first part of the description
                            description_parts = raw_description.split()
                            description = ' '.join(description_parts[:3] if len(description_parts) > 3 else description_parts)
                        
                        # Clean up the description - remove HSN, Qty, Unit info
                        # Remove HSN code pattern
                        description = re.sub(r'\s+\d{6,8}\b', '', description)
                        # Remove quantity and NOS pattern
                        description = re.sub(r'\s+\d+\s+NOS\b', '', description)
                        
                        items.append({
                            'file_name': file_name,
                            'invoice_number': invoice_number,
                            'item_no': item_no,
                            'description': description.strip(),
                            'qty_value': qty_value,
                            'qty_unit': qty_unit,
                            'rate': amount,  # SWAPPED: Using amount as rate
                            'amount': rate,  # SWAPPED: Using rate as amount
                            'hsn_sac': hsn
                        })
                        processed_item_numbers.add(item_no)
    
    # Sort items by item number (to ensure correct order)
    items.sort(key=lambda x: int(x['item_no']))
    
    # Debug output
    print("\n--- Extracted Item Details ---")
    print(f"Found {len(items)} items")
    for item in items:
        print(f"Item {item['item_no']}: {item['description']} - {item['qty_value']} {item['qty_unit']} - Rate: {item['rate']} - Amount: {item['amount']}")
    print("----------------------------\n")
    
    return items

# === PROCESS PDF AND WRITE TO CSV ===
def process_pdf(file_path):
    try:
        # Extract header data
        header_data = extract_header_from_pdf(file_path)
        
        # Format the date for the output filename
        try:
            invoice_date_obj = datetime.strptime(header_data['invoice_date'], "%d-%b-%y")
            month_file = invoice_date_obj.strftime("%b%y")
        except ValueError:
            print(f"Warning: Invalid invoice date: '{header_data['invoice_date']}'")
            # Use current month/year as fallback
            month_file = datetime.now().strftime("%b%y")
        
        headers_csv = os.path.join(OUTPUT_DIR, f"{month_file}Headers.csv")
        
        # Create output directory if it doesn't exist
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        # Check if the file exists to determine if we need to write headers
        headers_file_exists = os.path.isfile(headers_csv)
        
        with open(headers_csv, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # Write headers if file is new
            if not headers_file_exists:
                writer.writerow([
                    'File Name',  # Added filename as the first field in CSV header
                    'Invoice Number', 'Invoice Date',
                    'Consignee Name', 'Consignee Address', 'Consignee GSTIN', 'Consignee State', 
                    'Consignee Contact No', 'Consignee Email',
                    'Buyer Name', 'Buyer Address', 'Buyer GSTIN', 'Buyer State',
                    'Buyer Contact No', 'Buyer Email',
                    'Place of Supply', 'Destination'
                ])
            
            # Write data row
            writer.writerow([
                header_data['file_name'],  # Added filename as the first field in CSV data
                header_data['invoice_number'],
                header_data['invoice_date'],
                header_data['consignee_name'],
                header_data['consignee_address'],
                header_data['consignee_gstin'],
                header_data['consignee_state'],
                header_data['consignee_contact'],
                header_data['consignee_email'],
                header_data['buyer_name'],
                header_data['buyer_address'],
                header_data['buyer_gstin'],
                header_data['buyer_state'],
                header_data['buyer_contact'],
                header_data['buyer_email'],
                header_data['place_of_supply'],
                header_data['destination']
            ])
        
        # Extract and write item details to a separate CSV
        items = extract_items_from_pdf(file_path)
        items_csv = os.path.join(OUTPUT_DIR, f"{month_file}ItemDetails.csv")
        
        # Check if the items file exists
        items_file_exists = os.path.isfile(items_csv)
        
        with open(items_csv, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # Write headers if file is new - UPDATED ORDER
            if not items_file_exists:
                writer.writerow([
                    'File Name', 'Invoice Number', 'Item No', 'Description',
                    'Quantity', 'Unit', 'Rate', 'Amount', 'HSN/SAC'
                ])
            
            # Write item rows - UPDATED ORDER
            for item in items:
                writer.writerow([
                    item['file_name'],
                    item['invoice_number'],
                    item['item_no'],
                    item['description'],
                    item['qty_value'],  # New order: Quantity value
                    item['qty_unit'],    # New order: Unit
                    item['rate'],        # New order: Rate
                    item['amount'],      # New order: Amount
                    item['hsn_sac']
                ])
        
        print(f"✅ Extracted data from {os.path.basename(file_path)}")
        print(f"   Headers appended to: {headers_csv}")
        print(f"   Items appended to: {items_csv}")
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

# === WATCHDOG EVENT HANDLER ===
class PDFHandler(FileSystemEventHandler):
    def on_created(self, event):
        # Only process files, not directories
        if not event.is_directory and event.src_path.lower().endswith('.pdf'):
            print(f"🔔 New file detected: {event.src_path}")
            
            # Wait a moment to ensure the file is completely written
            # Some applications may write files in chunks
            time.sleep(1)
            
            # Check if file still exists (it might have been moved by another process)
            if os.path.exists(event.src_path):
                print(f"📄 Processing: {os.path.basename(event.src_path)}")
                handle_file(event.src_path)
            else:
                print(f"⚠️ File no longer exists: {event.src_path}")

# === SIGNAL HANDLER FOR GRACEFUL EXIT ===
def signal_handler(sig, frame):
    print("\n🛑 Stopping PDF invoice monitoring (Ctrl+C pressed)")
    observer.stop()
    observer.join()
    sys.exit(0)

# === MAIN ===
if __name__ == "__main__":
    # Ensure directories exist
    for directory in [INPUT_DIR, ARCHIVE_DIR, ERROR_DIR, OUTPUT_DIR]:
        os.makedirs(directory, exist_ok=True)
    
    print("🚀 PDF Invoice Header Extractor with Watchdog")
    print(f"📁 Input Directory: {INPUT_DIR}")
    print(f"📁 Output Directory: {OUTPUT_DIR}")
    print(f"📁 Archive Directory: {ARCHIVE_DIR}")
    print(f"📁 Error Directory: {ERROR_DIR}")
    
    # Process any existing files first
    process_existing_files()
    
    # Set up the watchdog observer
    observer = Observer()
    event_handler = PDFHandler()
    observer.schedule(event_handler, INPUT_DIR, recursive=False)
    
    # Register signal handler for Ctrl+C
    signal.signal(signal.SIGINT, signal_handler)
    
    # Start the observer
    print("\n👀 Watching for new PDF files in input directory...")
    print("⌨️  Press Ctrl+C to stop monitoring\n")
    
    observer.start()
    
    try:
        # Keep the main thread alive
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        # This is a backup in case the signal handler doesn't catch it
        print("\n🛑 Stopping PDF invoice monitoring (Ctrl+C pressed)")
        observer.stop()
    
    observer.join()
    print("✨ Monitoring stopped. Goodbye!")