import os
import csv
import re
import traceback
import time
import signal
import sys
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
import pdfplumber
import shutil
from dotenv import load_dotenv
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# --- Load Environment ---
load_dotenv()

# --- Setup Rotating Logging ---
LOG_FILE = os.getenv('CLAUDE_LOG_FILE', 'claude_extractor.log')
LOG_MAX_BYTES = int(os.getenv('CLAUDE_LOG_MAX_BYTES', 5242880))  # Default 5MB
LOG_BACKUP_COUNT = int(os.getenv('CLAUDE_LOG_BACKUP_COUNT', 3))  # Default 3 backups

file_handler = RotatingFileHandler(
    LOG_FILE,
    maxBytes=LOG_MAX_BYTES,
    backupCount=LOG_BACKUP_COUNT,
    encoding='utf-8' # Added encoding
)
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

logging.basicConfig(
    level=logging.INFO,
    handlers=[file_handler, console_handler]
)

# === CONFIGURATION ===
# Read directory paths from environment variables with defaults
INPUT_DIR = os.getenv('INPUT_DIR', 'files/input')
ARCHIVE_DIR = os.getenv('ARCHIVE_DIR', 'files/archive')
ERROR_DIR = os.getenv('ERROR_DIR', 'files/error')
OUTPUT_DIR = os.getenv('OUTPUT_DIR', 'files/output')

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
    
    # Log raw content for debugging if needed (set level to DEBUG)
    # logging.debug("\n--- Raw PDF Content ---")
    # for i, line in enumerate(lines):
    #     logging.debug(f"Line {i}: {line}")
    # logging.debug("----------------------\n")
    
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
                logging.debug(f"Found date near invoice number: {header_data['invoice_date']}")
                break
    
    # If date not found near invoice, try other approaches
    # Approach 1: Look for "Dated" followed by date
    if not header_data['invoice_date']:
        for idx, line in enumerate(lines):
            if 'Dated' in line:
                date_match = re.search(r'Dated\s+(\d{1,2}-[A-Za-z]{3}-\d{2})', line)
                if date_match:
                    header_data['invoice_date'] = date_match.group(1)
                    logging.debug(f"Found date via 'Dated' pattern: {header_data['invoice_date']}")
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
                    logging.debug(f"Found date via general pattern: {header_data['invoice_date']}")
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
                        logging.debug(f"Found date near bill of lading: {header_data['invoice_date']}")
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
                logging.debug(f"Found destination via pattern 1: {header_data['destination']}")
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
                    logging.debug(f"Found destination via pattern 2: {header_data['destination']}")
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
                        logging.debug(f"Found destination via pattern 3: {header_data['destination']}")
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
                            logging.debug(f"Found destination via pattern 4: {header_data['destination']}")
                    else:
                        # Destination and Motor Vehicle on different lines
                        dest_text = lines[dest_idx].split('Destination')[1].strip(':').strip()
                        if dest_text:
                            header_data['destination'] = dest_text
                            destination_found = True
                            logging.debug(f"Found destination via pattern 5: {header_data['destination']}")
    
    # Method 4: Look for common destination patterns like "Destination: Mumbai"
    if not destination_found:
        for idx, line in enumerate(lines):
            dest_pattern = re.search(r'Destination\s*[:-]\s*([A-Za-z\s]+)(?:\s|$)', line, re.IGNORECASE)
            if dest_pattern:
                header_data['destination'] = dest_pattern.group(1).strip()
                destination_found = True
                logging.debug(f"Found destination via pattern 6: {header_data['destination']}")
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
    logging.debug("\n--- Extracted Header Data ---")
    for key, value in header_data.items():
        logging.debug(f"{key}: {value}")
    logging.debug("----------------------------\n")
    
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
            
            logging.debug(f"\nProcessing page {page_num + 1}")
            
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
                idx = item_start_idx
                while idx < item_end_idx:
                    line = lines[idx].strip()
                    
                    # Skip empty lines or table headers
                    if not line or line.startswith("Sl") or line.startswith("No."):
                        idx += 1
                        continue
                    
                    # Check if line starts with a number (potential item number)
                    item_num_match = re.match(r'^(\d+)\s+', line)
                    if not item_num_match:
                        idx += 1
                        continue
                    
                    item_no = item_num_match.group(1).strip()
                    
                    # Skip if we've already processed this item number
                    if item_no in processed_item_numbers:
                        idx += 1
                        continue
                    
                    # Log for debugging
                    logging.debug(f"Processing potential item line: {line}")

                    # --- Initial analysis of the main line ---
                    main_line_hsn_match = re.search(r'(\d{6,8})$', line) # HSN at end
                    main_line_qty_match = re.search(r'(\d+)\s+NOS', line) # Assuming NOS unit for now
                    main_line_decimals = re.findall(r'([\d,.]+\.\d{2})', line)
                    is_service_item = not main_line_qty_match # Tentative: service if no qty on main line

                    # Start building the description from the main line
                    description_lines = []
                    if item_num_match:
                        initial_description = line[len(item_num_match.group(0)):].strip()
                        description_lines.append(initial_description)

                    # --- Look ahead for additional description lines ---
                    next_idx = idx + 1
                    while next_idx < item_end_idx:
                        next_line = lines[next_idx].strip()

                        # Break if we find an end marker
                        if ("Amount Chargeable" in next_line or
                            "Total" in next_line or
                            "continued to page" in next_line or
                            "SUBJECT TO" in next_line):
                            break

                        # Skip empty lines
                        if not next_line:
                            next_idx += 1
                            continue

                        # Check if the next line starts with a number
                        next_item_num_match = re.match(r'^\d+\s+', next_line)

                        is_likely_new_item = False
                        if next_item_num_match:
                            # Check if this line looks like a *new* item line
                            next_line_hsn = re.search(r'\b\d{6,8}\b', next_line) # HSN anywhere
                            next_line_qty = re.search(r'\d+\s+NOS', next_line) # Qty anywhere
                            next_line_decimals = re.findall(r'([\d,.]+\.\d{2})', next_line)

                            # Conditions for being a new item line:
                            # 1. Has HSN?
                            # 2. Has Qty?
                            # 3. Has at least two decimal numbers (likely rate/amount)?
                            if next_line_hsn or next_line_qty or len(next_line_decimals) >= 2:
                                is_likely_new_item = True
                                # Optional: Add check for sequential item number?
                                # try:
                                #     if int(next_item_num_match.group(1)) > int(item_no):
                                #         is_likely_new_item = True
                                # except ValueError:
                                #     pass # Ignore if conversion fails

                        if is_likely_new_item:
                            logging.debug(f"Detected likely new item line: {next_line}. Stopping description.")
                            break # Stop accumulating description, it's a new item
                        else:
                            # --- ADDED TAX AND TOTAL LINE CHECKS ---
                            # Check 1: Does the line look like a tax line (CGST, SGST, IGST)?
                            is_tax_line = re.search(r'(Output\s+)?(CGST|SGST|IGST)', next_line, re.IGNORECASE)

                            # Check 2: Does the line look like *only* a total amount?
                            # Heuristic: Remove "Total" keyword (case-insensitive) and surrounding whitespace.
                            # Check if the *entire remaining string* is just a decimal number.
                            potential_total_text = re.sub(r'\bTotal\b', '', next_line, flags=re.IGNORECASE).strip()
                            # Allow for optional currency symbols or leading/trailing punctuation sometimes seen near totals
                            potential_total_text = re.sub(r'^[^\d]+|[^\d]+$', '', potential_total_text).strip() 
                            is_likely_total_line = re.fullmatch(r'[\d,.]+\.\d{2}', potential_total_text)

                            if is_tax_line:
                                logging.debug(f"Detected tax line: {next_line}. Stopping description.")
                                break # Stop accumulating description before adding tax line
                            elif is_likely_total_line:
                                 logging.debug(f"Detected likely total line: {next_line}. Stopping description.")
                                 break # Stop accumulating description before adding total line
                            # --- END TAX AND TOTAL LINE CHECKS ---

                            # This is a continuation line (passes all checks)
                            logging.debug(f"Adding description line: {next_line}")
                            description_lines.append(next_line)
                            next_idx += 1

                    # --- After the inner while loop ---

                    # Join all description lines
                    full_description = ' '.join(description_lines)

                    # --- Perform cleaning on full_description ---
                    # Use the HSN/Qty/Decimals found on the *main line* for final assignment and cleaning

                    # Extract final values from main line analysis
                    hsn = main_line_hsn_match.group(1) if main_line_hsn_match else ""
                    qty_value = main_line_qty_match.group(1) if main_line_qty_match else ""
                    qty_unit = "NOS" if main_line_qty_match else ""

                    # Remove HSN code from description
                    if hsn:
                        full_description = re.sub(rf'\b{hsn}\b', '', full_description)
                    # Also remove any other HSN-like numbers that might be in description text
                    full_description = re.sub(r'\b\d{6,8}\b', '', full_description)

                    # Remove Unit from description
                    if qty_unit:
                        full_description = re.sub(r'\bNOS\b', '', full_description, flags=re.IGNORECASE)

                    # Remove rate and amount values (using decimals found on main line) from description
                    for value in main_line_decimals:
                        # Use regex to avoid replacing parts of other numbers
                        full_description = re.sub(rf'(?<![\d.,]){re.escape(value)}(?![\d.,])', '', full_description)

                    # Remove quantity value from description
                    if qty_value:
                        full_description = re.sub(rf'\b{qty_value}\b', '', full_description)

                    # --- START TAX INFO REMOVAL ---
                    full_description = re.sub(r'Output\s+IGST\s*[-\d.% ]+', '', full_description, flags=re.IGNORECASE)
                    full_description = re.sub(r'Output\s+CGST\s*[-\d.% ]+', '', full_description, flags=re.IGNORECASE)
                    full_description = re.sub(r'Output\s+SGST\s*[-\d.% ]+', '', full_description, flags=re.IGNORECASE)
                    # --- END TAX INFO REMOVAL ---

                    # Clean up extra spaces
                    full_description = re.sub(r'\s+', ' ', full_description).strip()

                    # --- Assign final rate and amount based on main line analysis ---
                    rate = ""
                    amount = ""

                    if not qty_value: # Treat as service item if no qty on main line
                        if len(main_line_decimals) >= 1:
                            # Assume amount is the last decimal on the line for service items
                            amount = main_line_decimals[-1]
                            rate = ""
                    else: # Treat as regular item
                        if len(main_line_decimals) >= 2:
                            # Assuming Rate is first, Amount is second on the main line for product items
                            # (Adjust if this assumption is wrong for your PDFs)
                            rate = main_line_decimals[0]
                            amount = main_line_decimals[1]
                        elif len(main_line_decimals) == 1:
                            # If only one number for product, assume it's amount
                            amount = main_line_decimals[0]
                            rate = ""
                    
                    # Get the first line for the 'Item' field
                    first_line_item = description_lines[0].strip() if description_lines else ''
                    # Clean the first line similar to how full_description is cleaned (remove HSN, Qty, Rate, Amount, Tax)
                    if hsn:
                        first_line_item = re.sub(rf'\b{hsn}\b', '', first_line_item)
                    first_line_item = re.sub(r'\b\d{6,8}\b', '', first_line_item) # Remove other HSN-like
                    if qty_unit:
                        first_line_item = re.sub(r'\bNOS\b', '', first_line_item, flags=re.IGNORECASE)
                    for value in main_line_decimals:
                         first_line_item = re.sub(rf'(?<![\d.,]){re.escape(value)}(?![\d.,])', '', first_line_item)
                    if qty_value:
                        first_line_item = re.sub(rf'\b{qty_value}\b', '', first_line_item)
                    first_line_item = re.sub(r'Output\s+IGST\s*[-\d.% ]+', '', first_line_item, flags=re.IGNORECASE)
                    first_line_item = re.sub(r'Output\s+CGST\s*[-\d.% ]+', '', first_line_item, flags=re.IGNORECASE)
                    first_line_item = re.sub(r'Output\s+SGST\s*[-\d.% ]+', '', first_line_item, flags=re.IGNORECASE)
                    first_line_item = re.sub(r'\s+', ' ', first_line_item).strip()


                    items.append({
                        'file_name': file_name,
                        'invoice_number': invoice_number,
                        'item_no': item_no,
                        'item': first_line_item, # New field for first line
                        'description': full_description, # Keep full description
                        'qty_value': qty_value,
                        'qty_unit': qty_unit,
                        'rate': rate,
                        'amount': amount,
                        'hsn_sac': hsn
                    })
                    processed_item_numbers.add(item_no)

                    # Move to the next potential item line
                    idx = next_idx

    # Sort items by item number (to ensure correct order)
    items.sort(key=lambda x: int(x['item_no']))
    
    # Debug output
    logging.debug("\n--- Extracted Item Details ---")
    logging.debug(f"Found {len(items)} items")
    for item in items:
        logging.debug(f"Item {item['item_no']}: {item['description']} - {item['qty_value']} {item['qty_unit']} - Rate: {item['rate']} - Amount: {item['amount']}")
    logging.debug("----------------------------\n")
    
    return items

def process_pdf(file_path):
    try:
        # Extract header data
        header_data = extract_header_from_pdf(file_path)
        
        # Get the input file name without extension
        input_file_name = os.path.splitext(os.path.basename(file_path))[0]
        
        # Format the date for the output filename and MonthYear field
        month_year = "" # Default value
        try:
            invoice_date_obj = datetime.strptime(header_data['invoice_date'], "%d-%b-%y")
            date_format = invoice_date_obj.strftime("%d-%m-%y")
            month_year = invoice_date_obj.strftime("%b-%y") # Format as MMM-YY e.g. Apr-25
        except ValueError:
            logging.warning(f"Invalid invoice date: '{header_data['invoice_date']}' in file '{file_path}'. Cannot generate MonthYear.")
            # Use current date as fallback for filename
            now = datetime.now()
            date_format = now.strftime("%d-%m-%y")
            # Keep month_year empty or set a default if needed
            # month_year = now.strftime("%b-%y") # Optionally use current month/year as fallback
        
        # Add month_year to the header_data dictionary
        header_data['month_year'] = month_year

        # Create new file names as per requested format
        headers_csv = os.path.join(OUTPUT_DIR, f"{input_file_name}_{date_format}_Header.csv")
        items_csv = os.path.join(OUTPUT_DIR, f"{input_file_name}_{date_format}_Items.csv")
        
        # Create output directory if it doesn't exist
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        # For headers file - always write headers since we're creating new files for each PDF
        with open(headers_csv, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # Write headers - Added 'MonthYear'
            writer.writerow([
                'File Name',
                'Invoice Number', 'Invoice Date', 'MonthYear',
                'Consignee Name', 'Consignee Address', 'Consignee GSTIN', 'Consignee State', 
                'Consignee Contact No', 'Consignee Email',
                'Buyer Name', 'Buyer Address', 'Buyer GSTIN', 'Buyer State',
                'Buyer Contact No', 'Buyer Email',
                'Place of Supply', 'Destination'
            ])
            
            # Write data row - Added month_year
            # Write data row - Access month_year from header_data
            writer.writerow([
                header_data['file_name'],
                header_data['invoice_number'],
                header_data['invoice_date'],
                header_data['month_year'], # Access MonthYear value from dict
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
        
        # Extract item details
        items = extract_items_from_pdf(file_path)
        
        # For items file - always write headers since we're creating new files for each PDF
        with open(items_csv, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # Write headers - Added 'Item' column
            writer.writerow([
                'File Name', 'Invoice Number', 'Item No', 'Item', 'Description',
                'Quantity', 'Unit', 'Rate', 'Amount', 'HSN/SAC'
            ])
            
            # Write item rows - Added item['item']
            for item in items:
                writer.writerow([
                    item['file_name'],
                    item['invoice_number'],
                    item['item_no'],
                    item['item'], # New field value
                    item['description'],
                    item['qty_value'],
                    item['qty_unit'],
                    item['rate'],
                    item['amount'],
                    item['hsn_sac']
                ])
        
        logging.info(f"✅ Extracted data from {os.path.basename(file_path)}")
        logging.info(f"   Headers written to: {headers_csv}")
        logging.info(f"   Items written to: {items_csv}")
        return True
    
    except Exception as e:
        logging.error(f"❌ Error processing {file_path}: {e}")
        logging.exception("Traceback:") # Log the full traceback
        return False

# === FILE MOVING WITH TIMESTAMP ===
def move_file(file_path, target_dir):
    os.makedirs(target_dir, exist_ok=True)
    base_name = os.path.basename(file_path)
    name, ext = os.path.splitext(base_name)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    new_name = f"{name}_{timestamp}{ext}"
    shutil.move(file_path, os.path.join(target_dir, new_name))

# === FILE STABILITY CHECK ===
def is_file_stable(file_path, retries=3, delay=0.5):
    """Checks if a file size is stable over a short period."""
    last_size = -1
    for _ in range(retries):
        try:
            current_size = os.path.getsize(file_path)
            if current_size == last_size and current_size > 0:
                return True  # Size is stable and non-zero
            last_size = current_size
            time.sleep(delay)
        except FileNotFoundError:
            logging.warning(f"File not found during stability check: {file_path}")
            return False # File disappeared
        except Exception as e:
            logging.error(f"Error checking file stability for {file_path}: {e}")
            return False # Other error during check
    logging.warning(f"File size for {file_path} did not stabilize after {retries} retries.")
    return False

# === HANDLE FILE ===
def handle_file(file_path):
    """Processes a single PDF file after checking stability."""
    if not os.path.exists(file_path):
        logging.warning(f"File disappeared before handling: {file_path}")
        return

    if not is_file_stable(file_path):
        logging.warning(f"Skipping unstable or empty file: {file_path}")
        # Optionally move unstable files to error dir immediately
        # try:
        #     move_file(file_path, ERROR_DIR)
        #     logging.warning(f"Moved unstable file to error folder: {file_path}")
        # except Exception as move_err:
        #     logging.error(f"Could not move unstable file {file_path} to error folder: {move_err}")
        return

    try:
        success = process_pdf(file_path)
        if success:
            try:
                move_file(file_path, ARCHIVE_DIR)
                logging.info(f"📦 Archived: {os.path.basename(file_path)}") # Log only basename
            except Exception as move_err:
                logging.error(f"Error moving processed file {file_path} to archive: {move_err}")
                # Attempt to move to error dir as fallback
                try:
                    move_file(file_path, ERROR_DIR)
                    logging.warning(f"Moved processed file to error folder due to archive error: {os.path.basename(file_path)}")
                except Exception as fallback_move_err:
                     logging.error(f"Could not move file {file_path} to error folder after archive failure: {fallback_move_err}")
        else:
            # process_pdf returned False (error during extraction)
            try:
                move_file(file_path, ERROR_DIR)
                logging.warning(f"⚠️ Moved to error folder (processing error): {os.path.basename(file_path)}") # Log only basename
            except Exception as move_err:
                 logging.error(f"Could not move file {file_path} to error folder after processing error: {move_err}")

    except Exception as handle_err:
        logging.error(f"Unhandled error during handle_file for {file_path}: {handle_err}")
        logging.exception("Traceback for handle_file error:")
        # Attempt to move to error dir if any unexpected error occurs
        try:
            if os.path.exists(file_path): # Check again if it still exists
                 move_file(file_path, ERROR_DIR)
                 logging.warning(f"Moved file to error folder due to unhandled error: {os.path.basename(file_path)}")
        except Exception as final_move_err:
             logging.error(f"Could not move file {file_path} to error folder after unhandled error: {final_move_err}")


# === PROCESS EXISTING FILES ===
def process_existing_files():
    logging.info("🔍 Checking for existing files...")
    if not os.path.exists(INPUT_DIR):
        logging.info(f"📁 Input directory '{INPUT_DIR}' does not exist. Creating...")
        os.makedirs(INPUT_DIR)
        return
        
    file_count = 0
    for filename in os.listdir(INPUT_DIR):
        if filename.lower().endswith('.pdf'):
            file_path = os.path.join(INPUT_DIR, filename)
            logging.info(f"📄 Processing existing file: {filename}")
            handle_file(file_path)
            file_count += 1
    
    if file_count > 0:
        logging.info(f"✅ Processed {file_count} existing PDF files")
    else:
        logging.info("📭 No existing PDF files found in input directory")

# === WATCHDOG EVENT HANDLER ===
class PDFHandler(FileSystemEventHandler):
    def on_created(self, event):
        # Only process files, not directories
        if not event.is_directory and event.src_path.lower().endswith('.pdf'):
            logging.info(f"🔔 New file detected: {event.src_path}")
            
            # The stability check is now inside handle_file
            handle_file(event.src_path)

# === SIGNAL HANDLER FOR GRACEFUL EXIT ===
def signal_handler(sig, frame):
    logging.info("\n🛑 Stopping PDF invoice monitoring (Ctrl+C pressed)")
    observer.stop()
    observer.join()
    logging.info("Observer stopped.")
    sys.exit(0)

# === MAIN ===
if __name__ == "__main__":
    # Ensure directories exist
    for directory in [INPUT_DIR, ARCHIVE_DIR, ERROR_DIR, OUTPUT_DIR]:
        os.makedirs(directory, exist_ok=True)
    
    logging.info("🚀 PDF Invoice Header Extractor with Watchdog")
    logging.info(f"📁 Input Directory: {INPUT_DIR}")
    logging.info(f"📁 Output Directory: {OUTPUT_DIR}")
    logging.info(f"📁 Archive Directory: {ARCHIVE_DIR}")
    logging.info(f"📁 Error Directory: {ERROR_DIR}")
    
    # Process any existing files first
    process_existing_files()
    
    # Set up the watchdog observer
    observer = Observer()
    event_handler = PDFHandler()
    observer.schedule(event_handler, INPUT_DIR, recursive=False)
    
    # Register signal handler for Ctrl+C
    signal.signal(signal.SIGINT, signal_handler)
    
    # Start the observer
    logging.info("\n👀 Watching for new PDF files in input directory...")
    logging.info("⌨️  Press Ctrl+C to stop monitoring\n")
    
    observer.start()
    
    try:
        # Keep the main thread alive
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        # This is a backup in case the signal handler doesn't catch it
        logging.info("\n🛑 Stopping PDF invoice monitoring (KeyboardInterrupt)")
        observer.stop()
    
    observer.join()
    logging.info("✨ Monitoring stopped. Goodbye!")
