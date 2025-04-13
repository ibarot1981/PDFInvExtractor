import os
import shutil
import time
import csv
import re
import traceback
from datetime import datetime
import pdfplumber
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from collections import OrderedDict

# === CONFIGURATION ===
INPUT_DIR = 'files/input'
ARCHIVE_DIR = 'files/archive'
ERROR_DIR = 'files/error'
OUTPUT_DIR = 'files/output'

# === PDF PROCESSING FUNCTION ===
def process_pdf(file_path):
    with pdfplumber.open(file_path) as pdf:
        lines = []
        for page in pdf.pages:
            lines.extend(page.extract_text().splitlines())

    # --- Header parsing ---
    invoice_number = ""
    invoice_date_str = ""
    consignee_name = ""
    consignee_address = []

    in_consignee_block = False
    consignee_block_started = False

    place_of_delivery = ""

    for idx, line in enumerate(lines):
        # temp block
        if "Consignee" in line:
            print(f"\n🟡 Found line {idx}: {line}")
            print("🔍 Lines around this point:")
            for j in range(max(idx-2, 0), min(idx+10, len(lines))):
                print(f"{j}: {lines[j]}")
        # temp block ends

        line = line.strip()

        # Look for the line with invoice number and date
        if not invoice_number and re.search(r"SC\d{5}-\d{2}-\d{2}", line):
            match = re.search(r"(SC\d{5}-\d{2}-\d{2})", line)
            if match:
                invoice_number = match.group(1)

            date_match = re.findall(r"\b\d{1,2}-[A-Za-z]{3}-\d{2}\b", line)
            if date_match:
                invoice_date_str = date_match[-1]

        # Place of Supply
        if "Place of Supply" in line:
            place_match = re.search(r"Place of Supply\s*:\s*(.+)", line)
            if place_match:
                place_of_delivery = place_match.group(1).strip()

        # NEW: Consignee (Ship to) handling
        if "Consignee (Ship to)" in line:
            # Line 1: Name (split from extra fields)
            name_line = lines[idx + 1].strip()
            consignee_name = name_line.split("Dispatch Doc No.")[0].strip()

            # Prevent duplicate lines and limit to reasonable number of lines
            # Initialize
            consignee_name = ""
            consignee_address = []
            consignee_start = None
            stop_keywords = [
                "Buyer (Bill to)", "Terms of Delivery", "Dispatched through", 
                "Dispatch Doc No.", "Delivery Note Date", "Destination", "GSTIN", "State Name", "E-Mail"
            ]

            # Step 1: Locate 'Consignee (Ship to)' line
            for i, line in enumerate(lines):
                if "Consignee (Ship to)" in line:
                    consignee_start = i
                    break

            # Step 2: Parse next lines for name and address
            if consignee_start is not None:
                # The line immediately after usually contains name + extra text
                line_after = lines[consignee_start + 1].strip()
                
                # Use regex to extract only the name (before any keywords)
                split_line = re.split(r"Dispatch Doc No\.|Delivery Note Date|Dispatched through|Destination|Terms of Delivery|GSTIN|State Name|E-Mail", line_after)
                consignee_name = split_line[0].strip()

                # Collect address lines until a stop keyword is hit
                for line in lines[consignee_start + 2 : consignee_start + 12]:
                    clean_line = line.strip()
                    if not clean_line:
                        continue
                    if any(kw in clean_line for kw in stop_keywords):
                        break
                    consignee_address.append(clean_line)

            # Clean and join the address
            cleaned_address = ", ".join(dict.fromkeys(consignee_address))  # removes duplicates while preserving order
    # temp
    #cleaned_address = ", ".join(OrderedDict.fromkeys(consignee_address))
    print("📍 Consignee lines start at:", consignee_start)
    print("🏷️  Consignee Name:", consignee_name)
    print("📋 Raw collected address lines:", consignee_address)
    print("🧾 Cleaned Consignee Address:", cleaned_address)
    # temp ends

    consignee_address_str = " ".join(consignee_address).strip()

    # --- Prepare for CSV ---
    try:
        invoice_date_obj = datetime.strptime(invoice_date_str, "%d-%b-%y")
    except ValueError:
        raise ValueError(f"Invalid invoice date: '{invoice_date_str}'")
    month_file = invoice_date_obj.strftime("%b%y")
    output_csv = os.path.join(OUTPUT_DIR, f"{month_file}Invoices.csv")

    # --- Line item parsing ---
    data_rows = []
    item_pattern = re.compile(r"^(\d+)\s+(.*?)\s+(\d{5,})\s+(\d+)\s+NOS\s+([\d,]+\.\d{2})\s+NOS\s+([\d,]+\.\d{2})$")

    current_item = None
    line_items = []

    for line in lines:
        line = line.strip()
        match = item_pattern.match(line)
        if match:
            if current_item:
                line_items.append(current_item)
            item_no = match.group(1)
            base_desc = match.group(2)
            qty = match.group(4)
            rate = match.group(5).replace(",", "")
            total = match.group(6).replace(",", "")
            current_item = [
                invoice_date_str,
                invoice_number,
                consignee_name,
                cleaned_address,
                place_of_delivery,
                item_no,
                base_desc,
                qty,
                rate,
                total
            ]
        elif current_item:
            # Append continuation of description
            current_item[6] += " " + line

    # Final push
    if current_item:
        line_items.append(current_item)

    # Deduplicate items
    unique_items = []
    seen = set()
    for item in line_items:
        row_string = "|".join(str(x).strip() for x in item)
        if row_string not in seen:
            seen.add(row_string)
            unique_items.append(item)
            
    print(f"🧾 Total line items parsed: {len(unique_items)}")

    data_rows.extend(unique_items)
    
    print("Invoice Number:", invoice_number)
    print("Invoice Date:", invoice_date_str)
    print("Consignee Name:", consignee_name)
    print("Consignee Address:", consignee_address)
    print("Place of Delivery:", place_of_delivery)
    
    # --- Write to CSV ---
    write_header = not os.path.exists(output_csv)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(output_csv, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow([
                "Invoice Date", "Invoice Number", "Consignee Name", "Consignee Address",
                "Place of Delivery", "Item", "Item Description", "Qty", "Rate", "Total"
            ])
        writer.writerows(data_rows)

    print(f"✅ Parsed and appended to {output_csv}")

# === FILE MOVING WITH TIMESTAMP ===
def move_file(file_path, target_dir):
    os.makedirs(target_dir, exist_ok=True)
    base_name = os.path.basename(file_path)
    name, ext = os.path.splitext(base_name)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    new_name = f"{name}_{timestamp}{ext}"
    shutil.move(file_path, os.path.join(target_dir, new_name))

# === FILE HANDLER ===
class PDFHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory or not event.src_path.lower().endswith('.pdf'):
            return

        time.sleep(1)
        file_path = event.src_path
        handle_file(file_path)

def handle_file(file_path):
    try:
        process_pdf(file_path)
        move_file(file_path, ARCHIVE_DIR)
        print(f"📦 Archived: {file_path}")
    except Exception as e:
        print(f"❌ Error: {e}")
        traceback.print_exc()
        move_file(file_path, ERROR_DIR)
        print(f"⚠️ Moved to error folder: {file_path}")

# === PROCESS EXISTING FILES ===
def process_existing_files():
    for filename in os.listdir(INPUT_DIR):
        if filename.lower().endswith('.pdf'):
            file_path = os.path.join(INPUT_DIR, filename)
            handle_file(file_path)

# === WATCHER ===
def start_watching():
    os.makedirs(INPUT_DIR, exist_ok=True)
    process_existing_files()

    event_handler = PDFHandler()
    observer = Observer()
    observer.schedule(event_handler, INPUT_DIR, recursive=False)
    observer.start()
    print(f"👀 Watching directory: {INPUT_DIR}...")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

# === MAIN ===
if __name__ == "__main__":
    start_watching()
