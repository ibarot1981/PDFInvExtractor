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

# === CONFIGURATION ===
INPUT_DIR = 'files/input'
ARCHIVE_DIR = 'files/archive'
ERROR_DIR = 'files/error'
OUTPUT_DIR = 'files/output'

def process_pdf(file_path):
    with pdfplumber.open(file_path) as pdf:
        lines = []
        for page in pdf.pages:
            lines.extend(page.extract_text().splitlines())

    invoice_number = ""
    invoice_date_str = ""
    consignee_name = ""
    consignee_address = []
    place_of_delivery = ""

    stop_keywords = [
        "Buyer (Bill to)", "Terms of Delivery", "Dispatched through", 
        "Dispatch Doc No.", "Delivery Note Date", "Destination", "GSTIN", "State Name", "E-Mail"
    ]

    for idx, line in enumerate(lines):
        line = line.strip()
        if not invoice_number and re.search(r"SC\d{5}-\d{2}-\d{2}", line):
            match = re.search(r"(SC\d{5}-\d{2}-\d{2})", line)
            if match:
                invoice_number = match.group(1)
            date_match = re.findall(r"\b\d{1,2}-[A-Za-z]{3}-\d{2}\b", line)
            if date_match:
                invoice_date_str = date_match[-1]

        if "Place of Supply" in line:
            place_match = re.search(r"Place of Supply\s*:\s*(.+)", line)
            if place_match:
                place_of_delivery = place_match.group(1).strip()

        if "Consignee (Ship to)" in line:
            consignee_start = idx
            break
    else:
        consignee_start = None

    if consignee_start is not None:
        line_after = lines[consignee_start + 1].strip()
        split_line = re.split(r"Dispatch Doc No\.|Delivery Note Date|Dispatched through|Destination|Terms of Delivery|GSTIN|State Name|E-Mail", line_after)
        consignee_name = split_line[0].strip()
        for line in lines[consignee_start + 2 : consignee_start + 12]:
            clean_line = line.strip()
            if not clean_line or any(kw in clean_line for kw in stop_keywords):
                break
            consignee_address.append(clean_line)

    cleaned_address = ", ".join(dict.fromkeys(consignee_address))

    try:
        invoice_date_obj = datetime.strptime(invoice_date_str, "%d-%b-%y")
    except ValueError:
        raise ValueError(f"Invalid invoice date: '{invoice_date_str}'")
    month_file = invoice_date_obj.strftime("%b%y")
    output_csv = os.path.join(OUTPUT_DIR, f"{month_file}Invoices.csv")

    # === LINE ITEM PARSING ===
    #item_pattern = re.compile(r"^(\d+)\s+(.*?)\s+(\d{5,})\s+(\d+)\s+NOS\s+([\d,]+\.\d{2})\s+NOS\s+([\d,]+\.\d{2})$")
    item_pattern = re.compile(r"^(\d+)\s+(.+?)\s+(\d{5,})\s+(\d+)\s+NOS\s+([\d,.]+)\s+NOS\s+([\d,.]+)$")

    #service_item_pattern = re.compile(r"^(\d+)\s+(Local Repairs.*?)\s+(\d{5,})\s+([\d,]+\.\d{2})$")
    service_item_pattern = re.compile(r"^(\d+)\s+(Local Repairs.*?)\s+(\d{5,})(?:\s+([\d,.]+))?$")

    summary_keywords = [
        "Total", "Output CGST", "Output SGST", "Amount Chargeable",
        "HSN/SAC", "Taxable", "Company", "Declaration", "Terms & Conditions",
        "INR", "Authorised Signatory"
    ]

    data_rows = []
    current_item = None
    collecting_service = False
    service_item_no = ""
    service_description_lines = []
    service_amount = ""

    for line in lines:
        # temp
        print("🔍 Scanning line:", line)
        #
        line = line.strip()

        if any(kw.lower() in line.lower() for kw in summary_keywords):
            # Don't break — just skip summary/footer lines
            continue


        # Match standard items
        match = item_pattern.match(line)
        if match:
            if current_item:
                data_rows.append(current_item)
            item_no = match.group(1)
            base_desc = match.group(2)
            qty = match.group(4)
            rate = match.group(5).replace(",", "")
            total = match.group(6).replace(",", "")
            current_item = [
                invoice_date_str, invoice_number, consignee_name,
                cleaned_address, place_of_delivery,
                item_no, base_desc, qty, rate, total
            ]
            continue

        # Continuation of standard item
        if current_item:
            current_item[6] += " " + line
            continue

        # Match new-style service item
        match = service_item_pattern.match(line)
        if match:
            collecting_service = True
            service_item_no = match.group(1)
            service_description_lines = [match.group(2)]
            service_amount = match.group(4).replace(",", "")
            continue

        # Continue service item block
        if collecting_service:
            if re.search(r"\d{1,3}(?:,\d{3})*\.\d{2}", line):
                service_amount = re.search(r"\d{1,3}(?:,\d{3})*\.\d{2}", line).group().replace(",", "")
            else:
                service_description_lines.append(line)
    # Temp
    print(f"✅ Extracted {len(data_rows)} line items")
    for row in data_rows:
        print("🧾 Row:", row)
    # End Temp

    # In case last item wasn't appended
    if current_item:
        data_rows.append(current_item)

    # === WRITE TO CSV ===
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

def move_file(file_path, target_dir):
    os.makedirs(target_dir, exist_ok=True)
    base_name = os.path.basename(file_path)
    name, ext = os.path.splitext(base_name)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    new_name = f"{name}_{timestamp}{ext}"
    shutil.move(file_path, os.path.join(target_dir, new_name))

class PDFHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory or not event.src_path.lower().endswith('.pdf'):
            return
        time.sleep(1)
        handle_file(event.src_path)

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

def process_existing_files():
    for filename in os.listdir(INPUT_DIR):
        if filename.lower().endswith('.pdf'):
            file_path = os.path.join(INPUT_DIR, filename)
            handle_file(file_path)

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

if __name__ == "__main__":
    start_watching()
