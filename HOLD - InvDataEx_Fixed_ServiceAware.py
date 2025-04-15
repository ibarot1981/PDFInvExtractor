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
    import pdfplumber
    import csv
    import os
    from datetime import datetime

    with pdfplumber.open(file_path) as pdf:
        lines = []
        for page in pdf.pages:
            lines.extend(page.extract_text().splitlines())

    # === Extract header info ===
    invoice_number = ""
    invoice_date_str = ""
    consignee_name = ""
    consignee_address_lines = []
    place_of_delivery = ""

    stop_consignee_keywords = [
        "Buyer (Bill to)", "Terms of Delivery", "Dispatched through",
        "Dispatch Doc No.", "Delivery Note Date", "Destination",
        "GSTIN", "State Name", "E-Mail", "Bill of Lading", "Motor Vehicle"
    ]

    for i, line in enumerate(lines):
        if not invoice_number and "SC" in line:
            match = re.search(r"(SC\d{5}-\d{2}-\d{2})", line)
            if match:
                invoice_number = match.group(1)
            date_match = re.findall(r"\b\d{1,2}-[A-Za-z]{3}-\d{2}\b", line)
            if date_match:
                invoice_date_str = date_match[-1]

        if "Place of Supply" in line:
            match = re.search(r"Place of Supply\s*:\s*(.+)", line)
            if match:
                place_of_delivery = match.group(1).strip()

        if "Consignee (Ship to)" in line:
            # Line after this is name, then up to 5 lines of address
            consignee_name_line = lines[i + 1].strip()
            consignee_name = consignee_name_line.split("Dispatch Doc No.")[0].strip()

            for addr_line in lines[i + 2 : i + 10]:
                addr_line = addr_line.strip()
                if any(kw.lower() in addr_line.lower() for kw in stop_consignee_keywords):
                    break
                if addr_line:
                    consignee_address_lines.append(addr_line)
            break

    cleaned_address = ", ".join(dict.fromkeys(consignee_address_lines))

    # === Prepare CSV filename ===
    try:
        invoice_date_obj = datetime.strptime(invoice_date_str, "%d-%b-%y")
    except:
        raise ValueError(f"Invalid invoice date format: {invoice_date_str}")
    output_file = f"{invoice_date_obj.strftime('%b%y')}Invoices.csv"
    output_path = os.path.join(OUTPUT_DIR, output_file)

    # === Line item extraction ===
    data_rows = []
    item_no = ""
    description_lines = []
    qty = ""
    rate = ""
    total = ""
    in_summary = False

    summary_triggers = [
        "output cgst", "output sgst", "output igst", "amount chargeable",
        "tax amount", "total", "company", "bank", "authorised signatory",
        "declaration", "terms & conditions", "subject to", "value rate"
    ]

    def is_summary_line(text):
        return any(kw in text.lower() for kw in summary_triggers)

    def flush_item():
        if item_no and description_lines:
            data_rows.append([
                invoice_date_str, invoice_number, consignee_name,
                cleaned_address, place_of_delivery,
                item_no, " ".join(description_lines).strip(),
                qty, rate, total
            ])

    for line in lines:
        clean = line.strip()
        if not clean:
            continue

        if is_summary_line(clean):
            in_summary = True
            continue

        if in_summary:
            continue

        # Detect start of new item
        match = re.match(r"^(\d{1,3})\s+(.*)", clean)
        if match:
            flush_item()

            item_no = match.group(1)
            rest = match.group(2)
            description_lines = []
            qty = rate = total = ""

            std = re.search(r"(.*?)\s+(\d{5,})\s+(\d+)\s+NOS\s+([\d,.]+)\s+NOS\s+([\d,.]+)", rest)
            svc = re.search(r"(.*?)\s+(\d{5,})\s+([\d,.]+)", rest)

            if std:
                description_lines = [std.group(1)]
                qty = std.group(3)
                rate = std.group(4).replace(",", "")
                total = std.group(5).replace(",", "")
            elif svc:
                description_lines = [svc.group(1)]
                qty = ""
                rate = ""
                total = svc.group(3).replace(",", "")
            else:
                description_lines = [rest]
        else:
            # Continuation of description
            if item_no:
                description_lines.append(clean)

    flush_item()  # Flush last item

    # === Write to CSV ===
    write_header = not os.path.exists(output_path)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(output_path, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow([
                "Invoice Date", "Invoice Number", "Consignee Name", "Consignee Address",
                "Place of Delivery", "Item", "Item Description", "Qty", "Rate", "Total"
            ])
        writer.writerows(data_rows)

    print(f"✅ Extracted {len(data_rows)} line items → {output_path}")



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
    except Exception as e:
        print(f"❌ Error: {e}")
        traceback.print_exc()
        move_file(file_path, ERROR_DIR)

def process_existing_files():
    for filename in os.listdir(INPUT_DIR):
        if filename.lower().endswith('.pdf'):
            handle_file(os.path.join(INPUT_DIR, filename))

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
