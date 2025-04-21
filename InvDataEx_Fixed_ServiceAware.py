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
    import re
    from datetime import datetime

    # === Field Map ===
    fields = {
        "Invoice Number": "",
        "Invoice Date": "",
        "IRN": "",
        "Ack No": "",
        "Ack Date": "",
        "e-Way Bill No": "",
        "Dispatch Mode": "",
        "Motor Vehicle No": "",
        "Destination": "",
        "Delivery Note Date": "",
        "Buyer's Order No": "",

        "Consignee Name": "",
        "Consignee Address": "",
        "Consignee GSTIN": "",
        "Consignee State Name": "",
        "Consignee State Code": "",

        "Buyer Name": "",
        "Buyer Address": "",
        "Buyer GSTIN": "",
        "Buyer State Name": "",
        "Buyer State Code": "",
        "Place of Supply": ""
    }

    # === Extract first meaningful page ===
    with pdfplumber.open(file_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if text and "TAX INVOICE" in text:
                lines = text.splitlines()
                break
        else:
            raise ValueError("No valid invoice content found")

    lines = [line.strip() for line in lines if line.strip()]

    # === Regex patterns ===
    invoice_number_re = re.compile(r"(SC\d{5}-\d{2}-\d{2})")
    date_re = re.compile(r"\b\d{1,2}-[A-Za-z]{3}-\d{2,4}\b")
    gstin_re = re.compile(r"GSTIN/UIN\s*:\s*(\S+)")
    state_re = re.compile(r"State Name\s*:\s*(.+?),\s*Code\s*:\s*(\d+)")
    ewaybill_re = re.compile(r"e-Way Bill No\.\s+(\d+)")
    irn_re = re.compile(r"IRN\s*:\s*(\S+)")
    ack_no_re = re.compile(r"Ack No\.\s*(\d+)")
    ack_date_re = re.compile(r"Ack Date\s*:\s*(.+)")

    # === Main parsing ===
    i = 0
    while i < len(lines):
        line = lines[i]

        # General Info
        if match := irn_re.search(line):
            fields["IRN"] = match.group(1)
        elif match := ack_no_re.search(line):
            fields["Ack No"] = match.group(1)
        elif match := ack_date_re.search(line):
            fields["Ack Date"] = match.group(1).strip()
        elif "Invoice No." in line and not fields["Invoice Number"]:
            match = invoice_number_re.search(line)
            if match:
                fields["Invoice Number"] = match.group(1)
        elif match := ewaybill_re.search(line):
            fields["e-Way Bill No"] = match.group(1)
        elif "Dispatched through" in line and i + 1 < len(lines):
            fields["Dispatch Mode"] = lines[i + 1].strip()
        elif "Motor Vehicle No." in line and i + 1 < len(lines):
            fields["Motor Vehicle No"] = lines[i + 1].strip()
        elif "Destination" in line and i + 1 < len(lines):
            fields["Destination"] = lines[i + 1].strip()
        elif "Delivery Note Date" in line and i + 1 < len(lines):
            fields["Delivery Note Date"] = lines[i + 1].strip()
        elif "Bill of Lading" in line and i + 2 < len(lines):
            if "Dated" in lines[i + 1]:
                date_match = date_re.search(lines[i + 2])
                if date_match:
                    fields["Invoice Date"] = date_match.group(0)
        elif not fields["Place of Supply"] and "Place of Supply" in line:
            parts = line.split(":")
            if len(parts) > 1:
                fields["Place of Supply"] = parts[1].strip()

        # Consignee Block
        if "Consignee (Ship to)" in line:
            fields["Consignee Name"] = lines[i + 1].strip()
            address_lines = []
            for j in range(i + 2, i + 10):
                if j >= len(lines): break
                if "GSTIN" in lines[j] or "State Name" in lines[j]: break
                address_lines.append(lines[j])
            fields["Consignee Address"] = ", ".join(address_lines)
        elif "Buyer (Bill to)" in line:
            fields["Buyer Name"] = lines[i + 1].strip()
            address_lines = []
            for j in range(i + 2, i + 10):
                if j >= len(lines): break
                if "GSTIN" in lines[j] or "State Name" in lines[j]: break
                address_lines.append(lines[j])
            fields["Buyer Address"] = ", ".join(address_lines)

        # GSTINs and State Names
        if not fields["Consignee GSTIN"] and "GSTIN/UIN" in line:
            match = gstin_re.search(line)
            if match:
                fields["Consignee GSTIN"] = match.group(1)
        elif not fields["Buyer GSTIN"] and "GSTIN/UIN" in line and fields["Consignee GSTIN"]:
            match = gstin_re.search(line)
            if match:
                fields["Buyer GSTIN"] = match.group(1)

        if not fields["Consignee State Name"] and "State Name" in line:
            match = state_re.search(line)
            if match:
                fields["Consignee State Name"] = match.group(1).strip()
                fields["Consignee State Code"] = match.group(2).strip()
        elif not fields["Buyer State Name"] and "State Name" in line and fields["Consignee State Name"]:
            match = state_re.search(line)
            if match:
                fields["Buyer State Name"] = match.group(1).strip()
                fields["Buyer State Code"] = match.group(2).strip()
        i += 1

    # === Fallback Invoice Date ===
    if not fields["Invoice Date"]:
        for line in lines:
            match = date_re.search(line)
            if match:
                fields["Invoice Date"] = match.group(0)
                break

    # === Output File Prep ===
    invoice_date_str = fields["Invoice Date"]
    try:
        invoice_date_obj = datetime.strptime(invoice_date_str, "%d-%b-%y")
    except ValueError:
        raise ValueError(f"Invalid invoice date format: {invoice_date_str}")

    output_file = f"{invoice_date_obj.strftime('%b%y')}Invoices.csv"
    output_path = os.path.join("files/output", output_file)

    # === CSV Write ===
    os.makedirs("files/output", exist_ok=True)
    write_header = not os.path.exists(output_path)
    with open(output_path, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow([
                "Invoice Number", "Invoice Date", "IRN", "Ack No", "Ack Date", "e-Way Bill No",
                "Dispatch Mode", "Motor Vehicle No", "Destination", "Delivery Note Date", "Buyer's Order No",
                "Consignee Name", "Consignee Address", "Consignee GSTIN", "Consignee State Name", "Consignee State Code",
                "Buyer Name", "Buyer Address", "Buyer GSTIN", "Buyer State Name", "Buyer State Code", "Place of Supply"
            ])
        writer.writerow([fields[key] for key in fields])

    print(f"✅ Extracted header for invoice {fields['Invoice Number']} → {output_path}")



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
