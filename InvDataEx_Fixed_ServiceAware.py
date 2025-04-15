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

    with pdfplumber.open(file_path) as pdf:
        text = ""
        for page in pdf.pages:
            page_text = page.extract_text()
            if page_text and "TAX INVOICE" in page_text:
                text = page_text
                break

    if not text:
        raise ValueError("No valid invoice page found.")

    lines = [line.strip() for line in text.splitlines() if line.strip()]
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

    def find_value(label, offset=0, after=True):
        for i, line in enumerate(lines):
            if label in line:
                idx = i + offset if after else i
                if 0 <= idx < len(lines):
                    return lines[idx].strip()
        return ""

    # === General Fields ===
    for i, line in enumerate(lines):
        if not fields["IRN"] and line.startswith("IRN"):
            fields["IRN"] = line.split("IRN", 1)[-1].strip(": ").strip()

        if not fields["Ack No"] and line.startswith("Ack No"):
            fields["Ack No"] = line.split("Ack No.", 1)[-1].strip()

        if not fields["Ack Date"] and "Ack Date" in line:
            fields["Ack Date"] = line.split("Ack Date", 1)[-1].strip(": ").strip()

        if not fields["Invoice Number"] and "Invoice No." in line:
            match = re.search(r"(SC\d{5}-\d{2}-\d{2})", line)
            if match:
                fields["Invoice Number"] = match.group(1)

        if not fields["e-Way Bill No"] and "e-Way Bill No." in line:
            parts = line.split()
            for p in parts:
                if p.isdigit() and len(p) >= 10:
                    fields["e-Way Bill No"] = p
                    break

        if not fields["Dispatch Mode"] and "Dispatched through" in line:
            fields["Dispatch Mode"] = find_value("Dispatched through", offset=1)

        if not fields["Motor Vehicle No"] and "Motor Vehicle No" in line:
            fields["Motor Vehicle No"] = find_value("Motor Vehicle No", offset=1)

        if not fields["Destination"] and "Destination" in line:
            fields["Destination"] = find_value("Destination", offset=1)

        if not fields["Delivery Note Date"] and "Delivery Note Date" in line:
            fields["Delivery Note Date"] = find_value("Delivery Note Date", offset=1)

        if not fields["Invoice Date"] and "Bill of Lading" in line and i + 2 < len(lines):
            if "Dated" in lines[i + 1]:
                fields["Invoice Date"] = lines[i + 2].strip()

    # Fallback for Invoice Date if still blank
    if not fields["Invoice Date"]:
        for line in lines:
            match = re.search(r"\b\d{1,2}-[A-Za-z]{3}-\d{2,4}\b", line)
            if match:
                fields["Invoice Date"] = match.group(0)
                break

    # === Place of Supply ===
    for line in lines:
        if "Place of Supply" in line:
            parts = line.split(":")
            if len(parts) > 1:
                fields["Place of Supply"] = parts[1].strip()
            break

    # === Consignee Section ===
    for i, line in enumerate(lines):
        if "Consignee (Ship to)" in line:
            fields["Consignee Name"] = lines[i + 1].strip()
            address_lines = []
            for j in range(i + 2, i + 10):
                if j >= len(lines) or "GSTIN" in lines[j] or "State Name" in lines[j]:
                    break
                address_lines.append(lines[j])
            fields["Consignee Address"] = ", ".join(address_lines)

        if "GSTIN/UIN" in line and not fields["Consignee GSTIN"]:
            match = re.search(r"GSTIN/UIN\s*:\s*(\S+)", line)
            if match:
                fields["Consignee GSTIN"] = match.group(1)

        if "State Name" in line and not fields["Consignee State Name"]:
            match = re.search(r"State Name\s*:\s*(.+?),\s*Code\s*:\s*(\d+)", line)
            if match:
                fields["Consignee State Name"] = match.group(1).strip()
                fields["Consignee State Code"] = match.group(2).strip()
            break

    # === Buyer Section ===
    for i, line in enumerate(lines):
        if "Buyer (Bill to)" in line:
            fields["Buyer Name"] = lines[i + 1].strip()
            address_lines = []
            for j in range(i + 2, i + 10):
                if j >= len(lines) or "GSTIN" in lines[j] or "State Name" in lines[j]:
                    break
                address_lines.append(lines[j])
            fields["Buyer Address"] = ", ".join(address_lines)

        if "GSTIN/UIN" in line and not fields["Buyer GSTIN"]:
            match = re.search(r"GSTIN/UIN\s*:\s*(\S+)", line)
            if match:
                fields["Buyer GSTIN"] = match.group(1)

        if "State Name" in line and not fields["Buyer State Name"]:
            match = re.search(r"State Name\s*:\s*(.+?),\s*Code\s*:\s*(\d+)", line)
            if match:
                fields["Buyer State Name"] = match.group(1).strip()
                fields["Buyer State Code"] = match.group(2).strip()
            break

    # === Finalize Invoice Date for Output Filename ===
    invoice_date_str = fields["Invoice Date"]
    try:
        invoice_date_obj = datetime.strptime(invoice_date_str, "%d-%b-%y")
    except ValueError:
        raise ValueError(f"Invalid invoice date format: '{invoice_date_str}'")

    output_file = f"{invoice_date_obj.strftime('%b%y')}Invoices.csv"
    output_path = os.path.join("files/output", output_file)

    # === Write to CSV ===
    write_header = not os.path.exists(output_path)
    os.makedirs("files/output", exist_ok=True)
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
