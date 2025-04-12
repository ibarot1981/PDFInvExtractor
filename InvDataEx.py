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
OUTPUT_DIR = 'files/output'  # Change this to your desired output folder

# === PDF PROCESSING FUNCTION ===
def process_pdf(file_path):
    data_rows = []

    with pdfplumber.open(file_path) as pdf:
        invoice_date = None
        invoice_number = None
        consignee_name = None
        consignee_address = ""
        place_of_delivery = None
        output_csv = None
        invoice_date_str = "Unknown"

        for page in pdf.pages:
            text = page.extract_text()

            if not invoice_date:
                invoice_number = re.search(r"Invoice No\.\s*([A-Z0-9\-]+)", text)
                invoice_date = re.search(r"Ack Date\s*:\s*([0-9]{2}-[A-Za-z]{3}-[0-9]{2})", text)
                place_of_delivery = re.search(r"Destination\s*(.*)", text)
                consignee_match = re.search(r"Consignee \(Ship to\)\n(.*?)\n", text, re.DOTALL)
                consignee_address_block = re.search(r"Consignee \(Ship to\)\n(.*?)Buyer \(Bill to\)", text, re.DOTALL)

                invoice_number = invoice_number.group(1).strip() if invoice_number else "Unknown"
                invoice_date_str = invoice_date.group(1).strip() if invoice_date else "Unknown"
                place_of_delivery = place_of_delivery.group(1).strip() if place_of_delivery else "Unknown"

                if consignee_match:
                    consignee_name = consignee_match.group(1).strip()
                if consignee_address_block:
                    consignee_address = " ".join(consignee_address_block.group(1).splitlines()).strip()

                try:
                    dt = datetime.strptime(invoice_date_str, "%d-%b-%y")
                    month_file = dt.strftime("%b%y")
                except ValueError:
                    raise ValueError(f"Invalid date format in invoice: {invoice_date_str}")

                output_csv = os.path.join(OUTPUT_DIR, f"{month_file}Invoices.csv")

            tables = page.extract_tables()
            for table in tables:
                for i, row in enumerate(table):
                    if not row or len(row) < 5:
                        continue
                    if not isinstance(row[0], str) or not row[0].strip().isdigit():
                        continue

                    item_no = row[0].strip()
                    description = row[1].strip() if isinstance(row[1], str) else ''
                    total = row[2].strip().replace(",", "") if isinstance(row[2], str) else ''
                    rate = row[3].strip().replace(",", "") if isinstance(row[3], str) else ''
                    qty = row[4].strip() if isinstance(row[4], str) else ''

                    if i + 1 < len(table):
                        next_row = table[i + 1]
                        if next_row[0] == '' and isinstance(next_row[1], str):
                            description += ' ' + next_row[1].strip()

                    data_rows.append([
                        invoice_date_str,
                        invoice_number,
                        consignee_name,
                        consignee_address,
                        place_of_delivery,
                        item_no,
                        description,
                        qty,
                        rate,
                        total
                    ])

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    write_header = not os.path.exists(output_csv)
    with open(output_csv, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow([
                "Invoice Date", "Invoice Number", "Consignee Name", "Consignee Address",
                "Place of Delivery", "Item", "Item Description", "Qty", "Rate", "Total"
            ])
        writer.writerows(data_rows)

    print(f"✅ Processed and saved to {output_csv}")

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
