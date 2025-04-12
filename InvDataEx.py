import os
import shutil
import pdfplumber
import csv
import re
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import time

INPUT_DIR = 'files/input'
ARCHIVE_DIR = 'files/archive'
ERROR_DIR = 'files/error'
OUTPUT_DIR = 'files/output'  # Change this to your desired output folder

def process_pdf(file_path):
    """
    Dummy processing function.
    Replace with your actual logic.
    Raise an exception if processing fails.
    """
    print(f"Processing: {file_path}")
    if "fail" in os.path.basename(file_path).lower():
        raise ValueError("Simulated processing failure.")
    return True

def move_file(file_path, target_dir):
    os.makedirs(target_dir, exist_ok=True)
    shutil.move(file_path, os.path.join(target_dir, os.path.basename(file_path)))

class PDFHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return

        if event.src_path.lower().endswith('.pdf'):
            time.sleep(1)  # small delay to avoid reading incomplete files
            try:
                process_pdf(event.src_path)
                move_file(event.src_path, ARCHIVE_DIR)
                print(f"Archived: {event.src_path}")
            except Exception as e:
                print(f"Error processing {event.src_path}: {e}")
                move_file(event.src_path, ERROR_DIR)
                print(f"Moved to error: {event.src_path}")

if __name__ == "__main__":
    os.makedirs(INPUT_DIR, exist_ok=True)
    event_handler = PDFHandler()
    observer = Observer()
    observer.schedule(event_handler, INPUT_DIR, recursive=False)
    observer.start()
    print(f"Watching directory: {INPUT_DIR} for new PDF files...")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
