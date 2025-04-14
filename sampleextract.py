import re
import pdfplumber

with pdfplumber.open("files/input/multipagesample.pdf") as pdf:
    all_lines = []
    for i, page in enumerate(pdf.pages):
        print(f"\n--- Page {i+1} ---")
        lines = page.extract_text().splitlines()
        all_lines.extend(lines)
        for line in lines:
            print(line)

print("\n🔍 Filtered candidate item lines (those starting with number):")
for line in all_lines:
    if re.match(r"^\d{1,3}\s", line.strip()):
        print("✅", line)
