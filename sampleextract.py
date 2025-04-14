import pdfplumber

with pdfplumber.open("files/input/multipagesample.pdf") as pdf:
    for i, page in enumerate(pdf.pages):
        print(f"\n--- Page {i+1} ---")
        lines = page.extract_text().splitlines()
        for line in lines:
            print(line)