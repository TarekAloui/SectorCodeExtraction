import os
import sys
import pdfplumber
import pytesseract
from PIL import Image
import pandas as pd
import re


def ocr_pdf_page(page):
    """Convert a PDF page to text using OCR, handling image conversion errors."""
    try:
        image = page.to_image()
        text = pytesseract.image_to_string(image, lang="eng+fra+nld")
        return text
    except Exception as e:
        print(f"Error converting page to image for OCR: {str(e)}")
        return ""  # Return empty string if OCR fails


def extract_sector_codes(pdf_path):
    """Extract sector codes from a single PDF."""
    try:
        with pdfplumber.open(pdf_path) as pdf:
            text = ""
            print("Processing", pdf_path)
            for page in pdf.pages:
                print("Page", page.page_number)
                print("Page Objects:", page.objects)
                if "fontname" in page.objects:  # Check if page has text objects
                    page_text = page.extract_text()
                    if page_text:
                        text += page_text + "\n"
                    print(page_text)
                else:
                    text += ocr_pdf_page(page) + "\n"
            print("Extracted text from", pdf_path)

            pattern = r"paritaire[s]?[^:]*:\s*([\d\s.]+)"
            matches = re.findall(pattern, text, re.IGNORECASE)
            all_codes = []
            for match in matches:
                # Clean and split codes, then join with semicolon
                codes = re.sub(r"\s+", ";", match.strip())
                all_codes.append(codes)

            return "; ".join(all_codes), len(all_codes)
    except Exception as e:
        print(f"Error processing {pdf_path}: {str(e)}")
        return "", 0


def process_pdfs(input_path):
    """Process each PDF in a directory or a single PDF file."""
    results = []
    if os.path.isdir(input_path):
        for filename in os.listdir(input_path):
            if filename.lower().endswith(".pdf"):
                pdf_path = os.path.join(input_path, filename)
                codes, count = extract_sector_codes(pdf_path)
                results.append([filename, codes, count])
    elif input_path.lower().endswith(".pdf"):
        codes, count = extract_sector_codes(input_path)
        results.append([os.path.basename(input_path), codes, count])
    else:
        print("Invalid file or directory path")
        sys.exit(1)

    # Convert results to a DataFrame and save as CSV
    df = pd.DataFrame(
        results, columns=["Document Name", "Sector Codes", "Number of Codes"]
    )
    df.to_csv("sector_codes.csv", index=False)
    print("CSV file has been created: sector_codes.csv")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python extract_codes.py 'folder_path_or_pdf_path'")
        sys.exit(1)
    input_path = sys.argv[1]
    process_pdfs(input_path)
