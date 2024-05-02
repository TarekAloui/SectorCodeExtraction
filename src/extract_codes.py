import os
import sys
import pandas as pd
import re
from datetime import datetime
from langchain_community.document_loaders import UnstructuredFileLoader


def ensure_dir(file_path):
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)


def extract_text_from_pdf(pdf_path, strategy, output_dir):
    """Extract text from a PDF using LangChain's UnstructuredFileLoader with specified strategy."""
    try:
        loader = UnstructuredFileLoader(
            pdf_path, languages=["fra", "nld"], strategy=strategy
        )
        document = loader.load()
        text = "\n\n".join([doc.page_content for doc in document])
        return text
    except Exception as e:
        error_message = f"Error loading PDF {pdf_path}: {str(e)}"
        print(error_message)
        error_log_path = os.path.join(output_dir, "error_logs.txt")
        with open(error_log_path, "a") as error_file:
            error_file.write(f"{error_message}\n")

        skipped_files_path = os.path.join(output_dir, "skipped_files.txt")
        with open(skipped_files_path, "a") as error_file:
            error_file.write(f"{pdf_path}\n")

        return ""


def extract_sector_codes(pdf_path, initial_strategy="fast", output_dir="output"):
    """Extract sector codes from a single PDF with optional strategy switching."""
    print(f"Processing [{initial_strategy}] {pdf_path}")
    text = extract_text_from_pdf(pdf_path, initial_strategy, output_dir)

    pattern = r"(paritaire[s]?[^\d]*[\d\s.]+)"
    matches = re.findall(pattern, text, re.IGNORECASE)
    all_codes = [
        re.sub(
            r"[\s]+", ";", re.sub(r"[^\d.]+|\.{2,}", " ", match.strip()).strip()
        ).split(";")
        for match in matches
    ]
    all_codes = [item for sublist in all_codes for item in sublist]

    filtered_codes = []
    for code in all_codes:
        number_part = code.split(".")[0]
        if len(number_part) < 3 or not code.replace(".", "", 1).isdigit():
            if initial_strategy == "fast":
                print(
                    f"Detected short or invalid codes, switching to ocr_only strategy for better accuracy. Detected: {all_codes}"
                )
                return extract_sector_codes(pdf_path, "ocr_only", output_dir)
        else:
            filtered_codes.append(code)

    if initial_strategy == "fast" and not filtered_codes:
        print(
            f"Could not detect any valid codes, switching to ocr_only strategy for better accuracy."
        )
        return extract_sector_codes(pdf_path, "ocr_only", output_dir)

    processing_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    used_ocr_only = initial_strategy == "ocr_only"
    return (
        "; ".join(filtered_codes),
        len(filtered_codes),
        processing_time,
        used_ocr_only,
    )


def process_pdfs(input_path, output_dir):
    """Process each PDF in a directory or a single PDF file and save results gradually."""
    output_csv = os.path.join(output_dir, "output.csv")
    ensure_dir(output_csv)

    df = (
        pd.read_csv(output_csv)
        if os.path.exists(output_csv)
        else pd.DataFrame(
            columns=[
                "Document Name",
                "Sector Codes",
                "Number of Codes",
                "Processing Time",
                "Used OCR Only",
            ]
        )
    )

    if os.path.isdir(input_path):
        files = [f for f in os.listdir(input_path) if f.lower().endswith(".pdf")]
    else:
        files = (
            [os.path.basename(input_path)]
            if input_path.lower().endswith(".pdf")
            else []
        )

    for filename in files:
        pdf_path = (
            os.path.join(input_path, filename)
            if os.path.isdir(input_path)
            else input_path
        )
        try:
            result = extract_sector_codes(pdf_path, "fast", output_dir)
            if result:
                codes, count, processing_time, used_ocr_only = result
                df = (
                    df[df["Document Name"] != filename]
                    if (used_ocr_only or filename in df["Document Name"].tolist())
                    else df
                )
                df = pd.concat(
                    [
                        df,
                        pd.DataFrame(
                            [
                                {
                                    "Document Name": filename,
                                    "Sector Codes": codes,
                                    "Number of Codes": count,
                                    "Processing Time": processing_time,
                                    "Used OCR Only": used_ocr_only,
                                }
                            ]
                        ),
                    ],
                    ignore_index=True,
                )
                df.to_csv(output_csv, index=False)
        except Exception as e:
            error_file = os.path.join(output_dir, "processing_errors.txt")
            with open(error_file, "a") as ef:
                ef.write(f"Failed to process {pdf_path}: {e}\n")

    print("CSV file has been updated:", output_csv)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(
            "Usage: python extract_codes.py 'folder_path_or_pdf_path' 'output_folder_name'"
        )
        sys.exit(1)
    input_path = sys.argv[1]
    output_folder_name = sys.argv[2]
    output_dir = os.path.join("output", output_folder_name)
    ensure_dir(output_dir)  # Ensure the output directory exists
    process_pdfs(input_path, output_dir)
