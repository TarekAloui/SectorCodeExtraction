import os
import sys
import argparse
import pandas as pd
import re
from datetime import datetime
from langchain_community.document_loaders import UnstructuredFileLoader


def ensure_dir(file_path):
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)


def log_msg(message, log_file, print_console=True):
    if print_console:
        print(message)
    if log_file:
        with open(log_file, "a") as f:
            f.write(message + "\n")


def extract_text_from_pdf(
    pdf_path, strategy, output_dir, log=False, track_errors=False
):
    """Extract text from a PDF using LangChain's UnstructuredFileLoader with specified strategy."""
    try:
        loader = UnstructuredFileLoader(
            pdf_path, languages=["fra", "nld"], strategy=strategy
        )
        document = loader.load()
        text = "\n\n".join([doc.page_content for doc in document])
        return text
    except Exception as e:
        if track_errors:
            error_message = f"Error loading PDF {pdf_path}: {str(e)}"
            error_log_path = os.path.join(output_dir, "error_logs.txt")
            log_file_path = os.path.join(output_dir, "logs.txt") if log else None

            log_msg(error_message, error_log_path, print_console=False)
            log_msg(error_message, log_file_path)

        skipped_files_path = os.path.join(output_dir, "skipped_files.txt")
        log_msg(f"{pdf_path}", skipped_files_path, print_console=False)

        return ""


def extract_sector_codes(
    pdf_path,
    initial_strategy,
    output_dir,
    log=False,
    track_errors=False,
    redo_empty=False,
):
    """Extract sector codes from a single PDF with optional strategy switching."""
    log_file_path = os.path.join(output_dir, "logs.txt") if log else None
    log_msg(f"Processing [{initial_strategy}] {pdf_path}", log_file_path)
    text = extract_text_from_pdf(
        pdf_path, initial_strategy, output_dir, log, track_errors
    )

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
                log_msg(
                    f"Detected short or invalid codes, switching to ocr_only strategy for better accuracy. Detected: {all_codes}",
                    log_file_path,
                )
                return extract_sector_codes(
                    pdf_path, "ocr_only", output_dir, log, track_errors, redo_empty
                )
        else:
            filtered_codes.append(code)

    if initial_strategy == "fast" and not filtered_codes:
        if redo_empty:
            log_msg(
                f"Could not detect any valid codes, switching to ocr_only strategy for better accuracy.",
                log_file_path,
            )
            return extract_sector_codes(
                pdf_path, "ocr_only", output_dir, log, track_errors, redo_empty
            )
        else:
            log_msg(
                f"Could not detect any valid codes. You can set --redo-empty to double check with ocr_only strategy.",
                log_file_path,
            )

    processing_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    used_ocr_only = initial_strategy == "ocr_only"
    return (
        "; ".join(filtered_codes),
        len(filtered_codes),
        processing_time,
        used_ocr_only,
    )


def process_pdfs(
    input_path, output_dir, log=False, track_errors=False, redo_empty=False
):
    """Process each PDF in a directory or a single PDF file and save results gradually."""
    output_csv = os.path.join(output_dir, "output.csv")
    ensure_dir(output_csv)
    log_file = os.path.join(output_dir, "logs.txt") if log else None
    error_log_path = (
        os.path.join(output_dir, "error_logs.txt") if track_errors else None
    )

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
            result = extract_sector_codes(
                pdf_path, "fast", output_dir, log, track_errors, redo_empty
            )
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
            log_msg(f"Failed to process {pdf_path}: {e}", log_file)
            log_msg(
                f"Failed to process {pdf_path}: {e}",
                error_log_path,
                print_console=False,
            )

    log_msg(f"CSV file has been updated: {output_csv}", log_file)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_path", help="Path to the folder or PDF file to process")
    parser.add_argument("output_folder_name", help="Name of the output folder")
    parser.add_argument(
        "--redo-empty",
        action="store_true",
        help="Redoes processing for files with no sector codes with ocr_only strategy to double check",
    )
    parser.add_argument(
        "--log",
        action="store_true",
        help="Enable logging of general messages to logs.txt",
    )
    parser.add_argument(
        "--errors",
        action="store_true",
        help="Enable logging of error messages to error_logs.txt",
    )
    args = parser.parse_args()

    output_dir = os.path.join("output", args.output_folder_name)
    ensure_dir(output_dir)  # Ensure the output directory exists

    # Welcome message

    log_message = f"Analyzing {args.input_path}.\n\n"
    if args.redo_empty:
        log_message += """
        Files with no sector codes will be double-checked with OCR-only strategy.
        This may take longer but could provide more accurate results.\n\n
        """
    else:
        log_message += """
        Files with no sector codes will be not be double-checked with OCR-only strategy.
        This may be faster but it might not give the most accurate results
        You can use --redo-empty for a slower but more accurate result.\n\n
        """

    log_file_path = os.path.join(output_dir, "logs.txt") if args.log else None

    log_msg(log_message, log_file_path, print_console=True)

    process_pdfs(args.input_path, output_dir, args.log, args.errors, args.redo_empty)


if __name__ == "__main__":
    main()
