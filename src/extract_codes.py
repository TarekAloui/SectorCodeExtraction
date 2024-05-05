import os
import argparse
import pandas as pd
import re
import threading
from datetime import datetime
from langchain_community.document_loaders import UnstructuredFileLoader
from concurrent.futures import ThreadPoolExecutor


def ensure_dir(file_path):
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)


def log_msg(message, log_file, print_console=True):
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    formatted_message = f"{current_time} - {message}"
    if print_console:
        print(formatted_message)
    if log_file:
        with threading.Lock():
            with open(log_file, "a") as f:
                f.write(formatted_message + "\n")


def extract_text_from_pdf(
    pdf_path, strategy, output_dir, log=False, track_errors=False
):
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
    return {
        "Document Name": os.path.basename(pdf_path),
        "Sector Codes": "; ".join(filtered_codes),
        "Number of Codes": len(filtered_codes),
        "Processing Time": processing_time,
        "Used OCR Only": used_ocr_only,
    }


def process_single_pdf(
    pdf_path, strategy, output_dir, log=False, track_errors=False, redo_empty=False
):
    try:
        result = extract_sector_codes(
            pdf_path, strategy, output_dir, log, track_errors, redo_empty
        )
        if result:
            return result
    except Exception as e:
        log_file_path = os.path.join(output_dir, "logs.txt")
        error_log_path = os.path.join(output_dir, "error_logs.txt")
        log_msg(f"Failed to process {pdf_path}: {e}", log_file_path)
        log_msg(
            f"Failed to process {pdf_path}: {e}", error_log_path, print_console=False
        )
    return None


def process_pdfs(
    input_path,
    output_dir,
    num_threads=1,
    log=False,
    track_errors=False,
    redo_empty=False,
):
    output_csv = os.path.join(output_dir, "output.csv")
    ensure_dir(output_csv)
    log_file = os.path.join(output_dir, "logs.txt") if log else None
    error_log_path = (
        os.path.join(output_dir, "error_logs.txt") if track_errors else None
    )

    if os.path.exists(output_csv):
        df = pd.read_csv(output_csv)
    else:
        df = pd.DataFrame(
            columns=[
                "Document Name",
                "Sector Codes",
                "Number of Codes",
                "Processing Time",
                "Used OCR Only",
            ]
        )

    lock = threading.Lock()
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        if os.path.isdir(input_path):
            files = [
                os.path.join(input_path, f)
                for f in os.listdir(input_path)
                if f.lower().endswith(".pdf")
            ]
        else:
            files = [input_path] if input_path.lower().endswith(".pdf") else []

        for filename in files:
            futures.append(
                executor.submit(
                    process_single_pdf,
                    filename,
                    "fast",
                    output_dir,
                    log,
                    track_errors,
                    redo_empty,
                )
            )

        for future in futures:
            result = future.result()
            if result:
                with lock:
                    df = df[df["Document Name"] != result["Document Name"]]
                    df = pd.concat([df, pd.DataFrame([result])], ignore_index=True)
                    df.to_csv(output_csv, index=False)

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
    parser.add_argument(
        "--num-threads",
        type=int,
        default=1,
        help="Number of threads or cores to use for parallel processing",
    )
    args = parser.parse_args()

    output_dir = os.path.join("output", args.output_folder_name)
    ensure_dir(output_dir)

    log_message = f"Analyzing {args.input_path} with {args.num_threads} threads.\n"
    if args.redo_empty:
        log_message += "Files with no sector codes will be double-checked with OCR-only strategy. This may take longer but could provide more accurate results.\n"
    else:
        log_message += "Files with no sector codes will not be double-checked with OCR-only strategy. You can use --redo-empty for a slower but more accurate result.\n"

    log_file_path = os.path.join(output_dir, "logs.txt") if args.log else None
    log_msg(log_message, log_file_path, print_console=True)

    process_pdfs(
        args.input_path,
        output_dir,
        args.num_threads,
        args.log,
        args.errors,
        args.redo_empty,
    )


if __name__ == "__main__":
    main()
