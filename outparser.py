import json
import os
import sys
import re
import argparse
import extract_msg
import traceback
from datetime import datetime
from pathlib import Path
import concurrent.futures


def summarize_addresses(addresses, limit):
    """Summarize a list of addresses according to the given limit."""
    if not addresses:
        return ""

    addr_list = [clean_value(a.strip()) for a in addresses.split(";")]
    if limit == 0 or len(addr_list) <= limit:
        return ", ".join(addr_list)
    else:
        return ", ".join(addr_list[:limit]) + f", and {len(addr_list) - limit} others"

def safe_filename(name: str) -> str:
    # remove null bytes and non-printable chars
    name = re.sub(r"[\x00-\x1F\x7F]", "", name)
    # replace forbidden filesystem characters
    name = re.sub(r"[\\\\/:*?\"<>|]", "_", name)
    if not name:
        return "unnamed_attachment"
    return name

def clean_value(value):
    if value is None:
        return ""
    if isinstance(value, bytes):
        value = value.replace(b"\x00", b"").decode("cp1252", errors="replace")
    if isinstance(value, str):
        return value.replace("\x00", "")
    return str(value)

def strip_html_tags(text: str) -> str:
    if not text:
        return text

    return re.sub(r'<[^>]+>', '', text)

def process_msg_file(filepath, attachments_dir, from_limit, to_limit, strip_tags):
    """Parse a single .msg file into a JSON-compatible dict."""
    try:
        msg = extract_msg.Message(str(filepath))
        msg_date = msg.date or ""
        if isinstance(msg_date, datetime):
            msg_date = msg_date.isoformat()
        msg_from = summarize_addresses(msg.sender, from_limit)
        msg_to = summarize_addresses(msg.to, to_limit)
        msg_body = clean_value(msg.htmlBody) or clean_value(msg.body) or ""
        if strip_tags and msg_body:
            msg_body = strip_html_tags(msg_body)

        # Save attachments
        attachment_names = []
        for att in msg.attachments:
            filename = att.longFilename or att.shortFilename
            if not filename:
                continue
            filename = safe_filename(filename)

            unique_name = f"{Path(filepath).stem}_{filename}"
            save_path = Path(attachments_dir) / unique_name
            with open(save_path, 'wb') as f:
                f.write(att.data)
            attachment_names.append(clean_value(unique_name))

        return {
            "Date": msg_date,
            "From": msg_from,
            "To": msg_to,
            "Message": msg_body,
            "Attachments": attachment_names,
            "MessageID": clean_value(getattr(msg, "messageId", "UNKNOWN")),
            "SourceFile": str(filepath),
        }, None

    except Exception as e:
        traceback.print_exc()
        return None, f"Error parsing {filepath}: {e}"


def scan_directory(directory, recursive):
    """Yield .msg file paths."""
    if recursive:
        for root, _, files in os.walk(directory):
            for f in files:
                if f.lower().endswith(".msg"):
                    yield Path(root) / f
    else:
        for f in os.listdir(directory):
            if f.lower().endswith(".msg"):
                yield Path(directory) / f

def worker(filepath, attachments_dir, from_limit, to_limit, strip_tags):
    return process_msg_file(filepath, attachments_dir, from_limit, to_limit, strip_tags)

def main():
    parser = argparse.ArgumentParser(description="Parse .msg files into JSON.")
    parser.add_argument("directory", help="Directory to scan for .msg files")
    parser.add_argument("-r", "--recursive", action="store_true", help="Recursively scan subdirectories")
    parser.add_argument("-a", "--attachments-dir", default="Attachments", help="Directory to store attachments")
    parser.add_argument("-o", "--output", default="parsed_messages.json", help="Output JSON file")
    parser.add_argument("-f", "--from-limit", type=int, default=3, help="Max number of 'From' addresses to show (0 for unlimited)")
    parser.add_argument("-t", "--to-limit", type=int, default=3, help="Max number of 'To' addresses to show (0 for unlimited)")
    parser.add_argument("-w", "--workers", type=int, default=os.cpu_count(), help="Number of parallel workers")
    parser.add_argument("-x", "--strip-tags", action='store_true', help="Strip HTML tags from message bodies")
    parser.add_argument("-s", "--sort-date", default='asc', help="Sort by date after parsing (asc/desc/none)")
    args = parser.parse_args()

    Path(args.attachments_dir).mkdir(parents=True, exist_ok=True)

    errors = []
    files = list(scan_directory(args.directory, args.recursive))
    total = len(files)
    print(f'Input Dir: {args.directory}\nFiles to be parsed: {total}\nOutput File: {args.output}\nAttachs Dir: {args.attachments_dir}')
    print(f'Max Workers: {args.workers}')
    confirm = input(f'Are you sure? (y/n): ')
    if confirm.lower() != 'y':
        exit(1)

    with concurrent.futures.ProcessPoolExecutor(max_workers=args.workers) as executor, \
        open(args.output, "w", encoding="utf-8") as out:

        futures = {executor.submit(worker, f, args.attachments_dir, args.from_limit, args.to_limit, args.strip_tags): f for f in files}
        for i, fut in enumerate(concurrent.futures.as_completed(futures), 1):
            record, error = fut.result()
            print(f"[{i}/{total}] Processing: {futures[fut]}")
            if record:
                out.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
            if error:
                errors.append(error)

    print(f"\nProcessed {total-len(errors)} files successfully.")
    if errors:
        print(f"Encountered {len(errors)} errors:")
        for e in errors:
            print(" -", e)

    # Read file back into memory for optional sorting
    if args.sort_date != "none":
        print('Sorting records by date...')

        with open(args.output, "r", encoding="utf-8") as f:
            records = [json.loads(line) for line in f]
        reverse = args.sort_date == "desc"
        records.sort(key=lambda r: r.get("Date", ""), reverse=reverse)
        with open(args.output, "w", encoding="utf-8") as f:
            for r in records:
                f.write(json.dumps(r, ensure_ascii=False, default=str) + "\n")
        print('Sorting done.')

if __name__ == "__main__":
    main()

