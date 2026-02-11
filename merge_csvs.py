from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime
from pathlib import Path
import sys
from typing import List, Tuple, Optional, Dict, Any

import pandas as pd


COMMON_DELIMS = [",", ";", "\t", "|"]


def normalize_output_name(name: str) -> str:
    name = name.strip()
    if not name.lower().endswith(".csv"):
        name += ".csv"
    return name


def normalize_report_name(name: str) -> str:
    name = name.strip()
    if not name.lower().endswith(".json"):
        name += ".json"
    return name


def list_csv_files(input_dir: Path, recursive: bool) -> List[Path]:
    pattern = "**/*.csv" if recursive else "*.csv"
    return sorted([p for p in input_dir.glob(pattern) if p.is_file()])


def sniff_delimiter(csv_path: Path, encoding: str) -> Optional[str]:
    try:
        sample = csv_path.read_text(encoding=encoding, errors="replace")[:4096]
        if not sample.strip():
            return None
        dialect = csv.Sniffer().sniff(sample, delimiters="".join(COMMON_DELIMS))
        return dialect.delimiter
    except Exception:
        return None


def looks_like_single_column_header(columns: List[str]) -> bool:
    return len(columns) == 1 and any(d in columns[0] for d in COMMON_DELIMS)


def read_header_with_best_delim(
    csv_path: Path,
    encoding: str,
    preferred_delim: str,
    quotechar: str,
) -> Tuple[List[str], str]:
    # 1) try preferred delimiter
    cols = list(
        pd.read_csv(
            csv_path,
            nrows=0,
            encoding=encoding,
            sep=preferred_delim,
            quotechar=quotechar,
        ).columns
    )
    if not looks_like_single_column_header(cols):
        return cols, preferred_delim

    # 2) fallback: sniff delimiter and retry if different
    sniffed = sniff_delimiter(csv_path, encoding=encoding)
    if sniffed and sniffed != preferred_delim:
        cols2 = list(
            pd.read_csv(
                csv_path,
                nrows=0,
                encoding=encoding,
                sep=sniffed,
                quotechar=quotechar,
            ).columns
        )
        if not looks_like_single_column_header(cols2):
            return cols2, sniffed

    # 3) last resort: try other common delimiters
    for d in COMMON_DELIMS:
        if d == preferred_delim:
            continue
        cols3 = list(
            pd.read_csv(
                csv_path,
                nrows=0,
                encoding=encoding,
                sep=d,
                quotechar=quotechar,
            ).columns
        )
        if not looks_like_single_column_header(cols3):
            return cols3, d

    return cols, preferred_delim


def count_data_rows(csv_path: Path, encoding: str) -> int:
    with csv_path.open("r", encoding=encoding, newline="") as f:
        return max(sum(1 for _ in f) - 1, 0)


def headers_match(candidate: List[str], baseline: List[str], strict_order: bool) -> bool:
    if strict_order:
        return candidate == baseline
    return set(candidate) == set(baseline) and len(candidate) == len(baseline)


def _write_report(path: Path, obj: Dict[str, Any]) -> None:
    path.write_text(json.dumps(
        obj, indent=2, ensure_ascii=False), encoding="utf-8")


def merge_csvs(
    input_dir: Path,
    prefix: str,
    encoding: str,
    remove_duplicates: bool,
    name: Optional[str],
    delimiter: str,
    quotechar: str,
    strict_headers: bool,
    recursive: bool,
    report: Optional[str],
) -> int:
    report_obj: Dict[str, Any] = {
        "timestamp_local": datetime.now().isoformat(timespec="seconds"),
        "input_dir": str(input_dir),
        "recursive": recursive,
        "preferred_delimiter": delimiter,
        "quotechar": quotechar,
        "strict_headers": strict_headers,
        "remove_duplicates": remove_duplicates,
        "output_file": None,
        "total_merged_rows": 0,
        "duplicates_removed": 0,
        "baseline_header": None,
        "files": {"merged": [], "failed": []},
    }

    if not input_dir.exists() or not input_dir.is_dir():
        print(f"Input directory not found: {input_dir}")
        if report:
            _write_report(
                Path.cwd() / normalize_report_name(report), report_obj)
        return 0

    csv_files = list_csv_files(input_dir, recursive=recursive)
    if not csv_files:
        print(f"No CSV files found in: {input_dir}")
        if report:
            _write_report(
                Path.cwd() / normalize_report_name(report), report_obj)
        return 0

    baseline_header: Optional[List[str]] = None
    merged_files: List[Tuple[Path, int]] = []
    failed_files: List[Tuple[Path, str,
                             Optional[List[str]], Optional[List[str]]]] = []
    # Keep per-file delimiter so we can read correctly
    to_merge: List[Tuple[Path, str]] = []

    for f in csv_files:
        try:
            header, used_delim = read_header_with_best_delim(
                f, encoding=encoding, preferred_delim=delimiter, quotechar=quotechar
            )

            if baseline_header is None:
                baseline_header = header
                report_obj["baseline_header"] = baseline_header

            if not headers_match(header, baseline_header, strict_order=strict_headers):
                failed_files.append(
                    (f, "Header mismatch", baseline_header, header))
                continue

            rows = count_data_rows(f, encoding=encoding)
            merged_files.append((f, rows))
            to_merge.append((f, used_delim))

        except Exception as e:
            failed_files.append(
                (f, f"Read error: {type(e).__name__}: {e}", None, None))

    if not to_merge:
        print("Nothing to merge (no files matched a common header).")
        if failed_files:
            print("\nFailed files:")
            for f, reason, base_h, file_h in failed_files:
                if reason == "Header mismatch" and base_h is not None and file_h is not None:
                    print(f"  - {f.relative_to(input_dir)}: {reason}")
                    print(f"      baseline: {base_h}")
                    print(f"      file:     {file_h}")
                else:
                    print(f"  - {f.relative_to(input_dir)}: {reason}")

        report_obj["files"]["merged"] = [
            {"path": str(p), "rows": rows} for p, rows in merged_files]
        report_obj["files"]["failed"] = [
            {"path": str(p), "reason": reason,
             "baseline_header": base_h, "file_header": file_h}
            for p, reason, base_h, file_h in failed_files
        ]
        if report:
            _write_report(
                Path.cwd() / normalize_report_name(report), report_obj)
        return 0

    if name:
        output_file = Path.cwd() / normalize_output_name(name)
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = Path.cwd() / f"{prefix}_{timestamp}.csv"

    # Read + merge. In non-strict mode reorder to baseline order.
    dfs = []
    for p, used_delim in to_merge:
        df = pd.read_csv(p, encoding=encoding,
                         sep=used_delim, quotechar=quotechar)
        if not strict_headers and baseline_header is not None:
            df = df.reindex(columns=baseline_header)
        dfs.append(df)

    merged = pd.concat(dfs, ignore_index=True)

    duplicates_removed = 0
    if remove_duplicates:
        before = len(merged)
        merged = merged.drop_duplicates(keep="first").reset_index(drop=True)
        duplicates_removed = before - len(merged)

    # Output delimiter: use the preferred delimiter for writing
    merged.to_csv(output_file, index=False, encoding=encoding,
                  sep=delimiter, quotechar=quotechar)

    print(
        f"Found {len(csv_files)} CSV file(s) in {input_dir} (recursive={recursive}):")
    for f in csv_files:
        print(f"  - {f.relative_to(input_dir)}")

    print("\nMerged files:")
    for f, rows in merged_files:
        print(f"  - {f.relative_to(input_dir)}: {rows} row(s)")

    if failed_files:
        print("\nFailed files:")
        for f, reason, base_h, file_h in failed_files:
            if reason == "Header mismatch" and base_h is not None and file_h is not None:
                print(f"  - {f.relative_to(input_dir)}: {reason}")
                print(f"      baseline: {base_h}")
                print(f"      file:     {file_h}")
            else:
                print(f"  - {f.relative_to(input_dir)}: {reason}")

    print(f"\nOutput: {output_file.name}")
    print(f"Total merged rows: {len(merged)}")
    if remove_duplicates:
        print(f"Duplicates removed: {duplicates_removed}")

    report_obj["output_file"] = str(output_file)
    report_obj["total_merged_rows"] = int(len(merged))
    report_obj["duplicates_removed"] = int(duplicates_removed)
    report_obj["files"]["merged"] = [
        {"path": str(p), "rows": rows} for p, rows in merged_files]
    report_obj["files"]["failed"] = [
        {"path": str(p), "reason": reason,
         "baseline_header": base_h, "file_header": file_h}
        for p, reason, base_h, file_h in failed_files
    ]

    if report:
        report_path = Path.cwd() / normalize_report_name(report)
        _write_report(report_path, report_obj)
        print(f"Report: {report_path.name}")

    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Merge CSV files with matching headers.")
    parser.add_argument("--input-dir", default="csv")
    parser.add_argument("--prefix", default="merged")
    parser.add_argument("--encoding", default="utf-8")
    parser.add_argument("--delimiter", default=",",
                        help='Preferred output delimiter / first-try input delimiter (default: ",")')
    parser.add_argument("--quotechar", default='"',
                        help='CSV quote character (default: \'"\' )')

    parser.add_argument("--strict-headers", action="store_true",
                        help="Enforce exact header order match.")
    parser.add_argument("--recursive", action="store_true",
                        help="Scan input-dir recursively for CSV files.")
    parser.add_argument("--remove-duplicates", action="store_true",
                        help="Remove duplicate rows after merge.")
    parser.add_argument("--name", default=None,
                        help='Explicit output filename (e.g. "my_merge.csv"). Overwrites if exists.')
    parser.add_argument("--report", default=None,
                        help='Write a JSON report (e.g. "report.json").')

    args = parser.parse_args()

    if len(args.delimiter) != 1:
        print("Error: --delimiter must be a single character.", file=sys.stderr)
        return 1
    if len(args.quotechar) != 1:
        print("Error: --quotechar must be a single character.", file=sys.stderr)
        return 1

    try:
        return merge_csvs(
            input_dir=Path(args.input_dir),
            prefix=args.prefix,
            encoding=args.encoding,
            remove_duplicates=args.remove_duplicates,
            name=args.name,
            delimiter=args.delimiter,
            quotechar=args.quotechar,
            strict_headers=args.strict_headers,
            recursive=args.recursive,
            report=args.report,
        )
    except Exception as e:
        print(f"Unexpected error: {type(e).__name__}: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
