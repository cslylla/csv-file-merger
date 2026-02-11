# CSV File Merger

*A flexible command-line tool to merge CSV files with matching headers, built with Python and Pandas as part of the **Codecademy AI Maker Bootcamp**.*

## What It Does

- Header validation (strict or non-strict)
- Optional duplicate removal
- Recursive folder scanning
- Custom delimiter and quote character
- JSON report generation
- Timestamped or custom output filenames

## Features

- Merge any number of CSV files
- Strict or non-strict header validation
- Optional duplicate removal (`--remove-duplicates`)
- Recursive scanning of subfolders (`--recursive`)
- Custom delimiter support (`--delimiter`)
- `JSON` merge report (`--report`)
- Timestamped output by default
- Fully offline, CLI-based

## Installation

### 1. Clone the repository

```bash
git clone <repository-url>
cd csv-file-merger
```

### 2. (Optional) Activate virtual environment

Windows:

```bash
venv\Scripts\activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

## Usage

Place input files inside the `csv/` folder.

Basic merge:

```bash
py merge_csvs.py
```

**Common Options**
Merge recursively:

```bash
py merge_csvs.py --recursive
```

Remove duplicates:

```bash
py merge_csvs.py --remove-duplicates
```

Enforce strict header order:

```bash
py merge_csvs.py --strict-headers
```

Custom delimiter:

```bash
py merge_csvs.py --delimiter ";"
```

Generate JSON report:

```bash
py merge_csvs.py --report report.json
```

Custom output filename:

```bash
py merge_csvs.py --name my_output.csv
```

### Header Validation Rules

Default behavior:

- Column names must match
- Order does NOT matter

With `--strict-headers`:

- Column names must match
- Order MUST match exactly
Files with mismatched headers are excluded and listed in the summary.

### Output

If no name is provided:

`merged_YYYYMMDD_HHMMSS.csv`

If `--name` is used:
`my_output.csv`
A detailed terminal summary is printed after each run.
If `--report` is provided, a `JSON` report is generated containing:

- Merged files
- Failed files
- Baseline header
- Total rows
- Duplicate count (if enabled)

## Tech Stack

| Technology   | Purpose                 |
| ------------ | ----------------------- |
| Python 3.12+ | Core logic              |
| Pandas       | CSV parsing and merging |
| JSON         | Report generation       |
| argparse     | CLI interface           |

## Project Context

- Built as a demo-ready CLI data utility focused on:
- Clean architecture
- Robust validation
- Flexible input handling
- Production-style reporting
