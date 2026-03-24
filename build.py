#!/usr/bin/env python3
"""
HNImanshu — Build Script
=========================
Reads CSVs from the SAME folder as this script,
injects all data into HNImanshu_template.html,
and writes public/index.html — one self-contained file.

Your folder layout on the server:
  ~/HNImanshu/
  ├── nifty500_valuation.csv           ← from nifty500_val.py
  ├── niftymicrocap250_valuation.csv   ← from microcap250_val.py
  ├── nifty500_val.py
  ├── microcap250_val.py
  ├── build.py                         ← THIS FILE
  ├── HNImanshu_template.html
  ├── firebase.json
  ├── .firebaserc
  ├── cron.sh
  ├── logs/
  └── public/
      └── index.html                   ← OUTPUT (auto-generated)

Run:
  python3 build.py           # build only
  python3 build.py --deploy  # build + firebase deploy
"""

import pandas as pd
import json
import sys
import argparse
import subprocess
from datetime import datetime
from pathlib import Path

import requests

urls = {
    "nifty500_valuation.csv": "https://csvhsv.s3.ap-south-1.amazonaws.com/web/nifty500_valuation.csv",
    "niftymicrocap250_valuation.csv": "https://csvhsv.s3.ap-south-1.amazonaws.com/web/niftymicrocap250_valuation.csv"
}

#for filename, url in urls.items():
#    r = requests.get(url)
#    with open(filename, "wb") as f:
#        f.write(r.content)

BASE        = Path(__file__).parent
TEMPLATE    = BASE / "HNImanshu_template.html"
OUTPUT      = BASE / "public" / "index.html"
N500_CSV    = BASE / "nifty500_valuation.csv"
MC250_CSV   = BASE / "niftymicrocap250_valuation.csv"
PLACEHOLDER = "%%DATASETS_PLACEHOLDER%%"

N500_NAME_COL  = "COMPANY_NAME_x"
MC250_NAME_COL = "COMPANY_NAME"

KEY_COLS = [
    "SYMBOL", "INDUSTRY", "CMP", "PE", "BOOK_VALUE",
    "ROE_PCT", "ROCE_PCT", "DIV_YIELD_PCT",
    "UPSIDE_PCT", "MARGIN_OF_SAFETY_PCT", "COMPOSITE_FAIR_VALUE",
    "VALUATION_SCORE", "VALUATION_SCORE_PCT", "VALUATION_RANK",
    "FINAL_SCORE", "FINAL_RANK", "GRADE", "FV_GRADE", "SCREENER_URL",
    "MARKET_CAP_CR", "PL_OPM_PCT", "NET_MARGIN_CALC_PCT",
    "REVENUE_CAGR_3YR_PCT", "PAT_CAGR_3YR_PCT",
    "SH_PROMOTER_PCT_LATEST", "NET_DEBT_CR", "INTEREST_COVERAGE",
]


def load_csv(path: Path, name_col: str, label: str) -> list:
    print(f"  [{label}]  {path.name}")

    if not path.exists():
        print(f"\n  ERROR: File not found → {path}")
        print(f"  Run the pipeline first to generate this file.")
        sys.exit(1)

    df = pd.read_csv(path, low_memory=False)
    print(f"    Loaded   : {len(df)} rows × {len(df.columns)} cols")

    # Normalise company name column
    if name_col in df.columns:
        df = df.rename(columns={name_col: "COMPANY_NAME"})
    elif "COMPANY_NAME" not in df.columns:
        candidates = [c for c in df.columns
                      if "COMPANY" in c.upper() or "NAME" in c.upper()]
        if candidates:
            df = df.rename(columns={candidates[0]: "COMPANY_NAME"})
        else:
            print(f"  ERROR: cannot find company name column in {path.name}")
            sys.exit(1)

    needed  = ["COMPANY_NAME"] + KEY_COLS
    missing = [c for c in needed if c not in df.columns]
    if missing:
        print(f"  ERROR: missing columns: {missing}")
        sys.exit(1)

    df = df[needed].copy()
    df = df.replace([float("inf"), float("-inf")], None)
    df = df.where(pd.notnull(df), None)

    records = df.to_dict(orient="records")
    print(f"    Extracted: {len(records)} stocks  ✓")
    return records


def build(deploy: bool = False):
    print(f"\n{'═'*52}")
    print(f"  HNImanshu — Build")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'═'*52}\n")

    if not TEMPLATE.exists():
        print(f"ERROR: Template not found → {TEMPLATE}")
        print("Make sure HNImanshu_template.html is in the same folder as build.py")
        sys.exit(1)

    template = TEMPLATE.read_text(encoding="utf-8")

    if PLACEHOLDER not in template:
        print(f"ERROR: Template missing {PLACEHOLDER} marker")
        sys.exit(1)

    print("  Loading CSVs...")
    data_n500  = load_csv(N500_CSV,  N500_NAME_COL,  "Nifty 500")
    data_mc250 = load_csv(MC250_CSV, MC250_NAME_COL, "Microcap 250")

    print("\n  Building HTML...")
    from datetime import timezone, timedelta
    IST = timezone(timedelta(hours=5, minutes=30))
    now_ist = datetime.now(IST)
    last_updated_str = now_ist.strftime("%d-%b-%y\n%I:%M:%S %p")
    datasets_js = (
        "const DATASETS = {\n"
        f"  n500:  {json.dumps(data_n500,  ensure_ascii=False, separators=(',', ':'))},\n"
        f"  mc250: {json.dumps(data_mc250, ensure_ascii=False, separators=(',', ':'))}\n"
        "};"
    )
    html = template.replace(PLACEHOLDER, datasets_js)
    html = html.replace("%%LAST_UPDATED_PLACEHOLDER%%", last_updated_str)

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(html, encoding="utf-8")

    size_kb = OUTPUT.stat().st_size // 1024
    print(f"\n{'═'*52}")
    print(f"  ✓ Build complete!")
    print(f"{'═'*52}")
    print(f"  Output  : public/index.html  ({size_kb} KB)")
    print(f"  N500    : {len(data_n500)} stocks")
    print(f"  MC250   : {len(data_mc250)} stocks")

#    deploy = True
    if deploy:
        print(f"\n  Deploying to Firebase...")

    else:
        print(f"\n  To deploy: firebase deploy")
        print(f"  Or:        python3 build.py --deploy\n")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="HNImanshu — build index.html from CSVs")
    ap.add_argument("--deploy", action="store_true",
                    help="Deploy to Firebase after building")
    build(ap.parse_args().deploy)

