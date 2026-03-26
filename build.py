#!/usr/bin/env python3

import pandas as pd
import numpy as np
import json
import sys
import argparse
from datetime import datetime
from pathlib import Path

# =========================
# 📁 PATHS
# =========================

BASE        = Path(__file__).parent
TEMPLATE    = BASE / "HNImanshu_template.html"
OUTPUT      = BASE / "public" / "index.html"

N500_CSV    = BASE / "nifty500_valuation.csv"
SC250_CSV   = BASE / "niftysmallcap250_valuation.csv"
MC250_CSV   = BASE / "niftymicrocap250_valuation.csv"

PLACEHOLDER = "%%DATASETS_PLACEHOLDER%%"

N500_NAME_COL  = "COMPANY_NAME_x"
SC250_NAME_COL = "COMPANY_NAME"
MC250_NAME_COL = "COMPANY_NAME"

# =========================
# 📊 OUTPUT COLS
# =========================

KEY_COLS = [
    "SYMBOL","INDUSTRY","CMP","PE","BOOK_VALUE",
    "ROE_PCT","ROCE_PCT","DIV_YIELD_PCT",
    "UPSIDE_PCT","MARGIN_OF_SAFETY_PCT","COMPOSITE_FAIR_VALUE",
    "VALUATION_SCORE","VALUATION_SCORE_PCT","VALUATION_RANK",
    "FINAL_SCORE","FINAL_RANK","GRADE","FV_GRADE","SCREENER_URL",
    "MARKET_CAP_CR","PL_OPM_PCT","NET_MARGIN_CALC_PCT",
    "REVENUE_CAGR_3YR_PCT","PAT_CAGR_3YR_PCT",
    "SH_PROMOTER_PCT_LATEST","NET_DEBT_CR","INTEREST_COVERAGE",
    "PIOTROSKI_SCORE"
]

# =========================
# 🔥 SCORING ENGINE
# =========================

def score_metric(value, thresholds, reverse=False):
    if pd.isna(value):
        return 0

    if reverse:
        if value < thresholds[0]: return 1.4
        elif value < thresholds[1]: return 1.3
        elif value < thresholds[2]: return 1.2
        elif value < thresholds[3]: return 1.1
        else: return 0
    else:
        if value > thresholds[3]: return 1.4
        elif value > thresholds[2]: return 1.3
        elif value > thresholds[1]: return 1.2
        elif value > thresholds[0]: return 1.1
        else: return 0


# =========================
# 🧠 IMPROVED PIOTROSKI
# =========================

def calc_piotroski(df: pd.DataFrame) -> pd.Series:

    def calc_row(x):

        score = 0

        # -----------------------
        # PROFITABILITY
        # -----------------------
        score += score_metric(x.get("ROE_PCT"), [5,10,15,20])
        score += score_metric(x.get("ROCE_PCT"), [5,10,15,20])
        score += score_metric(x.get("NET_MARGIN_CALC_PCT"), [2,5,10,15])
        score += score_metric(x.get("PL_OPM_PCT"), [5,10,15,20])

        # -----------------------
        # GROWTH
        # -----------------------
        score += score_metric(x.get("REVENUE_CAGR_3YR_PCT"), [3,5,10,15])
        score += score_metric(x.get("PAT_CAGR_3YR_PCT"), [3,5,10,15])

        # -----------------------
        # SAFETY
        # -----------------------
        score += score_metric(x.get("NET_DEBT_CR"), [1000,500,0,-100], reverse=True)
        score += score_metric(x.get("INTEREST_COVERAGE"), [1,2,4,8])

        # -----------------------
        # PROMOTER
        # -----------------------
        score += score_metric(x.get("SH_PROMOTER_PCT_LATEST"), [30,40,50,60])

        return round(score, 2)

    return df.apply(calc_row, axis=1)


# =========================
# 📂 CSV LOADER
# =========================

def load_csv(path: Path, name_col: str, label: str, optional=False):
    print(f"  [{label}]  {path.name}")

    if not path.exists():
        if optional:
            print("    SKIPPED (file not found)")
            return None
        print(f"ERROR: File not found -> {path}")
        sys.exit(1)

    df = pd.read_csv(path, low_memory=False)
    print(f"    Loaded: {len(df)} rows")

    if name_col in df.columns:
        df = df.rename(columns={name_col: "COMPANY_NAME"})
    elif "COMPANY_NAME" not in df.columns:
        print("ERROR: Company name column missing")
        sys.exit(1)

    df = df.copy()

    # 🔥 Calculate score
    df["PIOTROSKI_SCORE"] = calc_piotroski(df)

    # Debug stats (important)
    print(
        "    Score stats:",
        "min=", df["PIOTROSKI_SCORE"].min(),
        "max=", df["PIOTROSKI_SCORE"].max(),
        "avg=", round(df["PIOTROSKI_SCORE"].mean(), 2)
    )

    needed = ["COMPANY_NAME"] + KEY_COLS
    df = df[[c for c in needed if c in df.columns]]

    df = df.replace([np.inf, -np.inf], None)
    df = df.where(pd.notnull(df), None)

    df["PIOTROSKI_SCORE"] = df["PIOTROSKI_SCORE"].apply(
        lambda x: float(x) if pd.notna(x) else None
    )

    return df.to_dict(orient="records")


# =========================
# 🏗 BUILD
# =========================

def build(deploy=False):
    print(f"\n{'='*52}")
    print("  HNImanshu — Build")
    print(f"{'='*52}\n")

    if not TEMPLATE.exists():
        print("ERROR: Template missing")
        sys.exit(1)

    template = TEMPLATE.read_text(encoding="utf-8")

    print("Loading CSVs...\n")

    data_n500  = load_csv(N500_CSV,  N500_NAME_COL,  "Nifty 500")
    data_sc250 = load_csv(SC250_CSV, SC250_NAME_COL, "Smallcap 250", optional=True)
    data_mc250 = load_csv(MC250_CSV, MC250_NAME_COL, "Microcap 250")

    if data_sc250 is None:
        data_sc250 = []

    datasets_js = (
        "const DATASETS = {\n"
        f"n500:{json.dumps(data_n500, separators=(',', ':'))},\n"
        f"sc250:{json.dumps(data_sc250, separators=(',', ':'))},\n"
        f"mc250:{json.dumps(data_mc250, separators=(',', ':'))}\n"
        "};"
    )

    html = template.replace(PLACEHOLDER, datasets_js)

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(html, encoding="utf-8")

    print("\n✅ Build Complete")
    print(f"Output: {OUTPUT}")

    if deploy:
        import subprocess
        subprocess.run(["firebase", "deploy"])


# =========================
# ▶ RUN
# =========================

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--deploy", action="store_true")
    args = ap.parse_args()

    build(args.deploy)
