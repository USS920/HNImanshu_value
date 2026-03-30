#!/usr/bin/env python3
"""
build.py — HNImanshu Stock Screener Build Script
=================================================
Score Architecture:
  PIOTROSKI_SCORE  (0–9)     Fresh YoY-based 9-signal F-Score computed from
                              the two most recent annual columns in the CSV.
  QUALITY_SCORE    (0–13.4)  Absolute-level quality addon (profitability /
                              growth / safety) with YoY direction bonuses.
  COMBINED_SCORE   (0–22.4)  Sum of both.

  FINAL_SCORE      (0–10)    MASTER = (VAL_NORM×0.40) + (PIOT_NORM×0.35) + (QUAL_NORM×0.25)
                              All three normalised to 0–10 before weighting.
  FINAL_RANK                 Rank by FINAL_SCORE descending.

F_SCORE from CSV is retained as a reference column only.

News:
  Reads multi_stock_news.csv (columns: stockname, datetime, news, link)
  and injects per-symbol news into the HTML as NEWS_DATA JS object.
  Headlines are hyperlinked to their source URLs.
"""

import pandas as pd
import numpy as np
import json
import sys
import re
import argparse
from pathlib import Path
from datetime import datetime
import zoneinfo

# =========================
# 📁 PATHS
# =========================

BASE        = Path(__file__).parent
TEMPLATE    = BASE / "HNImanshu_template.html"
OUTPUT      = BASE / "public" / "index.html"

N500_CSV    = BASE / "nifty500_valuation.csv"
SC250_CSV   = BASE / "niftysmallcap500_valuation.csv"
MC250_CSV   = BASE / "niftymicrocap250_valuation.csv"
NEWS_CSV    = BASE / "multi_stock_news.csv"

PLACEHOLDER      = "%%DATASETS_PLACEHOLDER%%"
NEWS_PLACEHOLDER = "// ── NEWS DATA ──\nvar NEWS_DATA = {\n  '__default__': []\n};"

N500_NAME_COL  = "COMPANY_NAME"
SC250_NAME_COL = "COMPANY_NAME"
MC250_NAME_COL = "COMPANY_NAME"

# =========================
# 📊 OUTPUT COLS
# =========================

KEY_COLS = [
    "SYMBOL", "INDUSTRY", "CMP", "PE", "BOOK_VALUE",
    "ROE_PCT", "ROCE_PCT", "DIV_YIELD_PCT",
    "UPSIDE_PCT", "MARGIN_OF_SAFETY_PCT", "COMPOSITE_FAIR_VALUE",
    "VALUATION_SCORE", "VALUATION_SCORE_PCT", "VALUATION_RANK",
    "FINAL_SCORE", "FINAL_RANK", "GRADE", "FV_GRADE", "SCREENER_URL",
    "MARKET_CAP_CR", "PL_OPM_PCT", "NET_MARGIN_CALC_PCT",
    "REVENUE_CAGR_3YR_PCT", "PAT_CAGR_3YR_PCT",
    "SH_PROMOTER_PCT_LATEST", "NET_DEBT_CR", "INTEREST_COVERAGE",
    "CF_OPERATING_CR", "NET_DEBT_TO_EBITDA",
    "F_SCORE",           # 0–9    · CSV pre-computed Piotroski (reference only)
    "PIOTROSKI_SCORE",   # 0–9    · Fresh YoY-based F-Score (primary)
    "QUALITY_SCORE",     # 0–13.4 · Absolute-level quality addon
    "COMBINED_SCORE",    # 0–22.4 · PIOTROSKI + QUALITY
    "LOW_PROMOTER_FLAG",
]

# =========================
# 🔥 SCORING ENGINE
# =========================

def score_metric(value, thresholds, reverse=False, max_pts=1.4):
    """
    4-tier linear scoring engine.
    thresholds: [t1, t2, t3, t4] always in ASCENDING order.
    Normal  (reverse=False): higher = better.
    Reverse (reverse=True):  lower  = better.
    """
    if pd.isna(value):
        return 0
    pts = [1.1, 1.2, 1.3, max_pts]
    if reverse:
        if   value < thresholds[0]: return pts[3]
        elif value < thresholds[1]: return pts[2]
        elif value < thresholds[2]: return pts[1]
        elif value < thresholds[3]: return pts[0]
        else: return 0
    else:
        if   value > thresholds[3]: return pts[3]
        elif value > thresholds[2]: return pts[2]
        elif value > thresholds[1]: return pts[1]
        elif value > thresholds[0]: return pts[0]
        else: return 0


# =========================
# 🔍 PRIOR-YEAR RESOLVER
# =========================

def _resolve_prior_cols(df: pd.DataFrame) -> dict:
    """
    Dynamically find the two most recent annual columns for each
    Piotroski base metric. Supports MAR/DEC/SEP/JUN fiscal years.
    """
    def _annual_cols(prefix):
        pat = re.compile(rf"^{re.escape(prefix)}_(MAR|DEC|SEP|JUN)(\d{{4}})$")
        hits = []
        for c in df.columns:
            m = pat.match(c)
            if m:
                month, year = m.group(1), int(m.group(2))
                month_order = {"MAR": 3, "JUN": 6, "SEP": 9, "DEC": 12}
                hits.append((year * 100 + month_order.get(month, 0), c))
        hits.sort(key=lambda x: -x[0])
        return [c for _, c in hits]

    families = {
        "pat":  _annual_cols("A_PL_PAT_CR"),
        "ta":   _annual_cols("B_BS_TOTAL_ASSETS_CR"),
        "cfo":  _annual_cols("C_CF_OPERATING_CR"),
        "opm":  _annual_cols("A_PL_OPM_PCT"),
        "rev":  _annual_cols("A_PL_REVENUE_CR"),
        "int_": _annual_cols("A_PL_INTEREST_CR"),
        "res":  _annual_cols("B_BS_RESERVES_CR"),
        "eps":  _annual_cols("A_PL_EPS_BASIC"),
    }

    result = {k: {"cur": pd.Series(np.nan, index=df.index),
                  "pri": pd.Series(np.nan, index=df.index)}
              for k in families}

    for key, cols in families.items():
        if len(cols) >= 1:
            result[key]["cur"] = pd.to_numeric(df[cols[0]], errors="coerce")
        if len(cols) >= 2:
            result[key]["pri"] = pd.to_numeric(df[cols[1]], errors="coerce")

    return result


# =========================
# 🧠 PIOTROSKI F-SCORE  (0–9)
# =========================

def calc_piotroski_fscore(df: pd.DataFrame) -> pd.Series:
    """
    Genuine 9-point Piotroski F-Score from raw CSV columns.

    Profitability:  F1 ROA>0  F2 CFO>0  F3 ΔROA>0  F4 CFO>Net Income
    Leverage:       F5 Leverage↓  F6 Equity ratio↑  F7 No dilution
    Efficiency:     F8 Gross margin↑  F9 Asset turnover↑
    """
    def s(col):
        if col in df.columns:
            return pd.to_numeric(df[col], errors="coerce")
        return pd.Series(np.nan, index=df.index)

    def nz(series):
        return series.replace(0, np.nan)

    pat_c = s("PL_PAT_CR")
    ta_c  = nz(s("BS_TOTAL_ASSETS_CR"))
    cfo_c = s("CF_OPERATING_CR")
    opm_c = s("PL_OPM_PCT")
    rev_c = s("PL_REVENUE_CR")
    int_c = s("PL_INTEREST_CR")
    res_c = s("BS_RESERVES_CR")
    eps_c = nz(s("PL_EPS_BASIC"))

    fam   = _resolve_prior_cols(df)
    pat_p = fam["pat"]["pri"]
    ta_p  = nz(fam["ta"]["pri"])
    opm_p = fam["opm"]["pri"]
    rev_p = fam["rev"]["pri"]
    int_p = fam["int_"]["pri"]
    res_p = fam["res"]["pri"]
    eps_p = nz(fam["eps"]["pri"])

    roa_c = pat_c / ta_c
    roa_p = pat_p / ta_p
    lev_c = nz(int_c) / ta_c
    lev_p = nz(int_p) / ta_p
    eq_c  = res_c / ta_c
    eq_p  = res_p / ta_p
    at_c  = rev_c / ta_c
    at_p  = rev_p / ta_p
    sh_c  = pat_c / eps_c
    sh_p  = pat_p / eps_p

    def b(cond):
        return cond.fillna(False).astype(int)

    signals = pd.DataFrame({
        "f1": b(roa_c > 0),
        "f2": b(cfo_c > 0),
        "f3": b(roa_c > roa_p),
        "f4": b(cfo_c > pat_c),
        "f5": b(lev_c < lev_p),
        "f6": b(eq_c  > eq_p),
        "f7": b(sh_c  <= sh_p * 1.02),
        "f8": b(opm_c > opm_p),
        "f9": b(at_c  > at_p),
    })

    return signals.sum(axis=1).astype(int)


# =========================
# 🏆 QUALITY SCORE  (0–13.4)
# =========================

def calc_quality_score(df: pd.DataFrame) -> pd.Series:
    """
    9-factor absolute-level quality score with YoY direction bonuses/penalties.
    Max theoretical ≈ 13.4
    """
    def calc_row(x):
        score = 0.0

        score += score_metric(x.get("ROE_PCT"),               [5, 10, 15, 20])
        score += score_metric(x.get("ROCE_PCT"),              [5, 10, 15, 20])
        score += score_metric(x.get("NET_MARGIN_CALC_PCT"),   [2,  5, 10, 15])
        score += score_metric(x.get("PL_OPM_PCT"),            [5, 10, 15, 20])
        score += score_metric(x.get("REVENUE_CAGR_3YR_PCT"),  [3,  5, 10, 15])
        score += score_metric(x.get("PAT_CAGR_3YR_PCT"),      [3,  5, 10, 15])
        score += score_metric(x.get("NET_DEBT_TO_EBITDA"),    [0, 1, 2, 3], reverse=True)
        score += score_metric(x.get("INTEREST_COVERAGE"),     [1, 2, 4, 8])
        score += score_metric(x.get("SH_PROMOTER_PCT_LATEST"),[30, 40, 50, 60], max_pts=1.0)

        pat_yoy = x.get("PAT_GROWTH_YOY_PCT")
        if pd.notna(pat_yoy):
            if   pat_yoy >   0: score += 0.2
            elif pat_yoy < -10: score -= 0.3

        rev_yoy = x.get("REVENUE_GROWTH_YOY_PCT")
        if pd.notna(rev_yoy):
            if   rev_yoy >  0: score += 0.1
            elif rev_yoy < -5: score -= 0.2

        roe     = x.get("ROE_PCT")
        roe_avg = x.get("ROE_AVG_3YR_PCT")
        if pd.notna(roe) and pd.notna(roe_avg) and roe_avg > 0:
            if   roe > roe_avg * 1.05: score += 0.2
            elif roe < roe_avg * 0.90: score -= 0.2

        cfo = x.get("CF_OPERATING_CR")
        if pd.notna(cfo):
            if   cfo > 0: score += 0.1
            elif cfo < 0: score -= 0.2

        return round(max(0.0, score), 2)

    return df.apply(calc_row, axis=1)


# =========================
# ⭐ MASTER FINAL SCORE  (0–10)
# =========================

def calc_final_score(df: pd.DataFrame) -> pd.DataFrame:
    """
    MASTER = (VAL_NORM × 0.40) + (PIOT_NORM × 0.35) + (QUAL_NORM × 0.25)

    VAL_NORM   — MOS% normalised: -50 → 0,  +100 → 10
    PIOT_NORM  — Piotroski 0–9  → 0–10
    QUAL_NORM  — Quality 0–13.4 → 0–10

    FINAL_SCORE: 0–10   FINAL_RANK: 1 = best

    Grades (0–10 scale):
      A ≥7.5  B+ ≥6.5  B ≥5.5  C ≥4.0  D ≥2.5  E <2.5
    """
    df = df.copy()

    mos = pd.to_numeric(
        df.get("MARGIN_OF_SAFETY_PCT", pd.Series(0, index=df.index)),
        errors="coerce"
    ).fillna(0).clip(-50, 100)

    val_norm  = ((mos + 50) / 150 * 10).clip(0, 10)
    piot_norm = (df["PIOTROSKI_SCORE"] / 9 * 10).clip(0, 10)
    qual_norm = (df["QUALITY_SCORE"] / 13.4 * 10).clip(0, 10)

    df["FINAL_SCORE"] = (
        val_norm  * 4 +
        piot_norm * 3.5 +
        qual_norm * 2.5
    ).round(2)

    df["FINAL_RANK"] = (
        df["FINAL_SCORE"]
        .rank(ascending=False, method="min", na_option="bottom")
        .astype(int)
    )

    def grade(s):
        if pd.isna(s): return "E"
        if s >= 75:   return "A"
        if s >= 65:   return "B+"
        if s >= 55:   return "B"
        if s >= 40:   return "C"
        if s >= 25:   return "D"
        return "E"

    df["GRADE"] = df["FINAL_SCORE"].apply(grade)
    return df


# =========================
# 📰 NEWS LOADER
# =========================

def _extract_source(url: str) -> str:
    """Extract a short source name from a Google News or direct URL."""
    try:
        # Try to get the source from URL parameters or domain
        import urllib.parse
        parsed = urllib.parse.urlparse(url)
        # Google News RSS URLs encode the source in the path/params
        # Fall back to the netloc, strip www.
        host = parsed.netloc.replace("www.", "")
        if host == "news.google.com":
            return "Google News"
        return host.split(".")[0].capitalize()
    except Exception:
        return ""


def load_news(path: Path) -> dict:
    """
    Load multi_stock_news.csv and return a dict:
      { "SYMBOL": [ {headline, time, link, source}, ... ], ... }

    CSV columns expected: stockname, datetime, news, link
    News items are sorted newest-first per symbol.
    """
    if not path.exists():
        print(f"  [NEWS]  {path.name} not found — skipping news injection")
        return {}

    print(f"  [NEWS]  {path.name}")
    try:
        df = pd.read_csv(path, low_memory=False)
    except Exception as e:
        print(f"    ERROR reading news CSV: {e}")
        return {}

    # Normalise column names (strip whitespace, lowercase check)
    df.columns = [c.strip() for c in df.columns]
    required = {"stockname", "datetime", "news", "link"}
    missing = required - set(df.columns)
    if missing:
        print(f"    ERROR: missing columns in news CSV: {missing}")
        return {}

    news_map = {}
    for _, row in df.iterrows():
        sym = str(row["stockname"]).strip().upper()
        if not sym:
            continue

        headline = str(row["news"]).strip()
        link     = str(row["link"]).strip()
        dt_raw   = str(row["datetime"]).strip()
        source   = _extract_source(link)

        # Format datetime nicely: "28 Mar 2026, 10:02 IST"
        time_str = dt_raw
        try:
            # Try parsing "2026-03-28 10:02 IST" style
            dt_clean = dt_raw.replace(" IST", "").strip()
            dt_obj   = datetime.strptime(dt_clean, "%Y-%m-%d %H:%M")
            time_str = dt_obj.strftime("%-d %b %Y, %I:%M %p")
        except Exception:
            pass  # keep raw string if parse fails

        item = {
            "headline": headline,
            "time":     time_str,
            "link":     link,
            "source":   source,
        }
        news_map.setdefault(sym, []).append(item)

    # Sort each symbol's news newest-first (preserving original CSV order if parse fails)
    for sym in news_map:
        try:
            news_map[sym].sort(
                key=lambda x: datetime.strptime(
                    df.loc[df["news"] == x["headline"], "datetime"]
                    .iloc[0].replace(" IST", "").strip(),
                    "%Y-%m-%d %H:%M"
                ),
                reverse=True
            )
        except Exception:
            pass  # original order is fine

    total_items = sum(len(v) for v in news_map.values())
    print(f"    Loaded: {total_items} news items across {len(news_map)} symbols")
    return news_map


def build_news_js(news_map: dict) -> str:
    """
    Render the NEWS_DATA JS block.
    Headlines are stored with their link so the template renders them as <a> tags.
    """
    if not news_map:
        return "// ── NEWS DATA ──\nvar NEWS_DATA = {\n  '__default__': []\n};"

    lines = ["// ── NEWS DATA ──", "var NEWS_DATA = {", "  '__default__': []"]
    for sym, items in sorted(news_map.items()):
        safe_items = json.dumps(items, ensure_ascii=False, separators=(",", ":"))
        lines.append(f",{json.dumps(sym)}:{safe_items}")
    lines.append("};")
    return "\n".join(lines)


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
    print(f"    Loaded: {len(df)} rows × {len(df.columns)} cols")

    if name_col in df.columns:
        df = df.rename(columns={name_col: "COMPANY_NAME"})
    elif "COMPANY_NAME" not in df.columns:
        print(df.columns)
        print("ERROR: Company name column missing")
        sys.exit(1)

    df = df.copy()

    # NET_DEBT_TO_EBITDA
    if "NET_DEBT_CR" in df.columns and "PL_EBITDA_CR" in df.columns:
        ebitda   = pd.to_numeric(df["PL_EBITDA_CR"], errors="coerce").replace(0, np.nan)
        net_debt = pd.to_numeric(df["NET_DEBT_CR"],  errors="coerce")
        df["NET_DEBT_TO_EBITDA"] = net_debt / ebitda
    else:
        df["NET_DEBT_TO_EBITDA"] = np.nan

    # Promoter flag
    if "SH_PROMOTER_PCT_LATEST" in df.columns:
        df["LOW_PROMOTER_FLAG"] = (
            pd.to_numeric(df["SH_PROMOTER_PCT_LATEST"], errors="coerce") < 25
        )
    else:
        df["LOW_PROMOTER_FLAG"] = False

    # ── Compute all scores ───────────────────────────────────────────────
    df["PIOTROSKI_SCORE"] = calc_piotroski_fscore(df)
    df["QUALITY_SCORE"]   = calc_quality_score(df)
    df["COMBINED_SCORE"]  = (df["PIOTROSKI_SCORE"] + df["QUALITY_SCORE"]).round(2)
    df = calc_final_score(df)

    # Debug stats
    for col in ["PIOTROSKI_SCORE", "QUALITY_SCORE", "COMBINED_SCORE", "FINAL_SCORE"]:
        s = df[col].dropna()
        print(f"    {col:20s}  min={s.min():.2f}  max={s.max():.2f}  avg={s.mean():.2f}")

    top5 = df.nsmallest(5, "FINAL_RANK")[["COMPANY_NAME", "FINAL_RANK", "FINAL_SCORE", "GRADE"]]
    print(f"    Top 5:\n{top5.to_string(index=False)}")

    # Trim to output columns
    needed = ["COMPANY_NAME"] + KEY_COLS
    df = df[[c for c in needed if c in df.columns]]

    df = df.replace([np.inf, -np.inf], None)
    df = df.where(pd.notnull(df), None)

    for col in ["PIOTROSKI_SCORE", "QUALITY_SCORE", "COMBINED_SCORE", "FINAL_SCORE"]:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda v: round(float(v), 2) if v is not None else None
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
        print("ERROR: Template missing —", TEMPLATE)
        sys.exit(1)

    template = TEMPLATE.read_text(encoding="utf-8")

    print("Loading CSVs...\n")

    data_n500  = load_csv(N500_CSV,  N500_NAME_COL,  "Nifty 500")
    data_sc250 = load_csv(SC250_CSV, SC250_NAME_COL, "Smallcap 250", optional=True)
    data_mc250 = load_csv(MC250_CSV, MC250_NAME_COL, "Microcap 250", optional=True)

    if data_sc250 is None:
        data_sc250 = []
    if data_mc250 is None:
        data_mc250 = []

    print("\nLoading News...\n")
    news_map = load_news(NEWS_CSV)
    news_js  = build_news_js(news_map)

    # ── Build DATASETS JS ──
    datasets_js = (
        "const DATASETS = {\n"
        f"n500:{json.dumps(data_n500, separators=(',', ':'))},\n"
        f"sc250:{json.dumps(data_sc250, separators=(',', ':'))},\n"
        f"mc250:{json.dumps(data_mc250, separators=(',', ':'))}\n"
        "};"
    )

    # ── Inject DATASETS ──
    html = template.replace(PLACEHOLDER, datasets_js)

    # ── Inject NEWS_DATA (replace the static placeholder block) ──
    html = html.replace(NEWS_PLACEHOLDER, news_js)

    # If placeholder wasn't found (template variation), try simpler replacement
    if news_js not in html and "var NEWS_DATA" in html:
        # Replace whatever NEWS_DATA block exists using regex
        html = re.sub(
            r"// ── NEWS DATA ──\s*\nvar NEWS_DATA\s*=\s*\{[^;]*\};",
            news_js,
            html,
            flags=re.DOTALL
        )

    # ── Timestamp ──
    now = datetime.now(zoneinfo.ZoneInfo("Asia/Kolkata")).strftime("%d %b %Y\n%I:%M %p IST")
    html = html.replace("%%LAST_UPDATED_PLACEHOLDER%%", now)

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    
    OUTPUT.write_text(html, encoding="utf-8")

    total = len(data_n500 or []) + len(data_sc250) + len(data_mc250)
    print(f"\n✅ Build Complete  —  {total} total stocks, {sum(len(v) for v in news_map.values())} news items")
    print(f"   Output: {OUTPUT}")

    if deploy:
        import subprocess
        subprocess.run(["firebase", "deploy"])


# =========================
# ▶ RUN
# =========================

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="HNImanshu Stock Screener Build")
    ap.add_argument("--deploy", action="store_true", help="Deploy to Firebase after build")
    args = ap.parse_args()
    build(args.deploy)
