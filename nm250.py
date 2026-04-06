"""
Nifty 500 — COMPLETE Fundamental Data Pipeline
=================================================
FIXES in this version vs the submitted code
────────────────────────────────────────────
BUG-1  compute_composite: quality/stability haircut (step 7) now rechecks
       len(values)<2 AND re-applies the CMP≥0.4 filter AFTER the haircut,
       not before.  The old code applied the filter before the haircut,
       then the haircut shrank values below CMP*0.4 and a second filter
       (wrongly placed) wiped them out → COMPOSITE_FAIR_VALUE = None for
       most stocks.

BUG-2  PL_ROW_MAP had "PL_EPS_BASIC" listed twice (eps(basic) AND eps).
       _map_rows skips already-assigned columns so the generic "eps" row
       was silently dropped on standalone screener pages. Fixed by giving
       the generic entry its own column name EPS_GENERIC, then merging.

BUG-3  fv_earnings_yield used a hardcoded 9% yield floor independent of
       WACC; now uses get_wacc(row) so quality-adjusted discount rates
       flow through.

BUG-4  build_master_csv DictWriter was created from first-row keys only;
       later rows with more quarterly columns were silently truncated.
       Fixed: collect all keys across ALL rows, write header once at end.
       (Memory cost is trivial for 500 stocks.)

BUG-5  compute_final_score get_col() used a loose substring match which
       could accidentally match the wrong column. Now uses an explicit
       preferred-column list with a fallback.

BUG-6  UPSIDE_PCT was computed twice inside compute_fair_values; the
       second write used df.get("CMP") (returns a Series — fine) but was
       redundant and confusing. Kept only one authoritative write.

BUG-7  G8 gate used our_pe < pe_scraped * 0.30 but core EPS is ALREADY
       stripped of other income, so a high-OI stock legitimately has a
       much higher implied PE than the screener's TTM PE.  Gate now only
       fires when our_pe < pe_scraped * 0.15 (extreme mismatch only).

Gates (any single failure → COMPOSITE_FAIR_VALUE = None):
  G1  Latest PAT must be POSITIVE
  G2  At least 3 years of PAT history required
  G3  Majority of PAT history must be profitable (>60% positive years)
  G4  Latest PAT must not be a >4x spike vs prior 3yr average
  G5  FINANCIAL stocks: negative ROE → disqualified
  G5b FINANCIAL stocks: Book Value must exist and be positive
  G6  Non-financial: OPM must exist and be ≥0
  G7  Implied PE must be 1x–300x
  G8  Core EPS vs scraped PE within 6.5x (extreme mismatch guard)

Usage:
  pip install requests pandas beautifulsoup4 lxml tqdm
  python nifty500_pipeline.py
  python nifty500_pipeline.py --skip-fetch
  python nifty500_pipeline.py --force-refresh
  python nifty500_pipeline.py --limit 20
"""

import argparse, time, sys, re, io, math, logging, csv, os
from pathlib import Path
from datetime import datetime, timedelta
import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from tqdm import tqdm
from datetime import datetime as dt
from zoneinfo import ZoneInfo
import warnings
warnings.filterwarnings("ignore", category=RuntimeWarning)
# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
# ★  USER CONTROLS
# ══════════════════════════════════════════════════════════════════════════════
SKIP_DOWNLOAD  = False
REFRESH_DAYS   = 30
REQUEST_DELAY  = 1.5

# ── Network constants ─────────────────────────────────────────────────────────
INDEX = "niftymicrocap250"
NSE_CSV_URL  = f"https://nsearchives.nseindia.com/content/indices/ind_{INDEX}_list.csv"

#INDEX = "niftysmallcap500"
#NSE_CSV_URL  = f"https://www.niftyindices.com/IndexConstituent/ind_{INDEX}_list.csv"

#INDEX = "nifty500"     # Keep all 3 lines; only the last assignment wins at runtime
#NSE_CSV_URL  = f"https://nsearchives.nseindia.com/content/indices/ind_{INDEX}list.csv"

OUTPUT_FILE = f"{INDEX}_valuation.csv"

# ── Delete stale output at startup ───────────────────────────────────────────
try:
    ist_time = datetime.now(ZoneInfo("Asia/Kolkata"))
    if ist_time.hour == 15:
        os.remove(OUTPUT_FILE)
        print(f"File deleted at {ist_time.strftime('%Y-%m-%d %H:%M:%S')} IST")
except Exception as e:
    ist_time = datetime.now(ZoneInfo("Asia/Kolkata"))
    print(f"Delete failed at {ist_time.strftime('%Y-%m-%d %H:%M:%S')} IST | Error: {e}")

# ── Network headers ───────────────────────────────────────────────────────────
NSE_HDR = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/123.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/market-data/live-equity-market",
}
SCREENER_HDR = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/123.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "DNT": "1",
}
SCREENER_BASE = "https://www.screener.in"

# ── Valuation constants ───────────────────────────────────────────────────────
WACC            = 0.12
TERMINAL_GROWTH = 0.035
RISK_FREE       = 0.07
EQUITY_PREMIUM  = 0.055

# ── Sector lists ──────────────────────────────────────────────────────────────
FINANCIAL_SECTORS = ["BANK", "NBFC", "INSURANCE", "FINANCE", "FINANCIAL", "HOUSING FINANCE"]
CYCLICAL_SECTORS  = [
    "REAL ESTATE", "METALS", "MINING", "CEMENT",
    "OIL & GAS", "POWER", "INFRASTRUCTURE", "COMMODITIES",
    "AUTO", "CAPITAL GOODS"
]
DEEP_CYCLICAL_SECTORS = ["AIRLINE", "AVIATION", "SHIPPING", "LOGISTICS"]
EPC_SECTORS           = ["INFRASTRUCTURE", "CONSTRUCTION", "EPC",
                          "ENGINEERING", "ROADS", "HIGHWAYS", "CIVIL"]


# ══════════════════════════════════════════════════════════════════════════════
# PARSING HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _num(text):
    if text is None: return None
    t = str(text).strip()
    mult = 1
    if t.endswith("Cr"):   mult, t = 1e7, t[:-2]
    elif t.endswith("L"):  mult, t = 1e5, t[:-1]
    elif t.endswith("K"):  mult, t = 1e3, t[:-1]
    try:
        return float(re.sub(r"[%,\s₹+]", "", t)) * mult
    except ValueError:
        return None

def _safe(v):
    if v is None: return None
    try:
        f = float(v)
        return None if (f != f or math.isinf(f)) else f
    except (TypeError, ValueError):
        return None

def _parse_table(section) -> pd.DataFrame:
    if section is None: return pd.DataFrame()
    tbl = section.find("table")
    if tbl is None: return pd.DataFrame()
    rows, headers = [], []
    for i, tr in enumerate(tbl.find_all("tr")):
        cells = [td.get_text(" ", strip=True) for td in tr.find_all(["th", "td"])]
        if i == 0: headers = cells
        elif cells: rows.append(cells)
    if not headers or not rows: return pd.DataFrame()
    w = max(len(r) for r in rows)
    headers = (headers + [""] * w)[:w]
    try:    return pd.DataFrame(rows, columns=headers)
    except: return pd.DataFrame(rows)

def _ycols(df):
    return [c for c in df.columns[1:] if re.search(r"\d{4}", str(c))]

def _qcols(df):
    return [c for c in df.columns[1:] if re.search(r"(Jan|Mar|Jun|Sep|Dec)\s*'?\d{2}", str(c), re.I)]

_QMONTH_ORDER = {"MAR": 0, "JUN": 1, "SEP": 2, "DEC": 3,
                 "JAN": 0, "APR": 1, "JUL": 2, "OCT": 3}

def _sort_q_keys_chrono(keys):
    """Sort quarterly column keys chronologically instead of alphabetically."""
    def _k(c):
        m = re.search(r"Q?([A-Z]{3})(\d{2,4})", c.upper())
        if not m: return (9999, 9)
        mo, yr = m.group(1), m.group(2)
        yr_i = int(yr) if len(yr) == 4 else 2000 + int(yr)
        return (yr_i, _QMONTH_ORDER.get(mo, 9))
    return sorted(keys, key=_k)

def _find_row(df, *keywords):
    for _, row in df.iterrows():
        label = str(row.iloc[0]).lower()
        if any(k.lower() in label for k in keywords):
            return row
    return None

def _val(row, col):
    if row is None or col not in row.index: return None
    return _num(str(row[col]))

def _cagr_pct(series):
    """
    Compute CAGR across a series of positive values.
    Returns None (not a capped value) if:
      - fewer than 2 positive values
      - result > 200% (near-zero base — unreliable; use None so it doesn't
        corrupt scoring; genuine 3yr revenue/PAT CAGR above 200% is almost
        always a base-effect artifact, not real sustainable growth)
    """
    vals = [v for v in series if v is not None and not math.isnan(v) and v > 0]
    if len(vals) < 2: return None
    try:
        cagr = ((vals[-1] / vals[0]) ** (1 / (len(vals) - 1)) - 1) * 100
        if abs(cagr) > 200:
            return None   # base-effect artifact, not reliable
        return round(cagr, 2)
    except: return None

def _avg(lst):
    vals = [v for v in lst if v is not None]
    return round(sum(vals) / len(vals), 2) if vals else None

def _g(row, *keys):
    for k in keys:
        v = _safe(row.get(k) if hasattr(row, "get") else (row[k] if k in row.index else None))
        if v is not None: return v
    return None

def _is_financial(row):
    sector = str(row.get("SECTOR", "") if hasattr(row, "get") else row.get("SECTOR", "")).upper()
    return any(f in sector for f in FINANCIAL_SECTORS)


# ══════════════════════════════════════════════════════════════════════════════
# WACC
# ══════════════════════════════════════════════════════════════════════════════

def get_wacc(row) -> float:
    base      = WACC
    de        = _safe(row.get("DEBT_TO_EQUITY")) or 0
    sector    = str(row.get("SECTOR", "")).upper()
    roe       = _safe(row.get("ROE_PCT")) or 0
    ic        = _safe(row.get("INTEREST_COVERAGE")) or 0
    oi_to_pbt = _safe(row.get("OTHER_INCOME_TO_PBT_PCT")) or 0

    if de > 2.0:   base += 0.03
    elif de > 1.0: base += 0.02
    elif de > 0.5: base += 0.01

    if any(c in sector for c in CYCLICAL_SECTORS): base += 0.015

    # Quality discount only when ROE is genuinely operational
    if roe > 25 and ic > 5 and de < 0.5 and oi_to_pbt < 30:
        base -= 0.01

    return round(min(max(base, 0.10), 0.18), 4)


# ══════════════════════════════════════════════════════════════════════════════
# STEP 1 — NSE NIFTY 500
# ══════════════════════════════════════════════════════════════════════════════

def fetch_nifty500() -> pd.DataFrame:
    log.info("Fetching Nifty 500 from NSE archives …")
    s = requests.Session()
    s.headers.update(NSE_HDR)
    try:
        r = s.get(NSE_CSV_URL, timeout=25)
        r.raise_for_status()
        df = pd.read_csv(io.StringIO(r.text))
        df.columns = [c.strip().upper().replace(" ", "_") for c in df.columns]
        sym = next((c for c in df.columns if "SYMBOL" in c), None)
        if sym and sym != "SYMBOL":
            df.rename(columns={sym: "SYMBOL"}, inplace=True)
        log.info(f"  ✓ {len(df)} symbols")
        return df
    except Exception as e:
        raise RuntimeError(f"Cannot fetch Nifty 500: {e}") from e


# ══════════════════════════════════════════════════════════════════════════════
# STEP 2 — SCREENER SCRAPER
# ══════════════════════════════════════════════════════════════════════════════

RATIO_LABEL_MAP = {
    "market cap":           "MARKET_CAP_CR",
    "current price":        "CMP",
    "high / low":           "HIGH_52W",
    "stock p/e":            "PE",
    "book value":           "BOOK_VALUE",
    "dividend yield":       "DIV_YIELD_PCT",
    "roce":                 "ROCE_PCT",
    "roe":                  "ROE_PCT",
    "face value":           "FACE_VALUE",
    "price to book":        "PB",
    "eps":                  "EPS_TTM",
    "debt to equity":       "DEBT_TO_EQUITY",
    "industry pe":          "INDUSTRY_PE",
    "promoter holding":     "PROMOTER_HOLDING_PCT",
    "fii holding":          "FII_HOLDING_PCT",
    "dii holding":          "DII_HOLDING_PCT",
    "opm":                  "OPM_PCT",
    "npm":                  "NPM_PCT",
    "payout ratio":         "PAYOUT_RATIO_PCT",
    "earnings yield":       "EARNINGS_YIELD_PCT",
    "peg ratio":            "PEG_RATIO",
    "price to sales":       "PRICE_TO_SALES",
    "ev/ebitda":            "EV_EBITDA",
    "enterprise value":     "EV_CR",
    "intrinsic value":      "INTRINSIC_VALUE_SCREENER",
    "graham number":        "GRAHAM_NUM_SCREENER",
    "current ratio":        "CURRENT_RATIO",
    "quick ratio":          "QUICK_RATIO",
    "debtor days":          "DEBTOR_DAYS",
    "inventory days":       "INVENTORY_DAYS",
    "payable days":         "PAYABLE_DAYS",
    "cash conversion":      "CASH_CONVERSION_CYCLE",
    "working capital days": "WC_DAYS",
    "asset turnover":       "ASSET_TURNOVER",
    "interest coverage":    "INTEREST_COVERAGE",
    "sales growth":         "SALES_GROWTH_PCT",
    "profit growth":        "PROFIT_GROWTH_PCT",
    "return on assets":     "ROA_PCT",
    "return on equity":     "ROE_PCT",
}

BS_ROW_MAP = [
    (["cash and cash equiv", "cash & cash equiv"],                  "BS_CASH_CR"),
    (["bank balance", "other bank balances", "fixed deposit"],      "BS_BANK_BALANCES_CR"),
    (["current investments", "short term investments"],             "BS_CURRENT_INVEST_CR"),
    (["non-current investments", "non current investments",
      "long term investments"],                                     "BS_NONCURRENT_INVEST_CR"),
    (["trade receivables", "debtors"],                              "BS_TRADE_RECV_CR"),
    (["inventories", "inventory"],                                  "BS_INVENTORIES_CR"),
    (["other current assets"],                                      "BS_OTHER_CURR_ASSETS_CR"),
    (["total current assets"],                                      "BS_TOTAL_CURR_ASSETS_CR"),
    (["net block", "property plant", "ppe"],                        "BS_NET_BLOCK_CR"),
    (["gross block"],                                               "BS_GROSS_BLOCK_CR"),
    (["capital work in progress", "cwip"],                          "BS_CWIP_CR"),
    (["goodwill"],                                                   "BS_GOODWILL_CR"),
    (["intangible"],                                                 "BS_INTANGIBLES_CR"),
    (["right-of-use", "right of use", "lease asset", "rou"],       "BS_ROU_ASSETS_CR"),
    (["total assets"],                                              "BS_TOTAL_ASSETS_CR"),
    (["short term borrowing", "short-term borrowing"],              "BS_ST_BORROWINGS_CR"),
    (["long term borrowing", "long-term borrowing"],                "BS_LT_BORROWINGS_CR"),
    (["total borrowing", "total debt"],                             "BS_TOTAL_BORROWINGS_CR"),
    (["trade payables", "creditors"],                               "BS_TRADE_PAY_CR"),
    (["other current liabilit"],                                    "BS_OTHER_CURR_LIAB_CR"),
    (["total current liabilit"],                                    "BS_TOTAL_CURR_LIAB_CR"),
    (["share capital", "paid up"],                                   "BS_SHARE_CAPITAL_CR"),
    (["reserves", "surplus", "other equity"],                       "BS_RESERVES_CR"),
    (["total equity", "shareholders", "networth", "net worth"],     "BS_TOTAL_EQUITY_CR"),
]

# FIX BUG-2: "eps (basic)" and plain "eps" had the same target column PL_EPS_BASIC.
# _map_rows skips already-assigned columns, so the second entry was silently dropped.
# Solution: give the generic "eps" row a distinct intermediate key EPS_GENERIC,
# then the scraper merges it into PL_EPS_BASIC only if PL_EPS_BASIC is still None.
PL_ROW_MAP = [
    (["sales", "revenue from operations", "net revenue"],           "PL_REVENUE_CR"),
    (["other income"],                                               "PL_OTHER_INCOME_CR"),
    (["total income", "total revenue"],                              "PL_TOTAL_INCOME_CR"),
    (["operating profit", "ebitda", "ebita", "financing profit"],   "PL_EBITDA_CR"),
    (["interest", "finance cost", "finance charges"],               "PL_INTEREST_CR"),
    (["depreciation", "amortisation", "amortization"],              "PL_DEPR_CR"),
    (["exceptional item"],                                           "PL_EXCEPTIONAL_CR"),
    (["profit before tax", "pbt"],                                   "PL_PBT_CR"),
    (["tax %", "tax rate"],                                          "PL_TAX_RATE_PCT"),
    (["tax", "income tax"],                                          "PL_TAX_AMOUNT_CR"),
    (["net profit", "pat", "profit after tax"],                     "PL_PAT_CR"),
    (["eps (basic)", "basic eps"],                                   "PL_EPS_BASIC"),
    (["eps (diluted)", "diluted eps"],                               "PL_EPS_DILUTED"),
    (["eps"],                                                         "EPS_GENERIC"),   # ← was duplicate PL_EPS_BASIC
    (["dividend"],                                                    "PL_DIVIDEND_CR"),
    (["opm %", "opm%", "operating margin", "financing margin"],     "PL_OPM_PCT"),
]

CF_ROW_MAP = [
    (["cash from operating", "operating activities"],               "CF_OPERATING_CR"),
    (["cash from investing", "investing activities"],               "CF_INVESTING_CR"),
    (["cash from financing", "financing activities"],               "CF_FINANCING_CR"),
    (["purchase of fixed", "capex", "capital expenditure",
      "purchase of property", "purchase of ppe"],                   "CF_CAPEX_CR"),
    (["dividend paid", "dividends paid"],                           "CF_DIVID_PAID_CR"),
    (["change in working capital", "working capital changes"],      "CF_WC_CHANGE_CR"),
    (["net change in cash", "net increase in cash"],               "CF_NET_CASH_CHG_CR"),
]


def get_core_eps(row):
    return _g(row, "CORE_EPS", "PL_EPS_BASIC", "EPS_TTM")

def _map_rows(df, row_map, col_key):
    result = {}
    assigned = set()
    for keywords, col_name in row_map:
        if col_name in assigned: continue
        row = _find_row(df, *keywords)
        if row is not None:
            result[col_name] = _val(row, col_key)
            assigned.add(col_name)
    return result

def _map_all_years(df, row_map, ycols, prefix=""):
    result = {}
    for keywords, base_col in row_map:
        row = _find_row(df, *keywords)
        if row is None: continue
        for yc in ycols:
            yr_label = re.sub(r"[^A-Z0-9]", "", yc.upper())
            col = f"{prefix}{base_col}_{yr_label}" if prefix else f"{base_col}_{yr_label}"
            result[col] = _val(row, yc)
    return result

def _all_q_rows(df, row_map, qcols, prefix=""):
    result = {}
    for keywords, base_col in row_map:
        row = _find_row(df, *keywords)
        if row is None: continue
        for qc in qcols:
            ql  = re.sub(r"['\s]", "", qc.upper())
            col = f"{prefix}{base_col}_Q{ql}"
            result[col] = _val(row, qc)
    return result


def scrape_screener(symbol: str, session: requests.Session) -> dict:
    r = {"SYMBOL": symbol}
    html = None

    for suffix in ["/consolidated/", "/"]:
        url = f"{SCREENER_BASE}/company/{symbol}{suffix}"
        try:
            resp = session.get(url, timeout=30)
            if resp.status_code == 404: continue
            resp.raise_for_status()
            html = resp.text
            r["SCREENER_URL"] = url
            break
        except Exception as e:
            r["SCREENER_ERROR"] = str(e)

    if not html:
        r.setdefault("SCREENER_ERROR", "page not loaded")
        return r

    soup = BeautifulSoup(html, "lxml")

    h1 = soup.find("h1")
    if h1: r["COMPANY_NAME"] = h1.get_text(strip=True)

    r["SCREENER_PAGE_TYPE"] = "consolidated" if "/consolidated/" in r.get("SCREENER_URL","") else "standalone"

    for tag in soup.select("a[href*='/company/']"):
        href = str(tag.get("href", "")).lower()
        if "sector" in href or "industry" in href:
            r["SECTOR"] = tag.get_text(strip=True)
            break

    for li in soup.select("ul.company-ratios li, #top-ratios li, .company-ratios li"):
        nt = li.find("span", class_="name")
        vt = li.find("span", class_="number")
        if not nt or not vt: continue
        label = nt.get_text(strip=True).lower()
        raw   = vt.get_text(strip=True)
        matched = False
        for k, col in RATIO_LABEL_MAP.items():
            if k in label:
                r[col] = _num(raw)
                matched = True
                break
        if not matched:
            key = "RATIO_" + re.sub(r"[^A-Z0-9]", "_", label.upper()).strip("_")
            r[key] = _num(raw)

    # ── P&L ──────────────────────────────────────────────────────────────────
    sec_pl   = soup.find("section", id="profit-loss")
    df_pl    = _parse_table(sec_pl)
    ycols_pl = _ycols(df_pl)

    if ycols_pl:
        annual_ycols = [c for c in ycols_pl
                        if not re.search(r'TTM|trailing', c, re.I)
                        and re.search(r'(Mar|Dec|Jun|Sep)\s*\d{4}', c, re.I)]
        ly = annual_ycols[-1] if annual_ycols else ycols_pl[-1]
        mapped = _map_rows(df_pl, PL_ROW_MAP, ly)
        # BUG-2 fix: promote EPS_GENERIC → PL_EPS_BASIC if not already set
        if mapped.get("PL_EPS_BASIC") is None and mapped.get("EPS_GENERIC") is not None:
            mapped["PL_EPS_BASIC"] = mapped["EPS_GENERIC"]
        mapped.pop("EPS_GENERIC", None)
        r.update(mapped)

        all_years = _map_all_years(df_pl, PL_ROW_MAP, ycols_pl, prefix="A_")
        # Rename EPS_GENERIC annual columns too
        for k in list(all_years.keys()):
            if "EPS_GENERIC" in k:
                new_k = k.replace("EPS_GENERIC", "PL_EPS_BASIC")
                if new_k not in all_years:
                    all_years[new_k] = all_years.pop(k)
                else:
                    all_years.pop(k)
        r.update(all_years)

        rev_row = _find_row(df_pl, "sales", "revenue from operations", "net revenue")
        pat_row = _find_row(df_pl, "net profit", "pat")
        exc_row = _find_row(df_pl, "exceptional")

        if len(ycols_pl) >= 2:
            py  = ycols_pl[-2]
            rp  = _val(rev_row, py); rl = _val(rev_row, ly)
            pp  = _val(pat_row, py); pl2= _val(pat_row, ly)
            if rp and rl and rp != 0:
                r["REVENUE_GROWTH_YOY_PCT"] = round((rl - rp) / abs(rp) * 100, 2)
            if pp and pl2 and pp != 0:
                r["PAT_GROWTH_YOY_PCT"]     = round((pl2 - pp) / abs(pp) * 100, 2)

        for yrs, lbl in [(3,"3YR"), (5,"5YR"), (10,"10YR")]:
            if len(ycols_pl) >= yrs + 1:
                sl = ycols_pl[-(yrs+1):]
                r[f"REVENUE_CAGR_{lbl}_PCT"] = _cagr_pct([_val(rev_row, c) for c in sl])
                adj = []
                for c in sl:
                    yr   = re.sub(r"[^A-Z0-9]", "", c.upper())
                    pat  = _safe(r.get(f"A_PL_PAT_CR_{yr}"))
                    oi_y = _safe(r.get(f"A_PL_OTHER_INCOME_CR_{yr}"))
                    exc  = _val(exc_row, c) if exc_row is not None else None
                    if pat is not None:
                        if oi_y: pat = pat - oi_y * 0.75
                        if exc:  pat = pat - exc
                    adj.append(pat)
                r[f"PAT_CAGR_{lbl}_PCT"] = _cagr_pct(adj)

        opm_row = _find_row(df_pl, "opm %", "operating margin", "financing margin")
        if opm_row is not None:
            opm_vals = [_val(opm_row, c) for c in ycols_pl]
            r["OPM_AVG_3YR_PCT"] = _avg(opm_vals[-3:])
            r["OPM_AVG_5YR_PCT"] = _avg(opm_vals[-5:])

        rl2 = _safe(r.get("PL_REVENUE_CR"))
        el  = _safe(r.get("PL_EBITDA_CR"))
        pl3 = _safe(r.get("PL_PAT_CR"))
        if el and rl2 and rl2 != 0:
            r["OPM_CALC_PCT"]        = round(el / rl2 * 100, 2)
        if pl3 and rl2 and rl2 != 0:
            r["NET_MARGIN_CALC_PCT"] = round(pl3 / rl2 * 100, 2)
        int_ = _safe(r.get("PL_INTEREST_CR"))
        if el and int_ and int_ > 0:
            r["INTEREST_COVERAGE"] = round(el / int_, 2)

    # ── Quarters ──────────────────────────────────────────────────────────────
    sec_q  = soup.find("section", id="quarters")
    df_q   = _parse_table(sec_q)
    qcols  = _qcols(df_q)

    if qcols:
        lq = qcols[-1]
        r["LATEST_QUARTER"] = lq
        r.update({f"Q_{k}": v for k, v in _map_rows(df_q, PL_ROW_MAP, lq).items()})
        r.update(_all_q_rows(df_q, PL_ROW_MAP, qcols, prefix="Q_"))

        if len(qcols) >= 2:
            pq     = qcols[-2]
            qrev_r = _find_row(df_q, "sales", "revenue")
            qpat_r = _find_row(df_q, "net profit", "pat")
            lqr = _val(qrev_r, lq); pqr = _val(qrev_r, pq)
            lqp = _val(qpat_r, lq); pqp = _val(qpat_r, pq)
            if pqr and lqr and pqr != 0:
                r["REVENUE_GROWTH_QOQ_PCT"] = round((lqr - pqr) / abs(pqr) * 100, 2)
            if pqp and lqp and pqp != 0:
                r["PAT_GROWTH_QOQ_PCT"]     = round((lqp - pqp) / abs(pqp) * 100, 2)

        if len(qcols) >= 4:
            qrev_r = _find_row(df_q, "sales", "revenue")
            qpat_r = _find_row(df_q, "net profit", "pat")
            r["TTM_REVENUE_CR"] = sum(v for c in qcols[-4:] if (v := _val(qrev_r, c)) is not None) or None
            r["TTM_PAT_CR"]     = sum(v for c in qcols[-4:] if (v := _val(qpat_r, c)) is not None) or None

    # ── Balance sheet ─────────────────────────────────────────────────────────
    sec_bs   = soup.find("section", id="balance-sheet")
    df_bs    = _parse_table(sec_bs)
    ycols_bs = _ycols(df_bs)

    if ycols_bs:
        lb = ycols_bs[-1]
        r.update(_map_rows(df_bs, BS_ROW_MAP, lb))
        r.update(_map_all_years(df_bs, BS_ROW_MAP, ycols_bs, prefix="B_"))

        debt = _safe(r.get("BS_TOTAL_BORROWINGS_CR")) or (
               (_safe(r.get("BS_ST_BORROWINGS_CR")) or 0) + (_safe(r.get("BS_LT_BORROWINGS_CR")) or 0))
        cash = _safe(r.get("BS_CASH_CR")) or 0
        r["NET_DEBT_CR"] = round(debt - cash, 2) if debt else None

        curr_a = _safe(r.get("BS_TOTAL_CURR_ASSETS_CR"))
        curr_l = _safe(r.get("BS_TOTAL_CURR_LIAB_CR"))
        if curr_a and curr_l and curr_l != 0 and not r.get("CURRENT_RATIO"):
            r["CURRENT_RATIO"] = round(curr_a / curr_l, 2)

        ebitda = _safe(r.get("PL_EBITDA_CR"))
        nd     = _safe(r.get("NET_DEBT_CR"))
        if nd is not None and ebitda and ebitda > 0:
            r["NET_DEBT_EBITDA"] = round(nd / ebitda, 2)

        rev = _safe(r.get("PL_REVENUE_CR"))
        ta  = _safe(r.get("BS_TOTAL_ASSETS_CR"))
        if rev and ta and ta > 0:  r["ASSET_TURNOVER"] = round(rev / ta, 2)

        nb = _safe(r.get("BS_NET_BLOCK_CR"))
        if rev and nb and nb > 0:  r["FIXED_ASSET_TURNOVER"] = round(rev / nb, 2)

        recv = _safe(r.get("BS_TRADE_RECV_CR"))
        if recv and rev and rev > 0:
            r["RECV_TO_SALES"] = round(recv / rev * 100, 2)
            if not r.get("DEBTOR_DAYS"): r["DEBTOR_DAYS"] = round(recv / rev * 365, 1)

        inv = _safe(r.get("BS_INVENTORIES_CR"))
        if inv and rev and rev > 0 and not r.get("INVENTORY_DAYS"):
            r["INVENTORY_DAYS"] = round(inv / rev * 365, 1)

        pay = _safe(r.get("BS_TRADE_PAY_CR"))
        if pay and rev and rev > 0 and not r.get("PAYABLE_DAYS"):
            r["PAYABLE_DAYS"] = round(pay / rev * 365, 1)

        dd  = _safe(r.get("DEBTOR_DAYS"))
        id_ = _safe(r.get("INVENTORY_DAYS"))
        pd_ = _safe(r.get("PAYABLE_DAYS"))
        if dd is not None and id_ is not None and pd_ is not None and not r.get("CASH_CONVERSION_CYCLE"):
            r["CASH_CONVERSION_CYCLE"] = round(dd + id_ - pd_, 1)

        mc  = _safe(r.get("MARKET_CAP_CR"))
        cmp = _safe(r.get("CMP"))
        if mc and cmp and cmp > 0 and not r.get("SHARES_CR"):
            r["SHARES_CR"] = round(mc / cmp, 4)

        eq_cap   = _safe(r.get("BS_SHARE_CAPITAL_CR"))
        face_val = _safe(r.get("FACE_VALUE"))
        if not r.get("SHARES_CR") and eq_cap and face_val and face_val > 0:
            r["SHARES_CR"] = round(eq_cap / face_val, 4)

        _eq_bs  = _safe(r.get("BS_TOTAL_EQUITY_CR"))
        _pat_bs = _safe(r.get("PL_PAT_CR"))
        if _pat_bs and _eq_bs and _eq_bs > 0:
            r["ROE_CALC_PCT"] = round(_pat_bs / _eq_bs * 100, 2)

    # ── ROE history ───────────────────────────────────────────────────────────
    roe_vals = []
    for yc in (ycols_bs if ycols_bs else []):
        yr_lbl = re.sub(r"[^A-Z0-9]", "", yc.upper())
        pat_y  = _safe(r.get(f"A_PL_PAT_CR_{yr_lbl}"))
        eq_y   = _safe(r.get(f"B_BS_TOTAL_EQUITY_CR_{yr_lbl}"))
        roe_vals.append(pat_y / eq_y * 100 if (pat_y and eq_y and eq_y > 0) else None)

    if roe_vals:
        r["ROE_AVG_3YR_PCT"]  = _avg([v for v in roe_vals[-3:]  if v is not None])
        r["ROE_AVG_5YR_PCT"]  = _avg([v for v in roe_vals[-5:]  if v is not None])
        r["ROE_AVG_10YR_PCT"] = _avg([v for v in roe_vals[-10:] if v is not None])

    # ── Core PAT (strip other income) ────────────────────────────────────────
    oi        = _safe(r.get("PL_OTHER_INCOME_CR"))
    tr        = _safe(r.get("PL_TAX_RATE_PCT"))
    _eq       = _safe(r.get("BS_TOTAL_EQUITY_CR"))
    _raw_pat  = _safe(r.get("PL_PAT_CR"))
    _core_pat = _raw_pat

    if _core_pat is not None:
        if oi is not None and tr is not None:
            _core_pat = _core_pat - oi * (1 - tr / 100)
        elif oi is not None:
            _core_pat = _core_pat - oi * 0.75

    if _raw_pat  is not None: r["PL_PAT_REPORTED_CR"] = round(_raw_pat, 2)
    if _core_pat is not None: r["PL_PAT_CR"]          = round(_core_pat, 2)

    shares = _safe(r.get("SHARES_CR"))
    if shares and _core_pat is not None and shares > 0:
        r["PL_EPS_BASIC"] = round(_core_pat / shares, 2)
        r["CORE_EPS"]     = r["PL_EPS_BASIC"]

    if _core_pat and _eq and _eq > 0:
        r["ROE_ADJ_EX_OTHER_INCOME_PCT"] = round(_core_pat / _eq * 100, 2)

    # ── Cash flow ─────────────────────────────────────────────────────────────
    sec_cf   = soup.find("section", id="cash-flow")
    df_cf    = _parse_table(sec_cf)
    ycols_cf = _ycols(df_cf)

    if ycols_cf:
        lc = ycols_cf[-1]
        r.update(_map_rows(df_cf, CF_ROW_MAP, lc))
        r.update(_map_all_years(df_cf, CF_ROW_MAP, ycols_cf, prefix="C_"))

        ocf = _safe(r.get("CF_OPERATING_CR"))
        cpx = _safe(r.get("CF_CAPEX_CR"))
        if ocf is not None and cpx is not None and not r.get("CF_FCF_CR"):
            r["CF_FCF_CR"] = round(ocf - abs(cpx), 2)

        fcf_vals = []
        for c in ycols_cf[-3:]:
            yr_lbl = re.sub(r"[^A-Z0-9]", "", c.upper())
            o = _safe(r.get(f"C_CF_OPERATING_CR_{yr_lbl}"))
            x = _safe(r.get(f"C_CF_CAPEX_CR_{yr_lbl}"))
            if o is not None and x is not None:
                fcf_vals.append(o - abs(x))
        if fcf_vals:
            r["CF_FCF_3YR_AVG_CR"] = round(sum(fcf_vals) / len(fcf_vals), 2)

        mc2 = _safe(r.get("MARKET_CAP_CR"))
        fcf = _safe(r.get("CF_FCF_CR"))
        if fcf and mc2 and mc2 > 0:
            r["FCF_YIELD_PCT"] = round(fcf / mc2 * 100, 2)

    # ── Shareholding ──────────────────────────────────────────────────────────
    sec_sh = soup.find("section", id="shareholding")
    df_sh  = _parse_table(sec_sh)
    if not df_sh.empty:
        sh_qcols = _qcols(df_sh)
        if not sh_qcols:
            sh_qcols = [c for c in df_sh.columns[1:] if re.search(r"\d{2}", str(c))]
        sh_map = {"promoter": "SH_PROMOTER_PCT", "fii": "SH_FII_PCT",
                  "dii": "SH_DII_PCT", "public": "SH_PUBLIC_PCT"}
        for _, sh_row in df_sh.iterrows():
            label = str(sh_row.iloc[0]).lower()
            for kw, col_base in sh_map.items():
                if kw in label:
                    for qc in sh_qcols[-8:]:
                        ql = re.sub(r"['\s]", "", str(qc).upper())
                        r[f"{col_base}_Q{ql}"] = _val(sh_row, qc)
                    if sh_qcols:
                        r[f"{col_base}_LATEST"] = _val(sh_row, sh_qcols[-1])
                    break
        prom = _safe(r.get("SH_PROMOTER_PCT_LATEST"))
        if prom: r["FREE_FLOAT_PCT"] = round(100 - prom, 2)

    # ── Derived ratios ────────────────────────────────────────────────────────
    _df_pl_safe = df_pl if not df_pl.empty else pd.DataFrame()
    _yc_pl_safe = ycols_pl if ycols_pl else []

    div_row = _find_row(_df_pl_safe, "dividend")
    if div_row is not None and _yc_pl_safe:
        div_vals = [_val(div_row, c) for c in _yc_pl_safe]
        r["DIVIDEND_CONSECUTIVE_YRS"] = sum(1 for v in div_vals if v and v > 0)
        r["DIVIDEND_UNINTERRUPTED"]   = all(v and v > 0 for v in div_vals) if div_vals else False

    exc_row2 = _find_row(_df_pl_safe, "exceptional")
    if exc_row2 is not None and _yc_pl_safe:
        ly2 = _yc_pl_safe[-1]
        r["HAS_EXCEPTIONAL_ITEMS"] = _val(exc_row2, ly2) is not None
        r["EXCEPTIONAL_AMOUNT_CR"] = _val(exc_row2, ly2)

    r["OTHER_INCOME_TO_PBT_PCT"] = None
    pbt = _safe(r.get("PL_PBT_CR"))
    oi2 = _safe(r.get("PL_OTHER_INCOME_CR"))
    if oi2 and pbt and pbt != 0:
        r["OTHER_INCOME_TO_PBT_PCT"] = round(oi2 / pbt * 100, 2)

    cwip = _safe(r.get("BS_CWIP_CR"))
    nb2  = _safe(r.get("BS_NET_BLOCK_CR"))
    if cwip and nb2 and nb2 > 0:
        r["CWIP_TO_NETBLOCK_PCT"] = round(cwip / nb2 * 100, 2)

    return r


# ══════════════════════════════════════════════════════════════════════════════
# STEP 3 — SMART BUILD
# ══════════════════════════════════════════════════════════════════════════════

def _csv_age_days(path: str):
    p = Path(path)
    if not p.exists(): return None
    try:
        df = pd.read_csv(p, usecols=["DATE_DOWNLOADED"], nrows=500)
        if "DATE_DOWNLOADED" in df.columns:
            dates = pd.to_datetime(df["DATE_DOWNLOADED"], errors="coerce").dropna()
            if not dates.empty:
                return (datetime.now() - dates.min().to_pydatetime()).days
    except: pass
    return (datetime.now() - datetime.fromtimestamp(p.stat().st_mtime)).days

def _load_existing_symbols(path: str) -> set:
    p = Path(path)
    if not p.exists(): return set()
    try:
        df = pd.read_csv(p, usecols=["SYMBOL"])
        return set(df["SYMBOL"].dropna().str.strip().str.upper().tolist())
    except: return set()


def build_master_csv(nse_df, symbols, output_path,
                     delay=REQUEST_DELAY, force_refresh=False):
    """
    Identical row-by-row DictWriter behaviour as the original working code:
      - Appends each scraped row immediately to disk (Ctrl+C safe, resume-safe)
      - Skips symbols already in the CSV
      - Tracks DATE_DOWNLOADED per row

    BUG-4 fix (column truncation) is handled with a post-loop re-merge:
      After all rows are written, we read the full CSV back, union its columns
      with any columns seen in new rows that the DictWriter may have dropped
      (because they appeared after the header was written), and rewrite once.
      This keeps the streaming write intact while ensuring no column is lost.
    """
    out_path = Path(output_path)
    today    = datetime.now().strftime("%Y-%m-%d")
    existing = _load_existing_symbols(output_path) if not force_refresh else set()
    if existing: log.info(f"  CSV already has {len(existing)} symbols — skipping those")

    todo = [s for s in symbols if s.upper() not in existing]
    if not todo:
        log.info("  All symbols done.")
        return pd.read_csv(output_path)

    log.info(f"  Need: {len(todo)}  |  Done: {len(existing)}")
    session = requests.Session()
    session.headers.update(SCREENER_HDR)

    write_header = not out_path.exists() or force_refresh
    if force_refresh and out_path.exists():
        out_path.unlink()
        write_header = True

    errors        = 0
    dw_fieldnames = None   # columns the DictWriter header was written with
    overflow_rows = {}     # {sym: {extra_col: val}} for late-appearing columns
    csv_file      = None

    try:
        csv_file = open(out_path, "a", newline="", encoding="utf-8")
        writer   = None

        for sym in tqdm(todo, desc="Screener.in", unit="stock"):
            row = scrape_screener(sym, session)
            row["DATE_DOWNLOADED"] = today

            if writer is None:
                # First row sets the DictWriter header — same as original code
                dw_fieldnames = list(row.keys())
                writer = csv.DictWriter(csv_file, fieldnames=dw_fieldnames,
                                        extrasaction="ignore", lineterminator="\n")
                if write_header:
                    writer.writeheader()
                    write_header = False
            else:
                # Save any keys that appeared AFTER the header was written
                # so we can merge them back into the CSV after the loop.
                extra = {k: v for k, v in row.items() if k not in dw_fieldnames}
                if extra:
                    overflow_rows[sym] = extra

            writer.writerow(row)
            csv_file.flush()

            if "SCREENER_ERROR" in row: errors += 1
            time.sleep(delay)

    except KeyboardInterrupt:
        log.warning("\n  ⚠ Interrupted — partial data saved to CSV")
    finally:
        if csv_file: csv_file.close()

    log.info(f"  ✓ {len(todo)-errors} new rows  |  Errors: {errors}")

    # ── Post-loop: read back, patch overflow columns with their actual values ──
    try:
        scraped_df = pd.read_csv(out_path)
    except Exception:
        scraped_df = pd.DataFrame()

    if scraped_df.empty:
        return scraped_df

    # Merge overflow columns — use pd.concat (not repeated inserts) to avoid
    # PerformanceWarning on highly-fragmented DataFrames.
    if overflow_rows:
        extra_cols: dict = {}
        for sym, extras in overflow_rows.items():
            idxs = scraped_df.index[scraped_df["SYMBOL"] == sym].tolist()
            for col, val in extras.items():
                if col not in extra_cols:
                    extra_cols[col] = {}
                for idx in idxs:
                    extra_cols[col][idx] = val
        if extra_cols:
            import pandas as _pd
            extra_df = _pd.DataFrame(extra_cols, index=scraped_df.index)
            scraped_df = _pd.concat([scraped_df, extra_df], axis=1)

    sym_col = next((c for c in nse_df.columns if "SYMBOL" in c.upper()), None)
    if sym_col:
        nse_extra = nse_df[[c for c in nse_df.columns
                             if c not in scraped_df.columns or c == sym_col]]
        master = scraped_df.merge(nse_extra, left_on="SYMBOL", right_on=sym_col, how="left")
    else:
        master = scraped_df

    master.to_csv(output_path, index=False)
    log.info(f"  ✓ Final CSV → {output_path}  ({len(master)} rows × {len(master.columns)} cols)")
    return master


# ══════════════════════════════════════════════════════════════════════════════
# STEP 4a — VALUATION SCORING
# ══════════════════════════════════════════════════════════════════════════════

SCORE_METRICS_FINANCIAL = [
    ("PB",                   True,  [0,0.5,1,1.5,2,3,5],   [10,9,7,5,3,1,0]),
    ("ROE_PCT",              False, [25,20,15,12,8,5],      [10,8,6,4,2,0]),
    ("DEBT_TO_EQUITY",       True,  [0,3,6,10,15,20],       [10,8,6,4,2,0]),
    ("NET_MARGIN_CALC_PCT",  False, [30,20,15,10,5],        [10,8,6,4,2,0]),
    ("REVENUE_CAGR_3YR",     False, [25,20,15,10,5,0],      [10,8,6,4,2,0]),
    ("PAT_CAGR_3YR",         False, [30,20,15,10,5,0],      [10,9,7,5,3,1,0]),
    ("DIV_YIELD_PCT",        False, [5,4,3,2,1,0.5],        [10,8,6,4,2,1]),
    ("PROMOTER_HOLDING",     False, [60,50,40,30],          [5,4,3,0]),
    ("CURRENT_RATIO",        False, [2,1.5,1.2,1],          [5,4,3,0]),
    ("EARNINGS_YIELD",       False, [10,7,5,3,2,1],         [10,8,6,4,2,0]),
    ("EPS_CAGR_3YR",         False, [30,20,15,10,5,0],      [10,8,6,4,2,0]),
]

SCORE_METRICS = [
    ("PE",                   True,  [0,10,15,20,25,30,40,60], [10,9,7,5,3,2,1,0]),
    ("PB",                   True,  [0,1,2,3,5,8,15],         [10,8,6,4,2,1,0]),
    ("EV_EBITDA",            True,  [0,6,10,15,20,30,50],     [10,9,7,5,3,1,0]),
    ("PRICE_TO_SALES",       True,  [0,0.5,1,2,4,8],          [10,8,6,4,2,0]),
    ("PEG_RATIO",            True,  [0,0.5,1.0,1.5,2.0,3.0],  [10,8,6,4,2,0]),
    ("ROE_PCT",              False, [40,30,25,20,15,10,5],    [10,9,7,5,3,2,0]),
    ("ROCE_PCT",             False, [35,25,20,15,10,5],       [10,8,6,4,2,0]),
    ("OPM",                  False, [40,30,20,15,10,5],       [10,8,6,4,2,0]),
    ("NET_MARGIN",           False, [25,15,10,7,4,2],         [10,8,6,4,2,0]),
    ("EARNINGS_YIELD",       False, [10,7,5,3,2,1],           [10,8,6,4,2,0]),
    ("REVENUE_GROWTH_YOY",   False, [40,25,20,15,10,5,0],     [10,9,7,5,3,1,0]),
    ("PAT_GROWTH_YOY",       False, [50,30,20,15,10,5,0],     [10,9,7,5,3,1,0]),
    ("REVENUE_CAGR_3YR",     False, [25,20,15,10,5,0],        [10,8,6,4,2,0]),
    ("EPS_CAGR_3YR",         False, [30,20,15,10,5,0],        [10,8,6,4,2,0]),
    ("PAT_CAGR_3YR",         False, [30,20,15,10,5,0],        [10,8,6,4,2,0]),
    ("DEBT_TO_EQUITY",       True,  [0,0.1,0.3,0.5,1.0,2.0], [10,8,6,4,2,0]),
    ("NET_DEBT_CR",          True,  [-1e9,0,500,2000,5000,1e7],[10,8,5,3,1,0]),
    ("CF_FCF_CR",            False, [1000,500,200,50,0],      [10,8,6,3,0]),
    ("CF_OPERATING_CR",      False, [2000,500,100,0],         [10,7,4,0]),
    ("CF_FCF_3YR_AVG",       False, [500,200,50,0],           [10,7,4,0]),
    ("DIV_YIELD_PCT",        False, [5,4,3,2,1,0.5],          [10,8,6,4,2,1]),
    ("PROMOTER_HOLDING",     False, [70,60,50,40,30],         [5,4,3,2,0]),
    ("FII_HOLDING",          False, [30,20,10,5],             [5,4,3,0]),
    ("REVENUE_GROWTH_QOQ",   False, [20,10,5,0,-5],           [5,4,3,2,0]),
    ("PAT_GROWTH_QOQ",       False, [25,15,5,0,-5],           [5,4,3,2,0]),
]

MAX_SCORE = sum(max(pts) for _, _, _, pts in SCORE_METRICS)


def compute_scores(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Clamp CAGR/growth columns to [-50%, +150%] before scoring.
    # Values like 5000% (NFL, GOKULAGRO) come from a near-zero base year
    # and completely corrupt the FINAL_SCORE composite.
    # Clamp growth/CAGR inputs before scoring.
    # Revenue CAGR > 80% for 3 years is almost always a near-zero base artifact.
    # PAT CAGR > 100% is possible in a recovery year but not a reliable signal.
    # NaN (from _cagr_pct returning None for > 200%) is correct — leave as NaN.
    for col, lo, hi in [
        ("REVENUE_CAGR_3YR_PCT",   -30,  80),
        ("REVENUE_CAGR_5YR_PCT",   -30,  80),
        ("REVENUE_CAGR_10YR_PCT",  -20,  60),
        ("PAT_CAGR_3YR_PCT",       -30, 100),
        ("PAT_CAGR_5YR_PCT",       -30, 100),
        ("PAT_CAGR_10YR_PCT",      -20,  80),
        ("EPS_CAGR_3YR_PCT",       -30, 100),
        ("REVENUE_GROWTH_YOY_PCT", -50, 100),
        ("PAT_GROWTH_YOY_PCT",     -80, 150),
        ("REVENUE_GROWTH_QOQ_PCT", -50,  80),
        ("PAT_GROWTH_QOQ_PCT",     -80, 120),
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").clip(lo, hi)

    # FIX BUG-5: explicit preferred-column mapping for get_col
    PREFERRED_COL = {
        "OPM":         ["OPM_CALC_PCT", "OPM_AVG_3YR_PCT", "OPM_PCT"],
        "NET_MARGIN":  ["NET_MARGIN_CALC_PCT", "NPM_PCT"],
        "REVENUE_CAGR_3YR": ["REVENUE_CAGR_3YR_PCT"],
        "PAT_CAGR_3YR":     ["PAT_CAGR_3YR_PCT"],
        "EPS_CAGR_3YR":     ["EPS_CAGR_3YR_PCT"],  # may not exist yet
        "REVENUE_GROWTH_YOY": ["REVENUE_GROWTH_YOY_PCT"],
        "PAT_GROWTH_YOY":     ["PAT_GROWTH_YOY_PCT"],
        "EARNINGS_YIELD":     ["EARNINGS_YIELD_PCT"],
        "PROMOTER_HOLDING":   ["PROMOTER_HOLDING_PCT", "SH_PROMOTER_PCT_LATEST"],
        "FII_HOLDING":        ["FII_HOLDING_PCT", "SH_FII_PCT_LATEST"],
        "REVENUE_GROWTH_QOQ": ["REVENUE_GROWTH_QOQ_PCT"],
        "PAT_GROWTH_QOQ":     ["PAT_GROWTH_QOQ_PCT"],
        "CF_FCF_3YR_AVG":     ["CF_FCF_3YR_AVG_CR"],
    }

    def _fc(df, key):
        k = key.upper()
        if k in PREFERRED_COL:
            for pref in PREFERRED_COL[k]:
                if pref in df.columns: return pref
        for c in df.columns:
            if k == c.upper(): return c
        for c in df.columns:
            if k in c.upper(): return c
        return None

    def _brk(v, lower, thr, pts):
        try: v = float(v)
        except: return 0
        if pd.isna(v): return 0
        if lower:
            for i in range(len(thr)-1):
                if v <= thr[i+1]: return pts[i]
            return pts[-1]
        else:
            for i, t in enumerate(thr):
                if v >= t: return pts[i]
            return 0

    df = df[[c for c in df.columns if not c.startswith("SCORE_")]]
    scols = []
    for key, lb, thr, pts in SCORE_METRICS:
        col = _fc(df, key); sc = f"SCORE_{key}"; scols.append(sc)
        df[sc] = df[col].apply(lambda v: _brk(v, lb, thr, pts)) if col else 0

    fin_scols = []
    for key, lb, thr, pts in SCORE_METRICS_FINANCIAL:
        col = _fc(df, key); sc = f"FSCORE_{key}"; fin_scols.append(sc)
        df[sc] = df[col].apply(lambda v: _brk(v, lb, thr, pts)) if col else 0

    fin_max = sum(max(pts) for _, _, _, pts in SCORE_METRICS_FINANCIAL)

    def row_score(row):
        sector = str(row.get("SECTOR","")).upper()
        if any(f in sector for f in FINANCIAL_SECTORS):
            raw = sum(row.get(sc, 0) for sc in fin_scols)
            return round(raw / fin_max * MAX_SCORE, 2)
        return sum(row.get(sc, 0) for sc in scols)

    df["VALUATION_SCORE"]     = df.apply(row_score, axis=1)
    df["VALUATION_SCORE_PCT"] = (df["VALUATION_SCORE"] / MAX_SCORE * 100).round(1)
    df["VALUATION_RANK"]      = df["VALUATION_SCORE"].rank(ascending=False, method="min").astype(int)
    df.drop(columns=fin_scols, inplace=True, errors="ignore")
    return df


# ══════════════════════════════════════════════════════════════════════════════
# STEP 4b — FAIR VALUE MODELS
# ══════════════════════════════════════════════════════════════════════════════

def fv_graham(row):
    eps = _g(row, "PL_EPS_BASIC", "EPS_TTM")
    bv  = _g(row, "BOOK_VALUE")
    return round(math.sqrt(22.5 * eps * bv), 2) if eps and bv and eps > 0 and bv > 0 else None

def fv_lynch(row):
    eps = _g(row, "PL_EPS_BASIC", "EPS_TTM")
    g   = _g(row, "EPS_CAGR_3YR_PCT", "PAT_CAGR_3YR_PCT")
    g   = min(g, 25) if g else None
    return round(eps * g, 2) if eps and g and eps > 0 and g > 0 else None

def fv_dcf(row):
    fcf    = _g(row, "CF_FCF_CR", "CF_FCF_3YR_AVG_CR")
    shares = _g(row, "SHARES_CR")
    g_rev  = _g(row, "REVENUE_CAGR_3YR_PCT")
    g_pat  = _g(row, "PAT_CAGR_3YR_PCT")
    if not (fcf and shares and fcf > 0 and shares > 0): return None
    if (_safe(row.get("FCF_YIELD_PCT")) or 0) > 20: return None
    wacc   = get_wacc(row)
    tg     = min(TERMINAL_GROWTH, wacc - 0.03)
    fcf_ps = fcf / shares
    g1 = min((g_rev or g_pat or 8) / 100, 0.15); g2 = g1 / 2
    pv, cf = 0.0, fcf_ps
    for yr in range(1, 6):  cf *= (1 + g1); pv += cf / (1 + wacc) ** yr
    for yr in range(6, 11): cf *= (1 + g2); pv += cf / (1 + wacc) ** yr
    tv   = cf * (1 + tg) / (wacc - tg)
    pv  += tv / (1 + wacc) ** 10
    return round(pv, 2) if pv > 0 else None

def fv_ev_ebitda(row):
    ebitda = _g(row, "PL_EBITDA_CR")
    oi     = _g(row, "PL_OTHER_INCOME_CR")
    if ebitda and oi: ebitda = ebitda - oi
    oi_to_pbt = _safe(row.get("OTHER_INCOME_TO_PBT_PCT")) or 0
    if oi_to_pbt > 50: return None
    cash   = _g(row, "BS_CASH_CR") or 0
    debt   = _g(row, "BS_TOTAL_BORROWINGS_CR") or 0
    shares = _g(row, "SHARES_CR")
    if not (ebitda and shares and ebitda > 0 and shares > 0): return None
    roe  = _g(row, "ROE_PCT", "ROE_CALC_PCT") or 15
    roce = _g(row, "ROCE_PCT") or 15
    if oi_to_pbt > 30: roe = min(roe, 15); roce = min(roce, 15)
    sector = str(row.get("SECTOR", "")).upper()
    if any(e in sector for e in EPC_SECTORS):  multiple = 12
    elif roe > 40 or roce > 40:                multiple = 20
    elif roe > 20 or roce > 20:                multiple = 15
    else:                                      multiple = 10
    ev = multiple * ebitda + cash - debt
    return round(ev / shares, 2) if ev > 0 else None

def fv_pe_mean(row):
    eps = _g(row, "PL_EPS_BASIC", "EPS_TTM")
    ipe = _g(row, "INDUSTRY_PE")
    tpe = min(ipe, 60) if ipe and ipe > 5 else 25
    return round(eps * tpe, 2) if eps and eps > 0 else None

def fv_pb(row):
    bv  = _g(row, "BOOK_VALUE")
    roe = _g(row, "ROE_PCT", "ROE_CALC_PCT")
    if not (bv and roe and bv > 0 and roe > 0): return None
    coe = RISK_FREE + EQUITY_PREMIUM
    pb  = min(max((roe / 100) / coe, 0.5), 15)
    return round(bv * pb, 2)

def fv_ddm(row):
    eps    = _g(row, "PL_EPS_BASIC", "EPS_TTM")
    payout = _g(row, "PAYOUT_RATIO_PCT")
    dy     = _g(row, "DIV_YIELD_PCT")
    cmp    = _g(row, "CMP")
    g_pat  = _g(row, "PAT_CAGR_3YR_PCT")
    dps    = (cmp * dy / 100) if (cmp and dy) else (eps * payout / 100 if eps and payout else None)
    if not (dps and dps > 0): return None
    g   = min((g_pat or 7) / 100, 0.15)
    coe = RISK_FREE + EQUITY_PREMIUM
    if coe <= g: return None
    fv  = dps * (1 + g) / (coe - g)
    return round(fv, 2) if fv > 0 else None

def fv_epv(row):
    ebitda = _g(row, "PL_EBITDA_CR")
    oi     = _g(row, "PL_OTHER_INCOME_CR")
    if ebitda and oi: ebitda = ebitda - oi
    depr   = _g(row, "PL_DEPR_CR") or 0
    tax    = _g(row, "PL_TAX_RATE_PCT")
    cash   = _g(row, "BS_CASH_CR") or 0
    debt   = _g(row, "BS_TOTAL_BORROWINGS_CR") or 0
    shares = _g(row, "SHARES_CR")
    if not (ebitda and shares and ebitda > 0 and shares > 0): return None
    ebit  = ebitda - abs(depr)
    nopat = ebit * (1 - (tax or 25) / 100)
    if nopat <= 0: return None
    epv = (nopat / get_wacc(row) + cash - debt) / shares
    return round(epv, 2) if epv > 0 else None

def fv_earnings_yield(row):
    """
    FIX BUG-3: use get_wacc(row) as the required earnings yield floor
    instead of a fixed 9%.  This correctly adjusts for leverage, sector
    cyclicality, and quality.
    """
    eps = get_core_eps(row)
    if not eps or eps <= 0: return None
    cmp = _g(row, "CMP"); pe = _g(row, "PE")
    if cmp and cmp > 0 and (cmp / eps) < 5: return None
    # Loose sanity: our implied PE shouldn't be wildly below scraped PE
    if pe and pe > 0 and cmp and cmp > 0 and (cmp / eps) < pe * 0.2: return None
    required_yield = get_wacc(row)
    return round(eps / required_yield, 2)

def fv_roe_pb(row):
    bv  = _g(row, "BOOK_VALUE")
    roe = _g(row, "ROE_PCT", "ROE_CALC_PCT")
    if not (bv and roe and roe > 0): return None
    oi_to_pbt = _safe(row.get("OTHER_INCOME_TO_PBT_PCT")) or 0
    if oi_to_pbt > 30: roe = min(roe, 15)
    coe = RISK_FREE + EQUITY_PREMIUM
    justified_pb = min(max((roe / 100) / coe, 0.5), 6)
    return round(bv * justified_pb, 2)

def fv_fcf_yield(row):
    fcf    = _g(row, "CF_FCF_CR", "CF_FCF_3YR_AVG_CR")
    shares = _g(row, "SHARES_CR")
    if not (fcf and shares and fcf > 0 and shares > 0): return None
    if (_safe(row.get("FCF_YIELD_PCT")) or 0) > 20: return None
    return round((fcf / shares) / 0.08, 2)

def fv_owner_earnings(row):
    ebitda = _g(row, "PL_EBITDA_CR")
    oi     = _g(row, "PL_OTHER_INCOME_CR") or 0
    capex  = _g(row, "CF_CAPEX_CR") or 0
    wc     = _g(row, "CF_WC_CHANGE_CR") or 0
    tax    = _g(row, "PL_TAX_RATE_PCT") or 25
    shares = _g(row, "SHARES_CR")
    if not (ebitda and shares and shares > 0): return None
    if (_safe(row.get("FCF_YIELD_PCT")) or 0) > 20: return None
    ebitda -= oi
    oe = (ebitda - abs(capex) - abs(wc)) * (1 - tax / 100)
    if oe <= 0: return None
    return round((oe / get_wacc(row)) / shares, 2)

def fv_growth_pe(row):
    eps = get_core_eps(row)
    g   = _g(row, "PAT_CAGR_3YR_PCT", "EPS_CAGR_3YR_PCT")
    if not (eps and g and eps > 0 and g > 0): return None
    return round(eps * min(g, 20) * 1.1, 2)

def fv_dcf_2stage(row):
    fcf    = _g(row, "CF_FCF_CR", "CF_FCF_3YR_AVG_CR")
    shares = _g(row, "SHARES_CR")
    g      = _g(row, "REVENUE_CAGR_3YR_PCT", "PAT_CAGR_3YR_PCT")
    if not (fcf and shares and fcf > 0 and shares > 0): return None
    if (_safe(row.get("FCF_YIELD_PCT")) or 0) > 20: return None
    wacc   = get_wacc(row)
    tg     = min(0.05, wacc - 0.03)
    g1     = min((g or 8) / 100, 0.12); g2 = tg
    fcf_ps = fcf / shares
    pv = 0; cf = fcf_ps
    for i in range(1, 6): cf *= (1 + g1); pv += cf / ((1 + wacc) ** i)
    tv  = cf * (1 + g2) / (wacc - g2)
    pv += tv / ((1 + wacc) ** 5)
    return round(pv, 2)


# ── Model registry ────────────────────────────────────────────────────────────
FV_MODELS_ALL = {
    "FV_GRAHAM":         fv_graham,
    "FV_LYNCH":          fv_lynch,
    "FV_DCF":            fv_dcf,
    "FV_EPV":            fv_epv,
    "FV_EV_EBITDA":      fv_ev_ebitda,
    "FV_PE_MEAN":        fv_pe_mean,
    "FV_PB":             fv_pb,
    "FV_DDM":            fv_ddm,
    "FV_EARNINGS_YIELD": fv_earnings_yield,
    "FV_ROE_PB":         fv_roe_pb,
    "FV_FCF_YIELD":      fv_fcf_yield,
    "FV_OWNER_EARNINGS": fv_owner_earnings,
    "FV_GROWTH_PE":      fv_growth_pe,
    "FV_DCF_2STAGE":     fv_dcf_2stage,
}

FV_MODELS_FINANCIAL_ALLOWED = {"FV_PB", "FV_DDM"}


def sanitize_fv_pairs(row, pairs):
    clean     = []
    fcf_yield = _safe(row.get("FCF_YIELD_PCT"))
    growth    = _safe(row.get("PAT_CAGR_3YR_PCT"))
    sector    = str(row.get("SECTOR", "")).upper()
    is_fin    = any(f in sector for f in FINANCIAL_SECTORS)

    for col, v in pairs:
        if is_fin and col not in FV_MODELS_FINANCIAL_ALLOWED:
            continue
        if any(d in sector for d in DEEP_CYCLICAL_SECTORS):
            if col in ["FV_DCF", "FV_DCF_2STAGE", "FV_FCF_YIELD", "FV_OWNER_EARNINGS"]:
                continue
            if col == "FV_EV_EBITDA":
                opm = _safe(row.get("OPM_AVG_3YR_PCT")) or 0
                if opm < 10: continue
        if any(c in sector for c in CYCLICAL_SECTORS):
            if col in ["FV_DCF", "FV_DCF_2STAGE"]:
                continue
        if col in ["FV_DCF", "FV_DCF_2STAGE", "FV_FCF_YIELD", "FV_OWNER_EARNINGS"]:
            if fcf_yield is not None and (fcf_yield > 15 or fcf_yield < -5):
                continue
        if col in ["FV_LYNCH", "FV_GROWTH_PE"]:
            if growth is None or growth < 0 or growth > 35:
                continue
        clean.append((col, v))

    return clean


def compute_fair_values(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    stale = [c for c in df.columns if c.startswith("FV_") or c in [
        "COMPOSITE_FAIR_VALUE","UPSIDE_PCT","MARGIN_OF_SAFETY_PCT",
        "FV_GRADE","FV_COUNT","FV_FAIL_REASON"
    ]]
    df.drop(columns=stale, inplace=True, errors="ignore")

    for name, func in FV_MODELS_ALL.items():
        df[name] = df.apply(func, axis=1)

    fv_cols = list(FV_MODELS_ALL.keys())

    # ══════════════════════════════════════════════════════════════════════
    # HARD GATE SYSTEM  (redesigned for higher coverage)
    #
    # Philosophy: gates block only stocks where FV is MEANINGLESS,
    # not stocks that are cheap/bad.  Bad stocks should still get a FV
    # (so the scoring can rank them low) — only truly broken data should block.
    #
    # Gates:
    #   G1  Latest core PAT must be positive
    #   G2  Need ≥3 years of PAT history in the CSV
    #   G3  Not > 60% of history is loss-making
    #   G4  PAT spike guard — but only blocks if spike > 8x AND recovery is fake
    #   G5  Financial sector: negative ROE → block
    #   G5b Financial sector: no book value → block
    #   G6  Non-financial: OPM must be between -50% and 1000%
    #       (scraper parse errors give values like 2371% — those are garbage)
    #   G7  Implied PE must be 0.5x–500x  (very wide — catches only absurdities)
    #   G8  REMOVED (was too aggressive; caused false positives for OI-adjusted EPS)
    # ══════════════════════════════════════════════════════════════════════
    def run_gates(row):
        sector     = str(row.get("SECTOR", "")).upper()
        is_fin     = any(f in sector for f in FINANCIAL_SECTORS)
        pat_latest = _safe(row.get("PL_PAT_CR"))
        roe        = _safe(row.get("ROE_PCT"))
        opm        = _safe(row.get("OPM_CALC_PCT")) or _safe(row.get("OPM_PCT"))

        # G1 — Latest PAT must be positive
        # NaN means scrape failure (page not loaded / table not found).
        # Negative means actual loss year. Both block FV, but for different reasons.
        # Fallback chain: PL_PAT_CR → PL_PAT_REPORTED_CR → TTM_PAT_CR
        if pat_latest is None:
            pat_rep = _safe(row.get("PL_PAT_REPORTED_CR"))
            if pat_rep is not None:
                pat_latest = pat_rep
            else:
                ttm_pat = _safe(row.get("TTM_PAT_CR"))
                if ttm_pat is not None:
                    pat_latest = ttm_pat
        if pat_latest is None:
            return False, "G1:SCRAPE_FAILURE"
        if pat_latest <= 0:
            return False, "G1:LOSS_YEAR"

        # G2 — Need ≥3 years of PAT history in the annual columns.
        # Also: if we have ≥4 quarterly PAT columns with values, treat that as
        # sufficient history for a recent IPO (they won't have 3 annual years yet).
        pat_keys = sorted([c for c in row.index if re.search(r"^A_PL_PAT_CR_[A-Z]{3}\d{4}$", c)])
        if not pat_keys:
            pat_keys = sorted([c for c in row.index
                                if c.startswith("A_PL_PAT_CR_") and re.search(r"\d{4}", c)])
        pat_vals = [_safe(row.get(k)) for k in pat_keys]
        pat_vals = [p for p in pat_vals if p is not None]
        using_quarterly_proxy = False
        if len(pat_vals) < 3:
            # Fallback: quarterly PAT columns, sorted chronologically
            q_pat_keys = _sort_q_keys_chrono([c for c in row.index
                                               if re.search(r"^Q_PL_PAT_CR_Q", c)])
            q_pat_vals = [_safe(row.get(k)) for k in q_pat_keys]
            q_pat_vals = [p for p in q_pat_vals if p is not None]
            if len(q_pat_vals) >= 4:
                pat_vals = q_pat_vals
                using_quarterly_proxy = True
            else:
                return False, "G2:INSUFFICIENT_HISTORY"

        # G3 — Not majority loss-making (> 60% negative years)
        neg_ratio = sum(1 for p in pat_vals if p <= 0) / len(pat_vals)
        if neg_ratio > 0.60:
            return False, f"G3:MOSTLY_LOSS_MAKING({neg_ratio:.0%}_negative)"

        # G4 — Spike guard: only blocks if PAT latest > 8x prior 3yr avg.
        # Skip when using quarterly proxy — seasonal businesses look like spikes.
        if not using_quarterly_proxy:
            avg_3yr = np.mean([p for p in pat_vals[-4:-1] if p and p > 0]) if len(pat_vals) >= 4 else None
            if avg_3yr and avg_3yr > 0 and pat_latest > avg_3yr * 8:
                return False, "G4:PAT_SPIKE_VS_HISTORY"

        # G5 — Financial sector: negative ROE → block
        if is_fin and roe is not None and roe < 0:
            return False, "G5:FINANCIAL_NEGATIVE_ROE"

        # G5b — Financial sector: no book value → block
        if is_fin:
            bv = _safe(row.get("BOOK_VALUE"))
            if not bv or bv <= 0:
                return False, "G5b:FINANCIAL_NO_BOOK_VALUE"

        # G6 — OPM sanity: only block obviously corrupt values
        #       OPM > 100% can be legitimate (software, exchanges), so we only
        #       block truly garbage values like 2371% that come from scrape errors
        #       where a revenue row was mistakenly read as EBITDA.
        if not is_fin and opm is not None:
            if opm < -50:          # deeply negative → genuinely loss-making at operating level
                return False, "G6:SEVERE_NEGATIVE_OPM"
            if opm > 1000:         # scraper parse error (e.g. ARVIND 2371%)
                return False, "G6:OPM_PARSE_ERROR"
        # Note: opm == None is now ALLOWED; models that need it simply return None

        # G7 — Implied PE sanity (very wide — 0.5x to 500x)
        mc  = _safe(row.get("MARKET_CAP_CR"))
        pat = _safe(row.get("PL_PAT_CR"))
        if mc and pat and pat > 0:
            implied_pe = mc / pat
            if implied_pe < 0.5 or implied_pe > 500:
                return False, f"G7:INSANE_IMPLIED_PE({implied_pe:.1f}x)"

        return True, "OK"

    # ══════════════════════════════════════════════════════════════════════
    # COMPOSITE FAIR VALUE
    # ══════════════════════════════════════════════════════════════════════
    def compute_composite(row):
        # 1. HARD GATES
        passed, _ = run_gates(row)
        if not passed:
            return None

        cmp_val = _safe(row.get("CMP"))
        sector  = str(row.get("SECTOR", "")).upper()

        # 2. COLLECT ALL VALID MODEL OUTPUTS
        pairs = []
        for c in fv_cols:
            v = row.get(c)
            if pd.notna(v):
                try:
                    v = float(v)
                    if v > 0:
                        pairs.append((c, v))
                except:
                    continue

        # 3. SANITIZE (sector/FCF/growth filters)
        clean_pairs = sanitize_fv_pairs(row, pairs)
        values = [v for _, v in clean_pairs]

        if not values:
            return None

        # 4. PE ANCHOR — always add an EPS-based anchor as a reference point
        eps = _safe(row.get("CORE_EPS")) or _safe(row.get("PL_EPS_BASIC"))
        if eps and eps > 0:
            if any(d in sector for d in DEEP_CYCLICAL_SECTORS):
                pe_anchor = 18
            elif any(c in sector for c in CYCLICAL_SECTORS):
                pe_anchor = 15
            else:
                pe_anchor = 22
            values.append(eps * pe_anchor)

        # 5. NEED AT LEAST 2 VALUES
        if len(values) < 2:
            return None

        # 6. CMP-BASED SANITY FILTER — remove extreme outliers vs market price
        #    Only apply if CMP is available.  Wide bounds: 10% to 400% of CMP.
        if cmp_val and cmp_val > 0:
            values = [v for v in values if cmp_val * 0.10 <= v <= cmp_val * 4.0]

        if len(values) < 2:
            # Fallback: if CMP filter wiped everything, use pre-filter median
            # capped at 2x CMP (better than returning None)
            all_vals = [v for _, v in clean_pairs]
            if eps and eps > 0:
                pe_a = 18 if any(d in sector for d in DEEP_CYCLICAL_SECTORS) else                        15 if any(c in sector for c in CYCLICAL_SECTORS) else 22
                all_vals.append(eps * pe_a)
            if len(all_vals) >= 2 and cmp_val and cmp_val > 0:
                med = float(np.median(all_vals))
                return round(min(med, cmp_val * 2.0), 2)
            return None

        # 7. OUTLIER TRIMMING — remove top & bottom only if we have ≥5 values
        values = sorted(values)
        if len(values) >= 5:
            values = values[1:-1]   # trim one from each end

        # 8. QUALITY HAIRCUT — apply only for genuinely low-quality stocks
        #    Criteria: both low growth AND low OPM.  Single bad metric → no haircut.
        growth  = _safe(row.get("PAT_CAGR_3YR_PCT")) or 0
        opm_avg = _safe(row.get("OPM_AVG_3YR_PCT")) or _safe(row.get("OPM_PCT")) or 0
        opm_avg = min(opm_avg, 100)   # cap at 100 to handle parse garbage

        if growth < 3 and opm_avg < 6:
            values = [v * 0.85 for v in values]

        # After haircut, re-apply a loose floor (5% of CMP)
        if cmp_val and cmp_val > 0:
            values = [v for v in values if v >= cmp_val * 0.05]

        if not values:
            return None

        # 9. OTHER-INCOME CAP — cap FV at 3x BV when OI dominates profit
        oi_to_pbt = _safe(row.get("OTHER_INCOME_TO_PBT_PCT")) or 0
        bv        = _safe(row.get("BOOK_VALUE")) or 0
        if oi_to_pbt > 50 and bv > 0:
            capped = [v for v in values if v <= bv * 3]
            if capped:
                values = capped

        if not values:
            return None

        # 10. FINAL — use median for robustness
        try:
            return round(float(np.median(values)), 2)
        except:
            return None

    df["COMPOSITE_FAIR_VALUE"] = df.apply(compute_composite, axis=1)

    # Store gate failure reason
    def get_fail_reason(row):
        passed, reason = run_gates(row)
        return reason if not passed else ""
    df["FV_FAIL_REASON"] = df.apply(get_fail_reason, axis=1)

    df["FV_COUNT"] = df.apply(
        lambda row: sum(1 for c in fv_cols if pd.notna(row[c]) and row[c] > 0), axis=1)

    # FIX BUG-6: single authoritative UPSIDE write
    cmp_series = df["CMP"] if "CMP" in df.columns else None
    if cmp_series is not None:
        df["UPSIDE_PCT"]           = ((df["COMPOSITE_FAIR_VALUE"] - cmp_series) / cmp_series * 100).round(2)
        df["MARGIN_OF_SAFETY_PCT"] = df["UPSIDE_PCT"]
    else:
        df["UPSIDE_PCT"]           = None
        df["MARGIN_OF_SAFETY_PCT"] = None

    def fv_grade(v):
        if pd.isna(v): return "UNDETERMINED"
        try: v = float(v)
        except: return "UNDETERMINED"
        if v >= 40:  return "DEEP VALUE"
        if v >= 25:  return "UNDERVALUED"
        if v >= 10:  return "FAIR"
        if v >= -10: return "FULLY PRICED"
        return "OVERVALUED"

    df["FV_GRADE"] = df["MARGIN_OF_SAFETY_PCT"].apply(fv_grade)

    # Diagnostic print
    total      = len(df)
    with_fv    = df["COMPOSITE_FAIR_VALUE"].notna().sum()
    no_fv      = df["COMPOSITE_FAIR_VALUE"].isna().sum()
    log.info(f"  FV computed: {with_fv}/{total}  |  Disqualified (gate/model): {no_fv}")
    print(df[["SYMBOL", "CMP", "COMPOSITE_FAIR_VALUE", "UPSIDE_PCT", "FV_FAIL_REASON"]].head(15).to_string())

    return df


# ══════════════════════════════════════════════════════════════════════════════
# STEP 4c — FINAL SCORE
# ══════════════════════════════════════════════════════════════════════════════

def cyclical_penalty(row):
    sector = str(row.get("SECTOR", "")).upper()
    return -10 if any(c in sector for c in CYCLICAL_SECTORS) else 0

def earnings_volatility(row):
    vals = [row[c] for c in row.index if "A_PL_PAT_CR_" in c and pd.notna(row[c]) and row[c] > 0]
    if len(vals) < 3: return 0
    cv = np.std(vals) / np.mean(vals) if np.mean(vals) else 99
    if cv < 0.2: return 10
    if cv < 0.4: return 5
    if cv < 0.7: return 0
    if cv < 1.0: return -5
    return -10

def fcf_consistency(row):
    vals = [row[c] for c in row.index if "C_CF_FCF_CR_" in c and pd.notna(row[c])]
    if len(vals) < 3: return 0
    pos = sum(1 for v in vals if v > 0)
    if pos >= 5: return 10
    if pos >= 3: return 5
    if pos >= 1: return 0
    return -10

def fraud_flag(row):
    p = 0
    if (row.get("OTHER_INCOME_TO_PBT_PCT") or 0) > 50: p -= 10
    if (row.get("CWIP_TO_NETBLOCK_PCT")     or 0) > 50: p -= 5
    ocf = row.get("CF_OPERATING_CR"); pat = row.get("PL_PAT_CR")
    if ocf and pat and pat > 0 and ocf < pat * 0.5: p -= 10
    if (row.get("DEBTOR_DAYS") or 0) > 120: p -= 5
    return p

def missing_data_penalty(row):
    critical = ["PE","ROE_PCT","DEBT_TO_EQUITY","REVENUE_CAGR_3YR_PCT",
                "PAT_CAGR_3YR_PCT","COMPOSITE_FAIR_VALUE"]
    missing = sum(1 for f in critical
                  if row.get(f) is None or (isinstance(row.get(f), float) and pd.isna(row.get(f))))
    if missing >= 4: return -30
    if missing >= 2: return -15
    if missing >= 1: return -5
    return 0

def piotroski_score(row):
    score = 0
    if (row.get("PL_PAT_CR")           or 0) > 0:  score += 1
    if (row.get("CF_OPERATING_CR")     or 0) > 0:  score += 1
    if (row.get("ROE_PCT")             or 0) > 15: score += 1
    if (row.get("DEBT_TO_EQUITY")      or 1) < 0.5: score += 1
    if (row.get("CURRENT_RATIO")       or 0) > 1:  score += 1
    if (row.get("PROMOTER_HOLDING_PCT") or 0) > 50: score += 1
    if (row.get("OPM_CALC_PCT")        or 0) > 15: score += 1
    if (row.get("ASSET_TURNOVER")      or 0) > 1:  score += 1
    return score

def quality_penalty(row):
    p = 0
    pat          = _safe(row.get("PL_PAT_CR"))
    pat_reported = _safe(row.get("PL_PAT_REPORTED_CR"))
    roe          = _safe(row.get("ROE_PCT"))
    opm          = _safe(row.get("OPM_CALC_PCT")) or _safe(row.get("OPM_PCT"))
    rev_cagr     = _safe(row.get("REVENUE_CAGR_3YR_PCT"))
    pat_cagr     = _safe(row.get("PAT_CAGR_3YR_PCT"))
    eps_cagr     = _safe(row.get("EPS_CAGR_3YR_PCT"))
    de           = _safe(row.get("DEBT_TO_EQUITY"))
    promoter     = _safe(row.get("PROMOTER_HOLDING_PCT")) or _safe(row.get("SH_PROMOTER_PCT_LATEST"))
    ocf          = _safe(row.get("CF_OPERATING_CR"))
    oi_pbt       = _safe(row.get("OTHER_INCOME_TO_PBT_PCT")) or 0
    sector       = str(row.get("SECTOR","")).upper()
    is_fin       = any(f in sector for f in FINANCIAL_SECTORS)

    if pat is not None and pat <= 0:                 p -= 40
    elif pat_reported is not None and pat_reported <= 0: p -= 30

    if roe is not None and roe < 0:                  p -= 30
    elif roe is not None and roe < 5:                p -= 15

    if not is_fin:
        if opm is None:                              p -= 20
        elif opm < 0:                                p -= 25
        elif opm < 5:                                p -= 10

    if rev_cagr is not None and rev_cagr < 0:        p -= 15
    elif rev_cagr is not None and rev_cagr < 3:      p -= 5

    if pat_cagr is not None and pat_cagr < 0:        p -= 15
    if eps_cagr is not None and eps_cagr < 0:        p -= 10

    if not is_fin:
        if de is not None and de > 3:                p -= 15
        elif de is not None and de > 2:              p -= 10

    if promoter is not None and promoter == 0:       p -= 10
    elif promoter is not None and promoter < 10:     p -= 5

    if ocf is not None and pat is not None and pat > 0:
        if ocf < 0:                                  p -= 15
        elif ocf < pat * 0.3:                        p -= 10
        elif ocf < pat * 0.5:                        p -= 5

    if oi_pbt > 75:                                  p -= 20
    elif oi_pbt > 50:                                p -= 15
    elif oi_pbt > 30:                                p -= 5

    return p


def compute_final_score(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    stale = [c for c in df.columns if c in ["FINAL_SCORE","FINAL_RANK","GRADE","DATA_QUALITY","F_SCORE"]]
    df.drop(columns=stale, inplace=True, errors="ignore")

    # FIX BUG-5: explicit column resolution instead of loose substring match
    def _get(col_names, default=0):
        for name in col_names:
            if name in df.columns:
                return df[name].fillna(default)
        return pd.Series([default] * len(df), index=df.index)

    val_score  = _get(["VALUATION_SCORE_PCT"])
    mos        = _get(["MARGIN_OF_SAFETY_PCT"]).clip(-50, 100)
    roe        = _get(["ROE_PCT", "ROE_CALC_PCT"]).clip(-50, 100)
    roce       = _get(["ROCE_PCT"]).clip(0, 80)
    opm        = _get(["OPM_CALC_PCT", "OPM_AVG_3YR_PCT", "OPM_PCT"]).clip(-50, 100)
    npm        = _get(["NET_MARGIN_CALC_PCT", "NPM_PCT"]).clip(-50, 60)
    rev_growth = _get(["REVENUE_CAGR_3YR_PCT"]).clip(-50, 60)   # cap CAGR at 60%
    pat_growth = _get(["PAT_CAGR_3YR_PCT"]).clip(-50, 60)
    debt       = _get(["DEBT_TO_EQUITY"], default=1)
    interest   = _get(["INTEREST_COVERAGE"])

    quality = roe*0.3 + roce*0.3 + opm*0.2 + npm*0.2
    growth  = rev_growth*0.4 + pat_growth*0.6
    safety  = (1/(1+debt))*50 + interest.clip(0,10)*5

    df["F_SCORE"] = df.apply(piotroski_score, axis=1)

    df["FINAL_SCORE"] = (
        val_score * 0.20 +
        mos       * 0.20 +
        quality   * 0.25 +
        growth    * 0.15 +
        safety    * 0.10 +
        df["F_SCORE"] * 2.5 +
        df.apply(cyclical_penalty,    axis=1) +
        df.apply(earnings_volatility, axis=1) +
        df.apply(fcf_consistency,     axis=1) +
        df.apply(fraud_flag,          axis=1) +
        df.apply(missing_data_penalty,axis=1) +
        df.apply(quality_penalty,     axis=1)
    ).round(2)

    df["DATA_QUALITY"] = "GOOD"
    df.loc[df["COMPOSITE_FAIR_VALUE"].isna(), "DATA_QUALITY"] = "NO_VALID_MODELS"

    df["FINAL_RANK"] = df["FINAL_SCORE"].rank(ascending=False, method="min").astype(int)

    def grade(s):
        if pd.isna(s): return "F"
        if s >= 80:    return "A"
        if s >= 65:    return "B"
        if s >= 50:    return "C"
        if s >= 35:    return "D"
        return "E"

    df["GRADE"] = df["FINAL_SCORE"].apply(grade)
    return df


# ══════════════════════════════════════════════════════════════════════════════
# DISPLAY
# ══════════════════════════════════════════════════════════════════════════════

def print_summary(df: pd.DataFrame, n: int = 20):
    cols = ["SYMBOL","CMP","COMPOSITE_FAIR_VALUE","UPSIDE_PCT",
            "PE","ROE_PCT","DEBT_TO_EQUITY",
            "REVENUE_CAGR_3YR_PCT","PAT_CAGR_3YR_PCT",
            "FCF_YIELD_PCT","MARGIN_OF_SAFETY_PCT","FINAL_SCORE","GRADE"]
    cols = [c for c in cols if c in df.columns]
    top  = df.sort_values("FINAL_SCORE", ascending=False).head(n)
    print("\n" + "="*110)
    print(f"🏆 TOP {n} VALUE STOCKS")
    print("="*110)
    print(top[cols].to_string(index=False))

    bottom = df[df["COMPOSITE_FAIR_VALUE"].isna()][
        ["SYMBOL","ROE_PCT","OPM_CALC_PCT","PL_PAT_CR","FV_FAIL_REASON"]
    ].head(20)
    if not bottom.empty and "FV_FAIL_REASON" in df.columns:
        print("\n── Sample disqualified stocks (FV = None) ──")
        print(bottom.to_string(index=False))


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit",         type=int,   default=None)
    ap.add_argument("--delay",         type=float, default=REQUEST_DELAY)
    ap.add_argument("--output",        default=OUTPUT_FILE)
    ap.add_argument("--skip-fetch",    action="store_true")
    ap.add_argument("--force-refresh", action="store_true")
    ap.add_argument("--top",           type=int,   default=50)
    args = ap.parse_args()
    out  = args.output

    do_download = True
    if args.skip_fetch or SKIP_DOWNLOAD:
        do_download = False
        log.info("Download skipped")
    if args.force_refresh:
        do_download = True

    if do_download and not args.force_refresh:
        age = _csv_age_days(out)
        if age is not None and age < REFRESH_DAYS:
            log.info(f"  CSV is {age}d old — using existing data")
            do_download = True

    if do_download:
        nse_df  = fetch_nifty500()
        sc      = next((c for c in nse_df.columns if "SYMBOL" in c.upper()), nse_df.columns[0])
        symbols = nse_df[sc].dropna().unique().tolist()
        if args.limit: symbols = symbols[:args.limit]
        master = build_master_csv(nse_df, symbols, out,
                                   delay=args.delay,
                                   force_refresh=args.force_refresh)
    else:
        if not Path(out).exists():
            log.error(f"CSV not found: {out}"); sys.exit(1)
        master = pd.read_csv(out)

    if master.empty:
        log.error("No data."); sys.exit(1)

    df = master.copy()
    log.info("Scoring …")
    df = compute_scores(df)
    log.info("Fair values …")
    df = compute_fair_values(df)
    log.info("Final score …")
    df = compute_final_score(df)

    df.to_csv(out, index=False)
    log.info(f"  ✓ {out}  ({len(df)} rows × {len(df.columns)} cols)")
    print_summary(df, n=args.top)
    log.info("Done! 🎯")


if __name__ == "__main__":
    main()
