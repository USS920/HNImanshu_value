"""
Nifty 500 — COMPLETE Fundamental Data Pipeline
=================================================
Smart download logic:
  - Appends results row-by-row to CSV as each stock is scraped
  - Skips symbols already present in the CSV (resume-safe)
  - Tracks DATE_DOWNLOADED per row
  - SKIP_DOWNLOAD = True  → skip all downloading, go straight to analysis
  - Auto-redownload if CSV is older than REFRESH_DAYS (default 30)
  - Graceful Ctrl+C: saves whatever was collected before exit

Usage:
  pip install requests pandas beautifulsoup4 lxml tqdm
  python nifty500_pipeline.py                   # smart run
  python nifty500_pipeline.py --skip-fetch      # force skip download
  python nifty500_pipeline.py --force-refresh   # force full re-download
  python nifty500_pipeline.py --limit 20        # test 20 stocks
"""

import argparse, time, sys, re, io, math, logging, csv, os
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict

import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from tqdm import tqdm

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
# ★  USER CONTROLS  — edit these instead of using command-line flags
# ══════════════════════════════════════════════════════════════════════════════
SKIP_DOWNLOAD  = False   # True  → skip ALL downloading, jump to analysis
REFRESH_DAYS   = 30      # re-download if CSV is older than this many days
REQUEST_DELAY  = 1.5     # seconds between Screener.in requests
# ══════════════════════════════════════════════════════════════════════════════

# ── Network constants ─────────────────────────────────────────────────────────
INDEX = "niftymicrocap250"
NSE_CSV_URL  = f"https://nsearchives.nseindia.com/content/indices/ind_{INDEX}_list.csv"


INDEX = "nifty500"     #Keep this line as i need both
NSE_CSV_URL  = f"https://nsearchives.nseindia.com/content/indices/ind_{INDEX}list.csv"

OUTPUT_FILE    = f"{INDEX}_valuation.csv"
try:
    os.remove(OUTPUT_FILE)
except:
    pass

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

# ── Valuation model parameters ────────────────────────────────────────────────
WACC            = 0.12
TERMINAL_GROWTH = 0.05
RISK_FREE       = 0.07
EQUITY_PREMIUM  = 0.055


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
        log.info(f"  ✓ {len(df)} symbols from Nifty 500 CSV")
        return df
    except Exception as e:
        raise RuntimeError(f"Cannot fetch Nifty 500 from NSE archives: {e}") from e


# ══════════════════════════════════════════════════════════════════════════════
# PARSING HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _num(text):
    if text is None:
        return None
    t = str(text).strip()
    mult = 1
    if t.endswith("Cr"):   mult, t = 1e7,  t[:-2]
    elif t.endswith("L"):  mult, t = 1e5,  t[:-1]
    elif t.endswith("K"):  mult, t = 1e3,  t[:-1]
    try:
        return float(re.sub(r"[%,\s₹+]", "", t)) * mult
    except ValueError:
        return None

def _safe(v):
    if v is None:
        return None
    try:
        f = float(v)
        return None if (f != f or math.isinf(f)) else f
    except (TypeError, ValueError):
        return None

def _parse_table(section) -> pd.DataFrame:
    if section is None:
        return pd.DataFrame()
    tbl = section.find("table")
    if tbl is None:
        return pd.DataFrame()
    rows, headers = [], []
    for i, tr in enumerate(tbl.find_all("tr")):
        cells = [td.get_text(" ", strip=True) for td in tr.find_all(["th", "td"])]
        if i == 0:
            headers = cells
        elif cells:
            rows.append(cells)
    if not headers or not rows:
        return pd.DataFrame()
    w = max(len(r) for r in rows)
    headers = (headers + [""] * w)[:w]
    try:
        return pd.DataFrame(rows, columns=headers)
    except Exception:
        return pd.DataFrame(rows)

def _ycols(df):
    return [c for c in df.columns[1:] if re.search(r"\d{4}", str(c))]

def _qcols(df):
    return [c for c in df.columns[1:] if re.search(r"(Jan|Mar|Jun|Sep|Dec)\s*'?\d{2}", str(c), re.I)]

def _find_row(df, *keywords):
    for _, row in df.iterrows():
        label = str(row.iloc[0]).lower()
        if any(k.lower() in label for k in keywords):
            return row
    return None

def _val(row, col):
    if row is None or col not in row.index:
        return None
    return _num(str(row[col]))

def _cagr_pct(series):
    vals = [v for v in series if v is not None and not math.isnan(v) and v > 0]
    if len(vals) < 2:
        return None
    try:
        return round(((vals[-1] / vals[0]) ** (1 / (len(vals) - 1)) - 1) * 100, 2)
    except Exception:
        return None

def _avg(lst):
    vals = [v for v in lst if v is not None]
    return round(sum(vals) / len(vals), 2) if vals else None


# ══════════════════════════════════════════════════════════════════════════════
# STEP 2 — SCREENER.IN FULL SCRAPER
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
    (["investment property"],                                       "BS_INVEST_PROPERTY_CR"),
    (["goodwill"],                                                   "BS_GOODWILL_CR"),
    (["intangible"],                                                 "BS_INTANGIBLES_CR"),
    (["right-of-use", "right of use", "lease asset", "rou"],       "BS_ROU_ASSETS_CR"),
    (["total assets"],                                              "BS_TOTAL_ASSETS_CR"),
    (["short term borrowing", "short-term borrowing"],              "BS_ST_BORROWINGS_CR"),
    (["long term borrowing", "long-term borrowing"],                "BS_LT_BORROWINGS_CR"),
    (["total borrowing", "total debt"],                             "BS_TOTAL_BORROWINGS_CR"),
    (["lease liabilit"],                                             "BS_LEASE_LIAB_CR"),
    (["trade payables", "creditors"],                               "BS_TRADE_PAY_CR"),
    (["other current liabilit"],                                    "BS_OTHER_CURR_LIAB_CR"),
    (["total current liabilit"],                                    "BS_TOTAL_CURR_LIAB_CR"),
    (["share capital", "paid up"],                                   "BS_SHARE_CAPITAL_CR"),
    (["reserves", "surplus", "other equity"],                       "BS_RESERVES_CR"),
    (["total equity", "shareholders", "networth", "net worth"],     "BS_TOTAL_EQUITY_CR"),
]

PL_ROW_MAP = [
    (["sales", "revenue from operations", "net revenue"],           "PL_REVENUE_CR"),
    (["other income"],                                               "PL_OTHER_INCOME_CR"),
    (["total income", "total revenue"],                              "PL_TOTAL_INCOME_CR"),
    (["operating profit", "ebitda", "ebita"],                       "PL_EBITDA_CR"),
    (["interest", "finance cost", "finance charges"],               "PL_INTEREST_CR"),
    (["depreciation", "amortisation", "amortization"],              "PL_DEPR_CR"),
    (["exceptional item"],                                           "PL_EXCEPTIONAL_CR"),
    (["profit before tax", "pbt"],                                   "PL_PBT_CR"),
    (["tax %", "tax rate"],                                          "PL_TAX_RATE_PCT"),
    (["tax", "income tax"],                                          "PL_TAX_AMOUNT_CR"),
    (["net profit", "pat", "profit after tax"],                     "PL_PAT_CR"),
    (["eps (basic)", "basic eps"],                                   "PL_EPS_BASIC"),
    (["eps (diluted)", "diluted eps"],                               "PL_EPS_DILUTED"),
    (["eps"],                                                         "PL_EPS_BASIC"),
    (["dividend"],                                                    "PL_DIVIDEND_CR"),
    (["opm %", "opm%", "operating margin"],                         "PL_OPM_PCT"),
]

CF_ROW_MAP = [
    (["cash from operating", "operating activities"],               "CF_OPERATING_CR"),
    (["cash from investing", "investing activities"],               "CF_INVESTING_CR"),
    (["cash from financing", "financing activities"],               "CF_FINANCING_CR"),
    (["purchase of fixed", "capex", "capital expenditure",
      "purchase of property", "purchase of ppe"],                   "CF_CAPEX_CR"),
    (["dividend paid", "dividends paid"],                           "CF_DIVID_PAID_CR"),
    (["interest received", "interest income received"],             "CF_INTEREST_RECV_CR"),
    (["dividend received", "dividends received"],                   "CF_DIVID_RECV_CR"),
    (["change in working capital", "working capital changes"],      "CF_WC_CHANGE_CR"),
    (["change in trade receivable", "change in debtors"],          "CF_WC_RECV_CHG_CR"),
    (["change in inventori"],                                        "CF_WC_INVENT_CHG_CR"),
    (["change in trade payable", "change in creditors"],           "CF_WC_PAY_CHG_CR"),
    (["net change in cash", "net increase in cash"],               "CF_NET_CASH_CHG_CR"),
    (["free cash flow", "fcf"],                                      "CF_FCF_CR"),
]


def _map_rows(df, row_map, col_key):
    result = {}
    assigned = set()
    for keywords, col_name in row_map:
        if col_name in assigned:
            continue
        row = _find_row(df, *keywords)
        if row is not None:
            result[col_name] = _val(row, col_key)
            assigned.add(col_name)
    return result

def _map_all_years(df, row_map, ycols, prefix=""):
    result = {}
    for keywords, base_col in row_map:
        row = _find_row(df, *keywords)
        if row is None:
            continue
        for yc in ycols:
            yr_label = re.sub(r"[^A-Z0-9]", "", yc.upper())
            col = f"{prefix}{base_col}_{yr_label}" if prefix else f"{base_col}_{yr_label}"
            result[col] = _val(row, yc)
    return result

def _all_q_rows(df, row_map, qcols, prefix=""):
    result = {}
    for keywords, base_col in row_map:
        row = _find_row(df, *keywords)
        if row is None:
            continue
        for qc in qcols:
            ql = re.sub(r"['\s]", "", qc.upper())
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
            if resp.status_code == 404:
                continue
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
    if h1:
        r["COMPANY_NAME"] = h1.get_text(strip=True)

    for tag in soup.select("a[href*='/company/']"):
        if "sector" in str(tag.get("href", "")).lower() or "industry" in str(tag.get("href","")).lower():
            r["SECTOR"] = tag.get_text(strip=True)
            break

    for li in soup.select("ul.company-ratios li, #top-ratios li, .company-ratios li"):
        nt = li.find("span", class_="name")
        vt = li.find("span", class_="number")
        if not nt or not vt:
            continue
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

    sec_pl   = soup.find("section", id="profit-loss")
    df_pl    = _parse_table(sec_pl)
    ycols_pl = _ycols(df_pl)

    if ycols_pl:
        ly = ycols_pl[-1]
        r.update(_map_rows(df_pl, PL_ROW_MAP, ly))
        r.update(_map_all_years(df_pl, PL_ROW_MAP, ycols_pl, prefix="A_"))

        rev_row = _find_row(df_pl, "sales", "revenue from operations", "net revenue")
        pat_row = _find_row(df_pl, "net profit", "pat")
        eps_row = _find_row(df_pl, "eps")

        if len(ycols_pl) >= 2:
            py = ycols_pl[-2]
            rp = _val(rev_row, py); rl = _val(rev_row, ly)
            pp = _val(pat_row, py); pl2 = _val(pat_row, ly)
            if rp and rl and rp != 0:
                r["REVENUE_GROWTH_YOY_PCT"] = round((rl - rp) / abs(rp) * 100, 2)
            if pp and pl2 and pp != 0:
                r["PAT_GROWTH_YOY_PCT"] = round((pl2 - pp) / abs(pp) * 100, 2)

        for yrs, lbl in [(3,"3YR"), (5,"5YR"), (10,"10YR")]:
            if len(ycols_pl) >= yrs + 1:
                sl = ycols_pl[-(yrs+1):]
                r[f"REVENUE_CAGR_{lbl}_PCT"] = _cagr_pct([_val(rev_row, c) for c in sl])
                r[f"PAT_CAGR_{lbl}_PCT"]     = _cagr_pct([_val(pat_row, c) for c in sl])
                r[f"EPS_CAGR_{lbl}_PCT"]     = _cagr_pct([_val(eps_row, c) for c in sl])

        opm_row = _find_row(df_pl, "opm %", "operating margin")
        if opm_row is not None:
            opm_vals = [_val(opm_row, c) for c in ycols_pl]
            r["OPM_AVG_3YR_PCT"] = _avg(opm_vals[-3:])
            r["OPM_AVG_5YR_PCT"] = _avg(opm_vals[-5:])

        rl2 = _safe(r.get("PL_REVENUE_CR"))
        el  = _safe(r.get("PL_EBITDA_CR"))
        pl3 = _safe(r.get("PL_PAT_CR"))
        if el and rl2 and rl2 != 0:
            r["OPM_CALC_PCT"]       = round(el / rl2 * 100, 2)
        if pl3 and rl2 and rl2 != 0:
            r["NET_MARGIN_CALC_PCT"] = round(pl3 / rl2 * 100, 2)
        int_ = _safe(r.get("PL_INTEREST_CR"))
        if el and int_ and int_ > 0:
            r["INTEREST_COVERAGE"] = round(el / int_, 2)

    sec_q  = soup.find("section", id="quarters")
    df_q   = _parse_table(sec_q)
    qcols  = _qcols(df_q)

    if qcols:
        lq = qcols[-1]
        r["LATEST_QUARTER"] = lq
        r.update({f"Q_{k}": v for k, v in _map_rows(df_q, PL_ROW_MAP, lq).items()})
        r.update(_all_q_rows(df_q, PL_ROW_MAP, qcols, prefix="Q_"))

        if len(qcols) >= 2:
            pq = qcols[-2]
            qrev_r = _find_row(df_q, "sales", "revenue")
            qpat_r = _find_row(df_q, "net profit", "pat")
            lqr = _val(qrev_r, lq); pqr = _val(qrev_r, pq)
            lqp = _val(qpat_r, lq); pqp = _val(qpat_r, pq)
            if pqr and lqr and pqr != 0:
                r["REVENUE_GROWTH_QOQ_PCT"] = round((lqr - pqr) / abs(pqr) * 100, 2)
            if pqp and lqp and pqp != 0:
                r["PAT_GROWTH_QOQ_PCT"] = round((lqp - pqp) / abs(pqp) * 100, 2)

        if len(qcols) >= 4:
            qrev_r = _find_row(df_q, "sales", "revenue")
            qpat_r = _find_row(df_q, "net profit", "pat")
            r["TTM_REVENUE_CR"] = sum(v for c in qcols[-4:] if (v := _val(qrev_r, c)) is not None) or None
            r["TTM_PAT_CR"]     = sum(v for c in qcols[-4:] if (v := _val(qpat_r, c)) is not None) or None

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
        if rev and ta and ta > 0:
            r["ASSET_TURNOVER"] = round(rev / ta, 2)

        nb = _safe(r.get("BS_NET_BLOCK_CR"))
        if rev and nb and nb > 0:
            r["FIXED_ASSET_TURNOVER"] = round(rev / nb, 2)

        recv = _safe(r.get("BS_TRADE_RECV_CR"))
        if recv and rev and rev > 0:
            r["RECV_TO_SALES"] = round(recv / rev * 100, 2)
            if not r.get("DEBTOR_DAYS"):
                r["DEBTOR_DAYS"] = round(recv / rev * 365, 1)

        inv = _safe(r.get("BS_INVENTORIES_CR"))
        if inv and rev and rev > 0 and not r.get("INVENTORY_DAYS"):
            r["INVENTORY_DAYS"] = round(inv / rev * 365, 1)

        pay = _safe(r.get("BS_TRADE_PAY_CR"))
        if pay and rev and rev > 0 and not r.get("PAYABLE_DAYS"):
            r["PAYABLE_DAYS"] = round(pay / rev * 365, 1)

        dd = _safe(r.get("DEBTOR_DAYS"))
        id_ = _safe(r.get("INVENTORY_DAYS"))
        pd_ = _safe(r.get("PAYABLE_DAYS"))
        if dd is not None and id_ is not None and pd_ is not None and not r.get("CASH_CONVERSION_CYCLE"):
            r["CASH_CONVERSION_CYCLE"] = round(dd + id_ - pd_, 1)

        mc  = _safe(r.get("MARKET_CAP_CR"))
        cmp = _safe(r.get("CMP"))
        if mc and cmp and cmp > 0:
            r["SHARES_CR"] = mc / cmp

        _eq_bs  = _safe(r.get("BS_TOTAL_EQUITY_CR"))
        _pat_bs = _safe(r.get("PL_PAT_CR"))
        if _pat_bs and _eq_bs and _eq_bs > 0:
            r["ROE_CALC_PCT"] = round(_pat_bs / _eq_bs * 100, 2)

    roe_vals = []
    for yc in (ycols_bs if ycols_bs else []):
        yr_lbl = re.sub(r"[^A-Z0-9]", "", yc.upper())
        pat_y  = _safe(r.get(f"A_PL_PAT_CR_{yr_lbl}"))
        eq_y   = _safe(r.get(f"B_BS_TOTAL_EQUITY_CR_{yr_lbl}"))
        if pat_y and eq_y and eq_y > 0:
            roe_vals.append(pat_y / eq_y * 100)
        else:
            roe_vals.append(None)

    if roe_vals:
        r["ROE_AVG_3YR_PCT"]  = _avg([v for v in roe_vals[-3:]  if v is not None])
        r["ROE_AVG_5YR_PCT"]  = _avg([v for v in roe_vals[-5:]  if v is not None])
        r["ROE_AVG_10YR_PCT"] = _avg([v for v in roe_vals[-10:] if v is not None])

    oi   = _safe(r.get("PL_OTHER_INCOME_CR"))
    tr   = _safe(r.get("PL_TAX_RATE_PCT"))
    _pat = _safe(r.get("PL_PAT_CR"))
    _eq  = _safe(r.get("BS_TOTAL_EQUITY_CR"))
    if _pat and oi and _eq and _eq > 0 and tr is not None:
        _pat_adj = _pat - oi * (1 - tr / 100)
        r["ROE_ADJ_EX_OTHER_INCOME_PCT"] = round(_pat_adj / _eq * 100, 2)

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

    sec_sh = soup.find("section", id="shareholding")
    df_sh  = _parse_table(sec_sh)
    if not df_sh.empty:
        sh_qcols = _qcols(df_sh)
        if not sh_qcols:
            sh_qcols = [c for c in df_sh.columns[1:] if re.search(r"\d{2}", str(c))]
        sh_map = {
            "promoter": "SH_PROMOTER_PCT",
            "fii":      "SH_FII_PCT",
            "dii":      "SH_DII_PCT",
            "public":   "SH_PUBLIC_PCT",
            "govt":     "SH_GOVT_PCT",
        }
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
        if prom:
            r["FREE_FLOAT_PCT"] = round(100 - prom, 2)

    sec_seg = (soup.find("section", id="segments") or
               soup.find("section", id="segment") or
               soup.find("div",     id="segment-results"))
    if sec_seg:
        df_seg = _parse_table(sec_seg)
        if not df_seg.empty:
            seg_ycols = _ycols(df_seg)
            if seg_ycols:
                ls = seg_ycols[-1]
                for _, seg_row in df_seg.iterrows():
                    seg_name = re.sub(r"[^A-Z0-9]", "_",
                                      str(seg_row.iloc[0]).upper().strip()).strip("_")[:30]
                    if seg_name:
                        r[f"SEG_{seg_name}_LATEST"] = _val(seg_row, ls)
                        for sc in seg_ycols[-5:]:
                            yr_lbl = re.sub(r"[^A-Z0-9]", "", sc.upper())
                            r[f"SEG_{seg_name}_{yr_lbl}"] = _val(seg_row, sc)

    _df_pl_safe = df_pl  if not df_pl.empty  else pd.DataFrame()
    _yc_pl_safe = ycols_pl if ycols_pl else []
    div_row = _find_row(_df_pl_safe, "dividend")
    if div_row is not None and _yc_pl_safe:
        div_vals = [_val(div_row, c) for c in _yc_pl_safe]
        r["DIVIDEND_CONSECUTIVE_YRS"] = sum(1 for v in div_vals if v and v > 0)
        r["DIVIDEND_UNINTERRUPTED"]   = all(v and v > 0 for v in div_vals) if div_vals else False

    exc_row = _find_row(_df_pl_safe, "exceptional")
    if exc_row is not None and _yc_pl_safe:
        ly2 = _yc_pl_safe[-1]
        r["HAS_EXCEPTIONAL_ITEMS"] = _val(exc_row, ly2) is not None
        r["EXCEPTIONAL_AMOUNT_CR"] = _val(exc_row, ly2)

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
# STEP 3 — SMART BUILD: append row-by-row, skip already-downloaded, track date
# ══════════════════════════════════════════════════════════════════════════════

def _csv_age_days(path: str) -> float | None:
    """Return age of CSV in days based on oldest DATE_DOWNLOADED value, or file mtime."""
    p = Path(path)
    if not p.exists():
        return None
    try:
        df = pd.read_csv(p, usecols=["DATE_DOWNLOADED"], nrows=500)
        if "DATE_DOWNLOADED" in df.columns:
            dates = pd.to_datetime(df["DATE_DOWNLOADED"], errors="coerce").dropna()
            if not dates.empty:
                oldest = dates.min()
                return (datetime.now() - oldest.to_pydatetime()).days
    except Exception:
        pass
    # Fallback: file modification time
    mtime = datetime.fromtimestamp(p.stat().st_mtime)
    return (datetime.now() - mtime).days


def _load_existing_symbols(path: str) -> set:
    """Return set of SYMBOL values already present in the CSV."""
    p = Path(path)
    if not p.exists():
        return set()
    try:
        df = pd.read_csv(p, usecols=["SYMBOL"])
        return set(df["SYMBOL"].dropna().str.strip().str.upper().tolist())
    except Exception:
        return set()


def build_master_csv(nse_df: pd.DataFrame,
                     symbols: list,
                     output_path: str,
                     delay: float = REQUEST_DELAY,
                     force_refresh: bool = False) -> pd.DataFrame:
    """
    Scrape symbols and append each row immediately to the CSV.

    Logic:
      - If a symbol is already in the CSV → skip (unless force_refresh)
      - Writes header on first row, then appends without re-writing header
      - Stamps DATE_DOWNLOADED on every row
      - Ctrl+C safe: whatever was written is already on disk
    """
    out_path = Path(output_path)
    today    = datetime.now().strftime("%Y-%m-%d")

    # Load already-downloaded symbols
    existing = _load_existing_symbols(output_path) if not force_refresh else set()
    if existing:
        log.info(f"  CSV already has {len(existing)} symbols — will skip those")

    # Symbols still to download
    todo = [s for s in symbols if s.upper() not in existing]
    if not todo:
        log.info("  All symbols already in CSV. Nothing to download.")
        return pd.read_csv(output_path)

    log.info(f"  Need to download: {len(todo)} symbols  |  Already done: {len(existing)}")

    session = requests.Session()
    session.headers.update(SCREENER_HDR)

    write_header = not out_path.exists() or force_refresh
    if force_refresh and out_path.exists():
        out_path.unlink()          # wipe and restart
        write_header = True

    errors = 0
    csv_file = None

    try:
        csv_file = open(out_path, "a", newline="", encoding="utf-8")
        writer   = None                     # initialised on first row

        for sym in tqdm(todo, desc="Screener.in", unit="stock"):
            row = scrape_screener(sym, session)
            row["DATE_DOWNLOADED"] = today  # ← stamp every row

            # ── Write to CSV immediately ──────────────────────────────────
            if writer is None:
                # Determine all column names on first row; preserve order
                fieldnames = list(row.keys())
                writer = csv.DictWriter(
                    csv_file, fieldnames=fieldnames,
                    extrasaction="ignore",
                    lineterminator="\n"
                )
                if write_header:
                    writer.writeheader()
                    write_header = False
            else:
                # Add any new columns that appeared (edge case: some stocks have
                # extra segment rows). We don't re-write header, just drop extras.
                pass

            writer.writerow(row)
            csv_file.flush()            # ensure it's on disk immediately

            if "SCREENER_ERROR" in row:
                errors += 1

            time.sleep(delay)

    except KeyboardInterrupt:
        log.warning("\n  ⚠ Interrupted by user — partial data saved to CSV")
    finally:
        if csv_file:
            csv_file.close()

    downloaded = len(todo) - errors
    log.info(f"  ✓ Downloaded {downloaded} new rows  |  Errors: {errors}")

    # Re-read full CSV (all appended rows) and enrich with NSE metadata
    try:
        scraped_df = pd.read_csv(out_path)
    except Exception:
        scraped_df = pd.DataFrame()

    if scraped_df.empty:
        return scraped_df

    # Left-join NSE metadata onto scraped data (keeps ALL scraped rows)
    sym_col = next((c for c in nse_df.columns if "SYMBOL" in c.upper()), None)
    if sym_col:
        # Only bring in NSE columns not already in scraped_df
        nse_extra = nse_df[[c for c in nse_df.columns
                             if c not in scraped_df.columns or c == sym_col]]
        master = scraped_df.merge(nse_extra, left_on="SYMBOL",
                                  right_on=sym_col, how="left")
    else:
        master = scraped_df

    # Overwrite with enriched version
    master.to_csv(output_path, index=False)
    log.info(f"  ✓ Final CSV → {output_path}  ({len(master)} rows × {len(master.columns)} cols)")
    return master


# ══════════════════════════════════════════════════════════════════════════════
# STEP 4a — VALUATION SCORING
# ══════════════════════════════════════════════════════════════════════════════

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

def _fcol(df, key):
    k = key.upper()
    for c in df.columns:
        if k in c.upper():
            return c
    return None

def _bracket(v, lower, thresholds, points):
    try:
        v = float(v)
    except (TypeError, ValueError):
        return 0
    if v != v:
        return 0
    if lower:
        for i in range(len(thresholds) - 1):
            if v <= thresholds[i + 1]:
                return points[i] if i < len(points) else 0
        return points[-1] if points else 0
    else:
        for i, t in enumerate(thresholds):
            if v >= t:
                return points[i] if i < len(points) else 0
        return 0

def compute_scores(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    scols = []
    for key, lb, thr, pts in SCORE_METRICS:
        col = _fcol(df, key)
        sc  = f"SCORE_{key}"
        scols.append(sc)
        df[sc] = df[col].apply(lambda v: _bracket(v, lb, thr, pts)) if col else 0
    df["VALUATION_SCORE"]     = df[scols].sum(axis=1)
    df["VALUATION_SCORE_PCT"] = (df["VALUATION_SCORE"] / MAX_SCORE * 100).round(1)
    df["VALUATION_RANK"]      = df["VALUATION_SCORE"].rank(ascending=False, method="min").astype(int)
    return df


# ══════════════════════════════════════════════════════════════════════════════
# STEP 4b — 8 FAIR VALUE MODELS
# ══════════════════════════════════════════════════════════════════════════════

def _g(row, *keys):
    for k in keys:
        v = _safe(row.get(k) if hasattr(row, "get") else (row[k] if k in row.index else None))
        if v is not None:
            return v
    return None

def fv_graham(row):
    eps = _g(row, "PL_EPS_BASIC", "EPS_TTM")
    bv  = _g(row, "BOOK_VALUE")
    return round(math.sqrt(22.5 * eps * bv), 2) if eps and bv and eps > 0 and bv > 0 else None

def fv_lynch(row):
    eps = _g(row, "PL_EPS_BASIC", "EPS_TTM")
    g   = _g(row, "EPS_CAGR_3YR_PCT", "PAT_CAGR_3YR_PCT")
    return round(eps * g, 2) if eps and g and eps > 0 and g > 0 else None

def fv_dcf(row):
    fcf    = _g(row, "CF_FCF_CR", "CF_FCF_3YR_AVG_CR")
    shares = _g(row, "SHARES_CR")
    g_rev  = _g(row, "REVENUE_CAGR_3YR_PCT")
    g_pat  = _g(row, "PAT_CAGR_3YR_PCT")
    if not (fcf and shares and fcf > 0 and shares > 0):
        return None
    fcf_ps = fcf / shares
    g1 = min((g_rev or g_pat or 10) / 100, 0.30)
    g2 = g1 / 2
    pv, cf = 0.0, fcf_ps
    for yr in range(1, 6):
        cf *= (1 + g1); pv += cf / (1 + WACC) ** yr
    for yr in range(6, 11):
        cf *= (1 + g2); pv += cf / (1 + WACC) ** yr
    tv  = cf * (1 + TERMINAL_GROWTH) / (WACC - TERMINAL_GROWTH)
    pv += tv / (1 + WACC) ** 10
    return round(pv, 2) if pv > 0 else None

def fv_ev_ebitda(row):
    ebitda = _g(row, "PL_EBITDA_CR")
    cash   = _g(row, "BS_CASH_CR") or 0
    debt   = _g(row, "BS_TOTAL_BORROWINGS_CR") or 0
    shares = _g(row, "SHARES_CR")
    if not (ebitda and shares and ebitda > 0 and shares > 0):
        return None
    ev = 15 * ebitda + cash - debt
    return round(ev / shares, 2) if ev > 0 else None

def fv_pe_mean(row):
    eps = _g(row, "PL_EPS_BASIC", "EPS_TTM")
    ipe = _g(row, "INDUSTRY_PE")
    tpe = min(ipe, 25) if ipe and ipe > 5 else 20
    return round(eps * tpe, 2) if eps and eps > 0 else None

def fv_pb(row):
    bv  = _g(row, "BOOK_VALUE")
    roe = _g(row, "ROE_PCT", "ROE_CALC_PCT")
    if not (bv and roe and bv > 0 and roe > 0):
        return None
    coe = RISK_FREE + EQUITY_PREMIUM
    pb  = min(max((roe / 100) / coe, 0.5), 10)
    return round(bv * pb, 2)

def fv_ddm(row):
    eps    = _g(row, "PL_EPS_BASIC", "EPS_TTM")
    payout = _g(row, "PAYOUT_RATIO_PCT")
    dy     = _g(row, "DIV_YIELD_PCT")
    cmp    = _g(row, "CMP")
    g_pat  = _g(row, "PAT_CAGR_3YR_PCT")
    dps    = (cmp * dy / 100) if (cmp and dy) else (eps * payout / 100 if eps and payout else None)
    if not (dps and dps > 0):
        return None
    g   = min((g_pat or 7) / 100, 0.15)
    coe = RISK_FREE + EQUITY_PREMIUM
    if coe <= g:
        return None
    fv  = dps * (1 + g) / (coe - g)
    return round(fv, 2) if fv > 0 else None

def fv_epv(row):
    ebitda = _g(row, "PL_EBITDA_CR")
    depr   = _g(row, "PL_DEPR_CR") or 0
    tax    = _g(row, "PL_TAX_RATE_PCT")
    cash   = _g(row, "BS_CASH_CR") or 0
    debt   = _g(row, "BS_TOTAL_BORROWINGS_CR") or 0
    shares = _g(row, "SHARES_CR")
    if not (ebitda and shares and ebitda > 0 and shares > 0):
        return None
    ebit  = ebitda - abs(depr)
    nopat = ebit * (1 - (tax or 25) / 100)
    if nopat <= 0:
        return None
    epv = (nopat / WACC + cash - debt) / shares
    return round(epv, 2) if epv > 0 else None

FV_MODELS = [
    ("FV_GRAHAM",      fv_graham,    0.12),
    ("FV_PETER_LYNCH", fv_lynch,     0.10),
    ("FV_DCF",         fv_dcf,       0.20),
    ("FV_EV_EBITDA",   fv_ev_ebitda, 0.18),
    ("FV_PE_MEAN_REV", fv_pe_mean,   0.15),
    ("FV_PB",          fv_pb,        0.10),
    ("FV_DDM",         fv_ddm,       0.08),
    ("FV_EPV",         fv_epv,       0.07),
]

def compute_fair_values(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col, fn, _ in FV_MODELS:
        df[col] = df.apply(fn, axis=1)

    def composite(row):
        tw, ws = 0.0, 0.0
        for col, _, w in FV_MODELS:
            v = _safe(row.get(col))
            if v and v > 0:
                ws += v * w; tw += w
        return round(ws / tw, 2) if tw > 0 else None

    df["COMPOSITE_FAIR_VALUE"] = df.apply(composite, axis=1)
    cmp = df.apply(lambda r: _safe(r.get("CMP")), axis=1)
    cfv = df["COMPOSITE_FAIR_VALUE"]
    df["UPSIDE_PCT"]           = ((cfv - cmp) / cmp * 100).round(1).where(cmp.notna() & cfv.notna())
    df["MARGIN_OF_SAFETY_PCT"] = ((cfv - cmp) / cfv * 100).round(1).where(cfv.notna() & (cfv > 0))

    def fv_grade(v):
        if v is None or (isinstance(v, float) and v != v): return "N/A"
        try: v = float(v)
        except: return "N/A"
        if v >= 40:  return "DEEP VALUE"
        if v >= 25:  return "UNDERVALUED"
        if v >= 10:  return "FAIR"
        if v >= -10: return "FULLY PRICED"
        return "OVERVALUED"

    df["FV_GRADE"] = df["MARGIN_OF_SAFETY_PCT"].apply(fv_grade)
    return df


# ══════════════════════════════════════════════════════════════════════════════
# STEP 4c — FINAL SCORE (ULTIMATE VERSION)
# ══════════════════════════════════════════════════════════════════════════════

CYCLICAL_SECTORS = [
    "REAL ESTATE", "METALS", "MINING",
    "CEMENT", "OIL & GAS", "POWER",
    "INFRASTRUCTURE", "COMMODITIES"
]

def cyclical_penalty(row):
    sector = str(row.get("SECTOR", "")).upper()
    return -10 if any(c in sector for c in CYCLICAL_SECTORS) else 0


def earnings_volatility(row):
    vals = [row[c] for c in row.index if "A_PL_PAT_CR_" in c and pd.notna(row[c]) and row[c] > 0]
    if len(vals) < 3:
        return 0

    std = np.std(vals)
    mean = np.mean(vals)
    if mean == 0:
        return -10

    cv = std / mean
    if cv < 0.2: return 10
    if cv < 0.4: return 5
    if cv < 0.7: return 0
    if cv < 1.0: return -5
    return -10


def fcf_consistency(row):
    vals = [row[c] for c in row.index if "C_CF_FCF_CR_" in c and pd.notna(row[c])]
    if len(vals) < 3:
        return 0

    pos = sum(1 for v in vals if v > 0)
    if pos >= 5: return 10
    if pos >= 3: return 5
    if pos >= 1: return 0
    return -10


def fraud_penalty(row):
    penalty = 0

    if row.get("OTHER_INCOME_TO_PBT_PCT", 0) > 50:
        penalty -= 10

    if row.get("CWIP_TO_NETBLOCK_PCT", 0) > 50:
        penalty -= 5

    ocf = row.get("CF_OPERATING_CR")
    pat = row.get("PL_PAT_CR")
    if ocf and pat and ocf < pat * 0.5:
        penalty -= 10

    if row.get("DEBTOR_DAYS", 0) > 120:
        penalty -= 5

    return penalty


def missing_data_penalty(row):
    critical = [
        "PE", "ROE_PCT", "DEBT_TO_EQUITY",
        "REVENUE_CAGR_3YR_PCT",
        "PAT_CAGR_3YR_PCT",
        "COMPOSITE_FAIR_VALUE"
    ]

    missing = sum(
        1 for f in critical
        if row.get(f) is None or (isinstance(row.get(f), float) and pd.isna(row.get(f)))
    )

    if missing >= 4: return -30
    if missing >= 2: return -15
    if missing >= 1: return -5
    return 0


def piotroski_score(row):
    score = 0
    if row.get("PL_PAT_CR", 0) > 0: score += 1
    if row.get("CF_OPERATING_CR", 0) > 0: score += 1
    if row.get("ROE_PCT", 0) > 15: score += 1
    if row.get("DEBT_TO_EQUITY", 1) < 0.5: score += 1
    if row.get("CURRENT_RATIO", 0) > 1: score += 1
    if row.get("PROMOTER_HOLDING_PCT", 0) > 50: score += 1
    if row.get("OPM_CALC_PCT", 0) > 15: score += 1
    if row.get("ASSET_TURNOVER", 0) > 1: score += 1
    return score


def compute_final_score(df):
    df = df.copy()

    def get_col(name):
        for c in df.columns:
            if name.upper() in c.upper():
                return df[c]
        return pd.Series([0]*len(df))

    val_score = get_col("VALUATION_SCORE_PCT").fillna(0)
    mos = get_col("MARGIN_OF_SAFETY_PCT").fillna(0).clip(-50, 100)

    roe = get_col("ROE_PCT").fillna(0)
    roce = get_col("ROCE_PCT").fillna(0)
    opm = get_col("OPM").fillna(0)
    npm = get_col("NET_MARGIN").fillna(0)

    rev_growth = get_col("REVENUE_CAGR_3YR").fillna(0)
    pat_growth = get_col("PAT_CAGR_3YR").fillna(0)

    debt = get_col("DEBT_TO_EQUITY").fillna(1)
    interest = get_col("INTEREST_COVERAGE").fillna(0)

    quality = roe*0.3 + roce*0.3 + opm*0.2 + npm*0.2
    growth = rev_growth*0.4 + pat_growth*0.6
    safety = (1/(1+debt))*50 + interest.clip(0,10)*5

    df["F_SCORE"] = df.apply(piotroski_score, axis=1)

    df["FINAL_SCORE"] = (
        val_score * 0.25 +
        mos * 0.25 +
        quality * 0.15 +
        growth * 0.10 +
        safety * 0.10 +
        df["F_SCORE"] * 2 +

        df.apply(cyclical_penalty, axis=1) +
        df.apply(earnings_volatility, axis=1) +
        df.apply(fcf_consistency, axis=1) +
        df.apply(fraud_penalty, axis=1) +
        df.apply(missing_data_penalty, axis=1)
    ).round(2)

    # ❗ FILTER BAD DATA (VERY IMPORTANT)
    df = df[
        df["COMPOSITE_FAIR_VALUE"].notna() &
        df["ROE_PCT"].notna() &
        df["PE"].notna()
    ]

    df["FINAL_RANK"] = df["FINAL_SCORE"].rank(ascending=False, method="min").astype(int)

    def grade(s):
        if s >= 80: return "A"
        if s >= 65: return "B"
        if s >= 50: return "C"
        if s >= 35: return "D"
        return "E"

    df["GRADE"] = df["FINAL_SCORE"].apply(grade)

    return df


# ══════════════════════════════════════════════════════════════════════════════
# DISPLAY SUMMARY
# ══════════════════════════════════════════════════════════════════════════════

def print_summary(df: pd.DataFrame, n: int = 20):

    cols = [
        "SYMBOL",
        "CMP",
        "COMPOSITE_FAIR_VALUE",
        "UPSIDE_PCT", 
        "PE",
        "ROE_PCT",
        "DEBT_TO_EQUITY",
        "REVENUE_CAGR_3YR_PCT",
        "PAT_CAGR_3YR_PCT",
        "FCF_YIELD_PCT",
        "MARGIN_OF_SAFETY_PCT",
        "FINAL_SCORE",
        "GRADE"
    ]

    cols = [c for c in cols if c in df.columns]

    top = df.sort_values("FINAL_SCORE", ascending=False).head(n)

    print("\n" + "="*100)
    print(f"🏆 TOP {n} VALUE STOCKS (CLEAN VIEW)")
    print("="*100)

    print(top[cols].to_string(index=False))


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    ap = argparse.ArgumentParser(description="Nifty 500 COMPLETE Fundamental + Valuation Pipeline")
    ap.add_argument("--limit",         type=int,   default=None,
                    help="Process only first N stocks (for testing)")
    ap.add_argument("--delay",         type=float, default=REQUEST_DELAY,
                    help=f"Seconds between requests (default {REQUEST_DELAY})")
    ap.add_argument("--output",        default=OUTPUT_FILE)
    ap.add_argument("--skip-fetch",    action="store_true",
                    help="Skip ALL downloading — go straight to scoring")
    ap.add_argument("--force-refresh", action="store_true",
                    help="Wipe CSV and re-download everything from scratch")
    ap.add_argument("--top",           type=int,   default=50)
    args = ap.parse_args()
    out  = args.output

    # ── Determine whether to download ────────────────────────────────────────
    # Priority: command-line flag > SKIP_DOWNLOAD variable > age check
    do_download = True

    if args.skip_fetch or SKIP_DOWNLOAD:
        do_download = False
        log.info("Download skipped (SKIP_DOWNLOAD=True or --skip-fetch flag)")

    if args.force_refresh:
        do_download = True
        log.info("Force refresh requested — will re-download everything")

    if do_download and not args.force_refresh:
        age = _csv_age_days(out)
        if age is not None and age < REFRESH_DAYS:
            log.info(f"  CSV is {age} days old (< {REFRESH_DAYS} days) — skipping download")
            log.info(f"  Set REFRESH_DAYS lower or use --force-refresh to re-download")
            do_download = False
        elif age is not None:
            log.info(f"  CSV is {age} days old (≥ {REFRESH_DAYS} days) — refreshing stale data")

    # ── Download ──────────────────────────────────────────────────────────────
    if do_download:
        if not Path(out).exists() or args.force_refresh:
            log.info("Starting fresh download …")
        else:
            log.info("Resuming / topping up existing CSV …")

        nse_df  = fetch_nifty500()
        sc      = next((c for c in nse_df.columns if "SYMBOL" in c.upper()), nse_df.columns[0])
        symbols = nse_df[sc].dropna().unique().tolist()
        if args.limit:
            symbols = symbols[:args.limit]
            log.info(f"  Limiting to {args.limit} stocks")

        master = build_master_csv(nse_df, symbols, out,
                                   delay=args.delay,
                                   force_refresh=args.force_refresh)
    else:
        if not Path(out).exists():
            log.error(f"CSV not found: {out}  — run without --skip-fetch first")
            sys.exit(1)
        log.info(f"Loading existing CSV: {out}")
        master = pd.read_csv(out)

    if master.empty:
        log.error("No data to score. Exiting.")
        sys.exit(1)

    # ── Score & analyse ───────────────────────────────────────────────────────
    log.info("Computing valuation scores …")
    df = compute_scores(master)

    log.info("Computing fair value models …")
    df = compute_fair_values(df)

    log.info("Computing final composite score …")
    df = compute_final_score(df)

    df.to_csv(out, index=False)
    log.info(f"  ✓ Scored CSV → {out}  ({len(df)} rows × {len(df.columns)} cols)")

    print_summary(df, n=args.top)
    log.info("Done! 🎯")


if __name__ == "__main__":
    main()
