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

Speed optimisations applied at build time:
  1. All three stock datasets written as separate minified JSON sidecar files
     (public/data_n500.json, public/data_sc250.json, public/data_mc250.json).
     The HTML receives only a tiny async-loader stub — no inline data blob.
  2. News written as public/news.json and loaded on-demand.
  3. HTML output is minified (whitespace collapsed, HTML comments stripped).
  4. index.html and all JSON sidecars are also written as pre-compressed
     .gz files so Firebase Hosting serves them with zero server-side overhead.
  5. JSON is serialised with the most compact separators and no indentation.
"""

import gzip
import json
import re
import sys
import argparse
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime
import zoneinfo

# ─────────────────────────────────────────────
# 📁  PATHS
# ─────────────────────────────────────────────

BASE     = Path(__file__).parent
TEMPLATE = BASE / "HNImanshu_template.html"
PUBLIC   = BASE / "public"
OUTPUT   = PUBLIC / "index.html"

N500_CSV  = BASE / "nifty500_valuation.csv"
SC250_CSV = BASE / "niftysmallcap500_valuation.csv"
MC250_CSV = BASE / "niftymicrocap250_valuation.csv"
NEWS_CSV  = BASE / "multi_stock_news.csv"

# Placeholders that must exist in the template
DATASETS_PLACEHOLDER = "%%DATASETS_PLACEHOLDER%%"
NEWS_PLACEHOLDER     = "// ── NEWS DATA ──\nvar NEWS_DATA = {\n  '__default__': []\n};"

N500_NAME_COL  = "COMPANY_NAME_x"
SC250_NAME_COL = "COMPANY_NAME"
MC250_NAME_COL = "COMPANY_NAME"

# ─────────────────────────────────────────────
# 📊  OUTPUT COLUMNS
# ─────────────────────────────────────────────

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
    "F_SCORE",
    "PIOTROSKI_SCORE",
    "QUALITY_SCORE",
    "COMBINED_SCORE",
    "LOW_PROMOTER_FLAG",
]

# ─────────────────────────────────────────────
# 🔥  SCORING ENGINE
# ─────────────────────────────────────────────

def score_metric(value, thresholds, reverse=False, max_pts=1.4):
    """4-tier linear scoring engine."""
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

# ─────────────────────────────────────────────
# 🔍  PRIOR-YEAR RESOLVER
# ─────────────────────────────────────────────

def _resolve_prior_cols(df: pd.DataFrame) -> dict:
    """Dynamically find the two most recent annual columns for each Piotroski metric."""
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

# ─────────────────────────────────────────────
# 🧠  PIOTROSKI F-SCORE  (0–9)
# ─────────────────────────────────────────────

def calc_piotroski_fscore(df: pd.DataFrame) -> pd.Series:
    def s(col):
        if col in df.columns:
            return pd.to_numeric(df[col], errors="coerce")
        return pd.Series(np.nan, index=df.index)

    def nz(series):
        return series.replace(0, np.nan)

    pat_c = s("PL_PAT_CR");    ta_c  = nz(s("BS_TOTAL_ASSETS_CR"))
    cfo_c = s("CF_OPERATING_CR"); opm_c = s("PL_OPM_PCT")
    rev_c = s("PL_REVENUE_CR");   int_c = s("PL_INTEREST_CR")
    res_c = s("BS_RESERVES_CR");  eps_c = nz(s("PL_EPS_BASIC"))

    fam   = _resolve_prior_cols(df)
    pat_p = fam["pat"]["pri"];  ta_p  = nz(fam["ta"]["pri"])
    opm_p = fam["opm"]["pri"];  rev_p = fam["rev"]["pri"]
    int_p = fam["int_"]["pri"]; res_p = fam["res"]["pri"]
    eps_p = nz(fam["eps"]["pri"])

    roa_c = pat_c / ta_c;      roa_p = pat_p / ta_p
    lev_c = nz(int_c) / ta_c; lev_p = nz(int_p) / ta_p
    eq_c  = res_c / ta_c;     eq_p  = res_p / ta_p
    at_c  = rev_c / ta_c;     at_p  = rev_p / ta_p
    sh_c  = pat_c / eps_c;    sh_p  = pat_p / eps_p

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

# ─────────────────────────────────────────────
# 🏆  QUALITY SCORE  (0–13.4)
# ─────────────────────────────────────────────

def calc_quality_score(df: pd.DataFrame) -> pd.Series:
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

# ─────────────────────────────────────────────
# ⭐  MASTER FINAL SCORE  (0–10)
# ─────────────────────────────────────────────

def calc_final_score(df: pd.DataFrame) -> pd.DataFrame:
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
        if s >= 75:    return "A"
        if s >= 65:    return "B+"
        if s >= 55:    return "B"
        if s >= 40:    return "C"
        if s >= 25:    return "D"
        return "E"

    df["GRADE"] = df["FINAL_SCORE"].apply(grade)
    return df

# ─────────────────────────────────────────────
# 📰  NEWS LOADER
# ─────────────────────────────────────────────

def _extract_source(url: str) -> str:
    try:
        import urllib.parse
        host = urllib.parse.urlparse(url).netloc.replace("www.", "")
        if host == "news.google.com":
            return "Google News"
        return host.split(".")[0].capitalize()
    except Exception:
        return ""


def load_news(path: Path) -> dict:
    if not path.exists():
        print(f"  [NEWS]  {path.name} not found — skipping")
        return {}

    print(f"  [NEWS]  {path.name}")
    try:
        df = pd.read_csv(path, low_memory=False)
    except Exception as e:
        print(f"    ERROR reading news CSV: {e}")
        return {}

    df.columns = [c.strip() for c in df.columns]
    required = {"stockname", "datetime", "news", "link"}
    missing = required - set(df.columns)
    if missing:
        print(f"    ERROR: missing columns: {missing}")
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

        time_str = dt_raw
        try:
            dt_obj   = datetime.strptime(dt_raw.replace(" IST", "").strip(), "%Y-%m-%d %H:%M")
            time_str = dt_obj.strftime("%-d %b %Y, %I:%M %p")
        except Exception:
            pass

        news_map.setdefault(sym, []).append({
            "headline": headline,
            "time":     time_str,
            "link":     link,
            "source":   source,
        })

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
            pass

    total = sum(len(v) for v in news_map.values())
    print(f"    Loaded: {total} news items across {len(news_map)} symbols")
    return news_map

# ─────────────────────────────────────────────
# 📂  CSV LOADER
# ─────────────────────────────────────────────

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

    if "SYMBOL" in df.columns:
        before = len(df)
        df = df.drop_duplicates(subset=["SYMBOL"], keep="first")
        dupes = before - len(df)
        if dupes:
            print(f"    Dropped {dupes} duplicate symbols")

    if "NET_DEBT_CR" in df.columns and "PL_EBITDA_CR" in df.columns:
        ebitda   = pd.to_numeric(df["PL_EBITDA_CR"], errors="coerce").replace(0, np.nan)
        net_debt = pd.to_numeric(df["NET_DEBT_CR"],  errors="coerce")
        df["NET_DEBT_TO_EBITDA"] = net_debt / ebitda
    else:
        df["NET_DEBT_TO_EBITDA"] = np.nan

    if "SH_PROMOTER_PCT_LATEST" in df.columns:
        df["LOW_PROMOTER_FLAG"] = (
            pd.to_numeric(df["SH_PROMOTER_PCT_LATEST"], errors="coerce") < 25
        )
    else:
        df["LOW_PROMOTER_FLAG"] = False

    df["PIOTROSKI_SCORE"] = calc_piotroski_fscore(df)
    df["QUALITY_SCORE"]   = calc_quality_score(df)
    df["COMBINED_SCORE"]  = (df["PIOTROSKI_SCORE"] + df["QUALITY_SCORE"]).round(2)
    df = calc_final_score(df)

    for col in ["PIOTROSKI_SCORE", "QUALITY_SCORE", "COMBINED_SCORE", "FINAL_SCORE"]:
        s = df[col].dropna()
        print(f"    {col:20s}  min={s.min():.2f}  max={s.max():.2f}  avg={s.mean():.2f}")

    top5 = df.nsmallest(5, "FINAL_RANK")[["COMPANY_NAME", "FINAL_RANK", "FINAL_SCORE", "GRADE"]]
    print(f"    Top 5:\n{top5.to_string(index=False)}")

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

# ─────────────────────────────────────────────
# 📰  NEWS JS BUILDER  (legacy inline fallback)
# ─────────────────────────────────────────────

def build_news_js(news_map: dict) -> str:
    """Used only when sidecar mode is off or as template fallback."""
    if not news_map:
        return "// ── NEWS DATA ──\nvar NEWS_DATA = {\n  '__default__': []\n};"
    lines = ["// ── NEWS DATA ──", "var NEWS_DATA = {", "  '__default__': []"]
    for sym, items in sorted(news_map.items()):
        safe_items = json.dumps(items, ensure_ascii=False, separators=(",", ":"))
        lines.append(f",{json.dumps(sym)}:{safe_items}")
    lines.append("};")
    return "\n".join(lines)


def inject_news(html: str, news_js: str) -> str:
    pattern = re.compile(
        r'//\s*[─\-─]+\s*NEWS DATA\s*[─\-─]+.*?var\s+NEWS_DATA\s*=\s*\{.*?\};',
        re.DOTALL
    )
    if pattern.search(html):
        return pattern.sub(news_js, html)
    print("  [WARN] NEWS_DATA block not found — appending before </script>")
    return html.replace('</script>', news_js + '\n</script>', 1)

# ─────────────────────────────────────────────
# 🗜  MINIFICATION HELPERS
# ─────────────────────────────────────────────

def minify_json(data) -> bytes:
    """
    Serialise data to the most compact JSON possible, then encode to UTF-8 bytes.
    - No spaces after separators (already done with separators arg)
    - ensure_ascii=False so Unicode stays as-is (shorter than \\uXXXX escapes)
    """
    return json.dumps(
        data,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def minify_html(html: str) -> str:
    """
    Lightweight HTML minifier — safe for standard HTML5 pages.
    Steps (in order):
      1. Strip HTML comments (but NOT IE conditionals or SSI includes).
      2. Collapse runs of whitespace inside text nodes to a single space.
      3. Collapse blank lines.
    We deliberately do NOT touch content inside <script> or <style> tags
    with aggressive stripping — that risks breaking JS template literals and
    multi-line strings.  Collapsing whitespace outside those blocks is safe
    and already removes 5–15 % of typical HTML size.
    """
    # 1. Remove HTML comments (skip <!--[ ... ]--> IE conditionals)
    html = re.sub(r"<!--(?!\[).*?-->", "", html, flags=re.DOTALL)

    # 2. Outside <script>/<style> blocks, collapse whitespace
    #    Strategy: split on script/style, process text chunks only.
    parts = re.split(r"(<script[\s\S]*?</script>|<style[\s\S]*?</style>)", html, flags=re.IGNORECASE)
    result = []
    for i, part in enumerate(parts):
        lower = part.lower().lstrip()
        if lower.startswith("<script") or lower.startswith("<style"):
            # Keep script/style verbatim (apart from leading/trailing blank lines)
            result.append(part.strip())
        else:
            # Collapse whitespace in HTML markup / text
            # Replace runs of whitespace (including newlines) with a single space
            part = re.sub(r"[ \t]*\n[ \t]*", "\n", part)   # trim line margins
            part = re.sub(r"\n{2,}", "\n", part)             # collapse blank lines
            part = re.sub(r"[ \t]{2,}", " ", part)           # collapse spaces/tabs
            result.append(part)

    return "".join(result)


def write_gzip(path: Path, data: bytes, level: int = 9) -> int:
    """Write bytes to path and also to path.gz.  Returns compressed size."""
    path.write_bytes(data)
    gz_path = path.with_suffix(path.suffix + ".gz")
    with gzip.open(gz_path, "wb", compresslevel=level) as f:
        f.write(data)
    ratio = len(data) / max(len(gzip.compress(data, compresslevel=level)), 1)
    print(f"    {path.name}: {len(data)//1024} KB  →  gz: {gz_path.stat().st_size//1024} KB  (ratio {ratio:.1f}×)")
    return gz_path.stat().st_size


# ─────────────────────────────────────────────
# 🔌  ASYNC LOADER STUB
# ─────────────────────────────────────────────
# This replaces the massive inline DATASETS blob in the HTML.
# The template's %%DATASETS_PLACEHOLDER%% becomes a tiny JS snippet that
# fetches each dataset on-demand the first time its tab is opened.

ASYNC_LOADER_JS = """\
const DATASETS = {};
const _DS_CACHE = {};
async function loadDataset(key) {
  if (_DS_CACHE[key]) return _DS_CACHE[key];
  const r = await fetch('data_' + key + '.json');
  if (!r.ok) throw new Error('Failed to load ' + key);
  _DS_CACHE[key] = await r.json();
  DATASETS[key] = _DS_CACHE[key];
  return _DS_CACHE[key];
}"""

# Async news loader stub — replaces the big inline NEWS_DATA block.
ASYNC_NEWS_JS = """\
// ── NEWS DATA ──
var NEWS_DATA = {'__default__':[]};
var _NEWS_LOADED = false;
async function ensureNews() {
  if (_NEWS_LOADED) return;
  _NEWS_LOADED = true;
  try {
    const r = await fetch('news.json');
    if (r.ok) {
      const d = await r.json();
      Object.assign(NEWS_DATA, d);
    }
  } catch(e) { console.warn('News fetch failed', e); }
}"""

# ─────────────────────────────────────────────
# 🏗  BUILD
# ─────────────────────────────────────────────

def build(deploy=False, inline=False):
    """
    inline=True  → legacy behaviour: embed all data directly in index.html.
    inline=False → fast mode (default): write sidecar JSON files, tiny loader stub in HTML.
    """
    print(f"\n{'='*60}")
    print("  HNImanshu — Build" + (" [INLINE mode]" if inline else " [SIDECAR mode — fast]"))
    print(f"{'='*60}\n")

    if not TEMPLATE.exists():
        print("ERROR: Template missing —", TEMPLATE)
        sys.exit(1)

    template = TEMPLATE.read_text(encoding="utf-8")

    print("Loading CSVs…\n")
    data_n500  = load_csv(N500_CSV,  N500_NAME_COL,  "Nifty 500")
    data_sc250 = load_csv(SC250_CSV, SC250_NAME_COL, "Smallcap 250", optional=True) or []
    data_mc250 = load_csv(MC250_CSV, MC250_NAME_COL, "Microcap 250", optional=True) or []

    print("\nLoading News…\n")
    news_map = load_news(NEWS_CSV)

    PUBLIC.mkdir(parents=True, exist_ok=True)

    # ── Timestamp ──────────────────────────────────────────────────────
    now_str = datetime.now(zoneinfo.ZoneInfo("Asia/Kolkata")).strftime("%d %b %Y\n%I:%M %p IST")

    # ══════════════════════════════════════════════════════════════════
    #  FAST (SIDECAR) MODE  ← default
    # ══════════════════════════════════════════════════════════════════
    if not inline:
        print("\nWriting sidecar JSON files…\n")

        sidecar_sizes = {}
        for key, data in [("n500", data_n500), ("sc250", data_sc250), ("mc250", data_mc250)]:
            dest = PUBLIC / f"data_{key}.json"
            raw  = minify_json(data)
            sidecar_sizes[key] = write_gzip(dest, raw)

        news_raw = minify_json(news_map)
        sidecar_sizes["news"] = write_gzip(PUBLIC / "news.json", news_raw)

        # Inject async loader stub instead of inline blob
        html = template.replace(DATASETS_PLACEHOLDER, ASYNC_LOADER_JS)

        # Replace inline NEWS_DATA block with async loader stub
        html = html.replace(NEWS_PLACEHOLDER, ASYNC_NEWS_JS)
        html = inject_news(html, ASYNC_NEWS_JS)

        html = html.replace("%%LAST_UPDATED_PLACEHOLDER%%", now_str)

        print("\nMinifying HTML…")
        html = minify_html(html)

        html_bytes = html.encode("utf-8")
        write_gzip(OUTPUT, html_bytes)

    # ══════════════════════════════════════════════════════════════════
    #  INLINE MODE  (legacy — everything embedded)
    # ══════════════════════════════════════════════════════════════════
    else:
        datasets_js = (
            "const DATASETS = {\n"
            f"n500:{json.dumps(data_n500, separators=(',', ':'))},\n"
            f"sc250:{json.dumps(data_sc250, separators=(',', ':'))},\n"
            f"mc250:{json.dumps(data_mc250, separators=(',', ':'))}\n"
            "};"
        )
        news_js = build_news_js(news_map)

        html = template.replace(DATASETS_PLACEHOLDER, datasets_js)
        html = html.replace(NEWS_PLACEHOLDER, news_js)
        if news_js not in html and "var NEWS_DATA" in html:
            html = re.sub(
                r"// ── NEWS DATA ──\s*\nvar NEWS_DATA\s*=\s*\{[^;]*\};",
                news_js, html, flags=re.DOTALL
            )
        html = inject_news(html, news_js)
        html = html.replace("%%LAST_UPDATED_PLACEHOLDER%%", now_str)

        print("\nMinifying HTML…")
        html = minify_html(html)

        html_bytes = html.encode("utf-8")
        write_gzip(OUTPUT, html_bytes)

    # ── Summary ────────────────────────────────────────────────────────
    total_stocks = len(data_n500 or []) + len(data_sc250) + len(data_mc250)
    total_news   = sum(len(v) for v in news_map.values())
    print(f"\n✅  Build complete — {total_stocks} stocks, {total_news} news items")
    print(f"    Output: {OUTPUT}")

    if not inline:
        print(
            "\n💡  Sidecar files written. Make sure your firebase.json serves\n"
            "    pre-compressed .gz files and sets Cache-Control headers:\n\n"
            '    "headers": [{\n'
            '      "source": "**/*.json",\n'
            '      "headers": [{"key": "Cache-Control", "value": "public, max-age=3600"}]\n'
            '    }]'
        )

    if deploy:
        import subprocess
        subprocess.run(["firebase", "deploy"])


# ─────────────────────────────────────────────
# ▶  ENTRY POINT
# ─────────────────────────────────────────────

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="HNImanshu Stock Screener Build")
    ap.add_argument("--deploy", action="store_true", help="Deploy to Firebase after build")
    ap.add_argument(
        "--inline", action="store_true",
        help="Legacy mode: embed all data inline in index.html (slow, big file)"
    )
    args = ap.parse_args()
    build(deploy=args.deploy, inline=args.inline)
