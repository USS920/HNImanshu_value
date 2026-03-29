import os
import sys
import time
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FutTimeoutError
from email.utils import parsedate_to_datetime
import requests
from io import StringIO
import pandas as pd
from gnews import GNews

# ══════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════

IST          = timezone(timedelta(hours=5, minutes=30))
TASK_TIMEOUT = 10
BATCH_SIZE   = 10
NEWS_DAYS    = 7

INDEX_URLS = {
    "niftymicrocap250": "https://nsearchives.nseindia.com/content/indices/ind_niftymicrocap250_list.csv",
    "nifty500":         "https://nsearchives.nseindia.com/content/indices/ind_nifty500list.csv",
    "niftysmallcap500": "https://www.niftyindices.com/IndexConstituent/ind_niftysmallcap500_list.csv",
}

KEYWORDS = [
    "bulk deal","block deal","order win","order received","rating upgrade",
    "rating downgrade","acquisition","merger","buyback","dividend","bonus",
    "split","insider","promoter","pledge","results","profit","loss","revenue",
    "guidance","qip","rights issue","delisting","fpo","halt","shutdown",
    "fraud","penalty","sebi","fire","explosion","recall",
]


# ══════════════════════════════════════════════
# Download index CSVs and build stocks list
# Uses full official names from CSV (e.g., "ITC Ltd.")
# ══════════════════════════════════════════════

def fetch_index_csvs():
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://www.nseindia.com/",
    })

    INDEXES = list(INDEX_URLS.keys())
    frames  = []

    for index in INDEXES:
        url = INDEX_URLS[index]
        print(f"  → Downloading {index} from {url} ...")
        try:
            r = session.get(url, timeout=10)
            r.raise_for_status()
            df = pd.read_csv(StringIO(r.text))
            cols = df.columns.str.strip()
            sym_col  = next((c for c in cols if "Symbol" in c or "symbol" in c), None)
            name_col = next((c for c in cols if "Company Name" in c or "name" in c), None)

            if not sym_col or not name_col:
                sym_col, name_col = cols[0], cols[1]

            df = df[[sym_col, name_col]]
            df = df.rename(columns={sym_col: "symbol", name_col: "name"})
            df = df[["symbol", "name"]].dropna()
            df["index"] = index
            frames.append(df)
            print(f"      → {len(df)} stocks")
        except Exception as e:
            print(f"  ✗  Failed to fetch {index}: {e}")

    if not frames:
        print("[ERROR] Unable to fetch any index list.")
        sys.exit(1)

    all_stocks = pd.concat(frames, ignore_index=True)
    unique_stocks = (
        all_stocks
        .drop_duplicates(subset=["symbol"], keep="first")
        .reset_index(drop=True)
    )
    unique_stocks.to_csv("stocks_nse_indices.csv", index=False, encoding="utf-8-sig")

    # Each tuple is (symbol, full_official_name)
    stocks = [(row["symbol"], row["name"]) for _, row in unique_stocks.iterrows()]
    print(f"  → Unique stocks from 3 indices: {len(stocks)}")
    return stocks


# ══════════════════════════════════════════════
# GOOGLE NEWS per stock
# Only keep headlines that contain:
#   - symbol, or
#   - full name, or
#   - first two words of full name
# Saves original full headline in `news` column anyway
# ══════════════════════════════════════════════

def google_news_for_stock(symbol, name, days=7):
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    articles = None

    for attempt in range(2):
        try:
            g        = GNews(
                language="en",
                country="IN",
                period=f"{days}d",
                max_results=100,
            )
            if hasattr(g, "session"):
                g.session = requests.Session()
                g.session.headers.update(g.headers)
                g.session.mount("http://", requests.adapters.HTTPAdapter(max_retries=0))
                g.session.mount("https://", requests.adapters.HTTPAdapter(max_retries=0))
            articles = g.get_news(name)
            break
        except Exception as e:
            print(f"  → {symbol}: retry {attempt+1}/2 after HTTP error: {e}")
            time.sleep(1 if attempt == 0 else 0)

    if not articles:
        print(f"  ⚠  {symbol}: no news")
        return pd.DataFrame()

    rows = []
    name_low  = name.lower().strip()
    sym_low   = symbol.lower()
    name_words = name_low.split()
    first_two = " ".join(name_words[:2]) if len(name_words) >= 2 else name_low

    key_phrases = [
        sym_low,              # e.g., "aether"
        name_low,             # e.g., "aether industries limited"
        first_two,            # e.g., "aether industries"
    ]

    for a in articles:
        try:
            dt = parsedate_to_datetime(a.get("published date",""))
        except Exception:
            continue
        if dt < cutoff:
            continue
        tit = str(a.get("title","")).lower()

        # Only keep if title contains symbol OR full name OR first two words of name
        if not any(phrase in tit for phrase in key_phrases):
            continue

        ist = dt.astimezone(IST)
        rows.append({
            "stockname":  symbol,
            "datetime":   ist.strftime("%Y-%m-%d %H:%M IST"),
            "news":       a.get("title",""),
            "publisher":  a.get("publisher",{}).get("title",""),
            "link":       a.get("url",""),
        })

    if not rows:
        print(f"  ⚠  {symbol}: no relevant datetime news")
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df.sort_values("datetime", ascending=False, inplace=True)
    df["score"] = df["news"].astype(str).apply(
        lambda x: sum(1 for k in KEYWORDS if k in x.lower())
    )
    return df


# ══════════════════════════════════════════════
# MAIN
# Each batch writes to CSV; old files are removed first.
# ══════════════════════════════════════════════

def main():
    news_days = NEWS_DAYS
    outfile   = "multi_stock_news.csv"

    # Remove old files before starting
    if os.path.exists("stocks_nse_indices.csv"):
        os.remove("stocks_nse_indices.csv")
    if os.path.exists(outfile):
        os.remove(outfile)

    print("\n" + "="*88)
    print("  STEP 1: Download NSE index lists and build unique stock list")
    print("  Each row: (symbol, full official company name) → sent to gnews")
    print("="*88)
    stocks = fetch_index_csvs()
    print(f"    → stock pairs written to stocks_nse_indices.csv\n")

    batches = []
    for i in range(0, len(stocks), BATCH_SIZE):
        batch = stocks[i:i+BATCH_SIZE]
        batches.append(batch)

    print("="*88)
    print(f"  STEP 2: Fetch Google News — {BATCH_SIZE} stocks per batch")
    print(f"  News window: last {news_days} days (default)")
    print("="*88)

    t0 = time.time()

    file_exists = False  # will be True after first write

    for i_batch, batch in enumerate(batches):
        print(f"\n== BATCH {i_batch+1}/{len(batches)} ==")
        batch_symbols = [s for s,_ in batch]

        with ThreadPoolExecutor(max_workers=len(batch_symbols)) as pool:
            futures = {
                pool.submit(google_news_for_stock, sym, name, news_days):
                sym for sym, name in batch
            }
            batch_frames = []

            try:
                for future in as_completed(futures, timeout=TASK_TIMEOUT + 10):
                    symbol = futures[future]
                    try:
                        df = future.result(timeout=TASK_TIMEOUT)
                        if df is None or df.empty:
                            print(f"  ⚠  {symbol}: no data or timeout")
                        else:
                            print(f"  ✓  {symbol}: {len(df)} news rows")
                            batch_frames.append(df)
                    except Exception as e:
                        print(f"  ✗  {symbol}: {e}")
            except FutTimeoutError:
                print(f"  ⚠  Batch {i_batch+1}: some tasks timed out; using only completed tasks.")

        if not batch_frames:
            print(f"  ⚠  No news for batch {i_batch+1}; skipping write.")
            continue

        batch_all = pd.concat(batch_frames, ignore_index=True)
        batch_all["dt_parsed"] = pd.to_datetime(
            batch_all["datetime"].str.replace(" IST",""), errors="coerce"
        )
        cutoff = datetime.now() - timedelta(days=news_days)
        batch_csv = (
            batch_all[batch_all["dt_parsed"] >= cutoff]
            [["stockname", "datetime", "news", "link"]]
        )

        header = not file_exists
        if not file_exists:
            batch_csv.to_csv(outfile, index=False, encoding="utf-8-sig")
            file_exists = True
        else:
            batch_csv.to_csv(outfile, index=False, header=False, mode="a", encoding="utf-8-sig")

        del batch_frames, batch_all, batch_csv

    elapsed_total = time.time() - t0

    print(f"\n" + "="*88)
    print(f"  ✓ Total time: {elapsed_total:.1f}s")
    print(f"  ✓ Stocks read: {len(stocks)} (from 3 indices)")
    print(f"  ✓ CSV saved (each batch appended) → {outfile}")
    print(f"  ✓ Columns: stockname,datetime,news,link")
    print("  ✓ Each gnews call used: google_news_for_stock(symbol, full_official_name, 7)\n")
    print("Example (from your CSV):")
    print("  ICICIAMC,ICICI Prudential Asset Management Company Ltd.")
    print("  ICICIPRULI,ICICI Prudential Life Insurance Company Ltd.")
    print("  IDBI,IDBI Bank Ltd.")
    print("  IDFCFIRSTB,IDFC First Bank Ltd.")
    print("  ITC,ITC Ltd.")
    print("  ITCHOTELS,ITC Hotels Ltd.")
    print("  AETHER,Aether Industries Limited")
    print("="*88 + "\n")


if __name__ == "__main__":
    main()
