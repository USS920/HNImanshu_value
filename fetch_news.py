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
NEWS_DAYS    = 15   # ← UPDATED

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
# FETCH INDEX STOCKS
# ══════════════════════════════════════════════

def fetch_index_csvs():
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.nseindia.com/",
    })

    frames = []

    for index, url in INDEX_URLS.items():
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
            df.columns = ["symbol", "name"]
            df = df.dropna()

            frames.append(df)

        except Exception:
            continue

    if not frames:
        sys.exit(1)

    all_stocks = pd.concat(frames, ignore_index=True)
    all_stocks = all_stocks.drop_duplicates(subset=["symbol"]).reset_index(drop=True)

    all_stocks.to_csv("stocks_nse_indices.csv", index=False, encoding="utf-8-sig")

    return [(row["symbol"], row["name"]) for _, row in all_stocks.iterrows()]


# ══════════════════════════════════════════════
# GOOGLE NEWS FETCH
# ══════════════════════════════════════════════

def google_news_for_stock(symbol, name, days=15):
    articles = None

    for _ in range(2):
        try:
            g = GNews(
                language="en",
                country="IN",
                period=f"{days}d",
                max_results=100,
            )

            articles = g.get_news(name)
            break
        except Exception:
            time.sleep(1)

    if not articles:
        return pd.DataFrame()

    rows = []

    name_low  = name.lower().strip()
    sym_low   = symbol.lower()
    words     = name_low.split()
    first_two = " ".join(words[:2]) if len(words) >= 2 else name_low

    key_phrases = [sym_low, name_low, first_two]

    for a in articles:
        try:
            dt = parsedate_to_datetime(a.get("published date", ""))
        except Exception:
            continue

        title = str(a.get("title", "")).lower()

        if not any(k in title for k in key_phrases):
            continue

        ist = dt.astimezone(IST)

        rows.append({
            "stockname": symbol,
            "datetime":  ist.strftime("%Y-%m-%d %H:%M IST"),
            "news":      a.get("title", ""),
            "link":      a.get("url", ""),
        })

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    df.sort_values("datetime", ascending=False, inplace=True)

    return df


# ══════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════

def main():
    outfile = "multi_stock_news.csv"

    if os.path.exists("stocks_nse_indices.csv"):
        os.remove("stocks_nse_indices.csv")

    if os.path.exists(outfile):
        os.remove(outfile)

    stocks = fetch_index_csvs()

    batches = [stocks[i:i+BATCH_SIZE] for i in range(0, len(stocks), BATCH_SIZE)]

    file_exists = False

    for batch in batches:

        with ThreadPoolExecutor(max_workers=len(batch)) as pool:
            futures = {
                pool.submit(google_news_for_stock, sym, name, NEWS_DAYS): sym
                for sym, name in batch
            }

            batch_frames = []

            try:
                for future in as_completed(futures, timeout=TASK_TIMEOUT + 10):
                    try:
                        df = future.result(timeout=TASK_TIMEOUT)
                        if df is not None and not df.empty:
                            batch_frames.append(df)
                    except Exception:
                        continue
            except FutTimeoutError:
                pass

        if not batch_frames:
            continue

        batch_all = pd.concat(batch_frames, ignore_index=True)

        # STRICT IST FILTERING (final authority)
        batch_all["dt_parsed"] = pd.to_datetime(
            batch_all["datetime"].str.replace(" IST", ""),
            errors="coerce"
        )

        cutoff = datetime.now(IST) - timedelta(days=NEWS_DAYS)

        batch_csv = batch_all[
            batch_all["dt_parsed"] >= cutoff
        ][["stockname", "datetime", "news", "link"]]

        if batch_csv.empty:
            continue

        if not file_exists:
            batch_csv.to_csv(outfile, index=False, encoding="utf-8-sig")
            file_exists = True
        else:
            batch_csv.to_csv(outfile, index=False, header=False, mode="a", encoding="utf-8-sig")

        del batch_frames, batch_all, batch_csv


if __name__ == "__main__":
    main()
