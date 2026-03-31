import os
import sys
import time
import random
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from email.utils import parsedate_to_datetime
import requests
from io import StringIO
import pandas as pd
from gnews import GNews

IST          = timezone(timedelta(hours=5, minutes=30))
NEWS_DAYS    = 15
MAX_WORKERS  = 20
MAX_RETRIES  = 2
TASK_TIMEOUT = 15
SAVE_EVERY   = 50

INDEX_URLS = {
    "niftymicrocap250": "https://nsearchives.nseindia.com/content/indices/ind_niftymicrocap250_list.csv",
    "nifty500":         "https://nsearchives.nseindia.com/content/indices/ind_nifty500list.csv",
    "niftysmallcap500": "https://www.niftyindices.com/IndexConstituent/ind_niftysmallcap500_list.csv",
}

_gnews = GNews(language="en", country="IN", period=f"{NEWS_DAYS}d", max_results=30)


# ══════════════════════════════════════════════
# FETCH STOCKS
# ══════════════════════════════════════════════

def fetch_index_csvs():
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Referer":    "https://www.nseindia.com/",
    })

    frames = []
    for url in INDEX_URLS.values():
        try:
            r = session.get(url, timeout=10)
            r.raise_for_status()
            df   = pd.read_csv(StringIO(r.text))
            cols = df.columns.str.strip()

            sym_col  = next((c for c in cols if "Symbol" in c), cols[0])
            name_col = next((c for c in cols if "Company Name" in c), cols[1])

            df = df[[sym_col, name_col]]
            df.columns = ["symbol", "name"]
            frames.append(df.dropna())

        except Exception:
            continue

    if not frames:
        print("❌ Failed to fetch index CSVs", flush=True)
        sys.exit(1)

    all_stocks = (
        pd.concat(frames, ignore_index=True)
        .drop_duplicates(subset=["symbol"])
        .reset_index(drop=True)
    )

    stocks = list(zip(all_stocks["symbol"], all_stocks["name"]))
    random.shuffle(stocks)

    return stocks


# ══════════════════════════════════════════════
# FETCH NEWS
# ══════════════════════════════════════════════

def fetch_stock(symbol, name):
    name_low  = name.lower()
    sym_low   = symbol.lower()
    first_two = " ".join(name_low.split()[:2])

    queries = [
        name,
        f"{symbol} stock",
        f"{name} news",
    ]

    all_rows = []

    for q in queries:

        articles = []

        for attempt in range(MAX_RETRIES):
            try:
                tmp = _gnews.get_news(q)

                if not tmp or len(tmp) < 3:
                    time.sleep(0.4 * (attempt + 1))
                    continue

                articles = tmp
                break

            except Exception:
                time.sleep(0.4 * (attempt + 1))
                continue

        if not articles:
            continue

        for a in articles:
            try:
                dt = parsedate_to_datetime(a.get("published date", ""))
            except Exception:
                continue

            title     = str(a.get("title", "")).strip()
            title_low = title.lower()

            if not (sym_low in title_low or name_low in title_low or first_two in title_low):
                continue

            all_rows.append({
                "stockname": symbol,
                "datetime":  dt.astimezone(IST).strftime("%Y-%m-%d %H:%M IST"),
                "news":      title,
                "link":      a.get("url", ""),
            })

        time.sleep(0.1)

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    df.drop_duplicates(subset=["news"], inplace=True)
    df.sort_values("datetime", ascending=False, inplace=True)

    return df


# ══════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════

def parse_dt(df):
    df["dt_parsed"] = pd.to_datetime(
        df["datetime"].str.replace(" IST", "", regex=False),
        errors="coerce"
    ).dt.tz_localize(IST)
    return df


def trim_and_dedup_old(df, cutoff):
    df = parse_dt(df)
    df = df[df["dt_parsed"] >= cutoff]
    df = df.drop_duplicates(subset=["stockname", "news"])
    return df[["stockname", "datetime", "news", "link"]]

def trim_and_dedup(df, cutoff):
    df = parse_dt(df)
    df = df[df["dt_parsed"] >= cutoff]
    df = df.drop_duplicates(subset=["stockname", "news"])  # already there
    df = df.drop_duplicates(subset=["news"])  # add this — catches cross-symbol dupes
    return df[["stockname", "datetime", "news", "link"]]
# ══════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════

def main():
    print("🚀 STARTED FETCH NEWS", flush=True)

    outfile = "multi_stock_news.csv"
    cutoff  = datetime.now(IST) - timedelta(days=NEWS_DAYS)

    if os.path.exists(outfile):
        existing_df = trim_and_dedup(pd.read_csv(outfile), cutoff)
        seen_keys   = set(zip(existing_df["stockname"], existing_df["news"]))
    else:
        existing_df = pd.DataFrame(columns=["stockname","datetime","news","link"])
        seen_keys   = set()

    stocks = fetch_index_csvs()
    total  = len(stocks)

    print(f"\n📋 {total} stocks | workers={MAX_WORKERS}\n", flush=True)

    accumulated = []
    done = 0
    found = 0

    t0 = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(fetch_stock, s, n):(s,n) for s,n in stocks}

        for f in as_completed(futures, timeout=TASK_TIMEOUT * total):
            sym, name = futures[f]
            done += 1

            try:
                df = f.result(timeout=TASK_TIMEOUT)
            except:
                df = pd.DataFrame()

            if df is not None and not df.empty:
                new_rows = df[
                    ~df.apply(lambda r: (r["stockname"], r["news"]) in seen_keys, axis=1)
                ]

                if not new_rows.empty:
                    seen_keys.update(zip(new_rows["stockname"], new_rows["news"]))
                    accumulated.append(new_rows)
                    found += len(new_rows)
                    print(f"✓ [{done}/{total}] {sym} → {len(new_rows)} new", flush=True)
                else:
                    print(f"· [{done}/{total}] {sym} → no new", flush=True)
            else:
                print(f"• [{done}/{total}] {sym} → no news", flush=True)

            if done % SAVE_EVERY == 0 and accumulated:
                new_df = pd.concat(accumulated, ignore_index=True)
                existing_df = trim_and_dedup(
                    pd.concat([existing_df, new_df], ignore_index=True),
                    cutoff
                )
                existing_df.to_csv(outfile, index=False, encoding="utf-8-sig")
                accumulated = []

                elapsed = time.time() - t0
                rate = done / elapsed
                eta = (total - done) / rate if rate else 0

                print(f"\n💾 Saved | {done}/{total} | ETA {eta/60:.1f} min\n", flush=True)

    if accumulated:
        new_df = pd.concat(accumulated, ignore_index=True)
        existing_df = trim_and_dedup(
            pd.concat([existing_df, new_df], ignore_index=True),
            cutoff
        )

    existing_df.to_csv(outfile, index=False, encoding="utf-8-sig")

    print(f"\n✅ Done | {found} new rows | total {len(existing_df)}", flush=True)


if __name__ == "__main__":
    main()
