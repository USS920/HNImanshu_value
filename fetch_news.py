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
NEWS_DAYS    = 7
MAX_WORKERS  = 10   # reduced: GNews blocks aggressive parallel calls
MAX_RETRIES  = 2
TASK_TIMEOUT = 20
SAVE_EVERY   = 50

INDEX_URLS = {
    "niftymicrocap250": "https://nsearchives.nseindia.com/content/indices/ind_niftymicrocap250_list.csv",
    "nifty500":         "https://nsearchives.nseindia.com/content/indices/ind_nifty500list.csv",
    "niftysmallcap500": "https://www.niftyindices.com/IndexConstituent/ind_niftysmallcap500_list.csv",
}

# One GNews instance per thread (avoids shared state issues)
def make_gnews():
    return GNews(language="en", country="IN", period=f"{NEWS_DAYS}d", max_results=15)


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
# FETCH NEWS (1 query per stock, not 3)
# ══════════════════════════════════════════════

def fetch_stock(symbol, name):
    gn        = make_gnews()
    name_low  = name.lower()
    sym_low   = symbol.lower()
    first_two = " ".join(name_low.split()[:2])

    # Use ONE best query — company name is most precise
    query = name

    articles = []
    for attempt in range(MAX_RETRIES):
        try:
            tmp = gn.get_news(query)
            if tmp and len(tmp) >= 1:
                articles = tmp
                break
            time.sleep(0.5 * (attempt + 1))
        except Exception:
            time.sleep(0.5 * (attempt + 1))

    if not articles:
        return pd.DataFrame()

    rows = []
    for a in articles:
        try:
            dt = parsedate_to_datetime(a.get("published date", ""))
        except Exception:
            continue

        title     = str(a.get("title", "")).strip()
        title_low = title.lower()
        link      = a.get("url", "")

        if not link:
            continue

        # Relevance filter
        if not (sym_low in title_low or name_low in title_low or first_two in title_low):
            continue

        rows.append({
            "stockname": symbol,
            "datetime":  dt.astimezone(IST).strftime("%Y-%m-%d %H:%M IST"),
            "news":      title,
            "link":      link,
        })

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df.drop_duplicates(subset=["link"], inplace=True)   # exact link dedup first
    df.sort_values("datetime", ascending=False, inplace=True)
    return df


# ══════════════════════════════════════════════
# FAST DEDUP USING TF-IDF STYLE FINGERPRINT
# ══════════════════════════════════════════════

def title_fingerprint(title: str) -> frozenset:
    """
    Bag-of-words fingerprint: remove stopwords, keep meaningful tokens.
    Two titles are near-duplicates if they share >= 60% of tokens.
    """
    STOPWORDS = {"the","a","an","in","of","to","for","and","or","is","are",
                 "on","at","with","by","from","its","as","this","that","was",
                 "be","has","have","had","it","s","co","ltd","limited"}
    tokens = set(title.lower().split()) - STOPWORDS
    return frozenset(tokens)


def jaccard(fp1: frozenset, fp2: frozenset) -> float:
    if not fp1 or not fp2:
        return 0.0
    return len(fp1 & fp2) / len(fp1 | fp2)


def dedup_similar_news(df: pd.DataFrame, threshold=0.6) -> pd.DataFrame:
    """
    Fast near-duplicate removal using pre-computed fingerprints.
    O(n * k) where k = kept titles so far (much faster than O(n²)).
    """
    keep_indices = []

    for sym, group in df.groupby("stockname", sort=False):
        kept_fps = []
        for idx, row in group.iterrows():
            fp = title_fingerprint(row["news"])
            if not any(jaccard(fp, kfp) >= threshold for kfp in kept_fps):
                kept_fps.append(fp)
                keep_indices.append(idx)

    return df.loc[keep_indices]


# ══════════════════════════════════════════════
# TRIM + FULL DEDUP PIPELINE
# ══════════════════════════════════════════════

def parse_dt(df):
    df["dt_parsed"] = pd.to_datetime(
        df["datetime"].str.replace(" IST", "", regex=False),
        errors="coerce"
    ).dt.tz_localize(IST)
    return df


def trim_and_dedup(df: pd.DataFrame, cutoff) -> pd.DataFrame:
    if df.empty:
        return df

    df = parse_dt(df)

    # 1. Time filter
    df = df[df["dt_parsed"] >= cutoff]

    # 2. Exact link dedup (fastest, do first)
    df = df.sort_values("dt_parsed", ascending=False)
    df = df.drop_duplicates(subset=["link"], keep="first")

    # 3. Exact title dedup
    df = df.drop_duplicates(subset=["stockname", "news"])

    # 4. Near-duplicate title dedup (per stock)
    df = dedup_similar_news(df, threshold=0.6)

    return df[["stockname", "datetime", "news", "link"]].reset_index(drop=True)


# ══════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════

def main():
    print("🚀 STARTED FETCH NEWS", flush=True)

    outfile = "multi_stock_news.csv"
    cutoff  = datetime.now(IST) - timedelta(days=NEWS_DAYS)
    
    # Delete existing CSV and start fresh
    #if os.path.exists(outfile):
        #os.remove(outfile)
        #print("🗑️ Deleted existing CSV, starting fresh", flush=True)

    if os.path.exists(outfile):
        existing_df = trim_and_dedup(pd.read_csv(outfile), cutoff)
        seen_links  = set(existing_df["link"].dropna())
        seen_keys   = set(zip(existing_df["stockname"], existing_df["news"]))
    else:
        existing_df = pd.DataFrame(columns=["stockname","datetime","news","link"])
        seen_links  = set()
        seen_keys   = set()

    stocks = fetch_index_csvs()
    total  = len(stocks)
    print(f"\n📋 {total} stocks | workers={MAX_WORKERS}\n", flush=True)

    accumulated = []
    done = found = 0
    t0 = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(fetch_stock, s, n): (s, n) for s, n in stocks}

        for f in as_completed(futures, timeout=TASK_TIMEOUT * total):
            sym, name = futures[f]
            done += 1

            try:
                df = f.result(timeout=TASK_TIMEOUT)
            except Exception:
                df = pd.DataFrame()

            if df is not None and not df.empty:
                # Filter by BOTH link and (stockname, news) to catch all dups
                mask = df.apply(
                    lambda r: r["link"] not in seen_links and
                              (r["stockname"], r["news"]) not in seen_keys,
                    axis=1
                )
                new_rows = df[mask]

                if not new_rows.empty:
                    seen_links.update(new_rows["link"])
                    seen_keys.update(zip(new_rows["stockname"], new_rows["news"]))
                    accumulated.append(new_rows)
                    found += len(new_rows)
                    print(f"✓ [{done}/{total}] {sym} → {len(new_rows)} new", flush=True)
                else:
                    print(f"· [{done}/{total}] {sym} → no new", flush=True)
            else:
                print(f"• [{done}/{total}] {sym} → no news", flush=True)

            # Periodic save
            if done % SAVE_EVERY == 0 and accumulated:
                new_df = pd.concat(accumulated, ignore_index=True)
                existing_df = trim_and_dedup(
                    pd.concat([existing_df, new_df], ignore_index=True),
                    cutoff
                )
                existing_df.to_csv(outfile, index=False, encoding="utf-8-sig")
                accumulated = []

                elapsed = time.time() - t0
                rate    = done / elapsed
                eta     = (total - done) / rate if rate else 0
                print(f"\n💾 Saved | {done}/{total} | ETA {eta/60:.1f} min\n", flush=True)

    # Final save
    if accumulated:
        new_df = pd.concat(accumulated, ignore_index=True)
        existing_df = pd.concat([existing_df, new_df], ignore_index=True)

    existing_df = trim_and_dedup(existing_df, cutoff)
    existing_df.to_csv(outfile, index=False, encoding="utf-8-sig")

    elapsed = time.time() - t0
    print(f"\n✅ Done | {found} new rows | total {len(existing_df)} | time {elapsed/60:.1f} min", flush=True)


if __name__ == "__main__":
    main()
