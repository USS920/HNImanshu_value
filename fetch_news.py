import os
import sys
import time
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from email.utils import parsedate_to_datetime
import requests
from io import StringIO
import pandas as pd
from gnews import GNews

IST          = timezone(timedelta(hours=5, minutes=30))
NEWS_DAYS    = 15
MAX_WORKERS  = 50        # raise if your network/CPU can handle it; GNews is I/O bound
MAX_RETRIES  = 2         # per-stock retry attempts before giving up
TASK_TIMEOUT = 15        # seconds per stock fetch
SAVE_EVERY   = 50        # write CSV after every N completed stocks (crash safety)

INDEX_URLS = {
    "niftymicrocap250": "https://nsearchives.nseindia.com/content/indices/ind_niftymicrocap250_list.csv",
    "nifty500":         "https://nsearchives.nseindia.com/content/indices/ind_nifty500list.csv",
    "niftysmallcap500": "https://www.niftyindices.com/IndexConstituent/ind_niftysmallcap500_list.csv",
}


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
            sym_col  = next((c for c in cols if "Symbol"       in c), cols[0])
            name_col = next((c for c in cols if "Company Name" in c), cols[1])
            df = df[[sym_col, name_col]]
            df.columns = ["symbol", "name"]
            frames.append(df.dropna())
        except Exception:
            continue

    if not frames:
        sys.exit(1)

    all_stocks = (
        pd.concat(frames, ignore_index=True)
        .drop_duplicates(subset=["symbol"])
        .reset_index(drop=True)
    )
    return list(zip(all_stocks["symbol"], all_stocks["name"]))


# ══════════════════════════════════════════════
# NEWS FETCH  (one call per stock)
# ══════════════════════════════════════════════

# Single shared GNews instance — it carries no mutable per-request state.
_gnews = GNews(language="en", country="IN", period=f"{NEWS_DAYS}d", max_results=100)


def fetch_stock(symbol: str, name: str) -> pd.DataFrame:
    """
    Fetch news for one stock. Returns a DataFrame (possibly empty).
    Retries up to MAX_RETRIES times internally so the pool never needs
    to resubmit — keeps concurrency high and avoids re-queuing overhead.
    """
    name_low  = name.lower()
    sym_low   = symbol.lower()
    first_two = " ".join(name_low.split()[:2])

    for attempt in range(MAX_RETRIES):
        try:
            articles = _gnews.get_news(name)
        except Exception:
            if attempt < MAX_RETRIES - 1:
                time.sleep(0.5 * (attempt + 1))   # brief back-off before retry
                continue
            return pd.DataFrame()

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

            if not (sym_low in title_low or name_low in title_low or first_two in title_low):
                continue

            rows.append({
                "stockname": symbol,
                "datetime":  dt.astimezone(IST).strftime("%Y-%m-%d %H:%M IST"),
                "news":      title,
                "link":      a.get("url", ""),
            })

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        df.sort_values("datetime", ascending=False, inplace=True)
        return df

    return pd.DataFrame()


# ══════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════

def _parse_dt(df: pd.DataFrame) -> pd.DataFrame:
    """Add a tz-aware dt_parsed column (modifies a copy)."""
    df = df.copy()
    df["dt_parsed"] = pd.to_datetime(
        df["datetime"].str.replace(" IST", "", regex=False),
        errors="coerce",
    ).dt.tz_localize(IST)
    return df


def _trim_and_dedup(df: pd.DataFrame, cutoff: datetime) -> pd.DataFrame:
    """Drop old rows and exact duplicate (stockname, news) pairs."""
    if "dt_parsed" not in df.columns:
        df = _parse_dt(df)
    df = df[df["dt_parsed"] >= cutoff]
    df = df.drop_duplicates(subset=["stockname", "news"])
    return df[["stockname", "datetime", "news", "link"]]


def _save(df: pd.DataFrame, path: str):
    df.to_csv(path, index=False, encoding="utf-8-sig")


# ══════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════

def main():
    outfile = "multi_stock_news.csv"
    cutoff  = datetime.now(IST) - timedelta(days=NEWS_DAYS)

    # ── Load + clean existing data once ──────────────────────────────────
    if os.path.exists(outfile):
        existing_df = _trim_and_dedup(pd.read_csv(outfile), cutoff)
        seen_keys   = set(zip(existing_df["stockname"], existing_df["news"]))
    else:
        existing_df = pd.DataFrame(columns=["stockname", "datetime", "news", "link"])
        seen_keys   = set()

    stocks = fetch_index_csvs()
    total  = len(stocks)
    print(f"\n📋 {total} stocks to fetch  |  workers={MAX_WORKERS}  |  cutoff={cutoff:%d %b %Y}\n")

    # ── Single flat thread pool over all stocks ───────────────────────────
    # No batching loop — every stock is submitted at once and results are
    # processed as they complete. This maximises CPU/network utilisation.
    accumulated  = []          # new rows collected since last save
    done_count   = 0
    found_count  = 0
    failed       = []

    t0 = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        future_to_stock = {
            pool.submit(fetch_stock, sym, name): (sym, name)
            for sym, name in stocks
        }

        for future in as_completed(future_to_stock, timeout=TASK_TIMEOUT * total):
            sym, name = future_to_stock[future]
            done_count += 1

            try:
                df = future.result(timeout=TASK_TIMEOUT)
            except Exception:
                df = pd.DataFrame()

            if df is not None and not df.empty:
                # Filter to only genuinely new rows (not already in existing or
                # accumulated this run) to keep dedup fast without re-scanning
                # the full DataFrame each time.
                new_rows = df[
                    ~df.apply(lambda r: (r["stockname"], r["news"]) in seen_keys, axis=1)
                ]
                if not new_rows.empty:
                    seen_keys.update(zip(new_rows["stockname"], new_rows["news"]))
                    accumulated.append(new_rows)
                    found_count += len(new_rows)
                    print(f"  ✓ [{done_count}/{total}] {sym} → {len(new_rows)} new")
                else:
                    print(f"  · [{done_count}/{total}] {sym} → already up-to-date")
            else:
                print(f"  • [{done_count}/{total}] {sym} → no news")
                failed.append(sym)

            # Periodic save — merge accumulated rows with existing and flush
            if done_count % SAVE_EVERY == 0 and accumulated:
                new_df      = pd.concat(accumulated, ignore_index=True)
                existing_df = _trim_and_dedup(
                    pd.concat([existing_df, new_df], ignore_index=True), cutoff
                )
                _save(existing_df, outfile)
                accumulated = []
                elapsed = time.time() - t0
                rate    = done_count / elapsed
                eta     = (total - done_count) / rate if rate > 0 else 0
                print(f"\n  💾 Checkpoint saved  |  {done_count}/{total} done"
                      f"  |  {found_count} new rows"
                      f"  |  ETA {eta/60:.1f} min\n")

    # ── Final save ────────────────────────────────────────────────────────
    if accumulated:
        new_df      = pd.concat(accumulated, ignore_index=True)
        existing_df = _trim_and_dedup(
            pd.concat([existing_df, new_df], ignore_index=True), cutoff
        )

    _save(existing_df, outfile)

    elapsed = time.time() - t0
    print(f"\n✅ Done in {elapsed/60:.1f} min"
          f"  |  {found_count} new rows added"
          f"  |  {len(existing_df)} total rows"
          f"  |  {len(failed)} stocks with no news")

    if failed:
        print(f"   No-news stocks: {', '.join(failed[:20])}"
              + (" ..." if len(failed) > 20 else ""))


if __name__ == "__main__":
    main()
