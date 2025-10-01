
"""
Fetch the latest UPCOMING earnings date for a list of tickers using yfinance.

Usage:
  1) pip install yfinance pandas
  2) Put your tickers in tickers.csv with a column named 'ticker' (one per row).
  3) Run: python earnings_dates_yf.py --in tickers.csv --out earnings_dates.csv

Notes:
  - We filter for earnings dates >= "today" and pick the earliest (the next event).
  - If no upcoming date is found, we fall back to the most recent past date.
  - Output columns:
      ticker, next_earnings_date, when, source, got_upcoming
  - 'when' is "AMC" (after market), "BMO" (before market), or blank if unknown.
"""
import argparse
from datetime import datetime, timezone
import sys
import pandas as pd
import yfinance as yf

def get_next_earnings_for_ticker(ticker: str) -> dict:
    now = datetime.now(timezone.utc)
    try:
        t = yf.Ticker(ticker)
        df = t.get_earnings_dates(limit=16)

        if df is None or len(df) == 0:
            return {"ticker": ticker, "next_earnings_date": None, "when": None, "source": "yfinance:get_earnings_dates(empty)", "got_upcoming": False}

        # Normalize date column/index
        if "Earnings Date" in df.columns:
            dt_col = pd.to_datetime(df["Earnings Date"], errors="coerce", utc=True)
        else:
            dt_col = pd.to_datetime(df.index, errors="coerce", utc=True)

        df = df.assign(_dt=dt_col).dropna(subset=["_dt"])

        # "When" column can be named differently across versions
        when_col = next((c for c in ["Time", "TimeOfDay", "When", "Time (ET)"] if c in df.columns), None)

        # Upcoming first, else most recent past
        upcoming = df[df["_dt"] >= now].sort_values("_dt")
        if len(upcoming) > 0:
            row = upcoming.iloc[0]
            return {"ticker": ticker, "next_earnings_date": row["_dt"].isoformat(), "when": (str(row.get(when_col)) if when_col else None), "source": "yfinance:get_earnings_dates", "got_upcoming": True}

        past = df[df["_dt"] < now].sort_values("_dt")
        if len(past) > 0:
            row = past.iloc[-1]
            return {"ticker": ticker, "next_earnings_date": row["_dt"].isoformat(), "when": (str(row.get(when_col)) if when_col else None), "source": "yfinance:get_earnings_dates", "got_upcoming": False}

        return {"ticker": ticker, "next_earnings_date": None, "when": None, "source": "yfinance:get_earnings_dates(no-dates)", "got_upcoming": False}

    except ImportError as e:
        return {"ticker": ticker, "next_earnings_date": None, "when": None, "source": f"error:ImportError:{e}", "got_upcoming": False}
    except Exception as e:
        return {"ticker": ticker, "next_earnings_date": None, "when": None, "source": f"error:{type(e).__name__}:{e}", "got_upcoming": False}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="infile", default="tickers.csv", help="CSV with a column named 'ticker'")
    ap.add_argument("--out", dest="outfile", default="earnings_dates.csv", help="Output CSV path")
    args = ap.parse_args()

    tickers = pd.read_csv(args.infile)
    if "ticker" not in tickers.columns:
        print("Input CSV must contain a 'ticker' column.", file=sys.stderr)
        sys.exit(2)

    results = [get_next_earnings_for_ticker(t) for t in tickers["ticker"].astype(str).str.strip() if t]
    out_df = pd.DataFrame(results)
    # Sort by date, keeping None at bottom
    out_df["_sort"] = pd.to_datetime(out_df["next_earnings_date"], errors="coerce", utc=True)
    out_df = out_df.sort_values(["_sort", "ticker"], na_position="last").drop(columns=["_sort"])
    out_df.to_csv(args.outfile, index=False)
    print(f"Wrote {len(out_df)} rows to {args.outfile}")

if __name__ == "__main__":
    main()
