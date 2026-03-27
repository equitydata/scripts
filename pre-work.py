#!/usr/bin/env python3
"""
Daily pre-processing for the NIFTY-50 pipeline.

Creates one CSV per NIFTY-50 symbol under `filter_data/` by filtering rows
from `sec_bhavdata_download/` using symbols from `index/MW-NIFTY-50-*.csv`.

Default output dir matches your existing folder: `../filter_data`.
"""

from __future__ import annotations

import argparse
import glob
import os
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import pandas as pd


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Create per-stock CSVs for NIFTY-50.")
    p.add_argument("--base-path", default="/Users/sdash/Downloads/equitydata", help="Repo base path.")
    p.add_argument(
        "--nifty-50-file",
        default="index/MW-NIFTY-50-19-Mar-2026.csv",
        help="NIFTY-50 symbols CSV inside --base-path.",
    )
    p.add_argument(
        "--bhavdata-glob",
        default="sec_bhavdata_download/*.csv",
        help="Bhavdata CSV glob inside --base-path.",
    )
    p.add_argument(
        "--output-dir",
        default="filter_data",
        help="Output directory (inside --base-path) for per-symbol CSVs.",
    )
    p.add_argument(
        "--preferred-series",
        default="EQ",
        help="Preferred SERIES value to keep (falls back per file if missing).",
    )
    p.add_argument(
        "--incremental",
        action="store_true",
        help="Only append rows newer than the existing per-symbol max(date).",
    )
    p.add_argument(
        "--max-files",
        type=int,
        default=0,
        help="Limit how many bhavdata files to process (0 means all).",
    )
    return p.parse_args()


def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip()
    return df


def load_nifty_symbols(nifty_csv: Path) -> List[str]:
    df = pd.read_csv(nifty_csv)
    df.columns = df.columns.str.replace(r"\s+", " ", regex=True).str.strip().str.upper()
    if "SYMBOL" not in df.columns:
        raise KeyError(f"Expected 'SYMBOL' in {nifty_csv}. Found: {list(df.columns)}")
    df["SYMBOL"] = df["SYMBOL"].astype(str).str.strip().str.upper()
    df = df[df["SYMBOL"] != "NIFTY 50"]
    symbols = df["SYMBOL"].tolist()
    print(f"NIFTY symbols loaded: {len(symbols)}")
    return symbols


def read_csv_robust(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path, encoding="utf-8")
    except Exception:
        return pd.read_csv(path, encoding="latin1", on_bad_lines="skip")


def ensure_required_columns(df: pd.DataFrame, required: Iterable[str], file: Path) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"Missing columns in {file}: {missing}. Found: {list(df.columns)[:30]}...")


def update_max_dates(output_dir: Path, symbols: List[str]) -> Dict[str, pd.Timestamp]:
    max_dates: Dict[str, pd.Timestamp] = {}
    for sym in symbols:
        f = output_dir / f"{sym}.csv"
        if not f.exists():
            continue
        try:
            tmp = pd.read_csv(f, usecols=["date"])
            if "date" in tmp.columns and not tmp.empty:
                max_dates[sym] = pd.to_datetime(tmp["date"], errors="coerce").max()
        except Exception:
            # If the file is malformed, just disable incremental for that symbol.
            pass
    return max_dates


def main() -> None:
    args = parse_args()

    base = Path(args.base_path)
    nifty_csv = base / args.nifty_50_file
    bhav_glob = str(base / args.bhavdata_glob)
    out_dir = base / args.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    preferred_series = args.preferred_series.strip().upper()

    symbols = load_nifty_symbols(nifty_csv)
    nifty_set = set(symbols)

    files = sorted(glob.glob(bhav_glob))
    if args.max_files and args.max_files > 0:
        files = files[: args.max_files]
    print(f"Bhavdata files to process: {len(files)}")

    max_dates: Dict[str, pd.Timestamp] = {}
    if args.incremental:
        max_dates = update_max_dates(out_dir, symbols)
        print(f"Incremental mode: loaded max dates for {len(max_dates)} symbols.")

    written_rows_total = 0
    processed_files = 0

    required = ["SYMBOL", "DATE1", "OPEN_PRICE", "HIGH_PRICE", "LOW_PRICE", "CLOSE_PRICE", "TTL_TRD_QNTY", "SERIES"]

    for file in files:
        file_path = Path(file)
        df = read_csv_robust(file_path)
        if df.empty:
            continue

        df = normalize_cols(df)
        ensure_required_columns(df, required, file_path)

        # Clean/parse date + normalize symbol.
        df["DATE1"] = df["DATE1"].astype(str).str.strip()
        df["DATE1"] = pd.to_datetime(df["DATE1"], errors="coerce")
        df = df.dropna(subset=["DATE1"])
        df["SYMBOL"] = df["SYMBOL"].astype(str).str.strip().str.upper()
        df["SERIES"] = df["SERIES"].astype(str).str.strip().str.upper()

        # Filter to the NIFTY 50 universe.
        df = df[df["SYMBOL"].isin(nifty_set)]
        if df.empty:
            continue

        # Keep preferred SERIES when available; otherwise fallback to the most frequent SERIES in this file.
        df_eq = df[df["SERIES"] == preferred_series]
        if not df_eq.empty:
            df = df_eq
        else:
            # fallback per file
            top_series = str(df["SERIES"].value_counts().index[0])
            df = df[df["SERIES"] == top_series]

        if df.empty:
            continue

        # Rename to standardized column names.
        df = df.rename(
            columns={
                "SYMBOL": "symbol",
                "DATE1": "date",
                "OPEN_PRICE": "open",
                "HIGH_PRICE": "high",
                "LOW_PRICE": "low",
                "CLOSE_PRICE": "close",
                "TTL_TRD_QNTY": "volume",
            }
        )
        df["series"] = df["SERIES"]
        df = df.drop(columns=["SERIES"])

        # Incremental filtering (per symbol).
        if args.incremental:
            # We filter after the pivot to per-symbol files, but before writing we drop old rows.
            pass

        # Append per symbol.
        out_cols = ["symbol", "date", "open", "high", "low", "close", "volume", "series"]
        df = df[out_cols].sort_values(["symbol", "date"])

        for sym, grp in df.groupby("symbol", sort=False):
            if args.incremental and sym in max_dates and pd.notna(max_dates[sym]):
                grp = grp[grp["date"] > max_dates[sym]]
                if grp.empty:
                    continue

            out_path = out_dir / f"{sym}.csv"
            header = not out_path.exists()
            grp.to_csv(out_path, mode="a", header=header, index=False)
            written_rows_total += len(grp)

        processed_files += 1

        if processed_files % 50 == 0:
            print(f"Processed {processed_files} files; rows written so far: {written_rows_total}")

    # Final sort + de-dup per file.
    print("Post-processing: sorting and de-duplicating per-symbol CSVs...")
    for sym in symbols:
        out_path = out_dir / f"{sym}.csv"
        if not out_path.exists():
            continue
        tmp = pd.read_csv(out_path)
        if "date" in tmp.columns and not tmp.empty:
            tmp["date"] = pd.to_datetime(tmp["date"], errors="coerce")
            tmp = tmp.dropna(subset=["date"]).sort_values("date")
        else:
            tmp = tmp.sort_index()
        # De-dup by date+symbol (date is the main key).
        if "symbol" in tmp.columns and "date" in tmp.columns:
            tmp = tmp.drop_duplicates(subset=["symbol", "date"], keep="last")
        else:
            tmp = tmp.drop_duplicates(keep="last")
        tmp.to_csv(out_path, index=False)

    print(f"Done. Total rows written: {written_rows_total}")


if __name__ == "__main__":
    main()

