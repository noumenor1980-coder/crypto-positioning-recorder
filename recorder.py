#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Crypto Positioning Recorder
============================
Μαζεύει ΔΩΡΕΑΝ δεδομένα δομής αγοράς (market structure) από τρία exchanges,
χρησιμοποιώντας ΜΟΝΟ public market-data endpoints — ΚΑΝΕΝΑ API key.

Τι μαζεύει ανά σύμβολο:
  - Binance : open interest, funding (premiumIndex), global & top-trader long/short
              account ratios, top-trader position ratio, taker buy/sell ratio
  - Bybit   : open interest, funding (tickers), account long/short ratio
  - MEXC    : open interest (holdVol), funding rate  (το MEXC δεν δίνει
              long/short ratio μέσω public API — γι' αυτό λείπει)

Αποθήκευση: "long format" Parquet, ένα αρχείο ανά exchange ανά ημέρα:
  data/<exchange>/<YYYY-MM-DD>.parquet
Κάθε γραμμή: arrival_iso, arrival_ms, event_ms, exchange, symbol, source, metric, value

Σχεδιαστικές αρχές:
  - Point-in-time: κρατάμε ΔΥΟ χρονοσημάνσεις — arrival (πότε το μαζέψαμε)
    και event (πότε ισχύει το δεδομένο, όπου το δίνει το exchange).
  - Ανθεκτικότητα: κάθε κλήση είναι απομονωμένη· αν ένα exchange/endpoint πέσει,
    τα υπόλοιπα συνεχίζουν κανονικά.
  - Σταθερό schema: νέα metrics = νέες τιμές στη στήλη `metric`, ΟΧΙ αλλαγή schema.

Τοπική εκτέλεση (Windows):  py -3.12 recorder.py
"""

from __future__ import annotations

import os
import sys
import time
import datetime as dt
from pathlib import Path

import requests
import polars as pl

# ───────────────────────── ΡΥΘΜΙΣΕΙΣ ─────────────────────────
# Βασικά assets (base). Ο κώδικας φτιάχνει μόνος το σύμβολο ανά exchange.
SYMBOLS = ["BTC", "ETH", "SOL", "BNB", "DOGE", "XRP"]

BINANCE_PERIOD = "15m"     # για τα /futures/data/ endpoints
BYBIT_PERIOD = "15min"     # για OI & ratio του Bybit

HTTP_TIMEOUT = 12.0        # δευτερόλεπτα ανά κλήση
HTTP_RETRIES = 3
SLEEP_BETWEEN_SYMBOLS = 0.20
DATA_DIR = Path("data")

# Σταθερό schema του Parquet (αποτρέπει εκπλήξεις στο concat)
SCHEMA = {
    "arrival_iso": pl.Utf8,
    "arrival_ms": pl.Int64,
    "event_ms": pl.Int64,     # nullable
    "exchange": pl.Utf8,
    "symbol": pl.Utf8,
    "source": pl.Utf8,
    "metric": pl.Utf8,
    "value": pl.Float64,
}

BINANCE = "https://fapi.binance.com"
BYBIT = "https://api.bybit.com"
MEXC = "https://contract.mexc.com"

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "positioning-recorder/1.0 (+github-actions)"})


# ───────────────────────── ΒΟΗΘΗΤΙΚΑ ─────────────────────────
def log(msg: str) -> None:
    ts = dt.datetime.now(dt.timezone.utc).strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def get_json(url: str, params: dict | None = None):
    """GET με retries. Επιστρέφει το JSON ή None (ποτέ δεν πετάει exception)."""
    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            r = SESSION.get(url, params=params, timeout=HTTP_TIMEOUT)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (418, 429):           # rate limit
                time.sleep(2 * attempt)
                continue
            # 4xx/5xx που δεν αξίζει retry
            log(f"  ! {r.status_code} {url}")
            return None
        except Exception as e:                         # noqa: BLE001
            if attempt == HTTP_RETRIES:
                log(f"  ! σφάλμα δικτύου {url}: {e}")
            time.sleep(1.0 * attempt)
    return None


def add(records: list, arrival_iso: str, arrival_ms: int, exch: str, sym: str,
        source: str, metric: str, value, event_ms=None) -> None:
    """Προσθέτει μία γραμμή long-format, αφού μετατρέψει την τιμή σε float."""
    if value is None:
        return
    try:
        v = float(value)
    except (TypeError, ValueError):
        return
    em = None
    if event_ms is not None:
        try:
            em = int(event_ms)
        except (TypeError, ValueError):
            em = None
    records.append({
        "arrival_iso": arrival_iso, "arrival_ms": arrival_ms, "event_ms": em,
        "exchange": exch, "symbol": sym, "source": source, "metric": metric, "value": v,
    })


# ───────────────────────── BINANCE ─────────────────────────
def fetch_binance(base: str, rec: list, a_iso: str, a_ms: int) -> None:
    s = f"{base}USDT"
    e = "binance"

    # 1) Open interest (τρέχον)
    j = get_json(f"{BINANCE}/fapi/v1/openInterest", {"symbol": s})
    if isinstance(j, dict):
        add(rec, a_iso, a_ms, e, s, "openInterest", "openInterest",
            j.get("openInterest"), j.get("time"))

    # 2) Funding / mark / index (premiumIndex)
    j = get_json(f"{BINANCE}/fapi/v1/premiumIndex", {"symbol": s})
    if isinstance(j, dict):
        et = j.get("time")
        add(rec, a_iso, a_ms, e, s, "premiumIndex", "lastFundingRate", j.get("lastFundingRate"), et)
        add(rec, a_iso, a_ms, e, s, "premiumIndex", "markPrice", j.get("markPrice"), et)
        add(rec, a_iso, a_ms, e, s, "premiumIndex", "indexPrice", j.get("indexPrice"), et)
        add(rec, a_iso, a_ms, e, s, "premiumIndex", "nextFundingTime", j.get("nextFundingTime"), et)

    # 3) Long/Short ratios + taker ratio (κάθε ένα: τελευταίο σημείο)
    ls_endpoints = {
        "globalLongShortAccountRatio": "/futures/data/globalLongShortAccountRatio",
        "topLongShortAccountRatio": "/futures/data/topLongShortAccountRatio",
        "topLongShortPositionRatio": "/futures/data/topLongShortPositionRatio",
        "takerlongshortRatio": "/futures/data/takerlongshortRatio",
    }
    for source, path in ls_endpoints.items():
        arr = get_json(f"{BINANCE}{path}", {"symbol": s, "period": BINANCE_PERIOD, "limit": 1})
        if isinstance(arr, list) and arr:
            d = arr[-1]
            et = d.get("timestamp")
            for key in ("longShortRatio", "longAccount", "shortAccount",
                        "buySellRatio", "buyVol", "sellVol"):
                if key in d:
                    add(rec, a_iso, a_ms, e, s, source, key, d.get(key), et)


# ───────────────────────── BYBIT ─────────────────────────
def fetch_bybit(base: str, rec: list, a_iso: str, a_ms: int) -> None:
    s = f"{base}USDT"
    e = "bybit"

    # 1) tickers: funding + OI + τιμές σε μία κλήση
    j = get_json(f"{BYBIT}/v5/market/tickers", {"category": "linear", "symbol": s})
    if isinstance(j, dict):
        lst = (j.get("result") or {}).get("list") or []
        if lst:
            it = lst[0]
            for key in ("openInterest", "openInterestValue", "fundingRate",
                        "markPrice", "indexPrice", "lastPrice", "nextFundingTime"):
                if key in it:
                    add(rec, a_iso, a_ms, e, s, "tickers", key, it.get(key))

    # 2) open-interest endpoint (με δικό του timestamp)
    j = get_json(f"{BYBIT}/v5/market/open-interest",
                 {"category": "linear", "symbol": s, "intervalTime": BYBIT_PERIOD, "limit": 1})
    if isinstance(j, dict):
        lst = (j.get("result") or {}).get("list") or []
        if lst:
            it = lst[0]
            add(rec, a_iso, a_ms, e, s, "openInterestHist", "openInterest",
                it.get("openInterest"), it.get("timestamp"))

    # 3) account long/short ratio
    j = get_json(f"{BYBIT}/v5/market/account-ratio",
                 {"category": "linear", "symbol": s, "period": BYBIT_PERIOD, "limit": 1})
    if isinstance(j, dict):
        lst = (j.get("result") or {}).get("list") or []
        if lst:
            it = lst[0]
            et = it.get("timestamp")
            add(rec, a_iso, a_ms, e, s, "accountRatio", "buyRatio", it.get("buyRatio"), et)
            add(rec, a_iso, a_ms, e, s, "accountRatio", "sellRatio", it.get("sellRatio"), et)


# ───────────────────────── MEXC ─────────────────────────
def fetch_mexc(base: str, rec: list, a_iso: str, a_ms: int) -> None:
    s = f"{base}_USDT"
    e = "mexc"

    # 1) Funding rate (το symbol μπαίνει στο path)
    j = get_json(f"{MEXC}/api/v1/contract/funding_rate/{s}")
    if isinstance(j, dict) and j.get("success"):
        d = j.get("data") or {}
        add(rec, a_iso, a_ms, e, s, "fundingRate", "fundingRate",
            d.get("fundingRate"), d.get("timestamp"))
        add(rec, a_iso, a_ms, e, s, "fundingRate", "nextSettleTime", d.get("nextSettleTime"))

    # 2) Ticker — holdVol = open interest
    j = get_json(f"{MEXC}/api/v1/contract/ticker", {"symbol": s})
    if isinstance(j, dict) and j.get("success"):
        d = j.get("data") or {}
        et = d.get("timestamp")
        add(rec, a_iso, a_ms, e, s, "ticker", "openInterest", d.get("holdVol"), et)
        add(rec, a_iso, a_ms, e, s, "ticker", "lastPrice", d.get("lastPrice"), et)
        add(rec, a_iso, a_ms, e, s, "ticker", "indexPrice", d.get("indexPrice"), et)
        add(rec, a_iso, a_ms, e, s, "ticker", "fairPrice", d.get("fairPrice"), et)


# ───────────────────────── ΕΓΓΡΑΦΗ ─────────────────────────
def write_records(df: pl.DataFrame, date_str: str) -> dict:
    """Γράφει ανά exchange στο data/<exchange>/<date>.parquet (read→concat→write)."""
    counts = {}
    for exch in df["exchange"].unique().to_list():
        sub = df.filter(pl.col("exchange") == exch)
        path = DATA_DIR / exch / f"{date_str}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            old = pl.read_parquet(path)
            sub = pl.concat([old, sub], how="vertical_relaxed")
        sub.write_parquet(path)
        counts[exch] = sub.height
    return counts


def notify_telegram(text: str) -> None:
    """Προαιρετική ειδοποίηση. Δουλεύει μόνο αν υπάρχουν τα env vars."""
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat = os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat:
        return
    try:
        SESSION.post(f"https://api.telegram.org/bot{token}/sendMessage",
                     data={"chat_id": chat, "text": text}, timeout=10)
    except Exception:  # noqa: BLE001
        pass


def main() -> int:
    arrival = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
    a_iso = arrival.strftime("%Y-%m-%dT%H:%M:%SZ")
    a_ms = int(arrival.timestamp() * 1000)
    date_str = arrival.strftime("%Y-%m-%d")

    log(f"Έναρξη snapshot {a_iso} — {len(SYMBOLS)} σύμβολα")
    records: list = []
    for base in SYMBOLS:
        fetch_binance(base, records, a_iso, a_ms)
        fetch_bybit(base, records, a_iso, a_ms)
        fetch_mexc(base, records, a_iso, a_ms)
        time.sleep(SLEEP_BETWEEN_SYMBOLS)

    if not records:
        log("ΣΦΑΛΜΑ: καμία εγγραφή — πιθανό πρόβλημα δικτύου ή μπλοκαρισμένα endpoints.")
        notify_telegram(f"⚠️ Recorder: 0 εγγραφές στο snapshot {a_iso}")
        return 1

    df = pl.DataFrame(records, schema=SCHEMA)
    counts = write_records(df, date_str)
    summary = " | ".join(f"{k}:{v}" for k, v in sorted(counts.items()))
    log(f"OK — {df.height} νέες εγγραφές αυτού του run. Σύνολα ημέρας → {summary}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
