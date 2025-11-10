"""
Finnhub -> Aggregate 1-min OHLC + volume + raw trades -> Save as Excel -> Upload to Supabase storage
Run: python finnhub_to_supabase.py
Set env vars: FINNHUB_API_KEY, SUPABASE_URL, SUPABASE_KEY
"""

import os
import asyncio
import json
import time
from datetime import datetime, timezone
from collections import defaultdict
import io

import pandas as pd
import websockets
from supabase import create_client  # supabase-py
from dotenv import load_dotenv

load_dotenv()

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_BUCKET = os.getenv("SUPABASE_BUCKET", "market-excel")

# Replace with symbols you want
SYMBOLS = ["AAPL", "TSLA"]  # example — add whatever tickers you need

# How often to flush/upload Excel file (seconds)
UPLOAD_INTERVAL = 60 * 5  # upload every 5 minutes (adjust as needed)

# Websocket url
WS_URL = f"wss://ws.finnhub.io?token={FINNHUB_API_KEY}"

# In-memory stores
raw_trades = defaultdict(list)        # symbol -> list of trade dicts
minute_candles = defaultdict(list)    # symbol -> list of candle dicts

# Active aggregation buckets: symbol -> {minute_ts: {open, high, low, close, volume, trade_count}}
agg_buckets = {}

# Supabase client
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def ts_to_min_key(ts_seconds):
    # returns minute key like 2025-11-11T12:34:00Z
    dt = datetime.fromtimestamp(ts_seconds, tz=timezone.utc).replace(second=0, microsecond=0)
    return dt.isoformat()

def update_agg(symbol, price, volume, ts):
    """Aggregate trade into the current minute bucket for symbol."""
    minute_key = ts_to_min_key(ts)
    key = (symbol, minute_key)
    if key not in agg_buckets:
        agg_buckets[key] = {
            "symbol": symbol,
            "minute": minute_key,
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "volume": volume,
            "trade_count": 1,
            "start_ts": int(datetime.fromisoformat(minute_key).timestamp())
        }
    else:
        b = agg_buckets[key]
        b["high"] = max(b["high"], price)
        b["low"] = min(b["low"], price)
        b["close"] = price
        b["volume"] += volume
        b["trade_count"] += 1

async def handle_message(msg_json):
    """Parse Finnhub message and update stores."""
    # Example message types: {"type":"trade","data":[{...}]}
    try:
        print(f"Received message: {msg_json}")  # Debug log
        t = msg_json.get("type")
        if t == "trade":
            print(f"Processing {len(msg_json.get('data', []))} trades")  # Debug log
            for trade in msg_json.get("data", []):
                # trade sample fields: 's' symbol, 'p' price, 't' unix ms timestamp, 'v' volume
                symbol = trade.get("s")
                price = float(trade.get("p"))
                # Finnhub trade timestamp is usually milliseconds
                ts_ms = int(trade.get("t"))
                ts_s = ts_ms / 1000.0
                vol = int(trade.get("v", 1))

                print(f"Trade: {symbol} @ ${price} vol:{vol}")  # Debug log

                # store raw trade
                raw_trades[symbol].append({
                    "symbol": symbol,
                    "price": price,
                    "volume": vol,
                    "timestamp_utc": datetime.fromtimestamp(ts_s, tz=timezone.utc).isoformat(),
                    "ts": ts_s
                })

                # update minute aggregation
                update_agg(symbol, price, vol, ts_s)

        # Finnhub may send other messages (ping, subscription ack) — ignore
    except Exception as e:
        print("Error handling message:", e)

async def subscribe(ws, symbols):
    for sym in symbols:
        sub = {"type": "subscribe", "symbol": sym}
        await ws.send(json.dumps(sub))
        await asyncio.sleep(0.05)  # tiny pause to be polite

async def websocket_loop():
    reconnect_delay = 1
    while True:
        try:
            async with websockets.connect(WS_URL, ping_interval=20) as ws:
                print("Connected to Finnhub websocket")
                await subscribe(ws, SYMBOLS)
                reconnect_delay = 1
                async for message in ws:
                    msg_json = json.loads(message)
                    await handle_message(msg_json)
        except Exception as e:
            print("Websocket error:", e)
            print(f"Reconnecting in {reconnect_delay}s...")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(60, reconnect_delay * 2)

def finalize_old_buckets(cutoff_seconds=None):
    """Finalize minute buckets older than cutoff: move to minute_candles and remove from agg_buckets."""
    now = time.time()
    if cutoff_seconds is None:
        cutoff_seconds = 90  # finalize buckets older than 90s (safety margin)
    keys_to_finalize = []
    for (symbol, minute_key), b in list(agg_buckets.items()):
        if now - b["start_ts"] >= cutoff_seconds:
            # finalize
            minute_candles[symbol].append({
                "symbol": symbol,
                "minute": minute_key,
                "open": b["open"],
                "high": b["high"],
                "low": b["low"],
                "close": b["close"],
                "volume": b["volume"],
                "trade_count": b["trade_count"],
                "start_ts": b["start_ts"]
            })
            keys_to_finalize.append((symbol, minute_key))
    for k in keys_to_finalize:
        del agg_buckets[k]

def build_excel_bytes():
    """Make an Excel file in-memory containing two sheets: 'candles' and 'trades'."""
    # Combine dataframes for all symbols
    candle_rows = []
    trade_rows = []
    for sym, rows in minute_candles.items():
        candle_rows.extend(rows)
    for sym, rows in raw_trades.items():
        trade_rows.extend(rows)

    df_c = pd.DataFrame(candle_rows)
    df_t = pd.DataFrame(trade_rows)

    # Ensure columns exist
    if df_c.empty:
        df_c = pd.DataFrame(columns=["symbol","minute","open","high","low","close","volume","trade_count","start_ts"])
    if df_t.empty:
        df_t = pd.DataFrame(columns=["symbol","price","volume","timestamp_utc","ts"])

    # Create Excel in-memory
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        df_c.to_excel(writer, sheet_name="candles", index=False)
        df_t.to_excel(writer, sheet_name="trades", index=False)
    out.seek(0)
    return out.read()

async def periodic_upload_loop():
    """Every UPLOAD_INTERVAL seconds, finalize buckets, build excel, upload to Supabase."""
    while True:
        try:
            # finalize any old minute buckets first
            finalize_old_buckets(cutoff_seconds=70)

            # Debug: show data counts
            total_trades = sum(len(trades) for trades in raw_trades.values())
            total_candles = sum(len(candles) for candles in minute_candles.values())
            print(f"Data summary: {total_trades} trades, {total_candles} candles")

            # produce excel
            excel_bytes = build_excel_bytes()
            # file name with utc timestamp
            fname = datetime.now(timezone.utc).strftime("market_data_%Y%m%dT%H%M%SZ.xlsx")
            path = f"{fname}"

            # upload to supabase storage
            res = supabase.storage.from_(SUPABASE_BUCKET).upload(
                path, 
                excel_bytes,
                file_options={"content-type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"}
            )
            # supabase-py returns dict-like response — print minimal info
            print(f"Uploaded {path} to Supabase bucket {SUPABASE_BUCKET}")
        except Exception as e:
            print("Upload error:", e)

        # sleep
        await asyncio.sleep(UPLOAD_INTERVAL)

async def main():
    # Basic check: env vars
    if not FINNHUB_API_KEY or not SUPABASE_URL or not SUPABASE_KEY:
        print("Missing FINNHUB_API_KEY or SUPABASE_URL or SUPABASE_KEY in environment.")
        return

    # Create bucket if it doesn't exist
    try:
        supabase.storage.create_bucket(SUPABASE_BUCKET)
        print(f"Created bucket: {SUPABASE_BUCKET}")
    except Exception as e:
        print(f"Bucket creation (may already exist): {e}")

    # Start websocket listener and upload loop concurrently
    await asyncio.gather(
        websocket_loop(),
        periodic_upload_loop()
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopped by user.")
