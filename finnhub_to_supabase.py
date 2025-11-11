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
SYMBOLS = ["BINANCE:BTCUSDT", "BTCUSD", "BTC-USD"]  # Try multiple formats

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

# Global counters
candle_count = 0
last_candle_time = None

def update_agg(symbol, price, volume, ts):
    """Aggregate trade into the current minute bucket for symbol."""
    global candle_count, last_candle_time
    
    minute_key = ts_to_min_key(ts)
    key = (symbol, minute_key)
    
    # Check if we're starting a new minute - finalize the previous one immediately
    current_minute = int(ts) // 60 * 60
    for (existing_symbol, existing_minute_key), bucket in list(agg_buckets.items()):
        existing_minute_ts = int(datetime.fromisoformat(existing_minute_key).timestamp())
        if existing_symbol == symbol and existing_minute_ts < current_minute:
            # This minute is complete - insert immediately
            candle_data = {
                "timestamp": existing_minute_key,
                "open": bucket["open"],
                "high": bucket["high"],
                "low": bucket["low"],
                "close": bucket["close"],
                "volume": bucket["volume"],
                "trades": bucket["trade_count"]
            }
            
            try:
                supabase.table("candles").insert(candle_data).execute()
                candle_count += 1
                last_candle_time = existing_minute_key
                print(f"🔥 Candle #{candle_count}: {existing_symbol} {existing_minute_key} O:{bucket['open']:.2f} H:{bucket['high']:.2f} L:{bucket['low']:.2f} C:{bucket['close']:.2f} V:{bucket['volume']:.4f} T:{bucket['trade_count']}")
            except Exception as e:
                print(f"❌ Error inserting candle: {e}")
            
            # Remove completed bucket
            del agg_buckets[(existing_symbol, existing_minute_key)]
    
    # Now update current minute bucket
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
    try:
        t = msg_json.get("type")
        
        if t == "ping":
            # Don't log every ping - too spammy
            return {"type": "pong"}
        elif t == "trade":
            print(f"📈 Processing {len(msg_json.get('data', []))} trades for BINANCE:BTCUSDT")
            for trade in msg_json.get("data", []):
                symbol = trade.get("s")
                price = float(trade.get("p"))
                ts_ms = int(trade.get("t"))
                ts_s = ts_ms / 1000.0
                vol = float(trade.get("v", 0))

                # Just update minute aggregation - no individual trade storage
                update_agg(symbol, price, vol, ts_s)
        else:
            # Log other message types to debug
            print(f"🔍 Received message type: {t} - {msg_json}")

    except Exception as e:
        print("Error handling message:", e)

async def subscribe(ws, symbols):
    for sym in symbols:
        sub = {"type": "subscribe", "symbol": sym}
        print(f"Subscribing to: {sym}")
        await ws.send(json.dumps(sub))
        await asyncio.sleep(0.05)  # tiny pause to be polite

async def websocket_loop():
    reconnect_delay = 1
    while True:
        try:
            print(f"🔌 Connecting to Finnhub websocket...")
            async with websockets.connect(
                WS_URL, 
                ping_interval=20, 
                ping_timeout=10,
                close_timeout=10
            ) as ws:
                print("✅ Connected to Finnhub websocket")
                await subscribe(ws, SYMBOLS)
                reconnect_delay = 1
                
                async for message in ws:
                    try:
                        msg_json = json.loads(message)
                        response = await handle_message(msg_json)
                        if response:
                            await ws.send(json.dumps(response))
                    except Exception as e:
                        print(f"❌ Message handling error: {e}")
                        continue
                        
        except websockets.exceptions.ConnectionClosed as e:
            print(f"🔌 Connection closed: {e}")
        except Exception as e:
            print(f"❌ Websocket error: {e}")
        
        print(f"🔄 Reconnecting in {reconnect_delay}s...")
        await asyncio.sleep(reconnect_delay)
        reconnect_delay = min(30, reconnect_delay * 1.5)  # Exponential backoff

def finalize_old_buckets(cutoff_seconds=None):
    """Finalize minute buckets older than cutoff: insert to Supabase and remove from agg_buckets."""
    now = time.time()
    if cutoff_seconds is None:
        cutoff_seconds = 90  # finalize buckets older than 90s (safety margin)
    keys_to_finalize = []
    for (symbol, minute_key), b in list(agg_buckets.items()):
        if now - b["start_ts"] >= cutoff_seconds:
            # Insert completed candle to Supabase database
            candle_data = {
                "timestamp": minute_key,
                "open": b["open"],
                "high": b["high"],
                "low": b["low"],
                "close": b["close"],
                "volume": b["volume"],
                "trades": b["trade_count"]  # Match your column name
            }
            
            try:
                # Insert into Supabase table
                result = supabase.table("candles").insert(candle_data).execute()
                print(f"✅ Candle: {symbol} {minute_key} O:{b['open']:.2f} H:{b['high']:.2f} L:{b['low']:.2f} C:{b['close']:.2f} V:{b['volume']:.4f} T:{b['trade_count']}")
            except Exception as e:
                print(f"❌ Error inserting candle: {e}")
            
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
            # Use fixed file name - will overwrite existing file
            fname = "market_data_live.xlsx"
            path = f"{fname}"

            # Delete existing file first, then upload new one
            try:
                supabase.storage.from_(SUPABASE_BUCKET).remove([path])
            except:
                pass  # File might not exist yet

            # upload to supabase storage
            res = supabase.storage.from_(SUPABASE_BUCKET).upload(
                path, 
                excel_bytes,
                file_options={"content-type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"}
            )
            # supabase-py returns dict-like response — print minimal info
            print(f"Updated {path} in Supabase bucket {SUPABASE_BUCKET}")
        except Exception as e:
            print("Upload error:", e)

        # sleep
        await asyncio.sleep(UPLOAD_INTERVAL)

async def continuous_candle_loop():
    """Every 10 seconds, check for completed minute candles and insert to Supabase."""
    while True:
        try:
            finalize_old_buckets(cutoff_seconds=70)
        except Exception as e:
            print("Candle finalization error:", e)
        await asyncio.sleep(10)  # Check every 10 seconds

async def status_reporter():
    """Report status every 10 minutes"""
    while True:
        await asyncio.sleep(600)  # 10 minutes
        print(f"📊 Status: {candle_count} candles created. Last: {last_candle_time}")

async def main():
    # Basic check: env vars
    if not FINNHUB_API_KEY or not SUPABASE_URL or not SUPABASE_KEY:
        print("Missing FINNHUB_API_KEY or SUPABASE_URL or SUPABASE_KEY in environment.")
        return

    print("🚀 Starting Bitcoin data collector...")
    print(f"🔑 Using Finnhub API key: {FINNHUB_API_KEY[:10]}...")
    print("📊 Data will be stored in Supabase 'candles' table")
    print("⚠️  If you only see ping messages, your Finnhub API key may not have real-time data access")

    # Start websocket listener and status reporter
    await asyncio.gather(
        websocket_loop(),
        status_reporter()
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopped by user.")
