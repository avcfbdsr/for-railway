"""
Finnhub -> Aggregate 1-min OHLC + volume + raw trades -> Save as Excel -> Upload to Supabase storage
Run: python finnhub_to_supabase.py
Set env vars: FINNHUB_API_KEY, SUPABASE_URL, SUPABASE_KEY
"""

import os
import asyncio
import json
import time
from datetime import datetime, timezone, timedelta
from collections import defaultdict
import io

import pandas as pd
import websockets
from supabase import create_client  # supabase-py
from dotenv import load_dotenv

load_dotenv()

# Indian Standard Time (UTC+5:30)
IST = timezone(timedelta(hours=5, minutes=30))

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

# Global counters
candle_count = 0
last_candle_time = None

async def insert_candle_with_retry(candle_data, max_retries=3):
    """Insert candle to database with retry logic"""
    for attempt in range(max_retries):
        try:
            result = supabase.table("candles").insert(candle_data).execute()
            return True
        except Exception as e:
            print(f"❌ Database insert attempt {attempt+1} failed: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
            else:
                print(f"❌ Failed to insert candle after {max_retries} attempts")
                return False

def ts_to_min_key(ts_seconds):
    # returns minute key in IST like 2025-11-11T09:46:00+05:30
    dt = datetime.fromtimestamp(ts_seconds, tz=IST).replace(second=0, microsecond=0)
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
            # Convert UTC timestamp to IST
            utc_dt = datetime.fromisoformat(existing_minute_key.replace('+05:30', '+00:00'))
            ist_dt = utc_dt.replace(tzinfo=timezone.utc).astimezone(IST)
            
            candle_data = {
                "timestamp": ist_dt.isoformat(),
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
                ist_time = datetime.fromisoformat(existing_minute_key).strftime("%H:%M IST")
                print(f"🔥 Candle #{candle_count} at {ist_time}: {existing_symbol} O:{bucket['open']:.2f} H:{bucket['high']:.2f} L:{bucket['low']:.2f} C:{bucket['close']:.2f} V:{bucket['volume']:.4f} T:{bucket['trade_count']}")
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
    """Parse Finnhub message and update stores with bulletproof error handling."""
    try:
        if not isinstance(msg_json, dict):
            return None
            
        t = msg_json.get("type")
        
        if t == "ping":
            return {"type": "pong"}
        elif t == "trade":
            data = msg_json.get("data", [])
            if not data:
                return None
                
            print(f"📈 Processing {len(data)} trades for BINANCE:BTCUSDT")
            
            for trade in data:
                try:
                    if not isinstance(trade, dict):
                        continue
                        
                    symbol = trade.get("s")
                    price = trade.get("p")
                    timestamp = trade.get("t")
                    volume = trade.get("v")
                    
                    # Validate required fields
                    if not all([symbol, price is not None, timestamp is not None]):
                        continue
                        
                    price = float(price)
                    ts_ms = int(timestamp)
                    ts_s = ts_ms / 1000.0
                    vol = float(volume) if volume is not None else 0.0
                    
                    # Sanity checks
                    if price <= 0 or ts_s <= 0 or vol < 0:
                        continue
                        
                    update_agg(symbol, price, vol, ts_s)
                    
                except (ValueError, TypeError, KeyError) as e:
                    print(f"❌ Invalid trade data: {e}")
                    continue
                except Exception as e:
                    print(f"❌ Trade processing error: {e}")
                    continue
        else:
            # Log unknown message types for debugging
            print(f"🔍 Unknown message type: {t}")

    except Exception as e:
        print(f"❌ Message handling error: {e}")
    
    return None

async def subscribe(ws, symbols):
    for sym in symbols:
        sub = {"type": "subscribe", "symbol": sym}
        print(f"Subscribing to: {sym}")
        await ws.send(json.dumps(sub))
        await asyncio.sleep(0.05)  # tiny pause to be polite

async def websocket_loop():
    reconnect_delay = 1
    max_reconnect_delay = 300  # 5 minutes max
    
    while True:
        try:
            print(f"🔌 Connecting to Finnhub websocket...")
            async with websockets.connect(
                WS_URL, 
                ping_interval=20, 
                ping_timeout=10,
                close_timeout=10,
                max_size=2**20,  # 1MB max message size
                compression=None  # Disable compression for stability
            ) as ws:
                print("✅ Connected to Finnhub websocket")
                
                # Subscribe with retry logic
                for attempt in range(3):
                    try:
                        await subscribe(ws, SYMBOLS)
                        break
                    except Exception as e:
                        print(f"❌ Subscription attempt {attempt+1} failed: {e}")
                        if attempt == 2:
                            raise
                        await asyncio.sleep(1)
                
                reconnect_delay = 1  # Reset delay on successful connection
                
                # Message processing loop with error isolation
                async for message in ws:
                    try:
                        if not message or len(message) == 0:
                            continue
                            
                        msg_json = json.loads(message)
                        response = await handle_message(msg_json)
                        
                        if response:
                            try:
                                await ws.send(json.dumps(response))
                            except Exception as send_error:
                                print(f"❌ Failed to send response: {send_error}")
                                # Don't break connection for send failures
                                
                    except json.JSONDecodeError as e:
                        print(f"❌ Invalid JSON received: {e}")
                        continue  # Skip bad messages
                    except Exception as e:
                        print(f"❌ Message processing error: {e}")
                        continue  # Don't break connection for message errors
                        
        except websockets.exceptions.ConnectionClosed as e:
            print(f"🔌 Connection closed: {e}")
        except websockets.exceptions.InvalidURI as e:
            print(f"❌ Invalid websocket URI: {e}")
            await asyncio.sleep(60)  # Wait longer for config issues
        except websockets.exceptions.InvalidHandshake as e:
            print(f"❌ Handshake failed: {e}")
        except OSError as e:
            print(f"❌ Network error: {e}")
        except asyncio.TimeoutError as e:
            print(f"❌ Connection timeout: {e}")
        except Exception as e:
            print(f"❌ Unexpected websocket error: {e}")
        
        # Exponential backoff with jitter
        jitter = min(5, reconnect_delay * 0.1)
        sleep_time = reconnect_delay + jitter
        print(f"🔄 Reconnecting in {sleep_time:.1f}s...")
        await asyncio.sleep(sleep_time)
        reconnect_delay = min(max_reconnect_delay, reconnect_delay * 1.5)

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
    """Report status every 10 minutes with error handling"""
    while True:
        try:
            await asyncio.sleep(600)  # 10 minutes
            current_time = datetime.now(IST).strftime("%H:%M IST")
            active_buckets = len(agg_buckets)
            print(f"📊 Status at {current_time}: {candle_count} candles created, {active_buckets} active buckets. Last: {last_candle_time}")
        except Exception as e:
            print(f"❌ Status reporter error: {e}")
            await asyncio.sleep(60)  # Wait 1 minute on error

async def main():
    """Main function with bulletproof error handling"""
    try:
        # Basic check: env vars
        if not FINNHUB_API_KEY or not SUPABASE_URL or not SUPABASE_KEY:
            print("❌ Missing required environment variables")
            print("Required: FINNHUB_API_KEY, SUPABASE_URL, SUPABASE_KEY")
            return

        print("🚀 Starting Bitcoin data collector...")
        print(f"🔑 Using Finnhub API key: {FINNHUB_API_KEY[:10]}...")
        print("📊 Data will be stored in Supabase 'candles' table")
        print("⚠️  If you only see ping messages, your Finnhub API key may not have real-time data access")
        print("🛡️  Bulletproof mode: Will never stop running")

        # Test Supabase connection
        try:
            test_result = supabase.table("candles").select("*").limit(1).execute()
            print("✅ Supabase connection verified")
        except Exception as e:
            print(f"⚠️  Supabase connection warning: {e}")
            print("🔄 Will continue anyway - connection may recover")

        # Start all services with error isolation
        tasks = [
            asyncio.create_task(websocket_loop()),
            asyncio.create_task(status_reporter())
        ]
        
        # Run forever with automatic restart on any failure
        while True:
            try:
                done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                
                # If any task completed (crashed), restart it
                for task in done:
                    try:
                        await task  # Get the exception if any
                    except Exception as e:
                        print(f"❌ Task crashed: {e}")
                    
                    # Restart the crashed task
                    if task in tasks:
                        task_index = tasks.index(task)
                        if task_index == 0:  # websocket_loop
                            print("🔄 Restarting websocket loop...")
                            tasks[0] = asyncio.create_task(websocket_loop())
                        elif task_index == 1:  # status_reporter
                            print("🔄 Restarting status reporter...")
                            tasks[1] = asyncio.create_task(status_reporter())
                
                await asyncio.sleep(1)  # Brief pause before checking again
                
            except Exception as e:
                print(f"❌ Main loop error: {e}")
                await asyncio.sleep(5)  # Wait before retrying
                
    except Exception as e:
        print(f"❌ Critical error in main: {e}")
        print("🔄 Restarting in 10 seconds...")
        await asyncio.sleep(10)
        await main()  # Recursive restart

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopped by user.")
