"""
Gemini 2.5 Flash Trading Bot
- Fetches 6000 Bitcoin candles from Supabase
- Analyzes with Gemini 2.5 Flash for trading signals
- Executes paper trades with demo capital
- Tracks all trades for 3-month testing period
"""

import os
import asyncio
import json
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional
import google.generativeai as genai
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

# Configuration
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")  # Add this to Railway env vars

# Trading Configuration
DEMO_CAPITAL = 10000.0  # $10,000 demo capital
RISK_PER_TRADE = 0.02   # 2% risk per trade
ANALYSIS_INTERVAL = 300  # Analyze every 5 minutes

# Indian Standard Time
IST = timezone(timedelta(hours=5, minutes=30))

# Initialize clients
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
genai.configure(api_key=GEMINI_API_KEY)

# Trading state
current_position = None
demo_balance = DEMO_CAPITAL
total_trades = 0
winning_trades = 0
trade_history = []

class Trade:
    def __init__(self, entry_price: float, stop_loss: float, take_profit: float, 
                 position_size: float, direction: str):
        self.entry_price = entry_price
        self.stop_loss = stop_loss
        self.take_profit = take_profit
        self.position_size = position_size
        self.direction = direction  # 'long' or 'short'
        self.entry_time = datetime.now(IST)
        self.exit_time = None
        self.exit_price = None
        self.pnl = 0.0
        self.status = 'open'  # 'open', 'closed', 'stopped'

async def fetch_latest_candles(limit: int = 6000) -> List[Dict]:
    """Fetch latest candles from Supabase"""
    try:
        response = supabase.table("candles").select("*").order("timestamp", desc=True).limit(limit).execute()
        candles = response.data
        # Reverse to get chronological order (oldest first)
        return list(reversed(candles))
    except Exception as e:
        print(f"❌ Error fetching candles: {e}")
        return []

def format_candles_for_gemini(candles: List[Dict]) -> str:
    """Format candles data for Gemini analysis"""
    if not candles:
        return "No candle data available"
    
    # Get last 100 candles for analysis (6000 is too much for one prompt)
    recent_candles = candles[-100:]
    
    formatted_data = "Bitcoin (BTCUSDT) Market Data - Last 100 Minutes:\n"
    formatted_data += "Timestamp (IST) | Open | High | Low | Close | Volume | Trades\n"
    formatted_data += "-" * 80 + "\n"
    
    for candle in recent_candles:
        timestamp = candle.get('timestamp', '')
        open_price = candle.get('open', 0)
        high_price = candle.get('high', 0)
        low_price = candle.get('low', 0)
        close_price = candle.get('close', 0)
        volume = candle.get('volume', 0)
        trades = candle.get('trades', 0)
        
        formatted_data += f"{timestamp} | {open_price:.2f} | {high_price:.2f} | {low_price:.2f} | {close_price:.2f} | {volume:.4f} | {trades}\n"
    
    return formatted_data

async def get_gemini_trading_signal(candles_data: str, current_price: float) -> Dict:
    """Get trading signal from Gemini 2.5 Flash"""
    try:
        model = genai.GenerativeModel('gemini-2.0-flash-exp')
        
        prompt = f"""
        You are a professional Bitcoin trading analyst. Analyze the following market data and provide a trading recommendation.

        {candles_data}

        Current Bitcoin Price: ${current_price:.2f}

        Based on technical analysis of the candlestick patterns, volume, and price action, provide:

        1. SIGNAL: Should I BUY, SELL, or HOLD?
        2. ENTRY: Exact entry price
        3. STOP_LOSS: Stop loss price (risk management)
        4. TAKE_PROFIT: Take profit target
        5. CONFIDENCE: Your confidence level (1-10)
        6. REASONING: Brief explanation of your analysis

        Respond in this exact JSON format:
        {{
            "signal": "BUY/SELL/HOLD",
            "entry": 105000.50,
            "stop_loss": 104000.00,
            "take_profit": 107000.00,
            "confidence": 8,
            "reasoning": "Strong bullish momentum with volume confirmation"
        }}

        Focus on:
        - Support/Resistance levels
        - Candlestick patterns
        - Volume analysis
        - Trend direction
        - Risk/Reward ratio (minimum 1:2)
        """
        
        response = model.generate_content(prompt)
        
        # Parse JSON response
        response_text = response.text.strip()
        if response_text.startswith('```json'):
            response_text = response_text[7:-3]
        elif response_text.startswith('```'):
            response_text = response_text[3:-3]
            
        signal_data = json.loads(response_text)
        return signal_data
        
    except Exception as e:
        print(f"❌ Gemini API error: {e}")
        return {"signal": "HOLD", "confidence": 0, "reasoning": f"API Error: {e}"}

def calculate_position_size(entry_price: float, stop_loss: float, risk_amount: float) -> float:
    """Calculate position size based on risk management"""
    risk_per_unit = abs(entry_price - stop_loss)
    if risk_per_unit == 0:
        return 0
    position_size = risk_amount / risk_per_unit
    return round(position_size, 6)

async def execute_paper_trade(signal: Dict, current_price: float) -> Optional[Trade]:
    """Execute paper trade based on Gemini signal"""
    global current_position, demo_balance, total_trades
    
    if signal['signal'] == 'HOLD' or signal['confidence'] < 6:
        return None
    
    # Close existing position if opposite signal
    if current_position and current_position.status == 'open':
        if (current_position.direction == 'long' and signal['signal'] == 'SELL') or \
           (current_position.direction == 'short' and signal['signal'] == 'BUY'):
            await close_position(current_price, "Signal reversal")
    
    # Don't open new position if we already have one in same direction
    if current_position and current_position.status == 'open':
        return None
    
    # Calculate risk amount (2% of current balance)
    risk_amount = demo_balance * RISK_PER_TRADE
    
    entry_price = signal.get('entry', current_price)
    stop_loss = signal.get('stop_loss', current_price * 0.98)  # Default 2% SL
    take_profit = signal.get('take_profit', current_price * 1.04)  # Default 4% TP
    
    direction = 'long' if signal['signal'] == 'BUY' else 'short'
    position_size = calculate_position_size(entry_price, stop_loss, risk_amount)
    
    if position_size <= 0:
        return None
    
    # Create new trade
    trade = Trade(entry_price, stop_loss, take_profit, position_size, direction)
    current_position = trade
    total_trades += 1
    
    # Log trade
    trade_log = {
        "trade_id": total_trades,
        "timestamp": trade.entry_time.isoformat(),
        "direction": direction,
        "entry_price": entry_price,
        "stop_loss": stop_loss,
        "take_profit": take_profit,
        "position_size": position_size,
        "risk_amount": risk_amount,
        "confidence": signal['confidence'],
        "reasoning": signal['reasoning'],
        "status": "opened"
    }
    
    # Save to Supabase
    try:
        supabase.table("paper_trades").insert(trade_log).execute()
    except Exception as e:
        print(f"❌ Error saving trade: {e}")
    
    print(f"🚀 NEW TRADE #{total_trades}")
    print(f"   Direction: {direction.upper()}")
    print(f"   Entry: ${entry_price:.2f}")
    print(f"   Stop Loss: ${stop_loss:.2f}")
    print(f"   Take Profit: ${take_profit:.2f}")
    print(f"   Position Size: {position_size:.6f} BTC")
    print(f"   Risk: ${risk_amount:.2f}")
    print(f"   Confidence: {signal['confidence']}/10")
    print(f"   Reasoning: {signal['reasoning']}")
    
    return trade

async def close_position(current_price: float, reason: str) -> None:
    """Close current position"""
    global current_position, demo_balance, winning_trades
    
    if not current_position or current_position.status != 'open':
        return
    
    current_position.exit_price = current_price
    current_position.exit_time = datetime.now(IST)
    current_position.status = 'closed'
    
    # Calculate PnL
    if current_position.direction == 'long':
        pnl = (current_price - current_position.entry_price) * current_position.position_size
    else:  # short
        pnl = (current_position.entry_price - current_price) * current_position.position_size
    
    current_position.pnl = pnl
    demo_balance += pnl
    
    if pnl > 0:
        winning_trades += 1
    
    # Update trade in database
    try:
        supabase.table("paper_trades").update({
            "exit_price": current_price,
            "exit_time": current_position.exit_time.isoformat(),
            "pnl": pnl,
            "status": "closed",
            "close_reason": reason
        }).eq("trade_id", total_trades).execute()
    except Exception as e:
        print(f"❌ Error updating trade: {e}")
    
    print(f"📊 TRADE CLOSED #{total_trades}")
    print(f"   Exit Price: ${current_price:.2f}")
    print(f"   PnL: ${pnl:.2f}")
    print(f"   Reason: {reason}")
    print(f"   New Balance: ${demo_balance:.2f}")
    
    trade_history.append(current_position)
    current_position = None

async def check_position_management(current_price: float) -> None:
    """Check if current position should be closed (SL/TP hit)"""
    if not current_position or current_position.status != 'open':
        return
    
    if current_position.direction == 'long':
        if current_price <= current_position.stop_loss:
            await close_position(current_price, "Stop Loss Hit")
        elif current_price >= current_position.take_profit:
            await close_position(current_price, "Take Profit Hit")
    else:  # short
        if current_price >= current_position.stop_loss:
            await close_position(current_price, "Stop Loss Hit")
        elif current_price <= current_position.take_profit:
            await close_position(current_price, "Take Profit Hit")

async def print_trading_stats():
    """Print current trading statistics"""
    win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0
    total_return = ((demo_balance - DEMO_CAPITAL) / DEMO_CAPITAL * 100)
    
    print(f"\n📈 TRADING STATISTICS")
    print(f"   Demo Balance: ${demo_balance:.2f}")
    print(f"   Total Return: {total_return:.2f}%")
    print(f"   Total Trades: {total_trades}")
    print(f"   Winning Trades: {winning_trades}")
    print(f"   Win Rate: {win_rate:.1f}%")
    print(f"   Current Position: {'Yes' if current_position else 'No'}")

async def main():
    """Main trading bot loop"""
    print("🤖 Gemini 2.5 Flash Trading Bot Starting...")
    print(f"💰 Demo Capital: ${DEMO_CAPITAL}")
    print(f"⚡ Analysis Interval: {ANALYSIS_INTERVAL}s")
    print(f"🎯 Risk per Trade: {RISK_PER_TRADE*100}%")
    
    # Create trades table if not exists
    try:
        supabase.table("paper_trades").select("*").limit(1).execute()
        print("✅ Paper trades table ready")
    except:
        print("⚠️  Create 'paper_trades' table in Supabase with columns: trade_id, timestamp, direction, entry_price, stop_loss, take_profit, position_size, risk_amount, confidence, reasoning, status, exit_price, exit_time, pnl, close_reason")
    
    while True:
        try:
            # Fetch latest candles
            candles = await fetch_latest_candles(6000)
            if not candles:
                print("❌ No candles available, waiting...")
                await asyncio.sleep(60)
                continue
            
            # Get current price from latest candle
            current_price = float(candles[-1].get('close', 0))
            if current_price <= 0:
                await asyncio.sleep(60)
                continue
            
            # Check position management (SL/TP)
            await check_position_management(current_price)
            
            # Format data for Gemini
            candles_data = format_candles_for_gemini(candles)
            
            # Get trading signal from Gemini
            print(f"🧠 Analyzing market with Gemini 2.5 Flash...")
            signal = await get_gemini_trading_signal(candles_data, current_price)
            
            print(f"📊 Gemini Signal: {signal.get('signal', 'UNKNOWN')}")
            print(f"🎯 Confidence: {signal.get('confidence', 0)}/10")
            print(f"💭 Reasoning: {signal.get('reasoning', 'No reasoning provided')}")
            
            # Execute trade if signal is strong enough
            if signal.get('confidence', 0) >= 6:
                await execute_paper_trade(signal, current_price)
            
            # Print stats
            await print_trading_stats()
            
            # Wait for next analysis
            await asyncio.sleep(ANALYSIS_INTERVAL)
            
        except Exception as e:
            print(f"❌ Main loop error: {e}")
            await asyncio.sleep(60)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("🛑 Trading bot stopped by user")
        if current_position:
            print(f"⚠️  Open position exists: {current_position.direction} at ${current_position.entry_price}")
