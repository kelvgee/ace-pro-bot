import os, threading, time, yfinance as yf
from flask import Flask
from datetime import datetime
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

BOT_TOKEN = os.getenv("BOT_TOKEN")
PAIRS = ["XAUUSD=X", "GBPJPY=X", "EURJPY=X", "GBPUSD=X", "BTC-USD", "EURUSD=X"]
SCAN_INTERVAL = 300
sent_cache = {}

app = Flask(__name__)
@app.route('/')
def home(): return "ACE PRO ELITE V2 1MIN SNIPER RUNNING"
def run_flask():
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 10000)))

def get_htf_trend(ticker):
    try:
        df = yf.download(ticker, period="1d", interval="5m", progress=False)
        if len(df) < 50: return None
        close = df['Close']
        ema50 = close.ewm(span=50).mean().iloc[-1]
        return "BULL" if close.iloc[-1] > ema50 else "BEAR"
    except: return None

def elite_check(ticker):
    try:
        df = yf.download(ticker, period="1d", interval="1m", progress=False)
        if len(df) < 250: return None
        df = df.tail(250)
        close = df['Close']
        high = df['High']
        low = df['Low']
        vol = df['Volume'] if 'Volume' in df else close
        price = close.iloc[-1]
        ema200 = close.ewm(span=200).mean().iloc[-1]
        trend = "BULL" if price > ema200 else "BEAR"
        delta = close.diff()
        gain = delta.where(delta>0,0).ewm(alpha=1/14).mean()
        loss = -delta.where(delta<0,0).ewm(alpha=1/14).mean()
        rsi = 100 - (100/(1+(gain/loss)))
        rsi_val = rsi.iloc[-1]
        rsi_ok = (55 < rsi_val < 75 and trend=="BULL") or (25 < rsi_val < 45 and trend=="BEAR")
        ema12 = close.ewm(span=12).mean()
        ema26 = close.ewm(span=26).mean()
        macd_line = ema12 - ema26
        signal_line = macd_line.ewm(span=9).mean()
        macd_ok = (macd_line.iloc[-1] > signal_line.iloc[-1] and trend=="BULL") or (macd_line.iloc[-1] < signal_line.iloc[-1] and trend=="BEAR")
        typical = (high + low + close)/3
        vwap = (typical * vol).cumsum() / vol.cumsum()
        vwap_val = vwap.iloc[-1]
        vwap_ok = (price > vwap_val and trend=="BULL") or (price < vwap_val and trend=="BEAR")
        vol_ma = vol.rolling(20).mean().iloc[-1]
        vol_ok = vol.iloc[-1] > vol_ma * 1.3
        atr = (high - low).rolling(14).mean().iloc[-1]
        hl2 = (high + low)/2
        upper = hl2 + 3*atr
        lower = hl2 - 3*atr
        st_ok = (price > lower.iloc[-1] and trend=="BULL") or (price < upper.iloc[-1] and trend=="BEAR")
        htf = get_htf_trend(ticker)
        htf_ok = (htf == trend) if htf else True
        score = sum([rsi_ok, macd_ok, vwap_ok, vol_ok, st_ok, htf_ok, True])
        if score >= 6:
            hour = datetime.utcnow().hour
            if not (7 <= hour <= 20): return None
            if ticker in sent_cache and time.time() - sent_cache[ticker] < 1800: return None
            sent_cache[ticker] = time.time()
            symbol = ticker.replace("=X","").replace("-USD","")
            return f"🔥 ACE ELITE SIGNAL 🔥\n\nPair: {symbol}\nTrend: {trend}\nPrice: {price:.2f}\nScore: {score}/7\nRSI: {rsi_val:.1f}\nTime: 1MIN\n\nBUY {trend} NOW! 1MIN Expiry"
        return None
    except: return None

async def on_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔥 ACE PRO ELITE V2 ACTIVATED\n\n✅ 1MIN TF | 1MIN Duration\n✅ Scan: Every 5MIN\n✅ Gate: 6/7 Filters")
    for job in context.job_queue.get_jobs_by_name(str(update.effective_chat.id)):
        job.schedule_removal()
    context.job_queue.run_repeating(scan_job, interval=SCAN_INTERVAL, first=10, chat_id=update.effective_chat.id, name=str(update.effective_chat.id))

async def scan_job(context: ContextTypes.DEFAULT_TYPE):
    chat_id = context.job.chat_id
    for pair in PAIRS:
        sig = elite_check(pair)
        if sig:
            await context.bot.send_message(chat_id=chat_id, text=sig)

def main():
    threading.Thread(target=run_flask, daemon=True).start()
    if not BOT_TOKEN:
        print("NO BOT_TOKEN SET IN RENDER!")
        return
    app_bot = ApplicationBuilder().token(BOT_TOKEN).build()
    app_bot.add_handler(CommandHandler("start", on_start))
    app_bot.run_polling()

if __name__ == "__main__":
    main()
