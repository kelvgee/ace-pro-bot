import os, time, requests, threading, random
from datetime import datetime
import pytz
from flask import Flask

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
EAT = pytz.timezone("Africa/Nairobi")
PAIRS = ["EURUSD","GBPUSD","USDJPY","XAUUSD","BTCUSD","EURJPY","AUDUSD","USDCAD"]

app = Flask(__name__)
@app.route('/')
def home():
    return "ACE PRO BOT LIVE"

def send(msg):
    print(f"SENDING: {msg[:50]}")
    if not BOT_TOKEN or not CHAT_ID:
        print("ERROR: BOT_TOKEN or CHAT_ID MISSING!")
        return
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        r = requests.post(url, json={"chat_id": CHAT_ID, "text": msg}, timeout=10)
        print(f"Telegram response: {r.text[:200]}")
    except Exception as e:
        print(f"Send error: {e}")

def handle_commands():
    print("Command handler started")
    try:
        requests.get(f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook", timeout=10)
        requests.get(f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates?offset=-1", timeout=10)
        print("Webhook cleared")
    except Exception as e:
        print(f"Webhook clear error: {e}")
    offset = 0
    start_t = time.time()
    while True:
        try:
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates?offset={offset}&timeout=30"
            r = requests.get(url, timeout=35).json()
            for upd in r.get("result", []):
                offset = upd["update_id"] + 1
                text = upd.get("message", {}).get("text", "").strip().lower()
                print(f"Got command: {text}")
                if text.startswith("/start"):
                    send("✅ ACE PRO BOT LIVE!\nTF: 5M | Duration: 5 MIN\nType /status")
                if text.startswith("/status"):
                    uptime = int((time.time()-start_t)/60)
                    now = datetime.now(EAT)
                    next_min = (now.minute //5*5 +5)%60
                    next_hour = now.hour if now.minute+5<60 else (now.hour+1)%24
                    send(f"📊 STATUS: ONLINE 🟢\nUptime: {uptime} min\nTF: 5M | Dur: 5 MIN\nTime: {now.strftime('%H:%M')} EAT\nNext: {next_hour:02d}:{next_min:02d}")
        except Exception as e:
            print(f"Command loop error: {e}")
            time.sleep(5)

def signal_loop():
    print("Signal loop started")
    time.sleep(5)
    send("✅ ACE PRO DEPLOYED - LIVE NOW\nTF: 5M | Duration: 5 MIN")
    while True:
        try:
            now = datetime.now(EAT)
            if now.minute % 5 == 0 and now.second < 30:
                pair = random.choice(PAIRS)
                direction = random.choice(["BUY","SELL"])
                entry = now.strftime("%H:%M")
                send(f"{pair} - {direction}\nEntry: NOW ({entry})\nDuration: 5 MIN")
                time.sleep(290)
            time.sleep(5)
        except Exception as e:
            print(f"Signal loop error: {e}")
            time.sleep(10)

if __name__ == "__main__":
    print(f"STARTING BOT - Token exists: {bool(BOT_TOKEN)} Chat exists: {bool(CHAT_ID)}")
    threading.Thread(target=handle_commands, daemon=True).start()
    threading.Thread(target=signal_loop, daemon=True).start()
    app.run(host='0.0.0.0', port=10000)
