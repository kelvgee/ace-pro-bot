import os, time, requests, threading
from datetime import datetime
import pytz
from flask import Flask
import random

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
EAT = pytz.timezone("Africa/Nairobi")
PAIRS = ["EURUSD","GBPUSD","USDJPY","XAUUSD","BTCUSD"]

app = Flask(__name__)
@app.route('/')
def home(): return "BOT LIVE"

def send(msg):
    requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage", json={"chat_id": CHAT_ID, "text": msg}, timeout=10)

def handle_commands():
    try:
        requests.get(f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook")
        requests.get(f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates?offset=-1")
    except: pass
    offset = 0
    start = time.time()
    while True:
        try:
            r = requests.get(f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates?offset={offset}&timeout=30", timeout=35).json()
            for u in r.get("result", []):
                offset = u["update_id"]+1
                txt = u.get("message",{}).get("text","").strip().lower()
                if txt.startswith("/start"):
                    send("✅ ACE PRO BOT LIVE!\nType /status")
                if txt.startswith("/status"):
                    now = datetime.now(EAT)
                    up = int((time.time()-start)/60)
                    send(f"📊 STATUS: ONLINE 🟢\nUptime: {up} min\nTime: {now.strftime('%H:%M')} EAT\nTF: 5M\nPairs: 5")
        except: time.sleep(5)

def signal_loop():
    send("✅ ACE PRO DEPLOYED - LIVE NOW")
    while True:
        try:
            now = datetime.now(EAT)
            if now.minute % 5 == 0 and now.second < 15:
                pair = random.choice(PAIRS)
                d = random.choice(["BUY","SELL"])
                send(f"{pair} - {d}\nEntry: NOW\nDuration: 5 MIN")
                time.sleep(290)
            time.sleep(5)
        except: time.sleep(10)

if __name__ == "__main__":
    threading.Thread(target=handle_commands, daemon=True).start()
    threading.Thread(target=signal_loop, daemon=True).start()
    app.run(host='0.0.0.0', port=10000)
