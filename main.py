import os, time, requests, random
from datetime import datetime
import pytz

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

EAT = pytz.timezone("Africa/Nairobi")
PAIRS = ["EURUSD","GBPUSD","USDJPY","XAUUSD","BTCUSD","EURJPY","AUDUSD","USDCAD"]

def send(msg):
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": CHAT_ID, "text": msg}, timeout=10)
        print(f"Sent: {msg}")
    except Exception as e:
        print(e)

def deep_analysis_5m():
    """
    YOUR 7 FILTERS - 5M DEEP ANALYSIS
    Replace with real indicators later
    For now simulating 6/7 elite logic
    """
    best_pair = None
    best_dir = None
    best_score = 0

    for pair in PAIRS:
        # Simulate 7 filters score
        score = random.randint(3, 7) # 3 to 7
        if score > best_score:
            best_score = score
            best_pair = pair
            best_dir = random.choice(["BUY", "SELL"])

    if best_score >= 6:
        return best_pair, best_dir, best_score
    else:
        return None, None, best_score

def main():
    send("✅ ACE PRO POCKET - FINAL\nTF: 5M | Duration: 5 MIN\nSignal every 5 min (BEST or CHOPPY)")
    
    while True:
        try:
            now = datetime.now(EAT)
            
            if now.minute % 5 == 0 and now.second < 10:
                pair, direction, score = deep_analysis_5m()
                entry = now.strftime("%H:%M")
                
                if pair and direction:
                    msg = f"{pair} - {direction}\nEntry: NOW ({entry})\nDuration: 5 MIN"
                    send(msg)
                else:
                    next_min = (now.minute + 5) % 60
                    next_hour = now.hour if now.minute + 5 < 60 else (now.hour + 1) % 24
                    msg = f"CHOPPY - NO TRADE\nWait next 5 min ({next_hour:02d}:{next_min:02d})"
                    send(msg)
                
                time.sleep(290) # Wait almost 5 min

            time.sleep(5)

        except Exception as e:
            print(f"Error: {e}")
            time.sleep(10)

if __name__ == "__main__":
    main()
