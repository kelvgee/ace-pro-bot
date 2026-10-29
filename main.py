# ACE PRO SIGNAL BOT - multi-pair Telegram signals using the V7 entry engine.
# SIGNALS ONLY: this file contains NO code that places trades. You enter trades yourself in Pocket Option.
#
# Secrets (environment variables on Render, or text files next to this script when running on your PC):
#   BOT_TOKEN    (or telegram_token.txt)      - Telegram bot token
#   CHAT_ID      (or telegram_chat_id.txt)    - your Telegram chat id
#   DERIV_TOKEN  (or deriv_token.txt)         - Deriv token, used ONLY to read price candles
# pip install websockets tzdata
#
# Telegram commands (answered only for your CHAT_ID):
#   /status  /stats  /why  /pause [minutes]  /resume  /payout EURUSD 90  /help
import asyncio, json, os, sys, math, time, csv, logging, threading, traceback, urllib.request, urllib.error
from logging.handlers import RotatingFileHandler
from datetime import datetime, timezone, timedelta
from collections import deque, Counter
from http.server import BaseHTTPRequestHandler, HTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
def P(name): return os.path.join(HERE, name)

def load_secret(env_name, file_name):
    v = os.environ.get(env_name)
    if v and v.strip(): return v.strip()
    p = P(file_name)
    if os.path.exists(p):
        with open(p, encoding="utf-8") as f: return f.read().strip()
    return ""

# key -> Deriv symbol, label, payout % you see in Pocket Option (edit here, or change live with /payout), pip size
PAIRS = {
    "EURUSD": {"deriv": "frxEURUSD", "label": "EUR/USD", "payout": 92, "pip": 0.0001},
    "CADJPY": {"deriv": "frxCADJPY", "label": "CAD/JPY", "payout": 82, "pip": 0.01},
    "CHFJPY": {"deriv": "frxCHFJPY", "label": "CHF/JPY", "payout": 81, "pip": 0.01},
    "AUDJPY": {"deriv": "frxAUDJPY", "label": "AUD/JPY", "payout": 78, "pip": 0.01},
    "USDJPY": {"deriv": "frxUSDJPY", "label": "USD/JPY", "payout": 78, "pip": 0.01},
}
DERIV2KEY = {v["deriv"]: k for k, v in PAIRS.items()}

CONFIG = {
    "BOT_TOKEN": load_secret("BOT_TOKEN", "telegram_token.txt"),
    "CHAT_ID": load_secret("CHAT_ID", "telegram_chat_id.txt"),
    "DERIV_TOKEN": load_secret("DERIV_TOKEN", "deriv_token.txt"),
    "DERIV_APP_ID": "34LEfLoJGqUivh4NHki4R",
    "API_BASE": "https://api.derivws.com",
    "ACCOUNT_TYPE": "demo",
    # ---------- when it runs (London local time, so daylight saving is automatic; Mon-Fri only) ----------
    "SESSION_START_LONDON": "08:15",       # London open + 15 min (skips the noisy first minutes) = 11:15 Nairobi in winter
    "SESSION_END_LONDON": "16:00",         # last signal = 19:00 Nairobi in winter
    "WEEKDAYS_ONLY": True,
    "CONNECT_LEAD_MIN": 2,
    # ---------- signal rules ----------
    "MIN_PAYOUT_PCT": 80,                  # pairs whose payout (PAIRS / /payout) is below this are skipped
    "MAX_SIGNALS_PER_CANDLE": 1,           # best pair only when several qualify
    "PAIR_COOLDOWN_MIN": 15,
    "MAX_SIGNALS_PER_DAY": 12,
    "MAX_CONSEC_LOSSES": 3,
    "LOSS_PAUSE_MIN": 60,
    "TRADE_SECONDS": 300,
    "MAX_SIGNAL_AGE_SEC": 25,              # a signal later than this after the candle closed is dropped
    "ROUND_WAIT_SEC": 6,                   # wait for all pairs of one 5-min candle before ranking them
    # ---------- V7 entry engine ----------
    "MIN_ADX": 16, "EMA_GAP_MIN_PCT": 0.02,
    "MIN_VOTES": 3, "AFTER_LOSS_EXTRA_VOTES": 1, "OPPOSING_15M_EXTRA_VOTES": 1,
    "PULLBACK_BARS": 3, "PULLBACK_TOUCH_ATR": 0.25,
    "EXH_RSI_LOW": 30, "EXH_RSI_HIGH": 70, "EXH_STOCH_LOW": 10, "EXH_STOCH_HIGH": 90, "EXH_BB_LOW": 6, "EXH_BB_HIGH": 94,
    "EXH_MIN_COUNT": 2, "HARD_RSI_LOW": 22, "HARD_RSI_HIGH": 78, "MAX_EXT_ATR": 2.8,
    "STRONG_TREND_ENABLED": True, "STRONG_ADX": 25,
    "ER_BARS": 12, "ER_MIN": 0.12, "CROSS_MAX": 5,
    "LEVEL_FILTER": True, "LEVEL_LOOKBACK": 40, "LEVEL_TOL_ATR": 0.30, "LEVEL_PROX_ATR": 1.2, "BOX_MAX_ATR": 4.5, "LEVEL_MIN_TOUCHES": 2,
    "DATA_GAP_CHECK_MIN": 30,
    # ---------- files / connection ----------
    "LOG_FILE": P("ace_signal_bot.log"), "SIGNALS_FILE": P("signals_log.csv"), "LOG_MAX_MB": 5,
    "HISTORY_CANDLES": 1500, "KEEPALIVE_SEC": 20, "STALL_SEC": 60, "RETRY_MIN_SEC": 5, "RETRY_MAX_SEC": 60,
    "STATUS_EVERY_SEC": 3600,
}

KENYA = timezone(timedelta(hours=3))
def _mono(): return time.monotonic()

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace"); sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception: pass
_console = logging.StreamHandler(); _console.setFormatter(logging.Formatter("%(asctime)s | %(message)s"))
_handlers = [_console]
try:
    _file = RotatingFileHandler(CONFIG["LOG_FILE"], maxBytes=int(CONFIG["LOG_MAX_MB"] * 1024 * 1024), backupCount=2, encoding="utf-8")
    _file.setFormatter(logging.Formatter("%(asctime)s | %(message)s")); _handlers.append(_file)
except Exception: pass
logging.basicConfig(level=logging.INFO, handlers=_handlers)
log = logging.getLogger("ACE")

# ============================ session window ============================
def _session_cfg():
    try:
        from zoneinfo import ZoneInfo
        return ZoneInfo("Europe/London"), CONFIG["SESSION_START_LONDON"], CONFIG["SESSION_END_LONDON"]
    except Exception:
        return KENYA, "11:15", "19:00"      # fixed Nairobi hours if tzdata is missing (pip install tzdata)
SESSION = _session_cfg()

def session_window(now_utc):
    """(start_utc, end_utc) of today's session, or None on weekends."""
    tz, s, e = SESSION
    local = now_utc.astimezone(tz)
    if CONFIG["WEEKDAYS_ONLY"] and local.weekday() > 4: return None
    def at(hhmm):
        h, m = [int(x) for x in hhmm.split(":")]
        return datetime(local.year, local.month, local.day, h, m, tzinfo=tz).astimezone(timezone.utc)
    return at(s), at(e)

# ============================ Telegram ============================
class TgError(Exception):
    def __init__(self, msg, code=None): super().__init__(msg); self.code = code

def tg_api(method, params=None, timeout=10):
    url = f"https://api.telegram.org/bot{CONFIG['BOT_TOKEN']}/{method}"
    req = urllib.request.Request(url, data=json.dumps(params or {}).encode(), headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        raise TgError(f"Telegram HTTP {e.code} on {method}", e.code)
    except Exception as e:
        raise TgError(f"Telegram {method} failed: {type(e).__name__}")

def tg_send(text, reply_to=None):
    """Returns the Telegram message_id (int, -1 if unknown) on success, None on failure. Never raises."""
    if not CONFIG["BOT_TOKEN"] or not CONFIG["CHAT_ID"]:
        log.error(" Telegram not configured - message not sent"); return None
    params = {"chat_id": CONFIG["CHAT_ID"], "text": text}
    if reply_to and reply_to > 0: params["reply_to_message_id"] = reply_to; params["allow_sending_without_reply"] = True
    for attempt in (1, 2):
        try:
            r = tg_api("sendMessage", params, timeout=8)
            return int((r.get("result") or {}).get("message_id") or 0) or -1
        except TgError as e:
            log.error(f" send failed (try {attempt}): {e}")
            if e.code in (400, 401, 403): break
            time.sleep(1)
    return None

# ============================ REST helpers (same flow V7 uses) ============================
def _rest(method, path):
    req = urllib.request.Request(CONFIG["API_BASE"] + path, method=method, data=(b"" if method == "POST" else None),
        headers={"Authorization": f"Bearer {CONFIG['DERIV_TOKEN']}", "Deriv-App-ID": str(CONFIG["DERIV_APP_ID"]),
                 "Content-Type": "application/json", "User-Agent": "Mozilla/5.0 AsetPro"})
    try:
        with urllib.request.urlopen(req, timeout=20) as r: return json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"HTTP {e.code} on {path}: {e.read().decode(errors='ignore')[:200]}")
    except (urllib.error.URLError, OSError) as e:
        raise RuntimeError(f"internet off or Deriv unreachable ({getattr(e, 'reason', e)})")

def _pick_account(resp):
    data = resp.get("data", resp)
    if isinstance(data, dict): data = data.get("accounts", data.get("list", [data]))
    for a in data:
        if not isinstance(a, dict): continue
        kind = str(a.get("account_type", a.get("type", ""))).lower()
        acc_id = a.get("account_id") or a.get("id") or a.get("loginid")
        if acc_id and kind == CONFIG["ACCOUNT_TYPE"]: return acc_id
    raise RuntimeError(f"No {CONFIG['ACCOUNT_TYPE']} account found")

def get_ws_url_blocking():
    acc_id = _pick_account(_rest("GET", "/trading/v1/options/accounts"))
    return _rest("POST", f"/trading/v1/options/accounts/{acc_id}/otp")["data"]["url"]

async def get_ws_url(): return await asyncio.to_thread(get_ws_url_blocking)

# ============================ indicators ============================
def ema(s, p):
    if len(s) < p: return None
    k = 2 / (p + 1); e = sum(s[:p]) / p
    for x in s[p:]: e = x * k + e * (1 - k)
    return e

def ema_series(s, p):
    """out[i] is the EMA at s[p-1+i]."""
    if len(s) < p: return []
    k = 2 / (p + 1); e = sum(s[:p]) / p; out = [e]
    for x in s[p:]:
        e = x * k + e * (1 - k); out.append(e)
    return out

def rsi(s, p=14):
    if len(s) < p + 1: return 50
    gains = []; losses = []
    for i in range(1, len(s)):
        d = s[i] - s[i - 1]; gains.append(max(0, d)); losses.append(max(0, -d))
    ag = sum(gains[:p]) / p; al = sum(losses[:p]) / p
    r = 100 if al == 0 else 100 - (100 / (1 + ag / al))
    for i in range(p, len(gains)):
        ag = (ag * (p - 1) + gains[i]) / p; al = (al * (p - 1) + losses[i]) / p
        r = 100 if al == 0 else 100 - (100 / (1 + ag / al))
    return r

def adx(highs, lows, closes, p=14):
    if len(closes) < p * 2 + 1: return 0, 0, 0
    pdm = []; mdm = []; tr = []
    for i in range(1, len(closes)):
        up = highs[i] - highs[i - 1]; down = lows[i - 1] - lows[i]
        pdm.append(up if up > down and up > 0 else 0); mdm.append(down if down > up and down > 0 else 0)
        tr.append(max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1])))
    def di_dx(t, a, b):
        if t <= 0: return 0, 0, 0
        pd = 100 * a / t; md = 100 * b / t; sm = pd + md
        return (100 * abs(pd - md) / sm if sm else 0), pd, md
    t = sum(tr[:p]); a = sum(pdm[:p]); b = sum(mdm[:p])
    dx, pdi, mdi = di_dx(t, a, b); dxs = [dx]
    for i in range(p, len(tr)):
        t = t - t / p + tr[i]; a = a - a / p + pdm[i]; b = b - b / p + mdm[i]
        dx, pdi, mdi = di_dx(t, a, b); dxs.append(dx)
    v = sum(dxs[:p]) / p
    for d in dxs[p:]: v = (v * (p - 1) + d) / p
    return v, pdi, mdi

def atr(highs, lows, closes, p=14):
    if len(closes) < p + 1: return None
    trs = [max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1])) for i in range(1, len(closes))]
    return sum(trs[-p:]) / p if len(trs) >= p else None

def macd(s, f=12, sl=26, sig=9):
    """Real MACD -> (line, signal, histogram). V7 hard-coded the signal line to 0; this uses the proper 9-period EMA."""
    if len(s) < sl + sig: return None, None, None
    ef = ema_series(s, f); es = ema_series(s, sl); off = sl - f
    line = [ef[j + off] - es[j] for j in range(len(es))]
    sg = ema_series(line, sig)
    if not sg: return None, None, None
    return line[-1], sg[-1], line[-1] - sg[-1]

def bb(s, p=20, m=2):
    if len(s) < p: return None, None, None
    ma = sum(s[-p:]) / p; std = math.sqrt(sum((x - ma) ** 2 for x in s[-p:]) / p)
    return ma + m * std, ma, ma - m * std

def stoch(highs, lows, closes, k=14, d=3, sm=3):
    if len(closes) < k + d: return None, None
    kvals = []
    for i in range(k, len(closes) + 1):
        hh = max(highs[i - k:i]); ll = min(lows[i - k:i])
        kvals.append(50 if hh == ll else (closes[i - 1] - ll) / (hh - ll) * 100)
    kn = sum(kvals[-sm:]) / sm if len(kvals) >= sm else kvals[-1]
    dn = sum(kvals[-d:]) / d if len(kvals) >= d else None
    return kn, dn

def efficiency(candles, n):
    if len(candles) < n: return 1.0
    cl = [c.c for c in candles[-n:]]
    path = sum(abs(cl[i] - cl[i - 1]) for i in range(1, len(cl)))
    return abs(cl[-1] - cl[0]) / path if path > 0 else 0.0

def ema_crosses(closes, period, n):
    if len(closes) < period + n: return 0
    k = 2 / (period + 1); e = sum(closes[:period]) / period; sides = []
    for x in closes[period:]:
        e = x * k + e * (1 - k); sides.append(1 if x > e else -1)
    sides = sides[-n:]
    return sum(1 for i in range(1, len(sides)) if sides[i] != sides[i - 1])

# ============================ 1-minute support / resistance ============================
def _cluster(vals, tol, min_touch=2, from_top=False):
    out = []; grp = []
    for v in sorted(vals, reverse=from_top):
        if grp and abs(v - grp[0]) > 2 * tol:
            if len(grp) >= min_touch: out.append((sum(grp) / len(grp), len(grp)))
            grp = []
        grp.append(v)
    if len(grp) >= min_touch: out.append((sum(grp) / len(grp), len(grp)))
    return out

def find_levels(c1, tol, min_touch=2):
    hs = [c.h for c in c1]; ls = [c.l for c in c1]; ph = []; pl = []
    for i in range(2, len(c1) - 2):
        if hs[i] > max(hs[i - 2], hs[i - 1], hs[i + 1], hs[i + 2]): ph.append(hs[i])
        if ls[i] < min(ls[i - 2], ls[i - 1], ls[i + 1], ls[i + 2]): pl.append(ls[i])
    return _cluster(ph, tol, min_touch, True), _cluster(pl, tol, min_touch, False)

def nearest_levels(c1, price, a1):
    c = c1[-CONFIG["LEVEL_LOOKBACK"]:]
    res, sup = find_levels(c, CONFIG["LEVEL_TOL_ATR"] * a1, CONFIG["LEVEL_MIN_TOUCHES"])
    above = sorted([x for x in res if x[0] > price]); below = sorted([x for x in sup if x[0] < price], reverse=True)
    return (above[0] if above else None), (below[0] if below else None)

def level_check(c1, side, price, a1):
    if not CONFIG["LEVEL_FILTER"] or not a1 or len(c1) < 10: return False, ""
    r, s = nearest_levels(c1, price, a1); prox = CONFIG["LEVEL_PROX_ATR"] * a1
    if r and s and (r[0] - s[0]) <= CONFIG["BOX_MAX_ATR"] * a1:
        return True, f"boxed in between support {s[0]:.5g} and resistance {r[0]:.5g}"
    if side == "bull" and r and r[0] - price <= prox: return True, f"{max(r[0]-price,0)/a1:.1f} ATR under 1-min resistance {r[0]:.5g}"
    if side == "bear" and s and price - s[0] <= prox: return True, f"{max(price-s[0],0)/a1:.1f} ATR above 1-min support {s[0]:.5g}"
    return False, ""

class Candle:
    def __init__(self, ts): self.ts = ts; self.o = None; self.h = -1e18; self.l = 1e18; self.c = None
    def add(self, p):
        if self.o is None: self.o = p
        self.h = max(self.h, p); self.l = min(self.l, p); self.c = p

class MTF:
    """1-minute candles; 5m and 15m candles are built from them (completed buckets only)."""
    def __init__(self):
        self.m1 = deque(maxlen=3000); self.cur = None; self._cache = {}
    def on_tick(self, price, epoch):
        """Returns False (and ignores the tick) if it is older than the current candle."""
        ts = datetime.fromtimestamp(epoch, tz=timezone.utc).replace(second=0, microsecond=0)
        if self.cur is not None and ts < self.cur.ts: return False
        if self.cur is None or self.cur.ts != ts:
            if self.cur is not None and self.cur.o is not None: self.m1.append(self.cur)
            self.cur = Candle(ts)
        self.cur.add(price); return True
    def load_history(self, candles):
        good = []
        for c in candles or []:
            try:
                e = int(c["epoch"]); o = float(c["open"]); h = float(c["high"]); l = float(c["low"]); cl = float(c["close"])
            except Exception: continue
            if not all(math.isfinite(v) for v in (o, h, l, cl)): continue
            good.append((e, o, h, l, cl))
        good.sort(key=lambda x: x[0])
        if not good: return False
        self.m1.clear(); self.cur = None; self._cache = {}
        for i, (e, o, h, l, cl) in enumerate(good):
            cd = Candle(datetime.fromtimestamp(e, tz=timezone.utc).replace(second=0, microsecond=0))
            cd.o, cd.h, cd.l, cd.c = o, h, l, cl
            if i == len(good) - 1: self.cur = cd
            else: self.m1.append(cd)
        return True
    def candles(self, tf):
        if tf == "1m": return list(self.m1)
        size = 5 if tf == "5m" else 15
        cur_ts = self.cur.ts if self.cur is not None else None
        key = (len(self.m1), self.m1[-1].ts if self.m1 else None, cur_ts)
        cached = self._cache.get(tf)
        if cached and cached[0] == key: return cached[1]
        def close_group(g):
            a = Candle(datetime.fromtimestamp(int(g[0].ts.timestamp()) // (size * 60) * (size * 60), tz=timezone.utc))
            a.o = g[0].o; a.c = g[-1].c; a.h = max(x.h for x in g); a.l = min(x.l for x in g); return a
        out = []; grp = []; cur_b = None
        for c in self.m1:
            b = int(c.ts.timestamp()) // (size * 60)
            if b != cur_b:
                if grp: out.append(close_group(grp))          # a newer bucket exists, so this one is complete
                grp = []; cur_b = b
            grp.append(c)
        if grp:
            bucket_end = (int(grp[-1].ts.timestamp()) // (size * 60) + 1) * size * 60
            # the last bucket is complete when its final minute is in, or the live candle already belongs to a later bucket
            if grp[-1].ts.minute % size == size - 1 or (self.cur is not None and self.cur.ts.timestamp() >= bucket_end):
                out.append(close_group(grp))
        self._cache[tf] = (key, out)
        return out
    def hl(self, tf):
        d = self.candles(tf); return [c.h for c in d], [c.l for c in d], [c.c for c in d]

class SymState:
    def __init__(self):
        self.mtf = MTF(); self.last_bucket = None; self.last_epoch = None; self.last_price = None
        self.last_mono = 0.0; self.last_warm_log = 0.0

# ============================ the bot ============================
class Bot:
    def __init__(self):
        self.pairs = list(PAIRS.keys())
        self.payout = {k: float(v["payout"]) for k, v in PAIRS.items()}
        self.state = {k: SymState() for k in self.pairs}
        self.lock = threading.RLock()
        self.now = lambda: datetime.now(timezone.utc)      # injectable clock (tests)
        self.notify = tg_send                              # injectable sender (tests)
        self.rounds = {}; self.pending = []; self.cooldown = {}; self.sent_ids = set()
        self.paused_until = 0.0; self.loss_streak = 0
        self.day = self._today(); self.signals_today = 0
        self.tally = {"wins": 0, "losses": 0, "ties": 0, "void": 0}
        self.by_pair = {}; self.by_day = {}
        self.reject = Counter(); self.reject_hour = Counter(); self.last_eval = {}
        self.analyses = 0; self.drops = 0; self.fail_count = 0; self.feed_alarm = False
        self.session_day = None; self.summary_sent = None; self.last_status_log = _mono()
        self.last_any_tick = _mono(); self.started_notice = False
        self.load_stats()

    def _today(self): return self.now().astimezone(KENYA).strftime("%Y-%m-%d")

    # ---------- stats persistence ----------
    def load_stats(self):
        p = CONFIG["SIGNALS_FILE"]
        if not os.path.exists(p): return
        try:
            with open(p, newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    res = row.get("result", ""); sym = row.get("pair_key", ""); day = (row.get("time_kenya", "") or "")[:10]
                    if res in ("WIN", "LOSS", "TIE") and sym in PAIRS: self._count(sym, day, res)
        except Exception as e: log.error(f" could not read {p}: {e}")

    def _count(self, sym, day, res):
        key = {"WIN": "wins", "LOSS": "losses", "TIE": "ties", "VOID": "void"}[res]
        self.tally[key] += 1
        self.by_pair.setdefault(sym, {"wins": 0, "losses": 0, "ties": 0, "void": 0})[key] += 1
        self.by_day.setdefault(day, {"wins": 0, "losses": 0, "ties": 0, "void": 0})[key] += 1

    def roll_day(self):
        t = self._today()
        if t != self.day: self.day = t; self.signals_today = 0

    # ---------- session ----------
    def session_open(self):
        n = self.now(); w = session_window(n)
        return bool(w and w[0] <= n < w[1])

    def want_connection(self):
        n = self.now(); w = session_window(n)
        if w and w[0] - timedelta(minutes=CONFIG["CONNECT_LEAD_MIN"]) <= n < w[1]: return True
        return bool(self.pending)

    def session_text(self):
        w = session_window(self.now())
        if not w: return "weekend - closed"
        return f"{w[0].astimezone(KENYA):%H:%M}-{w[1].astimezone(KENYA):%H:%M} Nairobi (Mon-Fri)"

    # ---------- engine ----------
    def indicators(self, mtf, tf):
        closes = [c.c for c in mtf.candles(tf)]; highs, lows, _ = mtf.hl(tf)
        if len(closes) < 60: return None
        return {"price": closes[-1], "ema20": ema(closes, 20), "ema50": ema(closes, 50), "rsi": rsi(closes, 14), "macd": macd(closes),
                "adx": adx(highs, lows, closes), "bb": bb(closes, 20, 2), "atr": atr(highs, lows, closes, 14), "stoch": stoch(highs, lows, closes)}

    def trend_of(self, ind):
        if not ind: return "WARMING"
        e20, e50, a = ind["ema20"], ind["ema50"], ind["adx"][0]
        if e20 > e50 and a >= CONFIG["MIN_ADX"]: return "BULLISH"
        if e20 < e50 and a >= CONFIG["MIN_ADX"]: return "BEARISH"
        return "RANGING"

    def data_gap(self, mtf):
        n = CONFIG["DATA_GAP_CHECK_MIN"]; m1 = list(mtf.m1)[-n:]
        if len(m1) < n: return None
        for i in range(1, len(m1)):
            d = (m1[i].ts - m1[i - 1].ts).total_seconds()
            if d > 120: return f"missing 1-min data ({d:.0f}s hole)"
        return None

    def strong_trend_ok(self, side, ind5, c5):
        if not CONFIG["STRONG_TREND_ENABLED"]: return False, "strong-trend entry switched off"
        if ind5["adx"][0] < CONFIG["STRONG_ADX"]: return False, f"ADX {ind5['adx'][0]:.0f} below {CONFIG['STRONG_ADX']}"
        last = c5[-1]
        if side == "bear" and not last.c < last.o: return False, "last 5-min candle did not close down"
        if side == "bull" and not last.c > last.o: return False, "last 5-min candle did not close up"
        return True, "ok"

    def pullback_resume(self, side, c5, ind5):
        a = ind5["atr"]; e20 = ind5["ema20"]; e50 = ind5["ema50"]
        if not a: return False
        tol = CONFIG["PULLBACK_TOUCH_ATR"] * a; recent = c5[-CONFIG["PULLBACK_BARS"]:]; last = c5[-1]
        if side == "bull": return any(c.l <= e20 + tol for c in recent) and last.c > last.o and last.c > e50 and last.c >= e20 - tol
        return any(c.h >= e20 - tol for c in recent) and last.c < last.o and last.c < e50 and last.c <= e20 + tol

    def exhaustion(self, side, ind5, k, bb_pos):
        r = ind5["rsi"]
        if side == "bull":
            hard = r > CONFIG["HARD_RSI_HIGH"]
            flags = [("RSI %.0f overbought" % r) if r > CONFIG["EXH_RSI_HIGH"] else "",
                     ("Stoch %.0f overbought" % k) if k is not None and k > CONFIG["EXH_STOCH_HIGH"] else "",
                     ("BB %.0f%% top edge" % bb_pos) if bb_pos is not None and bb_pos > CONFIG["EXH_BB_HIGH"] else ""]
        else:
            hard = r < CONFIG["HARD_RSI_LOW"]
            flags = [("RSI %.0f oversold" % r) if r < CONFIG["EXH_RSI_LOW"] else "",
                     ("Stoch %.0f oversold" % k) if k is not None and k < CONFIG["EXH_STOCH_LOW"] else "",
                     ("BB %.0f%% bottom edge" % bb_pos) if bb_pos is not None and bb_pos < CONFIG["EXH_BB_LOW"] else ""]
        flags = [f for f in flags if f]
        return hard, len(flags), ", ".join(flags)

    def _no(self, ev, tag, reason):
        ev["decision"] = "NO_TRADE"; ev["tag"] = tag; ev["reason"] = reason; return ev

    def evaluate(self, sym, price):
        mtf = self.state[sym].mtf
        ev = {"decision": "NO_TRADE", "side": None, "setup": "NONE", "tag": "", "reason": "", "id": None, "score": 0, "mine": 0, "adx": 0, "t15": "?"}
        c5 = mtf.candles("5m"); ind5 = self.indicators(mtf, "5m")
        if not ind5: return self._no(ev, "DATA_WARMUP", "fewer than 60 completed 5-min candles")
        ind1 = self.indicators(mtf, "1m"); ind15 = self.indicators(mtf, "15m")
        t5 = self.trend_of(ind5); t15 = self.trend_of(ind15); ev["t15"] = t15
        e20, e50, p5 = ind5["ema20"], ind5["ema50"], ind5["price"]
        gap = abs(e20 - e50) / p5 * 100 if p5 else 0
        adx_v, pdi, mdi = ind5["adx"]; hist = ind5["macd"][2]; k, _d = ind5["stoch"]; up, _mid, lo = ind5["bb"]
        ev["adx"] = adx_v
        bb_pos = (price - lo) / (up - lo) * 100 if up is not None and lo is not None and up != lo else None
        er = efficiency(c5, CONFIG["ER_BARS"]); xs = ema_crosses([c.c for c in c5], 20, CONFIG["ER_BARS"])
        votes = [(e20 > e50, e20 < e50), (p5 > e20, p5 < e20), (ind5["rsi"] > 50, ind5["rsi"] < 50),
                 (hist is not None and hist > 0, hist is not None and hist < 0), (pdi > mdi, pdi < mdi)]
        bull = sum(1 for b_, _s in votes if b_); bear = sum(1 for _b, s_ in votes if s_)
        gm = self.data_gap(mtf)
        if gm: return self._no(ev, "DATA_GAP", gm)
        if t5 not in ("BULLISH", "BEARISH"): return self._no(ev, "NO_TREND", f"no trend (ADX {adx_v:.0f}, min {CONFIG['MIN_ADX']})")
        if gap < CONFIG["EMA_GAP_MIN_PCT"]: return self._no(ev, "WEAK_TREND", f"EMA gap {gap:.3f}% too small")
        side = "bull" if t5 == "BULLISH" else "bear"; ev["side"] = side
        pb = self.pullback_resume(side, c5, ind5); above = p5 > e20 if side == "bull" else p5 < e20
        if pb: setup = "PULLBACK_RESUME"
        elif above: setup = "CONTINUATION"
        else: return self._no(ev, "NO_SETUP", "wrong side of EMA20 and no pullback candle")
        ev["setup"] = setup
        mine = bull if side == "bull" else bear
        if pb and not above: mine += 1
        need = CONFIG["MIN_VOTES"] + (CONFIG["AFTER_LOSS_EXTRA_VOTES"] if self.loss_streak > 0 else 0)
        if (side == "bull" and t15 == "BEARISH") or (side == "bear" and t15 == "BULLISH"): need += CONFIG["OPPOSING_15M_EXTRA_VOTES"]
        ev["mine"] = mine
        if mine < need: return self._no(ev, "VOTES", f"{mine} votes, needs {need}")
        hard, n_ext, txt = self.exhaustion(side, ind5, k, bb_pos); soft = ""
        if hard: return self._no(ev, "EXHAUSTION", f"RSI {ind5['rsi']:.0f} beyond hard limit")
        if n_ext >= CONFIG["EXH_MIN_COUNT"]:
            ok_s, why_s = self.strong_trend_ok(side, ind5, c5)
            if not ok_s: return self._no(ev, "EXHAUSTION", f"{txt} | {why_s}")
            soft = f"strong-trend pass: {txt}"
        a = ind5["atr"]
        if a and abs(price - e20) > CONFIG["MAX_EXT_ATR"] * a: return self._no(ev, "STRETCHED", f"{abs(price-e20)/a:.1f} ATR from EMA20")
        if CONFIG["ER_MIN"] > 0:
            if er < CONFIG["ER_MIN"]: return self._no(ev, "CHOPPY", f"efficiency {er:.2f}")
            if xs > CONFIG["CROSS_MAX"]: return self._no(ev, "CHOPPY", f"EMA20 crossed {xs}x")
        if ind1 and ind1["atr"]:
            blocked, why_l = level_check(mtf.candles("1m"), side, price, ind1["atr"])
            if blocked: return self._no(ev, "LEVEL", why_l)
        ev["decision"] = "BUY_CANDIDATE" if side == "bull" else "SELL_CANDIDATE"; ev["tag"] = "OK"; ev["reason"] = soft or "all checks passed"
        ev["id"] = f"{sym}-{int(c5[-1].ts.timestamp())}-{side}"
        ev["score"] = mine * 10 + min(adx_v, 40) + (6 if setup == "PULLBACK_RESUME" else 0)
        return ev

    def safe_evaluate(self, sym, price):
        try: return self.evaluate(sym, price)
        except Exception as e:
            log.error(f" evaluate error on {sym}: {type(e).__name__}: {e}\n{traceback.format_exc()}")
            return {"decision": "NO_TRADE", "side": None, "setup": "NONE", "tag": "ERROR", "reason": type(e).__name__, "id": None, "score": 0, "mine": 0, "adx": 0, "t15": "?"}

    # ---------- ticks ----------
    def on_tick(self, sym, price, epoch):
        st = self.state[sym]
        if not (math.isfinite(price) and price > 0): return
        if not st.mtf.on_tick(price, epoch): return
        st.last_epoch = epoch; st.last_price = price; st.last_mono = _mono(); self.last_any_tick = _mono()
        self.check_outcomes(sym, price, epoch)
        c5 = st.mtf.candles("5m")
        if len(c5) < 60:
            if _mono() - st.last_warm_log > 60:
                st.last_warm_log = _mono(); log.info(f" {PAIRS[sym]['label']}: warming {len(c5)}/60 five-minute candles")
            return
        bucket = c5[-1].ts
        if bucket == st.last_bucket: return
        st.last_bucket = bucket; self.analyses += 1
        ev = self.safe_evaluate(sym, price)
        age = epoch - (bucket.timestamp() + 300)
        ev.update({"epoch": epoch, "price": price, "bucket": bucket, "age": age})
        if ev["decision"] != "NO_TRADE" and age > CONFIG["MAX_SIGNAL_AGE_SEC"]:
            self._no(ev, "STALE", f"signal {age:.0f}s after candle close - too late")
        self.last_eval[sym] = (epoch, ev["decision"], ev["tag"], ev["reason"])
        if ev["decision"] == "NO_TRADE":
            self.reject[ev["tag"]] += 1; self.reject_hour[ev["tag"]] += 1
        log.info(f" {PAIRS[sym]['label']} {ev['decision']} {ev['setup']} | {ev['tag']} | {ev['reason']}")
        rd = self.rounds.setdefault(bucket, {"t0": _mono(), "evs": {}}); rd["evs"][sym] = ev

    # ---------- rounds: rank candidates, send the best ----------
    def flush_rounds(self):
        nowm = _mono()
        for bucket in sorted(list(self.rounds)):
            rd = self.rounds[bucket]
            if not (len(rd["evs"]) >= len(self.pairs) or nowm - rd["t0"] >= CONFIG["ROUND_WAIT_SEC"]): continue
            del self.rounds[bucket]
            try: self.decide_round(rd)
            except Exception as e: log.error(f" decide_round error: {type(e).__name__}: {e}\n{traceback.format_exc()}")

    def decide_round(self, rd):
        cands = [(s, e) for s, e in rd["evs"].items() if e["decision"] != "NO_TRADE"]
        if not cands: return
        self.roll_day()
        if not self.session_open(): log.info(" signal skipped: session closed"); self.reject["SESSION_CLOSED"] += 1; return
        if self.now().timestamp() < self.paused_until: log.info(" signal skipped: bot paused"); self.reject["PAUSED"] += 1; return
        ranked = []
        for sym, ev in cands:
            if self.payout[sym] < CONFIG["MIN_PAYOUT_PCT"]:
                log.info(f" {PAIRS[sym]['label']} skipped: payout {self.payout[sym]:g}% below {CONFIG['MIN_PAYOUT_PCT']}%"); self.reject["PAYOUT_LOW"] += 1; continue
            if ev["epoch"] < self.cooldown.get(sym, 0):
                log.info(f" {PAIRS[sym]['label']} skipped: cooldown"); self.reject["COOLDOWN"] += 1; continue
            if ev["id"] in self.sent_ids: self.reject["DUPLICATE"] += 1; continue
            ranked.append((ev["score"] + (self.payout[sym] - 80) / 2, sym, ev))
        ranked.sort(key=lambda x: (-x[0], self.pairs.index(x[1])))
        for _score, sym, ev in ranked[:CONFIG["MAX_SIGNALS_PER_CANDLE"]]:
            if self.signals_today >= CONFIG["MAX_SIGNALS_PER_DAY"]:
                log.info(" signal skipped: daily signal limit reached"); self.reject["DAILY_CAP"] += 1; return
            self.send_signal(sym, ev)

    def signal_text(self, sym, ev):
        label = PAIRS[sym]["label"]; hhmm = datetime.fromtimestamp(ev["epoch"], tz=timezone.utc).astimezone(KENYA).strftime("%H:%M")
        head = f"🟢🟢🟢 {label} - BUY ⬆️" if ev["side"] == "bull" else f"🔴🔴🔴 {label} - SELL ⬇️"
        return f"{head}\nEntry: NOW ({hhmm})\nDuration: 5 MIN"

    def send_signal(self, sym, ev):
        mid = self.notify(self.signal_text(sym, ev))
        if mid is None: log.error(f" signal for {sym} could not be sent to Telegram"); return
        self.sent_ids.add(ev["id"]); self.signals_today += 1
        self.cooldown[sym] = ev["epoch"] + CONFIG["PAIR_COOLDOWN_MIN"] * 60
        self.pending.append({"sym": sym, "side": ev["side"], "entry": ev["price"], "due": ev["epoch"] + CONFIG["TRADE_SECONDS"],
                             "wall_due": self.now().timestamp() + CONFIG["TRADE_SECONDS"], "msg_id": mid if mid > 0 else None, "ev": ev,
                             "time": datetime.fromtimestamp(ev["epoch"], tz=timezone.utc).astimezone(KENYA).strftime("%Y-%m-%d %H:%M:%S")})
        log.info(f" SIGNAL SENT {PAIRS[sym]['label']} {ev['side']} ({ev['setup']}, score {ev['score']:.0f})")

    # ---------- results ----------
    def check_outcomes(self, sym, price, epoch):
        if not self.pending: return
        for p in list(self.pending):
            if p["sym"] == sym and epoch >= p["due"]:
                self.pending.remove(p)
                self.resolve(p, price, "VOID" if epoch - p["due"] > 120 else None)

    def expire_pending(self):
        for p in list(self.pending):
            if self.now().timestamp() > p["wall_due"] + 900:
                self.pending.remove(p); self.resolve(p, None, "VOID")

    def resolve(self, p, exit_price, forced=None):
        sym = p["sym"]; entry = p["entry"]; pip = PAIRS[sym]["pip"]; dec = 5 if pip < 0.01 else 3
        if forced == "VOID" or exit_price is None: res = "VOID"; pips = 0.0
        else:
            diff = (exit_price - entry) if p["side"] == "bull" else (entry - exit_price)
            res = "WIN" if diff > 1e-12 else ("LOSS" if diff < -1e-12 else "TIE"); pips = diff / pip
        self._count(sym, p["time"][:10], res)
        try:
            new = not os.path.exists(CONFIG["SIGNALS_FILE"])
            with open(CONFIG["SIGNALS_FILE"], "a", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                if new: w.writerow(["time_kenya", "pair_key", "pair", "side", "setup", "entry", "exit", "pips", "result", "votes", "adx", "score"])
                w.writerow([p["time"], sym, PAIRS[sym]["label"], "BUY" if p["side"] == "bull" else "SELL", p["ev"]["setup"], f"{entry:.{dec}f}",
                            "" if exit_price is None else f"{exit_price:.{dec}f}", f"{pips:.1f}", res, p["ev"]["mine"], f"{p['ev']['adx']:.0f}", f"{p['ev']['score']:.0f}"])
        except Exception as e: log.error(f" could not write {CONFIG['SIGNALS_FILE']}: {e}")
        label = PAIRS[sym]["label"]; side_txt = "BUY" if p["side"] == "bull" else "SELL"
        log.info(f" RESULT {label} {res} entry {entry} exit {exit_price} ({pips:+.1f} pips)")
        icon = {"WIN": "✅", "LOSS": "❌", "TIE": "➖ TIE (no win, no loss)", "VOID": "⚠️ no result (price data lost)"}[res]
        # sent as a reply to the signal; a lone emoji is shown big by Telegram. If the reply link is unknown, name the trade instead.
        self.notify(icon if p.get("msg_id") else f"{icon}\n{label} {side_txt} {p['time'][11:16]}", reply_to=p.get("msg_id"))
        if res == "LOSS":
            self.loss_streak += 1
            if self.loss_streak >= CONFIG["MAX_CONSEC_LOSSES"]:
                self.paused_until = self.now().timestamp() + CONFIG["LOSS_PAUSE_MIN"] * 60; self.loss_streak = 0
                self.notify(f"⏸ {CONFIG['MAX_CONSEC_LOSSES']} losses in a row - signals paused for {CONFIG['LOSS_PAUSE_MIN']} min. Send /resume to restart earlier.")
        elif res == "WIN": self.loss_streak = 0

    # ---------- text reports ----------
    def wr(self, t):
        n = t["wins"] + t["losses"]
        return f"{t['wins'] / n * 100:.0f}%" if n else "n/a"

    def stats_text(self):
        t = self.tally; best = max(self.payout.values())
        lines = [f"📊 ALL TIME: {t['wins']}W / {t['losses']}L / {t['ties']} ties | win rate {self.wr(t)}",
                 f"Break-even at {best:g}% payout = {100 / (100 + best) * 100:.1f}%"]
        td = self.by_day.get(self.day)
        if td: lines.append(f"Today: {td['wins']}W / {td['losses']}L | {self.wr(td)}")
        for sym, v in sorted(self.by_pair.items()):
            lines.append(f"{PAIRS[sym]['label']}: {v['wins']}W / {v['losses']}L ({self.wr(v)})")
        return "\n".join(lines)

    def summary_text(self):
        td = self.by_day.get(self.day, {"wins": 0, "losses": 0, "ties": 0, "void": 0})
        return f"📋 SESSION DONE\nSignals today: {self.signals_today}\n✅ {td['wins']}  ❌ {td['losses']}  win rate {self.wr(td)}\n\n" + self.stats_text()

    def handle_command(self, text):
        parts = text.strip().split()
        if not parts: return None
        cmd = parts[0].lower().split("@")[0]; args = parts[1:]
        with self.lock:
            if cmd in ("/start", "/help"):
                return ("ACE PRO signals bot\n/status - state\n/stats - win/loss\n/why - why no signal\n/pause [min] - stop signals (news)\n"
                        "/resume - start again\n/payout EURUSD 90 - set payout %\nSignals: 🟢 BUY / 🔴 SELL, then ✅ win or ❌ loss.")
            if cmd == "/status":
                now = _mono(); feeds = []
                for s in self.pairs:
                    st = self.state[s]; feeds.append(f"{PAIRS[s]['label']} {'live' if st.last_mono and now - st.last_mono < 30 else 'no data'} ({self.payout[s]:g}%)")
                paused = "no" if self.now().timestamp() >= self.paused_until else f"until {datetime.fromtimestamp(self.paused_until, tz=timezone.utc).astimezone(KENYA):%H:%M}"
                return (f"🤖 {'SESSION OPEN' if self.session_open() else 'session closed'} | hours {self.session_text()}\nPaused: {paused}\n"
                        f"Signals today: {self.signals_today} | open trades: {len(self.pending)}\n" + " | ".join(feeds))
            if cmd == "/stats": return self.stats_text()
            if cmd == "/why":
                if not self.last_eval: return "No analysis yet."
                lines = []
                for s in self.pairs:
                    if s in self.last_eval:
                        _e, dec, tag, reason = self.last_eval[s]; lines.append(f"{PAIRS[s]['label']}: {dec if dec != 'NO_TRADE' else tag} - {reason}")
                return "\n".join(lines)
            if cmd == "/pause":
                try: mins = float(args[0]) if args else 60.0
                except ValueError: return "Use: /pause 30"
                if not (0 < mins <= 1440): return "Minutes must be between 1 and 1440."
                self.paused_until = self.now().timestamp() + mins * 60
                return f"⏸ Signals paused for {mins:g} min. /resume to restart."
            if cmd == "/resume":
                self.paused_until = 0.0; self.loss_streak = 0; return "▶️ Signals resumed."
            if cmd == "/payout":
                if not args: return "\n".join(f"{PAIRS[s]['label']}: {self.payout[s]:g}%" for s in self.pairs) + f"\nMinimum to signal: {CONFIG['MIN_PAYOUT_PCT']}%\nChange: /payout EURUSD 90"
                key = "".join(ch for ch in args[0].upper() if ch.isalpha())
                if key not in PAIRS: return "Unknown pair. Use one of: " + ", ".join(self.pairs)
                try: val = float(args[1])
                except (IndexError, ValueError): return "Use: /payout EURUSD 90"
                if not (0 < val <= 100): return "Payout must be between 1 and 100."
                self.payout[key] = val
                return f"{PAIRS[key]['label']} payout set to {val:g}%" + ("" if val >= CONFIG["MIN_PAYOUT_PCT"] else f" (below {CONFIG['MIN_PAYOUT_PCT']}% - no signals for it)")
        return "Unknown command. Send /help"

    # ---------- housekeeping ----------
    def housekeeping(self):
        self.flush_rounds(); self.expire_pending(); self.roll_day()
        if self.session_open():
            self.session_day = session_window(self.now())[0].date().isoformat()
        elif self.session_day and self.summary_sent != self.session_day and not self.pending:
            self.summary_sent = self.session_day
            if self.signals_today > 0: self.notify(self.summary_text())
        if _mono() - self.last_status_log >= CONFIG["STATUS_EVERY_SEC"]:
            self.last_status_log = _mono()
            top = ", ".join(f"{k} {v}" for k, v in self.reject_hour.most_common(5)) or "none"
            log.info(f" STATUS analyses {self.analyses} | signals today {self.signals_today} | {self.tally['wins']}W/{self.tally['losses']}L | drops {self.drops} | main reasons: {top}")
            self.reject_hour = Counter()

    # ---------- Deriv connection ----------
    async def load_histories(self, ws):
        req = {}
        for i, sym in enumerate(self.pairs, start=1):
            req[i] = sym
            await ws.send(json.dumps({"ticks_history": PAIRS[sym]["deriv"], "adjust_start_time": 1, "count": CONFIG["HISTORY_CANDLES"],
                                      "end": "latest", "style": "candles", "granularity": 60, "req_id": i}))
        done = set(); loaded = set(); end = time.time() + 30
        while len(done) < len(self.pairs) and time.time() < end:
            try: raw = await asyncio.wait_for(ws.recv(), timeout=max(0.5, end - time.time()))
            except asyncio.TimeoutError: break
            m = json.loads(raw); rid = m.get("req_id")
            # identify the pair by req_id, or by the echoed request if Deriv does not return req_id
            sym = req.get(rid) if rid in req else DERIV2KEY.get(str((m.get("echo_req") or {}).get("ticks_history", "")))
            if not sym or sym in done: continue
            done.add(sym)
            if m.get("error"): log.error(f" history for {PAIRS[sym]['label']} refused: {str(m['error'])[:150]}"); continue
            if "candles" in m and self.state[sym].mtf.load_history(m["candles"]):
                loaded.add(sym); log.info(f" Loaded {len(m['candles'])} 1-min candles for {PAIRS[sym]['label']}")
        if not loaded: raise RuntimeError("no price history received")
        for sym in self.pairs:
            if sym not in loaded: log.warning(f" no history for {PAIRS[sym]['label']} - it will warm up slowly")

    async def keepalive(self, ws):
        try:
            while True:
                await asyncio.sleep(CONFIG["KEEPALIVE_SEC"]); await ws.send(json.dumps({"ping": 1}))
        except Exception: return

    async def run(self):
        import websockets
        wait = CONFIG["RETRY_MIN_SEC"]
        if not self.started_notice:
            self.started_notice = True
            self.notify(f"✅ ACE PRO signals ready\nHours: {self.session_text()}\nPairs: " + ", ".join(PAIRS[s]["label"] for s in self.pairs))
        while True:
            self.housekeeping()
            if not self.want_connection(): await asyncio.sleep(15); continue
            ka = None; t0 = time.time()
            try:
                url = await get_ws_url(); log.info(" Got authenticated WebSocket URL")
                async with websockets.connect(url, ping_interval=20, ping_timeout=60, max_queue=4096) as ws:
                    t0 = time.time(); self.last_any_tick = _mono()
                    ka = asyncio.create_task(self.keepalive(ws))
                    await self.load_histories(ws)
                    for sym in self.pairs: await ws.send(json.dumps({"ticks": PAIRS[sym]["deriv"], "subscribe": 1}))
                    wait = CONFIG["RETRY_MIN_SEC"]; got_tick = False; unknown_logged = False
                    while True:
                        self.housekeeping()
                        if not self.want_connection(): log.info(" Session over - disconnecting"); break
                        if _mono() - self.last_any_tick > CONFIG["STALL_SEC"]: raise RuntimeError(f"no prices for {CONFIG['STALL_SEC']}s")
                        try: raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                        except asyncio.TimeoutError: continue
                        msg = json.loads(raw)
                        if msg.get("error"): log.error(f" API error: {str(msg['error'])[:150]}"); continue
                        if msg.get("msg_type") != "tick": continue
                        t = msg.get("tick") or {}
                        sym = DERIV2KEY.get(str(t.get("symbol", ""))) or DERIV2KEY.get(str((msg.get("echo_req") or {}).get("ticks", "")))
                        if not sym:
                            if not unknown_logged: unknown_logged = True; log.warning(f" tick for an unknown symbol ignored: {str(msg)[:150]}")
                            continue
                        try: price = float(t["quote"]); epoch = int(t["epoch"])
                        except Exception: continue
                        if not got_tick:
                            got_tick = True; self.fail_count = 0
                            if self.feed_alarm: self.feed_alarm = False; self.notify("✅ Price feed restored")
                        try: self.on_tick(sym, price, epoch)
                        except Exception as e: log.error(f" tick error ({sym}): {type(e).__name__}: {e}\n{traceback.format_exc()}")
                if ka: ka.cancel()
            except Exception as e:
                if ka: ka.cancel()
                self.drops += 1; self.fail_count += 1
                log.error(f" connection problem after {time.time()-t0:.0f}s ({type(e).__name__}: {e}) - retry in {wait:.0f}s")
                if self.fail_count >= 3 and not self.feed_alarm and self.session_open():
                    self.feed_alarm = True; self.notify("⚠️ Price feed problem - no signals until it reconnects")
                await asyncio.sleep(wait); wait = min(wait * 1.5, CONFIG["RETRY_MAX_SEC"])

# ============================ Telegram commands thread ============================
def command_loop(bot):
    offset = None
    try: tg_api("deleteWebhook")
    except TgError as e: log.error(f" {e}")
    try:
        r = tg_api("getUpdates", {"offset": -1, "timeout": 0}); res = r.get("result", [])
        if res: offset = res[-1]["update_id"] + 1
    except TgError as e: log.error(f" {e}")
    while True:
        try:
            params = {"timeout": 25, "allowed_updates": ["message"]}
            if offset is not None: params["offset"] = offset
            r = tg_api("getUpdates", params, timeout=40)
            for upd in r.get("result", []):
                offset = upd["update_id"] + 1
                msg = upd.get("message") or {}; text = (msg.get("text") or "").strip()
                if not text.startswith("/"): continue
                if str((msg.get("chat") or {}).get("id", "")) != str(CONFIG["CHAT_ID"]): log.info(" ignored command from another chat"); continue
                reply = bot.handle_command(text)
                if reply: tg_send(reply)
        except TgError as e:
            if e.code == 409: log.error(" Telegram conflict: another copy of this bot is running. Stop the other one."); time.sleep(15)
            else: log.error(f" {e}"); time.sleep(5)
        except Exception as e:
            log.error(f" command loop error: {type(e).__name__}: {e}"); time.sleep(5)

# ============================ health page (keeps Render happy) ============================
class Health(BaseHTTPRequestHandler):
    def _ok(self, body=True):
        data = b"ACE PRO SIGNAL BOT LIVE"
        self.send_response(200); self.send_header("Content-Type", "text/plain"); self.send_header("Content-Length", str(len(data))); self.end_headers()
        if body: self.wfile.write(data)
    def do_GET(self): self._ok()
    def do_HEAD(self): self._ok(False)
    def log_message(self, *a): pass

def start_health_server():
    port = int(os.environ.get("PORT", "10000"))
    try:
        srv = HTTPServer(("0.0.0.0", port), Health)
        threading.Thread(target=srv.serve_forever, daemon=True).start(); log.info(f" Health page on port {port}")
    except OSError as e: log.error(f" health server not started: {e}")

def main():
    missing = [n for n in ("BOT_TOKEN", "CHAT_ID", "DERIV_TOKEN") if not CONFIG[n]]
    if missing:
        log.critical(" Missing: " + ", ".join(missing) + " (set environment variables, or create the .txt files next to the script)"); sys.exit(1)
    start_health_server()
    bot = Bot()
    threading.Thread(target=command_loop, args=(bot,), daemon=True).start()
    try: asyncio.run(bot.run())
    except KeyboardInterrupt: print("\nBot stopped (Ctrl+C).")

if __name__ == "__main__":
    main()
