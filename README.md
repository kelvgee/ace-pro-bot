# ACE PRO Signal Bot

Sends 5-minute trading signals to Telegram for manual entry on Pocket Option.
**Signals only: the code never places trades.**

## How it works
- Reads live forex prices from Deriv (demo token, read-only) and builds 1, 5 and 15 minute candles.
- Checks trend, momentum, exhaustion and support/resistance on every 5-minute candle.
- When a pair passes all checks, it sends one signal to Telegram, for example:
  `🟢🟢🟢 EUR/USD - BUY ⬆️ | Entry: NOW (12:05) | Duration: 5 MIN`
- About 5 minutes later it replies to the signal with ✅ (win) or ❌ (loss).

## Pairs
EUR/USD, AUD/JPY, USD/JPY.
CAD/JPY and CHF/JPY are switched off (Deriv returns no data for them). To enable them, remove the `#` on their lines in `PAIRS` in `main.py`.

## Settings (in `main.py`)
- `MIN_PAYOUT_PCT`: 70. Pairs paying less than this are skipped.
- Session: 08:15 to 16:00 London time (11:15 to 19:00 Nairobi in winter), Monday to Friday.
- One signal per candle, 15 minute cooldown per pair, 12 signals per day.
- Pauses for 60 minutes after 3 losses in a row.

## Telegram commands
- `/status` shows session state and whether each price feed is live
- `/stats` shows wins and losses
- `/why` shows why each pair did not signal
- `/pause [minutes]` and `/resume`
- `/payout EURUSD 90` changes a payout live (resets on restart)
- `/help`

## Deploy on Render
1. Build Command: `pip install -r requirements.txt`
2. Start Command: `python main.py`
3. Environment variables: `BOT_TOKEN`, `CHAT_ID`, `DERIV_TOKEN`
4. On the free plan, add a monitor (for example UptimeRobot) that pings the Render URL every 5 minutes so the service does not sleep.

## Notes
- Win/loss results are judged on Deriv prices, which can differ slightly from Pocket Option.
- Render's free disk resets on restart, so `signals_log.csv` and the stats are lost.
- Test on a demo account before using real money.
-
