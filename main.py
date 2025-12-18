import requests
import os
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

symbol_ids = [
    "KRAKEN_SPOT_BTC_USD",
    "KRAKEN_SPOT_ETH_USD",
    "KRAKEN_SPOT_SOL_USD",
    "KRAKEN_SPOT_XRP_USD",
    "KRAKEN_SPOT_ADA_USD",
    "KRAKEN_SPOT_DOGE_USD",
    "KRAKEN_SPOT_LTC_USD",
    "KRAKEN_SPOT_AVAX_USD",
    "KRAKEN_SPOT_LINK_USD",
    "KRAKEN_SPOT_DOT_USD",
]

API_KEY = os.getenv("COIN_API_KEY")

BASE_URL = "https://rest.coinapi.io/v1/ohlcv"
HEADERS = {
    "Accept": "application/json",
    "X-CoinAPI-Key": API_KEY,
}

# 6‑month window ending now (UTC)
now = datetime.now(timezone.utc)
six_months_ago = now - timedelta(days=6 * 30)  # rough 6‑month approximation

time_start = six_months_ago.isoformat(timespec="seconds").replace("+00:00", "Z")
time_end = now.isoformat(timespec="seconds").replace("+00:00", "Z")

PERIOD_ID = "15MIN"   # 15‑minute candles

def fetch_ohlcv(symbol_id: str):
    params = {
        "period_id": PERIOD_ID,
        "time_start": time_start,
        "time_end": time_end,
        "limit": 25000,
    }
    url = f"{BASE_URL}/{symbol_id}/history"
    resp = requests.get(url, headers=HEADERS, params=params)
    resp.raise_for_status()
    return resp.json()  # list of OHLCV candles

all_data = {}

for sid in symbol_ids:
    try:
        ohlcv = fetch_ohlcv(sid)
        all_data[sid] = ohlcv
        print(f"{sid}: retrieved {len(ohlcv)} candles")
    except requests.HTTPError as e:
        print(f"Error for {sid}: {e} - {getattr(e.response, 'text', '')}")



rows = []
for sid, candles in all_data.items():          # all_data from the previous step
    for c in candles:
        rows.append({
            "symbol_id": sid,
            "time_period_start": c["time_period_start"],
            "time_period_end": c["time_period_end"],
            "time_open": c.get("time_open"),
            "time_close": c.get("time_close"),
            "price_open": c["price_open"],
            "price_high": c["price_high"],
            "price_low": c["price_low"],
            "price_close": c["price_close"],
            "volume_traded": c["volume_traded"],
            "trades_count": c["trades_count"],
        })

df = pd.DataFrame(rows)
df.to_csv("kraken_15min_6mo_ohlcv.csv", index=False)
