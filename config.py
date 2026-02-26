import os
from dotenv import load_dotenv

load_dotenv()

DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID", "")
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN", "")
DHAN_TOTP_SECRET = os.getenv("DHAN_TOTP_SECRET", "")
DHAN_SECURITY_ID = int(os.getenv("DHAN_SECURITY_ID", "49081"))
DHAN_EXCHANGE_SEGMENT = os.getenv("DHAN_EXCHANGE_SEGMENT", "NSE_FNO")
FYERS_SYMBOL = os.getenv("FYERS_SYMBOL", "NSE:NIFTY26MARFUT")
REFRESH_INTERVAL_MS = int(os.getenv("REFRESH_INTERVAL_MS", "500"))
WALL_THRESHOLD = float(os.getenv("WALL_THRESHOLD", "3.0"))
ABSORPTION_TICKS = int(os.getenv("ABSORPTION_TICKS", "10"))
PORT = int(os.getenv("PORT", "8000"))
