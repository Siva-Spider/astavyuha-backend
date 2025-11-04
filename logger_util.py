# common_logger.py
import logging
import os
import datetime
import threading
from collections import deque
from zoneinfo import ZoneInfo

# ==========================================================
# 🌐 Global Autotrade Logger Configuration
# ==========================================================

# Set up base logging config
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Global logger instance for the entire app
logger = logging.getLogger("autotrade")
if not logger.hasHandlers():
    file_handler = logging.FileHandler("/tmp/autotrade.log")
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)


# ==========================================================
# 🧠 In-Memory Log Buffer (for UI / SSE / APIs)
# ==========================================================

_LOG_MAX = int(os.environ.get("AUTOTRADE_LOG_BUFFER", 500))
_log_buf = deque(maxlen=_LOG_MAX)
_log_lock = threading.Lock()

def push_log(message, level="info,inline=False"):
    """Add a log message to the in-memory buffer and standard logger."""
    ts = datetime.datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d %H:%M:%S")
    entry = {"type": "log", "ts": ts, "message": str(message), "level": level}

    with _log_lock:
        _log_buf.append(entry)
        
    if inline:
        print(f"{message}", end='\r', flush=True)
    else:
        print(f"{message}")
    
    # Write to actual Python logger
    level = level.lower()
    if level == "error":
        logger.error(message)
    elif level == "warning":
        logger.warning(message)
    else:
        logger.info(message)

def push_payload(name, data):
    """Push structured payloads (e.g., trade data, metrics) into the buffer."""
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    entry = {"type": "payload", "ts": ts, "name": name, "data": data}
    with _log_lock:
        _log_buf.append(entry)

def get_log_buffer():
    """Return all buffered logs (used for frontend streaming or debugging)."""
    with _log_lock:
        return list(_log_buf)

# ==========================================================
# ✅ Exports
# ==========================================================
__all__ = ["logger", "push_log", "push_payload", "get_log_buffer"]
