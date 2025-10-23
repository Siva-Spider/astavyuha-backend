# logger_util.py
import datetime
import logging
import threading
import os

_log_buf = []
_log_lock = threading.Lock()

LOGGER_NAME = "AutoTrader"
logger = logging.getLogger(LOGGER_NAME)
logger.setLevel(logging.INFO)

# add a single StreamHandler for terminal output if none present
if not logger.handlers:
    ch = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

# Do NOT propagate to root (prevents duplication)
logger.propagate = False

def push_log(msg: str, level: str = "info"):
    """
    Add message to buffer and write to the module logger (terminal).
    msg should be an already formatted string if you want consistent appearance.
    """
    try:
        ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        payload = {"type": "log", "ts": ts, "level": level, "message": str(msg)}
        with _log_lock:
            _log_buf.append(payload)
    except Exception:
        pass

    try:
        if level == "info":
            logger.info(msg)
        elif level == "warning":
            logger.warning(msg)
        elif level == "error":
            logger.error(msg)
        else:
            logger.debug(msg)
    except Exception:
        # best effort
        print(msg)

def get_log_buffer():
    with _log_lock:
        return list(_log_buf)

# ------- BroadcastHandler: attach this to root logger to forward records to buffer -------
class BroadcastHandler(logging.Handler):
    """
    Logging handler that formats records and forwards the formatted string to push_log().
    Attach this once to the root logger so all logging (any module) is forwarded.
    """
    def __init__(self, fmt=None, level=logging.NOTSET):
        super().__init__(level)
        if fmt is None:
            fmt = "%(asctime)s - %(levelname)s - %(message)s"
        self.formatter = logging.Formatter(fmt)

    def emit(self, record: logging.LogRecord):
        try:
            formatted = self.format(record)
            levelname = record.levelname.lower()
            push_log(formatted, levelname)
        except Exception:
            # never raise from handler
            try:
                print("BroadcastHandler emit failure:", record)
            except Exception:
                pass

def attach_broadcast_to_root():
    """
    Call this once during app startup (from app.py) to attach the BroadcastHandler
    to the root logger. If already attached, do nothing.
    """
    root = logging.getLogger()
    # avoid duplicates: check for existing instance
    for h in root.handlers:
        if isinstance(h, BroadcastHandler):
            return
    broadcast = BroadcastHandler(fmt="%(asctime)s - %(levelname)s - %(message)s")
    broadcast.setLevel(logging.INFO)
    root.addHandler(broadcast)
