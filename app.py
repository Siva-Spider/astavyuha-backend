# app.py
# Fixed single-file backend: duplicates removed (get_latest_instruments, stream_logs)
# Leader-lock + lock-renewer + memory logging + safe spawn wrapper included.

import os
import json
import time
import traceback
import gc
import logging
import datetime
import random
import sqlite3
import smtplib
import tempfile
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from flask import Flask, request, jsonify, Response, send_from_directory
from flask_cors import CORS

# gevent and monkey patching
import gevent
from gevent import monkey, spawn
from logger_util import push_log, get_log_buffer, attach_broadcast_to_root
import builtins
import threading
import importlib
monkey.patch_all()

# ====== Broker libs and project modules (keep as in your original) ======
import Upstox as us
import Zerodha as zr
import AngelOne as ar
import Groww as gr
import Fivepaisa as fp
from logger_module import logger

import get_lot_size as ls
from upstox_instrument_manager import LATEST_LINK_FILENAME, DATA_DIR, update_instrument_file
import Next_Now_intervals as nni
import combinding_dataframes as cdf
import indicators as ind
from tabulate import tabulate
from kiteconnect import KiteConnect
import threading
from collections import deque

# =====================================================================

app = Flask(__name__)
CORS(app)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("autotrade")

# ---------------- App globals (taken from original) ----------------
broker_map = {
    "u": "Upstox",
    "z": "Zerodha",
    "a": "AngelOne",
    "g": "Groww",
    "5": "5paisa"
}

stock_map = {
    "RELIANCE INDUSTRIES LTD": "RELIANCE",
    "HDFC BANK LTD": "HDFCBANK",
    "ICICI BANK LTD.": "ICICIBANK",
    "INFOSYS LIMITED": "INFY",
    "TATA CONSULTANCY SERV LT": "TCS",
    "STATE BANK OF INDIA": "SBIN",
    "AXIS BANK LTD": "AXISBANK",
    "KOTAK MAHINDRA BANK LTD": "KOTAKBANK",
    "ITC LTD": "ITC",
    "LARSEN & TOUBRO LTD.": "LT",
    "BAJAJ FINANCE LIMITED": "BAJFINANCE",
    "HINDUSTAN UNILEVER LTD": "HINDUNILVR",
    "SUN PHARMACEUTICAL IND L": "SUNPHARMA",
    "MARUTI SUZUKI INDIA LTD": "MARUTI",
    "NTPC LTD": "NTPC",
    "HCL TECHNOLOGIES LTD": "HCLTECH",
    "ULTRATECH CEMENT LIMITED": "ULTRACEMCO",
    "TATA MOTORS LIMITED": "TATAMOTORS",
    "TITAN COMPANY LIMITED": "TITAN",
    "BHARAT ELECTRONICS LTD": "BEL",
    "POWER GRID CORP. LTD": "POWERGRID",
    "TATA STEEL LIMITED": "TATASTEEL",
    "TRENT LTD": "TRENT",
    "ASIAN PAINTS LIMITED": "ASIANPAINT",
    "JIO FIN SERVICES LTD": "JIOFIN",
    "BAJAJ FINSERV LTD": "BAJAJFINSV",
    "GRASIM INDUSTRIES LTD": "GRASIM",
    "ADANI PORT & SEZ LTD": "ADANIPORTS",
    "JSW STEEL LIMITED": "JSWSTEEL",
    "HINDALCO INDUSTRIES LTD": "HINDALCO",
    "OIL AND NATURAL GAS CORP": "ONGC",
    "TECH MAHINDRA LIMITED": "TECHM",
    "BAJAJ AUTO LIMITED": "BAJAJ-AUTO",
    "SHRIRAM FINANCE LIMITED": "SHRIRAMFIN",
    "CIPLA LTD": "CIPLA",
    "COAL INDIA LTD": "COALINDIA",
    "SBI LIFE INSURANCE CO LTD": "SBILIFE",
    "HDFC LIFE INS CO LTD": "HDFCLIFE",
    "NESTLE INDIA LIMITED": "NESTLEIND",
    "DR. REDDY S LABORATORIES": "DRREDDY",
    "APOLLO HOSPITALS ENTER. L": "APOLLOHOSP",
    "EICHER MOTORS LTD": "EICHERMOT",
    "WIPRO LTD": "WIPRO",
    "TATA CONSUMER PRODUCT LTD": "TATACONSUM",
    "ADANI ENTERPRISES LIMITED": "ADANIENT",
    "HERO MOTOCORP LIMITED": "HEROMOTOCO",
    # ... (other original entries)
}

reverse_stock_map = {v: k for k, v in stock_map.items()}

trade_logs = []
active_trades = {}   # e.g. { "RELIANCE": True }
broker_sessions = {}
otp_store = {}
LOGGED_IN_JSON = "logged_in_users.json"
DB_FILE = "user_data_new.db"
OTP_FILE = "otp_store.json"
OTP_EXPIRY = 300

ADMIN_EMAIL = "sivag.prasad88@gmail.com"
GMAIL_APP_PASSWORD = "vueiidvhyuyhqqla"

# ---------------- database init (as in original) ----------------
def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # Approved users
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            userId TEXT UNIQUE,
            username TEXT,
            email TEXT,
            password TEXT,
            role TEXT,
            mobilenumber TEXT
        )
    """)

    # Pending registrations
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pending_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            userId TEXT UNIQUE,
            username TEXT,
            email TEXT,
            password TEXT,
            role TEXT,
            mobilenumber TEXT
        )
    """)

    # Rejected registrations (old backup table)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS rejected_users_old (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            userId TEXT UNIQUE,
            username TEXT,
            email TEXT,
            password TEXT,
            role TEXT,
            mobilenumber TEXT
        )
    """)

    # Rejected registrations (active table)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS rejected_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            userId TEXT UNIQUE,
            username TEXT,
            email TEXT,
            password TEXT,
            role TEXT,
            mobilenumber TEXT
        )
    """)

    conn.commit()
    conn.close()

# ----------------- Helper DB / util functions (as original) ----------------
def query_db(query, args=(), one=False):
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(query, args)
    rv = cur.fetchall()
    conn.commit()
    conn.close()
    return (rv[0] if rv else None) if one else rv

def load_logged_in_users():
    if not os.path.exists(LOGGED_IN_JSON):
        return {}
    with open(LOGGED_IN_JSON, "r") as f:
        return json.load(f)

def save_logged_in_users(users):
    with open(LOGGED_IN_JSON, "w") as f:
        json.dump(users, f)

# ----------------- Leader-lock + memory-hardening patch -----------------
try:
    import redis
    _redis = redis.from_url(os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/0"))
except Exception:
    _redis = None

LOCK_KEY = os.environ.get("AUTOTRADE_LOCK_KEY", "autotrade:leader")
LOCK_TTL = int(os.environ.get("AUTOTRADE_LOCK_TTL", "30"))  # seconds

# Paste this into your app.py replacing the current _acquire_lock and _renew_lock_loop

LOCK_FILE = os.environ.get("AUTOTRADE_LOCK_FILE", os.path.join(tempfile.gettempdir(), "autotrade_leader.lock"))

def _is_pid_running(pid):
    """Return True if a process with PID exists on this host (posix & windows-safe attempt)."""
    try:
        pid = int(pid)
    except Exception:
        return False
    if pid <= 0:
        return False
    try:
        # POSIX: signal 0 check; Windows: still works for existence in many Python builds
        os.kill(pid, 0)
    except PermissionError:
        # Process exists but we don't have permission — treat as running
        return True
    except OSError:
        return False
    except Exception:
        return False
    return True

def _acquire_lock():
    """
    Try to acquire the Redis leader lock. If Redis is unavailable, fall back to a file lock.
    Returns True if this process becomes leader.
    """
    # --- Try Redis first (if configured) ---
    if _redis:
        try:
            acquired = _redis.set(LOCK_KEY, str(os.getpid()), nx=True, ex=LOCK_TTL)
            if acquired:
                logger.info("Acquired leader lock in Redis (pid=%s)", os.getpid())
                return True

            owner = _redis.get(LOCK_KEY)
            if owner:
                owner = owner.decode() if isinstance(owner, bytes) else str(owner)
                try:
                    owner_pid = int(owner)
                except Exception:
                    # invalid owner value — try delete and claim
                    try:
                        _redis.delete(LOCK_KEY)
                    except Exception:
                        pass
                    time.sleep(0.05)
                    return bool(_redis.set(LOCK_KEY, str(os.getpid()), nx=True, ex=LOCK_TTL))
                # if owner pid not running on this host, attempt takeover
                if not _is_pid_running(owner_pid):
                    logger.warning("Redis lock owner pid=%s not running. Attempting takeover.", owner_pid)
                    try:
                        lua = """
                        if redis.call("get", KEYS[1]) == ARGV[1] then
                            return redis.call("del", KEYS[1])
                        else
                            return 0
                        end
                        """
                        _redis.eval(lua, 1, LOCK_KEY, str(owner_pid))
                    except Exception:
                        try:
                            _redis.delete(LOCK_KEY)
                        except Exception:
                            pass
                    time.sleep(0.05)
                    return bool(_redis.set(LOCK_KEY, str(os.getpid()), nx=True, ex=LOCK_TTL))
                # owner is alive -> cannot acquire
                return False
            else:
                # no owner key; try again
                return bool(_redis.set(LOCK_KEY, str(os.getpid()), nx=True, ex=LOCK_TTL))
        except Exception:
            # Redis connection error; fall through to file-lock fallback
            logger.warning("Redis unavailable; falling back to file-lock. Error: %s", traceback.format_exc())

    # --- File-lock fallback (single-host) ---
    try:
        # Attempt to create lock file atomically using os.O_EXCL
        flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
        # On Windows, O_EXCL semantics vary, but this works in most cases.
        fd = os.open(LOCK_FILE, flags)
        try:
            os.write(fd, str(os.getpid()).encode())
            os.close(fd)
            logger.info("Acquired leader lock with file %s (pid=%s)", LOCK_FILE, os.getpid())
            return True
        except Exception:
            try:
                os.close(fd)
            except Exception:
                pass
            raise

    except FileExistsError:
        # Lock file exists — check owner pid
        try:
            with open(LOCK_FILE, "r") as f:
                content = f.read().strip()
        except Exception:
            content = None

        try:
            owner_pid = int(content) if content else None
        except Exception:
            owner_pid = None

        # If owner PID is not running, remove stale lock and try to claim
        if owner_pid is None or not _is_pid_running(owner_pid):
            logger.warning("Stale file lock detected (owner=%s). Taking over.", owner_pid)
            try:
                os.remove(LOCK_FILE)
            except Exception:
                pass
            # try once more to create
            try:
                fd = os.open(LOCK_FILE, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.write(fd, str(os.getpid()).encode())
                os.close(fd)
                logger.info("Acquired leader lock with file after takeover %s (pid=%s)", LOCK_FILE, os.getpid())
                return True
            except Exception:
                return False
        else:
            # Owner alive -> cannot acquire
            return False
    except Exception as e:
        logger.exception("Unexpected file-lock acquire error: %s", e)
        return False

def _renew_lock_loop():
    """
    Renew Redis TTL if using Redis, otherwise update lock file timestamp occasionally.
    If lock is lost, stop renewing.
    """
    try:
        while True:
            if _redis:
                try:
                    val = _redis.get(LOCK_KEY)
                    if val and val.decode() == str(os.getpid()):
                        _redis.expire(LOCK_KEY, LOCK_TTL)
                    else:
                        break
                except Exception:
                    logger.debug("Error renewing redis lock: %s", traceback.format_exc())
            else:
                # Touch the lock file to update modified time (best-effort)
                try:
                    if os.path.exists(LOCK_FILE):
                        with open(LOCK_FILE, "w") as f:
                            f.write(str(os.getpid()))
                    else:
                        # lock file disappeared — stop renewing
                        break
                except Exception:
                    logger.debug("Error touching lock file: %s", traceback.format_exc())
            gevent.sleep(max(1, LOCK_TTL / 2))
    except Exception:
        logger.exception("Lock renew loop fatal: %s", traceback.format_exc())


def _log_memory(stage=""):
    try:
        import psutil
        proc = psutil.Process()
        rss_mb = proc.memory_info().rss / 1024 / 1024
        logger.info("[MEM] pid=%s stage=%s RSS=%.1fMB", os.getpid(), stage, rss_mb)
    except Exception:
        pass

def _safe_run_trading_loop(run_func, *args, **kwargs):
    """
    Acquire leader lock, spawn renewer, run the user's loop, release lock and GC.
    """
    if not _acquire_lock():
        logger.info("Not leader (pid=%s). Skipping trading loop in this worker.", os.getpid())
        return

    logger.info("Leader lock acquired by pid %s", os.getpid())
    spawn(_renew_lock_loop)

    try:
        try:
            run_func(*args, **kwargs)
        except Exception as e:
            logger.exception("Top-level trading loop exception: %s", e)
            gevent.sleep(5)
    finally:
        logger.info("Leader exiting, releasing lock (pid %s)", os.getpid())
        try:
            if _redis:
                cur = _redis.get(LOCK_KEY)
                if cur and cur.decode() == str(os.getpid()):
                    _redis.delete(LOCK_KEY)
        except Exception:
            logger.exception("Failed to delete lock on exit: %s", traceback.format_exc())
        gc.collect()
        _log_memory("leader_exit")

# ---------- START: in-memory log & SSE helpers ----------

# Circular buffer for recent log messages and payloads
_LOG_MAX = int(os.environ.get("AUTOTRADE_LOG_BUFFER", 500))
_log_buf = deque(maxlen=_LOG_MAX)
_log_lock = threading.Lock()

def push_payload(name: str, data):
    """
    Send structured payloads (like candle/indicator JSONs) to the buffer.
    `data` must be JSON-serializable (or convertible).
    """
    try:
        ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        payload = {"type": "payload", "ts": ts, "name": name, "data": data}
        with _log_lock:
            _log_buf.append(payload)
    except Exception as e:
        logger.exception("push_payload failed: %s", e)

# ----------------- instrument endpoint (kept single copy) ----------------
@app.route('/api/instruments/latest', methods=['GET'])
def get_latest_instruments():
    file_path = DATA_DIR / LATEST_LINK_FILENAME
    if not file_path.exists():
        update_instrument_file()
        if not file_path.exists():
            return jsonify({"error": "Instrument file is not yet available. Please wait for the daily update process to complete."}), 503
    return send_from_directory(DATA_DIR, LATEST_LINK_FILENAME, mimetype='application/gzip', as_attachment=True, download_name='complete.csv.gz')

# ----------------- Email helper (original) ----------------
def send_email(to_email, subject, body):
    try:
        msg = MIMEMultipart()
        msg['From'] = ADMIN_EMAIL
        msg['To'] = to_email
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(ADMIN_EMAIL, GMAIL_APP_PASSWORD)
        server.sendmail(ADMIN_EMAIL, [to_email, ADMIN_EMAIL], msg.as_string())
        server.quit()
        return True
    except Exception as e:
        logger.exception("Failed to send email: %s", e)
        return False

# ----------------- Broker connect endpoint (kept original) ----------------
@app.route('/api/connect-broker', methods=['POST'])
def connect_broker():
    data = request.get_json()
    brokers_data = data.get('brokers', [])
    responses = []

    for broker_item in brokers_data:
        broker_key = broker_item.get('name')
        creds = broker_item.get('credentials')
        broker_name = broker_map.get(broker_key)

        profile = None
        balance = None
        message = "Broker not supported or credentials missing."
        status = "failed"

        try:
            if broker_name == "Upstox":
                access_token = creds.get('access_token')
                profile = us.upstox_profile(access_token)
                balance = us.upstox_balance(access_token)
                if profile and balance:
                    status = "success"
                    message = "Connected successfully."
                else:
                    message = "Connection failed. Check your access token."

            elif broker_name == "Zerodha":
                api_key = creds.get('api_key')
                access_token = creds.get('access_token')
                profile = zr.zerodha_get_profile(api_key, access_token)
                balance = zr.zerodha_get_equity_balance(api_key, access_token)
                if profile and balance:
                    status = "success"
                    message = "Connected successfully."
                else:
                    message = "Connection failed. Check your API key and access token."

            elif broker_name == "AngelOne":
                api_key = creds.get('api_key')
                user_id = creds.get('user_id')
                pin = creds.get('pin')
                totp_secret = creds.get('totp_secret')
                obj, refresh_token, auth_token, feed_token = ar.angelone_connect(api_key, user_id, pin, totp_secret)
                profile, balance = ar.angelone_fetch_profile_and_balance(obj, refresh_token)
                if profile and balance:
                    status = "success"
                    message = "Connected successfully."
                    broker_sessions[broker_name] = {
                        "obj": obj,
                        "refresh_token": refresh_token,
                        "auth_token": auth_token,
                        "feed_token": feed_token
                    }
                else:
                    message = "Connection failed. Check your credentials."
            elif broker_name == "5paisa":
                app_key = creds.get('app_key')
                access_token = creds.get('access_token')
                client_code = creds.get("client_id")
                profile = {'User Name': client_code}
                balance = fp.fivepaisa_get_balance(app_key, access_token, client_code)
                if profile and balance:
                    status = "success"
                    message = "Connected successfully."
                else:
                    message = "Connection failed. Check your API key and access token."
            elif broker_name == "Groww":
                api_key = creds.get('api_key')
                access_token = creds.get('access_token')
                if api_key and access_token:
                    profile = {"User Name": f"Dummy {broker_name} User"}
                    balance = {"Available Margin": "10000.00"}
                    status = "success"
                    message = "Connected successfully."
                else:
                    message = "Connection failed. Missing API key or access token."
        except Exception as e:
            status = "failed"
            message = f"An error occurred: {str(e)}"

        responses.append({
            "broker": broker_name,
            "broker_key": broker_key,
            "status": status,
            "message": message,
            "profileData": {
                "profile": profile,
                "balance": balance,
                "status": status,
                "message": message
            }
        })
    return jsonify(responses)

# ----------------- Lot size endpoint (original) ----------------
@app.route('/api/get-lot-size', methods=['GET', 'POST'])
def get_lot_size():
    try:
        if request.method == 'POST':
            data = request.get_json()
            symbol_key = data.get('symbol_key')
            symbol_value = data.get('symbol_value')
            type_ = data.get('type')
        else:
            symbol_key = request.args.get('symbol_key')
            symbol_value = request.args.get('symbol_value')
            type_ = request.args.get('type')

        if not symbol_key:
            return jsonify({"error": "Stock symbol is required."}), 400
        if type_ == "EQUITY":
            lot_size, tick_size = ls.lot_size(symbol_key)
        elif type_ == "COMMODITY":
            lot_size, tick_size = ls.commodity_lot_size(symbol_key, symbol_value)
        else:
            lot_size = None
            tick_size = None

        if lot_size:
            return jsonify({"lot_size": lot_size, "tick_size":tick_size, "symbol": symbol_key})
        else:
            return jsonify({"message": "Lot size not found for the given symbol."}), 404
    except Exception as e:
        logger.exception("Error in get_lot_size: %s", e)
        return jsonify({"error": str(e)}), 500

# ----------------- Find positions helper (original) ----------------
def find_positions_for_symbol(broker, symbol, credentials):
    positions = []
    try:
        if broker.lower() == "upstox":
            access_token = credentials.get("access_token")
            positions = us.upstox_fetch_positions(access_token)
        elif broker.lower() == "zerodha":
            api_key = credentials.get("api_key")
            access_token = credentials.get("access_token")
            kite = KiteConnect(api_key)
            kite.set_access_token(access_token)
            positions_data = kite.positions()
            positions = positions_data.get("net", [])
        elif broker.lower() == "angelone":
            api_key = credentials.get("api_key")
            user_id = credentials.get("user_id")
            pin = credentials.get("pin")
            totp_secret = credentials.get("totp_secret")
            session = broker_sessions.get(broker)
            if not session:
                return []
            auth_token = session["auth_token"]
            positions = ar.angeeone_fetch_positions(api_key, auth_token)
        elif broker.lower() == "5paisa":
            app_key = credentials.get('app_key')
            access_token = credentials.get('access_token')
            client_code = credentials.get("client_id")
            positions = fp.fivepaisa_fetch_positions(app_key, access_token, client_code)

        matching = []
        for pos in positions:
            trading_symbol = pos.get("tradingsymbol", "")
            if trading_symbol.startswith(symbol):
                matching.append(pos)
        return matching
    except Exception as e:
        logger.exception("Error fetching positions for %s, %s: %s", broker, symbol, e)
        return []

# ----------------- Misc admin & user endpoints (preserved) ----------------
@app.route("/")
def home():
    return {"status": "Backend is running 🚀"}

@app.route("/api/register", methods=["POST"])
def register():
    data = request.json
    userId = data.get("userId", "").strip()
    username = data.get("username", "").strip()
    email = data.get("email", "").strip().lower()
    mobilenumber = data.get("mobilenumber", "").strip()
    password = data.get("password", "")
    role = data.get("role", "user")

    if not all([userId, username, email, password]):
        return jsonify({"success": False, "message": "All fields are required"}), 400
    if "@" not in email or "." not in email:
        return jsonify({"success": False, "message": "Invalid email address"}), 400
    if len(password) < 6:
        return jsonify({"success": False, "message": "Password must be at least 6 characters"}), 400

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO pending_users (userId, username, email, password, role, mobilenumber)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (userId, username, email, password, role, mobilenumber))
        conn.commit()
    except sqlite3.IntegrityError as e:
        conn.close()
        if "UNIQUE constraint failed" in str(e):
            return jsonify({"success": False, "message": "User ID or Email already exists"}), 200
        return jsonify({"success": False, "message": "Database error"}), 500
    except Exception as e:
        conn.close()
        logger.exception("Database insert error: %s", e)
        return jsonify({"success": False, "message": "Unexpected server error"}), 500

    try:
        send_email(email, "Welcome to AutoTrade", f"Hello {username},\n\nYour registration is received successfully.\nYou’ll get an approval email soon after verification.\n\nThank you for joining AutoTrade!")
    except Exception as e:
        logger.exception("Email sending failed: %s", e)

    conn.close()
    return jsonify({"success": True, "message": "Registration submitted successfully"}), 200

@app.route('/api/send-welcome-email', methods=['POST'])
def send_welcome_email():
    data = request.json
    email = data.get("email")
    first_name = data.get("firstName", "")
    if not email:
        return jsonify({"status": "error", "message": "Email not provided"}), 400
    msg = MIMEMultipart()
    msg['From'] = ADMIN_EMAIL
    msg['To'] = email
    msg['Cc'] = ADMIN_EMAIL
    msg['Subject'] = "Welcome to ASTA VYUHA"
    body = f"Hi {first_name},\n\nWelcome to ASTA VYUHA! Registration may take few hours. After validation you will get registration approved/rejected mail.\n\nRegards,\nASTA VYUHA Team"
    msg.attach(MIMEText(body, 'plain'))
    recipients = [email, ADMIN_EMAIL]
    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(ADMIN_EMAIL, GMAIL_APP_PASSWORD)
        server.sendmail(ADMIN_EMAIL, recipients, msg.as_string())
        server.quit()
        return jsonify({"status": "success", "message": "Welcome email sent!"})
    except Exception as e:
        logger.exception("Email sending failed: %s", e)
        return jsonify({"status": "error", "message": "Email sending failed"}), 500

@app.route('/api/send-support-mail', methods=['POST'])
def send_support_mail():
    data = request.json
    user_email = data.get("email")
    user_name = data.get("name")
    subject = data.get("subject")
    message_body = data.get("message")
    if not all([user_email, user_name, subject, message_body]):
        return jsonify({"status": "error", "message": "All fields are required"}), 400
    msg = MIMEMultipart()
    msg['From'] = ADMIN_EMAIL
    msg['To'] = ADMIN_EMAIL
    msg['Cc'] = user_email
    msg['Subject'] = f"Support Request: {subject}"
    body = f"Support request from {user_name} ({user_email}):\n\n{message_body}"
    msg.attach(MIMEText(body, 'plain'))
    recipients = [ADMIN_EMAIL, user_email]
    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(ADMIN_EMAIL, GMAIL_APP_PASSWORD)
        server.sendmail(ADMIN_EMAIL, recipients, msg.as_string())
        server.quit()
        return jsonify({"status": "success", "message": "Support email sent successfully!"})
    except Exception as e:
        logger.exception("Support email failed: %s", e)
        return jsonify({"status": "error", "message": "Email sending failed"}), 500

def load_otp_store():
    """Load OTP data from file or return empty dict if not exists."""
    if not os.path.exists(OTP_FILE):
        return {}
    with open(OTP_FILE, "r") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            return {}


def save_otp_store(data):
    """Save OTP data to file."""
    with open(OTP_FILE, "w") as f:
        json.dump(data, f, indent=2)


@app.route("/api/send-otp", methods=["POST"])
def send_otp():
    data = request.json
    email = data.get("email")
    user_id = data.get("userId")

    if not email or not user_id:
        return jsonify({"success": False, "message": "Email and userId required"}), 400

    # Generate a 6-digit OTP
    otp = str(random.randint(100000, 999999))
    timestamp = int(time.time())

    # Load OTP data
    otp_store = load_otp_store()

    # Save OTP with timestamp
    otp_store[user_id] = {"otp": otp, "timestamp": timestamp}
    save_otp_store(otp_store)

    logger.info("OTP for %s (%s): %s", user_id, email, otp)

    # Prepare email content
    subject = "Your Password Reset OTP"
    body = (
        f"Dear User,\n\n"
        f"Your OTP for password reset is: {otp}\n\n"
        f"This OTP is valid for 5 minutes.\n\n"
        f"If you didn’t request this, please ignore this email.\n\n"
        f"Best regards,\nYour Security Team"
    )

    # Send email
    email_sent = send_email(email, subject, body)
    if not email_sent:
        return jsonify({"success": False, "message": "Failed to send OTP email"}), 500

    return jsonify({"success": True, "message": "OTP sent successfully"}), 200


@app.route('/api/change-password', methods=['POST'])
def change_password():
    data = request.json
    user_id = request.args.get("userId")
    current_password = data.get("current_password")
    new_password = data.get("new_password")
    otp = data.get("otp")

    if not user_id:
        return jsonify({"success": False, "message": "Missing userId"}), 400

    if not current_password or not new_password or not otp:
        return jsonify({"success": False, "message": "All fields (current_password, new_password, otp) are required"}), 400

    # Fetch user from DB
    row = query_db("SELECT * FROM users WHERE userId = ?", [user_id], one=True)
    if not row:
        return jsonify({"success": False, "message": "User not found"}), 404

    # Load OTP store
    otp_store = load_otp_store()
    record = otp_store.get(user_id)

    # Check if OTP exists for this user
    if not record:
        return jsonify({"success": False, "message": "No OTP found for this user"}), 400

    # Check OTP expiry (5 minutes)
    if int(time.time()) - record["timestamp"] > OTP_EXPIRY:
        del otp_store[user_id]
        save_otp_store(otp_store)
        return jsonify({"success": False, "message": "OTP expired"}), 400

    # Verify OTP match
    if record["otp"] != otp:
        return jsonify({"success": False, "message": "Invalid OTP"}), 400

    # Verify current password
    if row["password"] != current_password:
        return jsonify({"success": False, "message": "Current password is incorrect"}), 400

    # Update password in DB
    query_db("UPDATE users SET password = ? WHERE userId = ?", [new_password, user_id])

    # Remove used OTP
    del otp_store[user_id]
    save_otp_store(otp_store)

    return jsonify({"success": True, "message": "Password changed successfully"}), 200
@app.route('/api/user-reset-password', methods=['POST'])
def user_reset_password():
    data = request.json
    user_id = request.args.get("userId")
    new_password = data.get("new_password")
    otp = data.get("otp")

    if not user_id:
        return jsonify({"success": False, "message": "Missing userId"}), 400

    if not new_password or not otp:
        return jsonify({"success": False, "message": "Both new_password and otp are required"}), 400

    # Fetch user from DB
    row = query_db("SELECT * FROM users WHERE userId = ?", [user_id], one=True)
    if not row:
        return jsonify({"success": False, "message": "User not found"}), 404

    # Load OTP store
    otp_store = load_otp_store()
    record = otp_store.get(user_id)

    # Check if OTP exists for this user
    if not record:
        return jsonify({"success": False, "message": "No OTP found for this user"}), 400

    # Check OTP expiry (5 minutes)
    if int(time.time()) - record["timestamp"] > OTP_EXPIRY:
        del otp_store[user_id]
        save_otp_store(otp_store)
        return jsonify({"success": False, "message": "OTP expired"}), 400

    # Verify OTP match
    if record["otp"] != otp:
        return jsonify({"success": False, "message": "Invalid OTP"}), 400

    # ✅ Update password directly (no current password check)
    query_db("UPDATE users SET password = ? WHERE userId = ?", [new_password, user_id])

    # Remove used OTP
    del otp_store[user_id]
    save_otp_store(otp_store)

    return jsonify({"success": True, "message": "Password reset successfully"}), 200

@app.route("/api/users", methods=["GET"])
def get_admin_users():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT userId, username, email, role, mobilenumber FROM users")
    users = [dict(zip(["userId","username", "email", "role", "mobilenumber"], row)) for row in cursor.fetchall()]
    cursor.execute("SELECT userId, username, email, role, mobilenumber FROM rejected_users")
    rejected = [dict(zip(["userId","username", "email", "role", "mobilenumber"], row)) for row in cursor.fetchall()]
    cursor.execute("SELECT userId, username, email, role, mobilenumber FROM pending_users")
    pending = [dict(zip(["userId","username", "email", "role", "mobilenumber"], row)) for row in cursor.fetchall()]
    conn.close()
    return jsonify({"users": users, "rejected": rejected, "pending": pending}), 200

@app.route("/api/admin/approve/<userId>", methods=["POST"])
def approve_user(userId):

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM pending_users WHERE userId = ?", (userId,))
    user = cursor.fetchone()
    if not user:
        conn.close()
        return jsonify({"success": False, "message": "User not found"}), 200
    cursor.execute("""
            INSERT INTO users ( userId, username, email, password, role, mobilenumber)
            VALUES (?, ?, ?, ?, ?,?)
        """, (user[1], user[2], user[3], user[4], user[5], user[6]))
    cursor.execute("DELETE FROM pending_users WHERE userId = ?", (userId,))
    conn.commit()
    conn.close()
    try:
        send_email(user[2], "Registration Approved", f"Hello {user[1]},\n\nYour registration has been approved. You can now log in and start using the system.\n\nRegards,\nAdmin Team")
    except Exception as e:
        logger.exception("Approval email failed: %s", e)
        return jsonify({"success": True, "message": f"User approved but email failed: {str(e)}"}), 500
    return jsonify({"success": True, "message": "User approved and email sent"}), 200

@app.route("/api/admin/reject/<userId>", methods=["POST"])
def reject_user(userId):

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM pending_users WHERE userId = ?", (userId,))
    user = cursor.fetchone()
    if not user:
        conn.close()
        return jsonify({"success": False, "message": "User not found"}), 200
    cursor.execute("""
            INSERT INTO rejected_users (userId, username, email, password, role, mobilenumber)
            VALUES (?,?, ?, ?, ?, ?)
        """, (user[1], user[2], user[3], user[4], user[5], user[6]))
    cursor.execute("DELETE FROM pending_users WHERE userId = ?", (userId,))
    conn.commit()
    conn.close()
    try:
        send_email(user[2], "Registration Rejected", f"Hello {user[1]},\n\nWe regret to inform you that your registration has been rejected.\n\nRegards,\nAdmin Team")
    except Exception as e:
        logger.exception("Rejection email failed: %s", e)
        return jsonify({"success": True, "message": f"User rejected but email failed: {str(e)}"}), 500
    return jsonify({"success": True, "message": "User rejected and email sent"}), 200

@app.route("/api/admin/reset-password/<userId>", methods=["POST"])
def reset_password(userId):
    from password_utils import generate_random_password
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT email FROM users WHERE userId = ?", (userId,))
    row = c.fetchone()
    if not row:
        conn.close()
        return jsonify({"success": False, "message": "User not found"}), 404
    user_email = row[0]
    new_password = generate_random_password()
    c.execute("UPDATE users SET password = ? WHERE userId = ?", (new_password, userId))
    conn.commit()
    conn.close()
    try:
        send_email(user_email, "Your password has been reset", f"New password: {new_password}")
    except Exception as e:
        logger.exception("Reset email failed: %s", e)
        return jsonify({"success": False, "message": f"Password reset but email failed: {str(e)}"}), 500
    return jsonify({"success": True, "message": "Password reset and email sent"})

@app.route("/api/admin/delete-user/<userId>", methods=["POST", "DELETE"])
def delete_user(userId):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM users WHERE userId = ?", (userId,))
    conn.commit()
    conn.close()
    return jsonify({"success": True, "message": "User deleted"}), 200

@app.route("/api/admin/delete-rejected/<userId>", methods=["POST", "DELETE"])
def delete_rejected_user(userId):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM rejected_users WHERE userId = ?", (userId,))
    conn.commit()
    conn.close()
    return jsonify({"success": True, "message": "Rejected user deleted"}), 200

# ----------------- stream logs endpoint (single copy) ----------------
@app.route("/api/stream-logs")
def stream_logs():
    def event_stream():
        seen = set()
        # loop forever, sending new unique messages from either buffer
        while True:
            try:
                # copy local buffer safely
                try:
                    with _log_lock:
                        local_items = list(_log_buf)
                except Exception:
                    local_items = []
                # try to get external buffer from logger_util (if available)
                try:
                    from logger_util import get_log_buffer
                    external_items = get_log_buffer() or []
                except Exception:
                    external_items = []

                # merge (local first so recent app messages show up), then stream unseen items
                merged = local_items + external_items

                for it in merged:
                    # create a stable dedupe key (timestamp + message text)
                    key = (it.get("ts"), str(it.get("message")))
                    if key in seen:
                        continue
                    seen.add(key)

                    event_name = it.get("type", "log")
                    try:
                        data_str = json.dumps(it, default=str)
                    except Exception:
                        data_str = json.dumps({"type": "log", "ts": it.get("ts"), "message": str(it.get("message"))})
                    yield f"event: {event_name}\ndata: {data_str}\n\n"

                # sleep briefly, then loop
                gevent.sleep(0.5)

                # If buffers shrank (rotation/trim), prune seen to last N items to avoid memory growth
                try:
                    combined_len = len(local_items) + len(external_items)
                    keep = max(500, combined_len)  # keep recent keys up to this many
                    tail = (local_items + external_items)[-keep:]
                    new_seen = set((it.get("ts"), str(it.get("message"))) for it in tail if it)
                    seen = new_seen
                except Exception:
                    # ignore pruning errors and continue
                    pass

            except GeneratorExit:
                # client disconnected
                break
            except Exception:
                # on unexpected exceptions, wait a bit and continue streaming
                try:
                    push_log("Stream-logs encountered an error; continuing.", "error")
                except Exception:
                    pass
                gevent.sleep(1)
        # normal generator end
    return Response(event_stream(), mimetype="text/event-stream")

@app.route('/api/get_profit_loss', methods=['POST'])
def get_profit_loss():
    data = request.get_json()
    access_token = data.get("access_token")
    segment = data.get("segment")
    from_date = data.get("from_date")
    to_date = data.get("to_date")
    year = data.get("year")

    # ✅ Convert financial year like "2025-2026" → "2526"
    fy_code = None
    if year and "-" in year:
        parts = year.split("-")
        fy_code = parts[0][-2:] + parts[1][-2:]
    elif year and len(year) == 9 and year[:4].isdigit():
        fy_code = year[2:4] + year[7:9]
    else:
        fy_code = year  # fallback if already short form

    if not all([access_token, segment, from_date, to_date, fy_code]):
        return jsonify({"success": False, "message": "Missing required parameters"}), 400

    result, charges = us.upstox_profit_loss(access_token, segment, from_date, to_date, fy_code)

    return jsonify({"success": True, "data": result, "rows": charges}), 200

# === TRADING LOOP FOR ALL STOCKS ===
def run_trading_logic_for_all(trading_parameters, selected_brokers, logger):
    print(trading_parameters)
    for stock in trading_parameters:
        active_trades[stock['symbol_value']] = True

    push_log("✅ Trading loop started for all selected stocks")

    push_log("⏳ Starting new trading cycle setup...")

    # STEP 1: Fetch instrument keys once at the beginning
    for stock in trading_parameters:
        if not active_trades.get(stock['symbol_value']):
            continue

        broker_key = stock.get('broker')
        broker_name = broker_map.get(broker_key)
        symbol = stock.get('symbol_value')
        name = stock.get('symbol_key')
        strategy = stock.get('strategy')
        company = reverse_stock_map.get(symbol, " ")
        interval = stock.get('interval')
        exchange_type = stock.get('type')

        msg = f"🔑 Fetching instrument key for {company} ({symbol}) via {broker_name}..."
        push_log(msg)

        instrument_key = None
        try:
            if exchange_type == "EQUITY":
                if broker_name.lower() == "upstox":
                    instrument_key = us.upstox_equity_instrument_key(company)
                elif broker_name.lower() == "zerodha":
                    broker_info = next((b for b in selected_brokers if b['name'] == broker_key), None)
                    if broker_info:
                        api_key = broker_info['credentials'].get("api_key")
                        access_token = broker_info['credentials'].get("access_token")
                        instrument_key = zr.zerodha_instruments_token(api_key, access_token, symbol)
                elif broker_name.lower() == "angelone":
                    instrument_key = ar.angelone_get_token_by_name(symbol)
                elif broker_name.lower() == "5paisa":
                    instrument_key = fp.fivepaisa_scripcode_fetch(symbol)
            elif exchange_type == "COMMODITY" and broker_name.lower() == "upstox":
                matched = us.upstox_commodity_instrument_key(name, symbol)
                instrument_key = matched['instrument_key'].iloc[0]

            if instrument_key:
                stock['instrument_key'] = instrument_key
                msg = f"✅ Found instrument key {instrument_key} for {symbol}"
            else:
                msg = f"⚠️ No instrument key found for {symbol}, skipping this stock."
                active_trades[stock['symbol_value']] = False
            push_log(msg)

        except Exception as e:
            msg = f"❌ Error fetching instrument key for {symbol}: {e}"
            push_log(msg, "error")
            active_trades[stock['symbol_value']] = False

    interval = trading_parameters[0].get("interval", "1minute")
    now_interval, next_interval = nni.round_to_next_interval(interval)
    msg = f"Present Interval Start : {now_interval}, Next Interval Start :{next_interval}"
    push_log(msg)

    # loop until all stocks disconnected
    while any(active_trades.values()):
        for stock in trading_parameters:
            symbol = stock.get('symbol_value')
            if symbol not in active_trades:  # <-- skip if removed
                print(f"Stock: {symbol} Disconnected - removing from trading_parameters")
                trading_parameters.remove(stock)
                continue
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if now >= next_interval:
            now_interval, next_interval = nni.round_to_next_interval(interval)
            print(active_trades)
            interval = trading_parameters[0].get("interval", "1minute")
            now_interval, next_interval = nni.round_to_next_interval(interval)

            # STEP 2: Fetch data + indicators
            for stock in trading_parameters:
                symbol = stock.get('symbol_value')
                if symbol not in active_trades:  # <-- skip if removed
                    continue
                broker_key = stock.get('broker')
                broker_name = broker_map.get(broker_key)
                company = stock.get('symbol_key')
                interval = stock.get('interval')
                instrument_key = stock.get('instrument_key')
                strategy = stock.get('strategy')
                exchange_type = stock.get('type')
                tick_size = stock.get('tick_size')

                msg = f"🕯 Fetching candles for {symbol}-{company} from {broker_name}"
                push_log(msg)

                combined_df = None
                try:
                    if broker_name.lower() == "upstox":
                        access_token = next(
                            (b['credentials']['access_token'] for b in selected_brokers if b['name'] == broker_key),
                            None
                            )
                        if access_token:
                            hdf = us.upstox_fetch_historical_data_with_retry(access_token, instrument_key, interval)
                            idf = us.upstox_fetch_intraday_data(access_token, instrument_key, interval)
                            if hdf is not None and idf is not None:
                                combined_df = cdf.combinding_dataframes(hdf, idf)

                    elif broker_name.lower() == "zerodha":
                        broker_info = next((b for b in selected_brokers if b['name'] == broker_key), None)
                        if broker_info:
                            kite = KiteConnect(broker_info['credentials'].get("api_key"))
                            kite.set_access_token(broker_info['credentials'].get("access_token"))
                            if interval == "1":
                                interval = ""
                            hdf = zr.zerodha_historical_data(kite, instrument_key, interval)
                            idf = zr.zerodha_intraday_data(kite, instrument_key, interval)
                            if hdf is not None and idf is not None:
                                combined_df = cdf.combinding_dataframes(hdf, idf)
                    elif broker_name.lower() == "angelone":
                        broker_info = next((b for b in selected_brokers if b['name'] == broker_key), None)
                        if broker_info:
                            api_key = broker_info['credentials'].get("api_key")
                            user_id = broker_info['credentials'].get("user_id")
                            pin = broker_info['credentials'].get("pin")
                            totp_secret = broker_info['credentials'].get("totp_secret")
                            session = broker_sessions.get(broker_name)
                            if not session:
                                return jsonify({"status": "failed", "message": "Broker not connected."})
                            obj = session["obj"]
                            auth_token = session["auth_token"]
                            interval = ar.number_to_interval(interval)
                            combined_df = ar.angelone_get_historical_data(api_key, auth_token, obj, "NSE",
                                                                          instrument_key, interval)
                    elif broker_name.lower() == "5paisa":
                        broker_info = next((b for b in selected_brokers if b['name'] == broker_key), None)
                        if broker_info:
                            app_key = broker_info['credentials'].get("app_key")
                            access_token = broker_info['credentials'].get("access_token")
                            combined_df = fp.fivepaisa_historical_data_fetch(access_token, instrument_key, interval, 25)

                except Exception as e:
                    msg = f"❌ Error fetching data for {symbol}: {e}"
                    push_log(msg,"error")

                if combined_df is None or combined_df.empty:
                    msg = f"❌ No data for {symbol}, skipping."
                    push_log(msg, "error")
                    continue

                msg = f"✅ Data ready for {symbol}"
                push_log(msg)
                gevent.sleep(0.5)
                indicators_df = ind.all_indicators(combined_df,strategy)
                row = indicators_df.tail(1).iloc[0]
                cols = indicators_df.columns.tolist()
                col_widths = [max(len(str(c)), len(str(row[c]))) + 2 for c in cols]

                def line():
                    return "+" + "+".join(["-" * w for w in col_widths]) + "+"

                header = "|" + "|".join([f"{c:^{w}}" for c, w in zip(cols, col_widths)]) + "|"
                values = "|" + "|".join([f"{str(row[c]):^{w}}" for c, w in zip(cols, col_widths)]) + "|"
                # --- log it line by line ---
                push_log(line())
                push_log(header)
                push_log(line())
                push_log(values)
                push_log(line())
                #push_log(tabulate(indicators_df.tail(1), headers="keys", tablefmt="pretty", showindex=False))

                # STEP 3: Check trade conditions
                msg = f"📊 Checking trade conditions for {symbol}"
                push_log(msg)
                lots = stock.get("lots")
                target_pct = stock.get("target_percentage")
                name = stock.get("symbol_value")

                try:
                    creds = next((b["credentials"] for b in selected_brokers if b["name"] == broker_key), None)
                    if broker_name.lower() == "upstox":
                        us.upstox_trade_conditions_check(lots, target_pct, indicators_df.tail(1), creds, company,
                                                         symbol, exchange_type, strategy)
                    elif broker_name.lower() == "zerodha":
                        zr.zerodha_trade_conditions_check(lots, target_pct, indicators_df.tail(1), creds, symbol,
                                                          strategy)
                    elif broker_name.lower() == "angelone":
                        session = broker_sessions.get(broker_name)
                        if not session:
                            return jsonify({"status": "failed", "message": "Broker not connected."})
                        obj = session["obj"]
                        auth_token = session["auth_token"]
                        interval = ar.number_to_interval(interval)
                        ar.angelone_trade_conditions_check(obj, auth_token, lots, target_pct, indicators_df, creds,
                                                           name, strategy)
                    elif broker_name.lower() == "5paisa":
                        fp.fivepaisa_trade_conditions_check(lots, target_pct, indicators_df, creds, stock, strategy)

                except Exception as e:
                    msg = f"❌ Error running strategy for {symbol}: {e}"
                    push_log(msg)

                # cleanup per symbol
                try:
                    del combined_df
                    del indicators_df
                except Exception:
                    pass
                gc.collect()
                gevent.sleep(0)

            push_log("✅ Trading cycle complete")

            msg = f"Present Interval Start : {now_interval}, Next Interval Start :{next_interval}"
            push_log(msg)
            push_log("Waiting for next interval beginning .....")
            gevent.sleep(1)

    push_log("All active trades ended. Exiting trading loop.")
    gc.collect()


# ----------------- START ALL TRADING endpoint (modified to safe spawn) ----------------
@app.route('/api/start-all-trading', methods=['POST'])
def start_all_trading():
    data = request.get_json()
    trading_parameters = data.get("tradingParameters", [])
    selected_brokers = data.get("selectedBrokers", [])
    print(trading_parameters)

    # Use the leader-wrapper so only one worker runs the heavy loop
    spawn(_safe_run_trading_loop, run_trading_logic_for_all, trading_parameters, selected_brokers, logger)

    return jsonify({"logs": ["🟢 Started trading for all stocks together. \n Waiting for the next interval....."]}), 202

# ----------------- Close position endpoints (original) ----------------
@app.route("/api/close-position", methods=["POST"])
def close_position():
    data = request.json
    symbol = data.get("symbol_value")
    broker = data.get("broker")
    credentials = data.get("credentials")
    session = broker_sessions.get("AngelOne")
    if not session:
        return jsonify({"status": "failed", "message": "Broker not connected."})
    obj = session["obj"]
    closed = []
    matches = find_positions_for_symbol(broker, symbol, credentials)
    for pos in matches:
        order_id = None
        if broker.lower() == "upstox":
            order_id = us.upstox_close_position(credentials, pos)
        elif broker.lower() == "zerodha":
            order_id = zr.zerodha_close_position(credentials, pos)
        elif broker.lower() == "angelone":
            order_id = ar.angelone_close_position(obj, pos)
        elif broker.lower() == "groww":
            order_id = gr.groww_close_position(credentials, pos)
        elif broker.lower() == "5paisa":
            order_id = fp.fivepaisa_close_position(credentials, pos)
        if order_id:
            closed.append({"symbol": symbol, "broker": broker, "order_id": order_id})
    if closed:
        return jsonify({"message": f"✅ Closed position for {symbol}", "closed": closed})
    else:
        return jsonify({"message": f"⚠️ No open position found for {symbol}"}), 404

@app.route("/api/close-all-positions", methods=["POST"])
def close_all_positions():
    data = request.json
    trading_parameters = data.get("tradingParameters", [])
    selected_brokers = data.get("selectedBrokers", [])
    closed = []
    for stock in trading_parameters:
        symbol = stock.get("symbol_value")
        broker_key = stock.get("broker")
        broker_name = broker_map.get(broker_key)
        credentials = next((b["credentials"] for b in selected_brokers if b["name"] == broker_key), None)
        if not broker_name or not credentials:
            continue
        matches = find_positions_for_symbol(broker_name, symbol, credentials)
        for pos in matches:
            order_id = None
            if broker_name.lower() == "upstox":
                order_id = us.upstox_close_position(credentials, pos)
            elif broker_name.lower() == "zerodha":
                order_id = zr.zerodha_close_position(credentials, pos)
            elif broker_name.lower() == "angelone":
                session = broker_sessions.get("AngelOne")
                if session:
                    obj = session["obj"]
                    order_id = ar.angelone_close_position(obj, pos)
            elif broker_name.lower() == "groww":
                order_id = gr.groww_close_position(credentials, pos)
            elif broker_name.lower() == "5paisa":
                order_id = fp.fivepaisa_close_position(credentials, pos)
            if order_id:
                closed.append({"symbol": symbol, "broker": broker_name, "order_id": order_id})
    return jsonify({"message": "✅ Closed all positions successfully", "closed": closed})

# ----------------- Disconnect stock endpoint (original) ----------------
@app.route("/api/disconnect-stock", methods=["POST"])
def disconnect_stock():
    data = request.json
    symbol = data.get("symbol_value")
    if symbol in active_trades:
        active_trades.pop(symbol, None)
        return jsonify({"message": f"❌ {symbol} Disconnection happens after the current trade cycle(Interval). "})
    return jsonify({"message": "⚠️ Stock not active"})

# ----------------- Login / logout / active users endpoints (original) ----------------
@app.route("/api/login", methods=["POST"])
def login():
    data = request.json
    user_id = data.get("userId")
    password = data.get("password")
    role = data.get("role")
    if role == "client":
        role = "user"
    if not user_id or not password:
        return jsonify({"success": False, "message": "UserId and password required"}), 400
    user = query_db("SELECT * FROM users WHERE userId=? AND role=?", [user_id, role], one=True)
    if not user or user["password"] != password:
        return jsonify({"success": False, "message": "Invalid credentials"}), 401
    logged_users = load_logged_in_users()
    logged_users[user_id] = {
        "userid": user["userId"],
        "username": user["username"],
        "email": user["email"],
        "role": user["role"],
        "mobilenumber": user["mobilenumber"],
        "login_time": time.time()
    }
    save_logged_in_users(logged_users)
    token = str(random.randint(100000, 999999))
    return jsonify({"success": True, "token": token, "profile": logged_users[user_id]})

@app.route("/api/logout", methods=["POST"])
def logout():
    data = request.json
    user_id = data.get("userId")
    if not user_id:
        return jsonify({"success": False, "message": "userId required"}), 400
    logged_users = load_logged_in_users()
    if user_id in logged_users:
        logged_users.pop(user_id)
        save_logged_in_users(logged_users)
    return jsonify({"success": True, "message": "Logged out successfully"})

@app.route("/api/active-users", methods=["GET"])
def active_users():
    logged_users = load_logged_in_users()
    return jsonify({"count": len(logged_users), "users": list(logged_users.values())})

@app.route('/api/logged-in-users/<userid>', methods=['POST'])
def get_logged_in_users(userid):
    try:
        with open("logged-in_users.json", "r") as f:
            data = json.load(f)
        user = data.get(userid)
        if user:
            return jsonify(user), 200
        return jsonify({"error": "User not found"}), 404
    except Exception as e:
        logger.exception("Get logged-in users failed: %s", e)
        return jsonify({"error": str(e)}), 500

# ----------------- Admin delete user endpoints preserved ----------------
# (delete_user, delete_rejected_user defined earlier)

# ----------------- Final block: run dev server when invoked directly ----------------
if __name__ == '__main__':
    # initialize database and default admin as in original
    init_db()
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users WHERE username='admin'")
    if not cursor.fetchone():
        cursor.execute("""
                INSERT INTO users (userId, role, username, email, password, mobilenumber)
                VALUES ('admin','admin', 'admin', 'vijayaranikraja@gmail.com', 'Admin@123', '6303317143')
            """)
        conn.commit()
    conn.close()
    # update instruments (original)
    try:
        update_instrument_file()
    except Exception:
        logger.exception("update_instrument_file failed at startup: %s", traceback.format_exc())

    port = int(os.environ.get('PORT', 5000))
    debug_flag = os.environ.get('FLASK_DEBUG', 'false').lower() == 'true'
    app.run(host='0.0.0.0', port=port, debug=debug_flag, use_reloader=False)
