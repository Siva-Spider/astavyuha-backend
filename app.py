from gevent import monkey
monkey.patch_all()

import os
import json
import time
import traceback
import gc
import datetime
import random
import sqlite3
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from flask import Flask, request, jsonify, Response, send_from_directory
from flask_cors import CORS
from zoneinfo import ZoneInfo

# ====== Broker libs and project modules (keep as in your original) ======
from logger_util import logger, push_log, push_payload, get_log_buffer
import get_lot_size as ls
from upstox_instrument_manager import LATEST_LINK_FILENAME, DATA_DIR, update_instrument_file
import Next_Now_intervals as nni
import combinding_dataframes as cdf
import indicators as ind
from kiteconnect import KiteConnect
import threading
from collections import deque

# =====================================================================

app = Flask(__name__)
CORS(app)

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
        msg = "Failed to send email: %s", e
        push_log(msg, "error")
        return False

# ----------------- Broker connect endpoint (kept original) ----------------
@app.route('/api/connect-broker', methods=['POST'])
def connect_broker():
    import Upstox as us
    import Zerodha as zr
    import Groww as gr
    import Fivepaisa as fp

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
                import AngelOne as ar
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
        msg = "Error in get_lot_size: %s", e
        push_log(msg, "error")
        return jsonify({"error": str(e)}), 500

# ----------------- Find positions helper (original) ----------------
def find_positions_for_symbol(broker, symbol, credentials):
    import Upstox as us
    import Zerodha as zr
    import AngelOne as ar
    import Groww as gr
    import Fivepaisa as fp

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
            positions = ar.angeleone_fetch_positions(api_key, auth_token)
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
        msg = "Error fetching positions for %s, %s: %s", broker, symbol, e
        push_log(msg, "error")
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
        msg = "Database insert error: %s", e
        push_log(msg, "error")
        return jsonify({"success": False, "message": "Unexpected server error"}), 500

    try:
        send_email(email, "Welcome to AutoTrade", f"Hello {username},\n\nYour registration is received successfully.\nYou’ll get an approval email soon after verification.\n\nThank you for joining AutoTrade!")
    except Exception as e:
        msg = "Email sending failed: %s", e
        push_log(msg, "warning")

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
        msg = "Email sending failed: %s", e
        push_log(msg, "warning")
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
        msg = "Support email failed: %s", e
        push_log(msg, "warning")
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

    msg = "OTP for %s (%s): %s", user_id, email, otp
    push_log(msg)

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
        msg = "Approval email failed: %s", e
        push_log(msg, "warning")
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
        msg = "Rejection email failed: %s", e
        push_log(msg, "warning")
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
        msg = "Reset email failed: %s", e
        push_log(msg, 'warning')
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
    """
    Safe non-blocking Server-Sent Events stream.
    Uses a background thread instead of blocking Gunicorn's worker.
    Compatible with --worker-class gevent or sync workers.
    """
    from queue import Queue, Empty

    # Shared log queue for async push
    log_queue = Queue()

    def producer():
        """Background thread that periodically fetches logs and puts them into the queue."""
        seen = set()
        while True:
            try:
                from logger_util import get_log_buffer
                items = get_log_buffer() or []
                for it in items:
                    key = (it.get("ts"), str(it.get("message")))
                    if key not in seen:
                        seen.add(key)
                        log_queue.put(it)
                # Sleep briefly without blocking Gunicorn
                time.sleep(0.5)
            except Exception:
                time.sleep(1)

    # Start producer thread (daemon so it stops with request)
    threading.Thread(target=producer, daemon=True).start()

    def event_stream():
        """Generator yielding events from the queue."""
        while True:
            try:
                item = log_queue.get(timeout=5)
                data_str = json.dumps(item, default=str)
                yield f"event: {item.get('type', 'log')}\ndata: {data_str}\n\n"
            except Empty:
                # Keep connection alive
                yield ": keep-alive\n\n"
            except GeneratorExit:
                break
            except Exception:
                time.sleep(1)

    return Response(event_stream(), mimetype="text/event-stream")


@app.route('/api/get_profit_loss', methods=['POST'])
def get_profit_loss():
    import Upstox as us

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
    import Upstox as us
    import Zerodha as zr
    import AngelOne as ar
    import Groww as gr
    import Fivepaisa as fp

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

        push_log(symbol)
        push_log(name)
        push_log(company)

        msg = f"🔑 Fetching instrument key for {company} ({symbol}) via {broker_name}..."
        push_log(msg)

        instrument_key = None
        try:
            if exchange_type == "EQUITY":
                retries = 3
                for attempt in range(retries):
                    push_log(company)
                    try:
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
                    except Exception as e:
                        push_log(f"Attempt {attempt+1}/{retries} failed fetching Upstox instrument key: {e}", "error")
                        time.sleep(1)
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
        now = datetime.datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d %H:%M:%S")

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
                time.sleep(0.5)
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
                push_log(values+"\n")
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
                time.sleep(0)

            push_log("✅ Trading cycle complete")

            msg = f"Present Interval Start : {now_interval}, Next Interval Start :{next_interval}"
            push_log(msg)
            push_log("Waiting for next interval beginning .....")
            time.sleep(1)

    push_log("All active trades ended. Exiting trading loop.")
    gc.collect()


# ----------------- START ALL TRADING endpoint (modified to safe spawn) ----------------
@app.route('/api/start-all-trading', methods=['POST'])
def start_all_trading():
    data = request.get_json()
    trading_parameters = data.get("tradingParameters", [])
    selected_brokers = data.get("selectedBrokers", [])

    # Run in background thread
    thread = threading.Thread(
        target=run_trading_logic_for_all,
        args=(trading_parameters, selected_brokers, logger),
        daemon=True
    )
    thread.start()

    return jsonify({"logs": ["🟢 Started trading for all stocks together"]})


# ----------------- Close position endpoints (original) ----------------
@app.route("/api/close-position", methods=["POST"])
def close_position():
    import Upstox as us
    import Zerodha as zr
    import AngelOne as ar
    import Groww as gr
    import Fivepaisa as fp

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
    import Upstox as us
    import Zerodha as zr
    import AngelOne as ar
    import Groww as gr
    import Fivepaisa as fp

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
        msg = "Get logged-in users failed: %s", e
        push_log(msg, "warning")
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
        msg = "update_instrument_file failed at startup: %s", traceback.format_exc()
        push_log(msg, "error")

    port = int(os.environ.get('PORT', 5000))
    debug_flag = os.environ.get('FLASK_DEBUG', 'false').lower() == 'true'
    app.run(host='0.0.0.0', port=port, debug=debug_flag, use_reloader=False)
