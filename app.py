import gevent
from gevent import monkey, spawn
monkey.patch_all()

from flask import Flask, request, jsonify, Response,  send_from_directory
import threading
from flask_cors import CORS
import Upstox as us
import Zerodha as zr
import AngelOne as ar
import Groww as gr
import Fivepaisa as fp
from logger_module import logger

import os
import get_lot_size as ls
from upstox_instrument_manager import LATEST_LINK_FILENAME, DATA_DIR, update_instrument_file
import Next_Now_intervals as nni
import combinding_dataframes as cdf
import indicators as ind
import datetime
import time
import json
from tabulate import tabulate
from kiteconnect import KiteConnect
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import sqlite3
import random
import string


app = Flask(__name__)
CORS(app, 
     resources={r"/*": {"origins": "https://siva-spider-autotrade.netlify.app"}},
     supports_credentials=True,
     allow_headers=["Content-Type", "Authorization"],
     methods=["GET", "POST", "OPTIONS", "PUT", "DELETE"])

@app.route("/")
def home():
    return {"status": "Backend is running 🚀"}

broker_map = {
    "u": "Upstox",
    "z": "Zerodha",
    "a": "AngelOne",
    "g": "Groww",
    "5": "5paisa"
}

# --- Stock Map ---
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
    "INDUSIND BANK LIMITED": "INDUSINDBK",
    "Nifty 50": "NIFTY",
    "Nifty Bank": "BANKNIFTY",
    "Nifty Fin Service": "FINNIFTY",
    "NIFTY MID SELECT": "MIDCPNIFTY",
}


# Reverse map to get full company name from symbol
reverse_stock_map = {v: k for k, v in stock_map.items()}
# In-memory storage for logs + active trade status
trade_logs = []
active_trades = {}   # { "NIFTY": True, "RELIANCE": False }
broker_sessions = {}
otp_store = {}
LOGGED_IN_JSON = "logged_in_users.json"

DB_FILE = "user_data_new.db"

def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # Approved users
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            userId TEXT UNIQUE,             -- 🔑 Unique login ID
            username TEXT,                  -- Full name
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

ADMIN_EMAIL = "vijayaranikraja@gmail.com"
GMAIL_APP_PASSWORD = "lymcvspuyltyhosj"

def log_stream():
    yield "data: 🟢 Trading started...\n\n"
    for i in range(1, 11):
        yield f"data: 🔔 Trade signal {i} at {time.strftime('%H:%M:%S')}\n\n"
        gevent.sleep(2)
    yield "data: ✅ Trading finished.\n\n"


@app.route("/api/stream-logs")
def stream_logs():
    def event_stream():
        last_index = 0
        while True:
            if logger.logs:
                # send any new logs
                new_logs = logger.logs[last_index:]
                for log in new_logs:
                    yield f"data: {log}\n\n"
                last_index += len(new_logs)
            gevent.sleep(1)  # avoid tight loop

    return Response(event_stream(), mimetype="text/event-stream")


@app.route('/api/instruments/latest', methods=['GET'])
def get_latest_instruments():

    # Variables are now globally imported from instrument_manager
    """
    Serves the latest_instruments.csv.gz file.

    send_from_directory correctly takes the directory path (DATA_DIR)
    and the filename (LATEST_LINK_FILENAME) separately, which prevents
    common pathing issues and IDE warnings related to single-argument usage.
    """

    file_path = DATA_DIR / LATEST_LINK_FILENAME

    # Ensure the file exists before attempting to serve it
    if not file_path.exists():
        # Attempt to run the update function if the file is missing
        update_instrument_file()

        # Check again after the attempt
        if not file_path.exists():
            # 503 Service Unavailable: file exists but is not ready
            # Using jsonify as requested for the JSON error response
            return jsonify({
                               "error": "Instrument file is not yet available. Please wait for the daily update process to complete."}), 503

    # Use send_from_directory to safely serve static content from a specified directory
    return send_from_directory(
        DATA_DIR,  # The directory where the file resides
        LATEST_LINK_FILENAME,  # The name of the file
        mimetype='application/gzip',
        as_attachment=True,
        download_name='complete.csv.gz'  # Suggest the original filename to the browser for correct handling
    )

def load_logged_in_users():
    if not os.path.exists(LOGGED_IN_JSON):
        return {}
    with open(LOGGED_IN_JSON, "r") as f:
        return json.load(f)

# ✅ Save logged-in users to JSON
def save_logged_in_users(users):
    with open(LOGGED_IN_JSON, "w") as f:
        json.dump(users, f)

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

    user = query_db(
        "SELECT * FROM users WHERE userId=? AND role=?", [user_id, role], one=True
    )

    if not user or user["password"] != password:
        return jsonify({"success": False, "message": "Invalid credentials"}), 401

    # ✅ Store full profile in JSON
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
    print(logged_users[user_id])
    # Return token (optional, for frontend session management)
    token = str(random.randint(100000, 999999))
    return jsonify({"success": True, "token": token, "profile": logged_users[user_id]})

# ---------------- Logout ----------------
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

# ---------------- Get live logged-in users count ----------------
@app.route("/api/active-users", methods=["GET"])
def active_users():
    logged_users = load_logged_in_users()
    return jsonify({"count": len(logged_users), "users": list(logged_users.values())})

# ✅ Helper function to query database
def query_db(query, args=(), one=False):
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(query, args)
    rv = cur.fetchall()
    conn.commit()
    conn.close()
    return (rv[0] if rv else None) if one else rv

# ✅ Get profile
@app.route('/api/profile', methods=['POST'])
def get_profile():
    data = request.get_json()
    user_id = data.get('userId')
    if not user_id:
        return jsonify({'success': False, 'message': 'User ID missing'}), 400

    conn = sqlite3.connect('user_data_new.db')
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
            SELECT userId, username, email, role, mobilenumber
            FROM users
            WHERE userId = ?
        """, (user_id,))
    row = cur.fetchone()
    conn.close()

    if row:
        user_data = dict(row)
        return jsonify({'success': True, **user_data})
    else:
        return jsonify({'success': False, 'message': 'User not found'}), 404

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
        return jsonify({"error": str(e)}), 500

# ✅ Send OTP
@app.route('/api/send-otp', methods=['POST'])
def send_otp():
    data = request.json
    email = data.get("email")
    if not email:
        return jsonify({"message": "Email required"}), 400

    otp = str(random.randint(100000, 999999))
    otp_store[email] = otp
    print(f"OTP for {email}: {otp}")  # TODO: Send via email/SMS
    return jsonify({"success": True, "message": "OTP sent"})

# ✅ Change password
@app.route('/api/change-password', methods=['POST'])
def change_password():
    data = request.json
    user_id = request.args.get("userId")  # Or get from session/JWT
    current_password = data.get("current_password")
    new_password = data.get("new_password")
    otp = data.get("otp")

    if not user_id:
        return jsonify({"message": "Missing userId"}), 400

    row = query_db("SELECT * FROM users WHERE userId = ?", [user_id], one=True)
    if not row:
        return jsonify({"message": "User not found"}), 404

    if otp_store.get(row["email"]) != otp:
        return jsonify({"message": "Invalid OTP"}), 400

    if row["password"] != current_password:
        return jsonify({"message": "Current password is incorrect"}), 400

    # Update password
    query_db("UPDATE users SET password = ? WHERE userId = ?", [new_password, user_id])
    otp_store.pop(row["email"], None)

    return jsonify({"success": True, "message": "Password changed successfully"})

# 1. Fetch all users
@app.route("/api/users", methods=["GET"])
def get_admin_users():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    # Users
    cursor.execute("SELECT userId, username, email, role, mobilenumber FROM users")
    users = [dict(zip(["userId","username", "email", "role", "mobilenumber"], row)) for row in cursor.fetchall()]

    # Rejected
    cursor.execute("SELECT userId, username, email, role, mobilenumber FROM rejected_users")
    rejected = [dict(zip(["userId","username", "email", "role", "mobilenumber"], row)) for row in cursor.fetchall()]

    # Pending
    cursor.execute("SELECT userId, username, email, role, mobilenumber FROM pending_users")
    pending = [dict(zip(["userId","username", "email", "role", "mobilenumber"], row)) for row in cursor.fetchall()]


    conn.close()
    return jsonify({"users": users, "rejected": rejected, "pending": pending}), 200


# 2. Approve pending user
@app.route("/api/admin/approve/<userId>", methods=["POST"])
def approve_user(userId):
    from email_utils import send_email

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

    # Send approval email
    try:
        send_email(
            user[2],  # email
            "Registration Approved",
            f"Hello {user[1]},\n\nYour registration has been approved. You can now log in and start using the system.\n\nRegards,\nAdmin Team"
        )
    except Exception as e:
        return jsonify({"success": True, "message": f"User approved but email failed: {str(e)}"}), 500

    return jsonify({"success": True, "message": "User approved and email sent"}), 200

# 3. Reject pending user
@app.route("/api/admin/reject/<userId>", methods=["POST"])
def reject_user(userId):
    print(userId)
    from email_utils import send_email

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM pending_users WHERE userId = ?", (userId,))
    user = cursor.fetchone()
    print(user)
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
    # Send rejection email
    try:
        send_email(
            user[2],  # email
            "Registration Rejected",
            f"Hello {user[1]},\n\nWe regret to inform you that your registration has been rejected. For more details, please contact support.\n\nRegards,\nAdmin Team"
        )
    except Exception as e:
        return jsonify({"success": True, "message": f"User rejected but email failed: {str(e)}"}), 500

    return jsonify({"success": True, "message": "User rejected and email sent"}), 200


@app.route("/api/admin/reset-password/<userId>", methods=["POST"])
def reset_password(userId):
    from password_utils import generate_random_password
    from email_utils import send_email
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    # Find user in database
    c.execute("SELECT email FROM users WHERE userId = ?", (userId,))
    row = c.fetchone()
    if not row:
        conn.close()
        return jsonify({"success": False, "message": "User not found"}), 404

    user_email = row[0]

    # 1. Generate random password
    new_password = generate_random_password()

    # 2. Update password in DB
    c.execute("UPDATE users SET password = ? WHERE userId = ?", (new_password, userId))
    conn.commit()
    conn.close()

    # 3. Send email to user with new password
    try:
        send_email(user_email, "Your password has been reset", f"New password: {new_password}")
    except Exception as e:
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

@app.route('/api/send-welcome-email', methods=['POST'])
def send_welcome_email():
    data = request.json
    email = data.get("email")
    first_name = data.get("firstName", "")

    if not email:
        return jsonify({"status": "error", "message": "Email not provided"}), 400

    # Compose the email
    msg = MIMEMultipart()
    msg['From'] = ADMIN_EMAIL
    msg['To'] = email
    msg['Cc'] = ADMIN_EMAIL
    msg['Subject'] = "Welcome to ASTA VYUHA"

    body = f"Hi {first_name},\n\nWelcome to ASTA VYUHA! We are excited to have you on board. Registration may take few hours. After user validation you will get registration approved/rejected mail.\n\nRegards,\nASTA VYUHA Team"
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
        print("Email sending failed:", e)
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

    # Compose email
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
        print("Email sending failed:", e)
        return jsonify({"status": "error", "message": "Email sending failed"}), 500

@app.route("/api/register", methods=["POST"])
def register():
    from email_utils import send_email

    data = request.json

    userId = data.get("userId", "").strip()
    username = data.get("username", "").strip()
    email = data.get("email", "").strip().lower()
    mobilenumber = data.get("mobilenumber", "").strip()
    password = data.get("password", "")
    role = data.get("role", "user")

    print(f"UserId: {userId} | Username: {username} | Email: {email} | Mobile: {mobilenumber} | Role: {role}")

    # ✅ Basic validation
    if not all([userId, username, email, password]):
        return jsonify({"success": False, "message": "All fields are required"}), 400

    if "@" not in email or "." not in email:
        return jsonify({"success": False, "message": "Invalid email address"}), 400

    if len(password) < 6:
        return jsonify({"success": False, "message": "Password must be at least 6 characters"}), 400

    # ✅ Save user to SQLite DB
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
        print("Database insert error:", e)
        return jsonify({"success": False, "message": "Unexpected server error"}), 500

    # ✅ Send Welcome Email
    try:
        send_email(
            email,
            "Welcome to AutoTrade",
            f"Hello {username},\n\nYour registration is received successfully. "
            "You’ll get an approval email soon after verification.\n\nThank you for joining AutoTrade!"
        )
    except Exception as e:
        print("Email sending failed:", e)

    conn.close()
    return jsonify({"success": True, "message": "Registration submitted successfully"}), 200

# === CONNECT BROKER ===
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
                obj, refresh_token, auth_token,feed_token = ar.angelone_connect(api_key, user_id, pin, totp_secret)
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
                #profile = "5Paisa don't have the Profile Fetch fecility"
                profile = {'User Name':client_code,}
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


# === LOT SIZE ===
@app.route('/api/get-lot-size', methods=['GET', 'POST'])
def get_lot_size():
    try:
        # Handle both GET and POST requests
        if request.method == 'POST':
            data = request.get_json()
            symbol_key = data.get('symbol_key')
            symbol_value = data.get('symbol_value')
            type_ = data.get('type')
        else:
            symbol_key = request.args.get('symbol_key')
            symbol_value = request.args.get('symbol_value')
            type_ = request.args.get('type')

        print(f"Received lot size request -> type: {type_}, key: {symbol_key}, value: {symbol_value}")

        if not symbol_key:
            return jsonify({"error": "Stock symbol is required."}), 400
        if type_ == "EQUITY":
            lot_size = ls.lot_size(symbol_key)
        elif type_ == "COMMODITY":
            lot_size = ls.commodity_lot_size(symbol_key, symbol_value)
        print(lot_size)

        if lot_size:
            return jsonify({"lot_size": lot_size, "symbol": symbol_key})
        else:
            return jsonify({"message": "Lot size not found for the given symbol."}), 404
    except Exception as e:
        print(f"Error in get_lot_size: {e}")
        return jsonify({"error": str(e)}), 500

def find_positions_for_symbol(broker, symbol, credentials):
    """
    Fetch positions for the given broker and return only those matching the symbol.
    """
    positions = []

    try:
        # --- Upstox ---
        if broker.lower() == "upstox":
            access_token = credentials.get("access_token")
            positions = us.upstox_fetch_positions(access_token)


        # --- Zerodha ---
        elif broker.lower() == "zerodha":
            api_key = credentials.get("api_key")
            access_token = credentials.get("access_token")
            kite = KiteConnect(api_key)
            kite.set_access_token(access_token)
            positions_data = kite.positions()
            positions = positions_data.get("net", [])

        # --- AngelOne ---
        elif broker.lower() == "angelone":
            api_key = credentials.get("api_key")
            user_id = credentials.get("user_id")
            pin = credentials.get("pin")
            totp_secret = credentials.get("totp_secret")
            session = broker_sessions.get(broker)
            if not session:
                return jsonify({"status": "failed", "message": "Broker not connected."})
            auth_token = session["auth_token"]
            positions = ar.angeeone_fetch_positions(api_key, auth_token)
        # --- 5 Paisa ---
        elif broker.lower() == "5paisa":
            app_key = credentials.get("app_key")
            access_token = credentials.get('access_token')
            client_code = credentials.get("client_id")
            positions = fp.fivepaisa_fetch_positions(app_key, access_token, client_code)

        # --- Groww (dummy example here) ---
        elif broker.lower() == "groww":
            # Assume you have functions like gr.groww_positions(), fp.fivepaisa_positions()
            positions = []  # placeholder

        # --- Filter matching positions ---
        matching = []
        for pos in positions:
            trading_symbol = pos.get("tradingsymbol", "")
            if trading_symbol.startswith(symbol):  # match beginning
                matching.append(pos)

        return matching

    except Exception as e:
        print(f"❌ Error fetching positions for {broker}, {symbol}: {e}")
        return []

# === TRADING LOOP FOR ALL STOCKS ===
def run_trading_logic_for_all(trading_parameters, selected_brokers,logger):
    # mark all as active initially
    print(trading_parameters)
    for stock in trading_parameters:
        active_trades[stock['symbol_value']] = True
    logger.write("✅ Trading loop started for all selected stocks")
    logger.write("\n⏳ Starting new trading cycle setup...")

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

        logger.write(f"🔑 Fetching instrument key for {company} ({symbol}) via {broker_name}...")
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
                   logger.write(company)
                   instrument_key = ar.angelone_get_token_by_name(symbol)
                elif broker_name.lower() == "5paisa":
                   instrument_key = fp.fivepaisa_scripcode_fetch(symbol)
            elif exchange_type == "COMMODITY":
                if broker_name.lower() == "upstox":
                    matched = us.upstox_commodity_instrument_key(name, symbol)
                    instrument_key = matched['instrument_key'].iloc[0]

            if instrument_key:
                stock['instrument_key'] = instrument_key
                logger.write(f"✅ Found instrument key {instrument_key} for {symbol}")
            else:
                logger.write(f"⚠️ No instrument key found for {symbol}, skipping this stock.")
                active_trades[stock['symbol_value']] = False

        except Exception as e:
            logger.write(f"❌ Error fetching instrument key for {symbol}: {e}")
            active_trades[stock['symbol_value']] = False

    # setup time intervals
    interval = trading_parameters[0].get("interval", "1minute")
    now_interval, next_interval = nni.round_to_next_interval(interval)
    logger.write(f"Present Interval Start : {now_interval}, Next Interval Start :{next_interval}")
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

                logger.write(f"🕯 Fetching candles for {symbol}-{company} from {broker_name}")

                combined_df = None
                try:
                    if broker_name.lower() == "upstox":
                        access_token = next((b['credentials']['access_token'] for b in selected_brokers if b['name'] == broker_key),
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
                            combined_df = ar.angelone_get_historical_data(api_key,auth_token, obj,"NSE", instrument_key, interval)
                    elif broker_name.lower() == "5paisa":
                        broker_info = next((b for b in selected_brokers if b['name'] == broker_key), None)
                        if broker_info:
                            app_key = broker_info['credentials'].get("app_key")
                            access_token = broker_info['credentials'].get("access_token")
                            combined_df = fp.fivepaisa_historical_data_fetch(access_token, instrument_key, interval,25)

                except Exception as e:
                    logger.write(f"❌ Error fetching data for {symbol}: {e}")

                if combined_df is None or combined_df.empty:
                    logger.write(f"❌ No data for {symbol}, skipping.")
                    continue

                logger.write(f"✅ Data ready for {symbol}")
                gevent.sleep(0.5)
                indicators_df = ind.all_indicators(combined_df)
                row = indicators_df.tail(1).iloc[0]
                cols = indicators_df.columns.tolist()
                col_widths = [max(len(str(c)), len(str(row[c]))) + 2 for c in cols]
                def line():
                    return "+" + "+".join(["-" * w for w in col_widths]) + "+"

                header = "|" + "|".join([f"{c:^{w}}" for c, w in zip(cols, col_widths)]) + "|"
                values = "|" + "|".join([f"{str(row[c]):^{w}}" for c, w in zip(cols, col_widths)]) + "|"
                # --- log it line by line ---
                logger.write(line())
                logger.write(header)
                logger.write(line())
                logger.write(values)
                logger.write(line())
                #logger.write(tabulate(indicators_df.tail(1), headers="keys", tablefmt="pretty", showindex=False))

                # STEP 3: Check trade conditions
                logger.write(f"📊 Checking trade conditions for {symbol}")
                lots = stock.get("lots")
                target_pct = stock.get("target_percentage")
                name = stock.get("symbol_value")

                try:
                    creds = next((b["credentials"] for b in selected_brokers if b["name"] == broker_key), None)
                    if broker_name.lower() == "upstox":
                        us.upstox_trade_conditions_check(lots, target_pct, indicators_df.tail(1), creds, company, symbol, exchange_type, strategy)
                    elif broker_name.lower() == "zerodha":
                        zr.zerodha_trade_conditions_check(lots, target_pct, indicators_df.tail(1), creds, symbol,strategy)
                    elif broker_name.lower() == "angelone":
                        session = broker_sessions.get(broker_name)
                        if not session:
                            return jsonify({"status": "failed", "message": "Broker not connected."})
                        obj = session["obj"]
                        auth_token = session["auth_token"]
                        interval = ar.number_to_interval(interval)
                        ar.angelone_trade_conditions_check(obj, auth_token, lots, target_pct, indicators_df, creds, name,strategy)
                    elif broker_name.lower() == "5paisa":
                        fp.fivepaisa_trade_conditions_check(lots, target_pct, indicators_df, creds, stock,strategy)

                except Exception as e:
                    logger.write(f"❌ Error running strategy for {symbol}: {e}")

            logger.write("✅ Trading cycle complete")
            logger.write(f"Present Interval Start : {now_interval}, Next Interval Start :{next_interval}")
            logger.write("Waiting for next interval beginning .....")
            gevent.sleep(1)  # wait before next cycle

# === START ALL TRADING ===
@app.route('/api/start-all-trading', methods=['POST'])
def start_all_trading():
    data = request.get_json()
    trading_parameters = data.get("tradingParameters", [])
    selected_brokers = data.get("selectedBrokers", [])

    # Use gevent.spawn instead of threading.Thread
    spawn(run_trading_logic_for_all, trading_parameters, selected_brokers, logger)

    return jsonify({"logs": ["🟢 Started trading for all stocks together"]})

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
    trading_parameters = data.get("tradingParameters", [])  # list of active stocks
    selected_brokers = data.get("selectedBrokers", [])      # broker credentials
    session = broker_sessions.get("AngelOne")
    if not session:
        return jsonify({"status": "failed", "message": "Broker not connected."})
    obj = session["obj"]

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
                order_id = ar.angelone_close_position(obj, pos)

            elif broker_name.lower() == "groww":
                order_id = gr.groww_close_position(credentials, pos)

            elif broker_name.lower() == "5paisa":
                order_id = fp.fivepaisa_close_position(credentials, pos)

            if order_id:
                closed.append({"symbol": symbol, "broker": broker_name, "order_id": order_id})

    return jsonify({
        "message": "✅ Closed all positions successfully",
        "closed": closed
    })


# === DISCONNECT STOCK ===
@app.route("/api/disconnect-stock", methods=["POST"])
def disconnect_stock():
    data = request.json
    print("DISCONNECT REQUEST DATA:", data)

    symbol = data.get("symbol_value")

    if symbol in active_trades:
        # ❌ Remove the symbol from active_trades completely
        active_trades.pop(symbol, None)
        return jsonify({"message": f"❌ {symbol} Disconnection happens after the current trade cycle(Interval). "})

    return jsonify({"message": "⚠️ Stock not active"})


if __name__ == '__main__':
    import os

    # initialize database
    init_db()

    # add default admin if not exists
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
    update_instrument_file()

    port = int(os.environ.get('PORT', 5000))
    debug_flag = os.environ.get('FLASK_DEBUG', 'false').lower() == 'true'
    app.run(host='0.0.0.0', port=port, debug=debug_flag, use_reloader=False)
