import json
import os
from email_utils import send_email   # <-- make sure send_email is in email_utils.py

USERS_FILE = "users.json"

def load_users():
    if os.path.exists(USERS_FILE):
        with open(USERS_FILE, "r") as f:
            return json.load(f)
    return []

def save_users(users):
    with open(USERS_FILE, "w") as f:
        json.dump(users, f, indent=4)

def register_user(username, email, role, password):
    users = load_users()

    # Check if user already exists
    for u in users:
        if u["username"] == username or u["email"] == email:
            return {"error": "User already exists"}

    # Create new user
    new_user = {
        "role": role,
        "username": username,
        "email": email,
        "password": password
    }
    users.append(new_user)
    save_users(users)

    # Send welcome email
    send_email(email, "Welcome", f"Your account is created.\nPassword: {password}")

    return new_user
