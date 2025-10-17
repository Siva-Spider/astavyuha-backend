import sqlite3

# New database file
DB_FILE = "user_data_new.db"

def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # Approved users
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            userId TEXT UNIQUE NOT NULL,
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

    # Rejected registrations
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

    # Insert admin user (only if not exists)
    cursor.execute("""
        INSERT OR IGNORE INTO users (userId, username, email, password, role, mobilenumber)
        VALUES (?, ?, ?, ?, ?, ?)
    """, ("admin", "admin", "vijayaranikraja@gmail.com", "Admin@123", "admin", "6303317143"))

    conn.commit()
    conn.close()
    print("✅ New database created and admin user added.")

if __name__ == "__main__":
    init_db()
