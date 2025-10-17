import sqlite3

conn = sqlite3.connect("user_data_new.db")
c = conn.cursor()

# Run query
c.execute("SELECT * FROM users")
rows = c.fetchall()

# Get column names
column_names = [desc[0] for desc in c.description]

# Print header
print(" | ".join(column_names))
print("-" * 80)

# Print rows
for row in rows:
    print(" | ".join(str(item) for item in row))

conn.close()
