import random
import string

def generate_random_password(length=10):
    characters = string.ascii_letters + string.digits  # A-Z, a-z, 0-9
    while True:
        password = ''.join(random.choice(characters) for _ in range(length))
        # Ensure at least one uppercase, one lowercase, one digit
        if (any(c.islower() for c in password) and
            any(c.isupper() for c in password) and
            any(c.isdigit() for c in password)):
            return password
