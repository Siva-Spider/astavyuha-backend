import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Update these with your Gmail details
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SENDER_EMAIL = "sivag.prasad88@gmail.com"
APP_PASSWORD = "vueiidvhyuyhqqla"   # <- use Gmail app password, not your login password

def send_email(to_email, subject, body):
    try:
        # Create message
        msg = MIMEMultipart()
        msg["From"] = SENDER_EMAIL
        msg["To"] = to_email
        msg["Subject"] = subject

        msg.attach(MIMEText(body, "plain"))

        # Connect to Gmail SMTP server
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(SENDER_EMAIL, APP_PASSWORD)

        server.sendmail(SENDER_EMAIL, to_email, msg.as_string())
        server.quit()

        print(f"✅ Email sent successfully to {to_email}")
    except Exception as e:
        print(f"❌ Email sending failed: {e}")
