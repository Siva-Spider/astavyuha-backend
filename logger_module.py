import datetime


class Logger:
    def __init__(self):
        self.logs = []

    def write(self, message: str):
        """Store log, print to terminal, and keep for frontend"""
        timestamped = f"{datetime.datetime.now().strftime('%H:%M:%S')} | {message}"
        self.logs.append(timestamped)
        print(timestamped, flush=True)

    def get_logs(self):
        """Return all collected logs (for API calls)"""
        return self.logs

    def clear(self):
        """Clear all logs (optional)"""
        self.logs = []

# make a global logger instance
logger = Logger()
