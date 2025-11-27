import pyodbc
import time
import os
import sys

# --- Configuration ---
# These variables should match your Docker Compose environment setup
SERVER = os.environ.get('DB_SERVER', 'localhost,1433')  # Use localhost when running from host machine
DATABASE = os.environ.get('DB_NAME', 'master') # Target 'master' to ensure the service is up
USERNAME = os.environ.get('DB_USER', 'SA')
PASSWORD = os.environ.get('DB_PASSWORD', 'YourStrong!Passw0rd') # Match password from docker-compose.yml
DRIVER = os.environ.get('ODBC_DRIVER', '{ODBC Driver 18 for SQL Server}')

# Connection string setup
CONNECTION_STRING = (
    f"DRIVER={DRIVER};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    f"UID={USERNAME};"
    f"PWD={PASSWORD};"
    f"TrustServerCertificate=yes;"  # Required for ODBC Driver 18 with self-signed certs
)

# --- Connection Check Function ---

def check_db_connection(max_retries=30, delay_seconds=5):
    """
    Attempts to connect to the SQL Server database.
    If the connection fails, it prints a log message and retries.
    """
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Attempting to connect to SQL Server at {SERVER}...")

    for attempt in range(1, max_retries + 1):
        try:
            # Attempt to establish connection
            conn = pyodbc.connect(CONNECTION_STRING, timeout=5)
            conn.close()
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] ✅ Connection successful after {attempt} attempt(s). Database is ready.")
            return True

        except pyodbc.Error as ex:
            sqlstate = ex.args[0]
            
            # Specific error handling for connection failures
            # Commonly: 08001 (Client unable to establish connection) or 28000 (Login failure)
            if '08001' in sqlstate:
                log_message = f"Connection failed (Attempt {attempt}/{max_retries}). Server is likely still starting or unavailable."
            elif '28000' in sqlstate:
                # Login failure, often due to password/user issue
                log_message = f"Connection failed (Attempt {attempt}/{max_retries}). Login error: {ex.args[1]}"
            else:
                # General ODBC error, may indicate an issue other than just startup
                log_message = f"ODBC Error (Attempt {attempt}/{max_retries}, State: {sqlstate}). Details: {ex.args[1].strip()}"
            
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] ❌ {log_message}")

        if attempt < max_retries:
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Waiting {delay_seconds} seconds before retrying...")
            time.sleep(delay_seconds)

    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 🛑 Failed to connect to SQL Server after {max_retries} attempts. Exiting.")
    sys.exit(1) # Exit with an error code if max retries are reached

if __name__ == '__main__':
    # You can change the number of retries or delay if needed
    check_db_connection(max_retries=60, delay_seconds=2) # Tries for up to 2 minutes