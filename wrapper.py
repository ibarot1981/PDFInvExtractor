import os
import time
import subprocess
import signal
import sys
import requests
import logging
from dotenv import load_dotenv
from logging.handlers import RotatingFileHandler

if os.name == 'nt':  # Only for Windows
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
    
# --- Load Environment ---
load_dotenv()

UPLOAD_INTERVAL = int(os.getenv('UPLOAD_INTERVAL', 120))
GRIST_SERVER_URL = os.getenv('GRIST_SERVER_URL', 'https://docs.getgrist.com')
CLAUDE_SCRIPT = 'claude_InvDataEx.py'
GRIST_UPLOADER_SCRIPT = 'grist_uploader.py'
LOG_FILE = os.getenv('WRAPPER_LOG_FILE', 'wrapper.log')

# --- Setup Rotating Logging ---
LOG_FILE = os.getenv('WRAPPER_LOG_FILE', 'wrapper.log')
LOG_MAX_BYTES = int(os.getenv('WRAPPER_LOG_MAX_BYTES', 5242880))  # Default 5MB if not set
LOG_BACKUP_COUNT = int(os.getenv('WRAPPER_LOG_BACKUP_COUNT', 3))  # Default 3 files if not set

file_handler = RotatingFileHandler(
    LOG_FILE,
    maxBytes=LOG_MAX_BYTES,
    backupCount=LOG_BACKUP_COUNT,
    encoding='utf-8'  # Explicitly set encoding
)
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
console_handler.encoding = 'utf-8' # Explicitly set encoding for console handler

logging.basicConfig(
    level=logging.INFO,
    handlers=[file_handler, console_handler]
)

# Global process references
claude_process = None
uploader_process = None

def is_grist_available():
    """Check if Grist server is reachable."""
    try:
        response = requests.get(GRIST_SERVER_URL, timeout=5)
        if response.status_code == 200:
            logging.info("✅ Grist server is available.")
            return True
        else:
            logging.warning(f"⚠️ Grist server returned status {response.status_code}.")
            return False
    except Exception as e:
        logging.error(f"🔴 Grist server check failed: {e}")
        return False

def start_claude_extractor():
    """Start the PDF extractor script."""
    global claude_process
    logging.info(f"🚀 Starting {CLAUDE_SCRIPT}...")
    claude_process = subprocess.Popen([sys.executable, CLAUDE_SCRIPT])

def restart_claude_extractor():
    """Restart the extractor script if it dies."""
    global claude_process
    if claude_process and claude_process.poll() is not None:
        logging.warning(f"⚠️ {CLAUDE_SCRIPT} exited unexpectedly. Restarting...")
        start_claude_extractor()

def run_grist_uploader():
    """Run the Grist uploader script once if not already running."""
    global uploader_process
    if uploader_process and uploader_process.poll() is None:
        logging.info("⏳ Grist uploader is still running. Skipping this upload cycle.")
        return

    logging.info(f"📤 Starting {GRIST_UPLOADER_SCRIPT}...")
    uploader_process = subprocess.Popen([sys.executable, GRIST_UPLOADER_SCRIPT])

def signal_handler(sig, frame):
    logging.info("🛑 Received interrupt signal. Shutting down processes...")
    if claude_process:
        claude_process.terminate()
    if uploader_process:
        uploader_process.terminate()
    sys.exit(0)

if __name__ == "__main__":
    logging.info("🌟 Starting Wrapper Script...")
    logging.info(f"Upload interval set to {UPLOAD_INTERVAL} seconds.")

    # Register signal handler for Ctrl+C
    signal.signal(signal.SIGINT, signal_handler)

    # Start claude extractor
    start_claude_extractor()

    try:
        last_upload_time = 0
        while True:
            time.sleep(5)
            restart_claude_extractor()

            current_time = time.time()
            if current_time - last_upload_time >= UPLOAD_INTERVAL:
                if is_grist_available():
                    run_grist_uploader()
                else:
                    logging.warning("⏳ Grist server not available. Skipping this upload cycle.")
                last_upload_time = current_time
    except KeyboardInterrupt:
        signal_handler(None, None)
