import os
import threading
from flask import Flask

from main import start_scheduler, invia_offerta

app = Flask(__name__)

# Evita di avviare lo scheduler più volte
_scheduler_started = False
_scheduler_lock = threading.Lock()

def ensure_scheduler_started():
    global _scheduler_started
    with _scheduler_lock:
        if _scheduler_started:
            return
        threading.Thread(target=start_scheduler, daemon=True).start()
        _scheduler_started = True

# Avvialo subito all'import (ok se usi 1 worker)
ensure_scheduler_started()

@app.get("/health")
def health():
    return "OK", 200

@app.get("/run")
def run_now():
    try:
        invia_offerta()
        return "✅ Offerta pubblicata", 200
    except Exception as e:
        # Render log mostrerà comunque lo stacktrace se non lo sopprimi in main.py
        return f"❌ Errore: {e}", 500

@app.get("/")
def root():
    # Così eviti 404 sui probe che chiamano /
    return "OK", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
