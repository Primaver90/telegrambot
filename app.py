from flask import Flask
import threading
from main import start_scheduler, invia_offerta

app = Flask(__name__)

# Avvio lo scheduler in background
threading.Thread(target=start_scheduler, daemon=True).start()

@app.get("/health")
def health():
    return "OK"

@app.get("/run")
def run_now():
    try:
        invia_offerta()
        return "✅ Offerta pubblicata"
    except Exception as e:
        return f"❌ Errore: {e}"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
