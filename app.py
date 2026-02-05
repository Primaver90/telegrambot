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
        ok = invia_offerta()
        if ok:
            return "✅ Offerta pubblicata davvero"
        return "⚠️ Nessuna offerta valida trovata (filtri/duplicati). Riprova tra poco."
    except Exception as e:
        return f"❌ Errore: {e}"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
