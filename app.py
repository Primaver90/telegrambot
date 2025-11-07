from flask import Flask
import threading
from main import start_scheduler

app = Flask(__name__)

# Avvio bot in thread separato
threading.Thread(target=start_scheduler, daemon=True).start()

@app.get("/health")
def health():
    return "OK"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
