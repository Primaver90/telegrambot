import os
import threading
import time
import schedule
from flask import Flask, jsonify
from main import invia_offerta

app = Flask(__name__)

@app.get("/")
def index():
    return "OK"

@app.get("/health")
def health():
    return jsonify(status="ok")

def job_loop():
    schedule.every(14).minutes.do(invia_offerta)
    while True:
        schedule.run_pending()
        time.sleep(5)

threading.Thread(target=job_loop, daemon=True).start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port)
