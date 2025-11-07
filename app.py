# app.py
import os
from flask import Flask, jsonify
import threading
import main

app = Flask(__name__)

if os.environ.get("SCHEDULER_STARTED") != "1":
    os.environ["SCHEDULER_STARTED"] = "1"
    threading.Thread(target=main.start_scheduler, daemon=True).start()

@app.get("/health")
def health():
    return jsonify({"ok": True})

@app.post("/run")
def run_now():
    threading.Thread(target=main.invia_offerta, daemon=True).start()
    return jsonify({"queued": True})
