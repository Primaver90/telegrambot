from flask import Flask, jsonify, request
import threading
from main import start_scheduler, _tick

app = Flask(__name__)

# avvia lo scheduler una sola volta in background
threading.Thread(target=start_scheduler, daemon=True).start()

@app.get("/health")
def health():
    return jsonify({"status": "ok"})

# /run accetta sia POST che GET per comodità di test dal browser
@app.route("/run", methods=["POST", "GET"])
def run():
    try:
        _tick()
        return jsonify({"status": "forzato"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
