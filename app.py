from flask import Flask
import threading
import main

app = Flask(__name__)

@app.route("/")
def home():
    return "✅ Bot attivo e in esecuzione su Render"

@app.route("/run", methods=["POST"])
def run():
    threading.Thread(target=main.invia_offerta).start()
    return "⏳ Offerta in elaborazione...", 200

@app.route("/health")
def health():
    return "OK", 200

if __name__ == "__main__":
    from waitress import serve
    serve(app, host="0.0.0.0", port=8080)
