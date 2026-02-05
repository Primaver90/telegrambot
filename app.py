from flask import Flask
import threading
from main import start_scheduler, invia_offerta

app = Flask(__name__)

# Avvio lo scheduler in background
threading.Thread(target=start_scheduler, daemon=True).start()

@app.get("/health")
from telegram import Bot

@app.get("/ping")
def ping():
    try:
        bot = Bot(token=os.environ.get("TELEGRAM_BOT_TOKEN"))
        chat_id = os.environ.get("TELEGRAM_CHAT_ID")
        bot.send_message(chat_id=chat_id, text="✅ Ping OK: Telegram funziona")
        return "OK"
    except Exception as e:
        return f"ERRORE: {e}"
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
