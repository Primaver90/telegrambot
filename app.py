"""Avvia un solo scheduler e ricevitore Telegram per container Render."""
import fcntl
import os
import threading
from flask import Flask

app = Flask(__name__)

def check_amazon():
    """Verifica di sola lettura: non pubblica e non cambia la rotazione keyword."""
    try:
        import main
        data, _ = main.creators_search_items("Apple", 1)
        items = main.safe_get(data, "searchResult", "items", default=None)
        if items is None:
            items = data.get("items", []) or []
        print(f"Verifica Amazon LwA OK: {len(items)} prodotti ricevuti", flush=True)
    except Exception as exc:
        # Non mostra credenziali o contenuti delle risposte API.
        print(f"Verifica Amazon non riuscita: {type(exc).__name__}. Controllare credenziali/accesso API.", flush=True)

_lock_file = None
_startup_ok = False


def start_services():
    global _lock_file, _startup_ok
    if os.environ.get('BOT_DISABLE_BACKGROUND') == '1':
        return
    try:
        import main
        main._require_env()
        main._build_token_url()
        _lock_file = open('/tmp/telegrambot-services.lock', 'a')
        try:
            fcntl.flock(_lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            # Il lock viene liberato dal sistema alla morte del processo.
            _lock_file.close()
            _lock_file = None
            _startup_ok = True
            return
        from manual_offers import run_polling
        threading.Thread(target=main.start_scheduler, daemon=True).start()
        threading.Thread(target=run_polling, daemon=True).start()
        threading.Thread(target=check_amazon, daemon=True).start()
        print("Bot aggiornato: LwA e comandi privati avviati", flush=True)
        _startup_ok = True
    except Exception as exc:
        print(f'Avvio bot fallito: {type(exc).__name__}. Controlla le variabili di ambiente.')


start_services()


@app.get('/')
@app.get('/health')
def health():
    return ('OK', 200) if _startup_ok else ('Bot non avviato: verifica configurazione', 503)


@app.get('/run')
def run_now():
    # Il vecchio endpoint pubblico pubblicava senza autenticazione.
    return 'Usa la chat privata del bot per preparare e confermare un’offerta.', 403


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', '10000')))
