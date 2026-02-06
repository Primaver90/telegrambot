from flask import Flask
import os
import threading
import traceback

app = Flask(__name__)

_main = None
_import_trace = None
_scheduler_started = False


def _load_main():
    """Importa main e salva eventuale traceback completo (utile su Render)."""
    global _main, _import_trace
    if _main is not None or _import_trace is not None:
        return
    try:
        import main as m
        _main = m
    except Exception:
        _import_trace = traceback.format_exc()


def _start_scheduler_once():
    """Evita doppio avvio in caso di più import/worker usando un lock file su /tmp."""
    global _scheduler_started
    if _scheduler_started:
        return

    _load_main()
    if _main is None:
        return  # se main non importa, non avvio nulla

    lock_path = "/tmp/scheduler.lock"
    try:
        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.close(fd)
    except FileExistsError:
        # già avviato in questo container
        _scheduler_started = True
        return

    t = threading.Thread(target=_main.start_scheduler, daemon=True)
    t.start()
    _scheduler_started = True


# Avvio scheduler in background (una sola volta)
_start_scheduler_once()


@app.get("/health")
def health():
    forwarding_info = ""
    if _import_trace:
        # Mostra solo un pezzo per non sparare 200KB in risposta
        tail = _import_trace[-2000:]
        forwarding_info = f"\n\nIMPORT_ERROR(main.py):\n{tail}"
    return "OK" + forwarding_info, 200


@app.get("/run")
def run_now():
    _load_main()
    if _main is None:
        tail = (_import_trace or "Errore sconosciuto")[-2000:]
        return f"❌ main.py non importabile.\n\n{tail}", 500

    try:
        ok = _main.invia_offerta()
        if ok:
            return "✅ Offerta pubblicata davvero", 200
        return "⚠️ Nessuna offerta valida trovata (filtri/duplicati o prezzi non disponibili). Riprova tra poco.", 200
    except Exception:
        return f"❌ Errore runtime:\n\n{traceback.format_exc()[-2000:]}", 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
