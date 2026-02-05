import os
import re
import threading
import time
from io import BytesIO
from datetime import datetime, timedelta
from pathlib import Path
from collections import Counter

import schedule
import requests
import html

from PIL import Image, ImageDraw, ImageFont
from telegram import Bot, InlineKeyboardMarkup, InlineKeyboardButton
from amazon_paapi import AmazonApi

from flask import Flask, Response

# =========================
# CONFIG
# =========================

def _need_env(name: str) -> str:
    v = os.environ.get(name, "").strip()
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v

AMAZON_ACCESS_KEY = os.environ.get("AMAZON_ACCESS_KEY", "AKPAZS2VGY1748024339")
AMAZON_SECRET_KEY = os.environ.get("AMAZON_SECRET_KEY", "yiA1TX0xWWVtW1HgKpkR2LWZpklQXaJ2k9D4HsiL")
AMAZON_ASSOCIATE_TAG = os.environ.get("AMAZON_ASSOCIATE_TAG", "itech00-21")
AMAZON_COUNTRY = os.environ.get("AMAZON_COUNTRY", "IT")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "7687135950:AAHfRV6b4RgAcVU6j71wDfZS-1RTMJ15ajg")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "-1001010781022")

FONT_PATH = os.environ.get("FONT_PATH", "Montserrat-VariableFont_wght.ttf")
LOGO_PATH = os.environ.get("LOGO_PATH", "header_clean2.png")
BADGE_PATH = os.environ.get("BADGE_PATH", "minimo storico flat.png")

# Render: meglio /data se lo usi persistente. Fallback /tmp.
DATA_DIR = os.environ.get("DATA_DIR", "/data")
try:
    Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
except Exception:
    DATA_DIR = "/tmp/botdata"
    Path(DATA_DIR).mkdir(parents=True, exist_ok=True)

PUB_FILE = os.path.join(DATA_DIR, "pubblicati.txt")
PUB_TS = os.path.join(DATA_DIR, "pubblicati_ts.csv")
KW_INDEX = os.path.join(DATA_DIR, "kw_index.txt")
SCHED_LOCK = os.path.join(DATA_DIR, "scheduler.lock")

MIN_DISCOUNT = int(os.environ.get("MIN_DISCOUNT", "15"))
MIN_PRICE = float(os.environ.get("MIN_PRICE", "15"))
MAX_PRICE = float(os.environ.get("MAX_PRICE", "1900"))

DEBUG_AMAZON = os.environ.get("DEBUG_AMAZON", "0").strip() == "1"

KEYWORDS = [
    "Apple",
    "Android",
    "iPhone",
    "MacBook",
    "tablet",
    "smartwatch",
    "auricolari Bluetooth",
    "smart TV",
    "monitor PC",
    "notebook",
    "gaming mouse",
    "gaming tastiera",
    "console",
    "soundbar",
    "smart home",
    "aspirapolvere robot",
    "telecamere WiFi",
    "caricatore wireless",
    "accessori smartphone",
    "accessori iPhone",
]

SEARCH_INDEX = os.environ.get("SEARCH_INDEX", "All")  # come prima
ITEMS_PER_PAGE = int(os.environ.get("ITEMS_PER_PAGE", "8"))
PAGES = int(os.environ.get("PAGES", "4"))

# Risorse PA-API: chiediamo esplicitamente OffersV2, Title e immagini
# (così evitiamo il caso “offers sì, price no”)
RESOURCES_V2 = [
    "ItemInfo.Title",
    "Images.Primary.Large",
    "OffersV2.Listings.Price",
    "OffersV2.Listings.Savings",
]

# fallback legacy (se AmazonApi/lib restituisce offers classiche)
RESOURCES_LEGACY = [
    "ItemInfo.Title",
    "Images.Primary.Large",
    "Offers.Listings.Price",
    "Offers.Listings.Savings",
]

bot = Bot(token=TELEGRAM_BOT_TOKEN)
amazon = AmazonApi(AMAZON_ACCESS_KEY, AMAZON_SECRET_KEY, AMAZON_ASSOCIATE_TAG, AMAZON_COUNTRY)

app = Flask(__name__)

# =========================
# HELPERS
# =========================

def log(msg: str):
    print(msg, flush=True)

def draw_bold_text(draw, position, text, font, fill="black", offset=1):
    x, y = position
    for dx in (-offset, 0, offset):
        for dy in (-offset, 0, offset):
            draw.text((x + dx, y + dy), text, font=font, fill=fill)

def parse_eur_amount(value) -> float | None:
    """
    Gestisce:
    - "1.299,00 €"
    - "1299,00€"
    - "1299.00"
    - "€ 1 299,00"
    """
    if value is None:
        return None
    s = str(value)
    s = s.replace("\u20ac", "").replace("€", "")
    s = s.replace("\xa0", " ").strip()
    s = s.replace(" ", "")

    # se contiene sia '.' che ',' -> '.' migliaia, ',' decimali
    if "." in s and "," in s:
        s = s.replace(".", "").replace(",", ".")
    # se contiene solo ',' -> decimali
    elif "," in s and "." not in s:
        s = s.replace(",", ".")
    # solo '.' ok

    s = re.sub(r"[^0-9.]", "", s)
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None

def _float_from_amount(obj) -> float | None:
    """
    Alcune lib modellano amount come numero, stringa o oggetto.
    """
    if obj is None:
        return None
    if isinstance(obj, (int, float)):
        return float(obj)
    # in certi casi è stringa "12.34" o "12,34 €"
    if isinstance(obj, str):
        return parse_eur_amount(obj)

    # oggetto con attr "amount" / "value" / "display_amount"
    for attr in ("amount", "value", "display_amount", "displayAmount"):
        v = getattr(obj, attr, None)
        if v is not None:
            return parse_eur_amount(v)
    return None

def genera_immagine_offerta(titolo, prezzo_nuovo, prezzo_vecchio, sconto, url_img, minimo_storico):
    img = Image.new("RGB", (1080, 1080), "white")
    draw = ImageDraw.Draw(img)

    logo = Image.open(LOGO_PATH).resize((1080, 165))
    img.paste(logo, (0, 0))

    if minimo_storico and sconto >= 30:
        badge = Image.open(BADGE_PATH).resize((220, 96))
        img.paste(badge, (24, 140), badge.convert("RGBA"))

    font_perc = ImageFont.truetype(FONT_PATH, 88)
    draw.text((830, 230), f"-{sconto}%", font=font_perc, fill="black")

    response = requests.get(url_img, timeout=15)
    prodotto = Image.open(BytesIO(response.content)).resize((600, 600))
    img.paste(prodotto, (240, 230))

    font_old = ImageFont.truetype(FONT_PATH, 72)
    font_new = ImageFont.truetype(FONT_PATH, 120)

    prezzo_old_str = f"€ {prezzo_vecchio:.2f}"
    prezzo_new_str = f"€ {prezzo_nuovo:.2f}"

    w_old = draw.textlength(prezzo_old_str, font=font_old)
    x_old = (1080 - int(w_old)) // 2
    draw_bold_text(draw, (x_old, 860), prezzo_old_str, font=font_old, fill="black", offset=1)
    draw.line((x_old - 10, 880, x_old + w_old + 10, 880), fill="black", width=10)

    w_new = draw.textlength(prezzo_new_str, font=font_new)
    x_new = (1080 - int(w_new)) // 2
    draw_bold_text(draw, (x_new, 910), prezzo_new_str, font=font_new, fill="darkred", offset=2)

    output = BytesIO()
    img.save(output, format="PNG")
    output.seek(0)
    return output

def load_pubblicati():
    if not os.path.exists(PUB_FILE):
        return set()
    with open(PUB_FILE, "r", encoding="utf-8") as f:
        return {line.strip().upper() for line in f if line.strip()}

def save_pubblicati(asin):
    asin = (asin or "").strip().upper()
    if not asin:
        return
    with open(PUB_FILE, "a", encoding="utf-8") as f:
        f.write(asin + "\n")
        f.flush()
        os.fsync(f.fileno())

def can_post(asin, hours=24):
    if not os.path.exists(PUB_TS):
        return True
    cutoff = datetime.utcnow() - timedelta(hours=hours)
    with open(PUB_TS, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split(";", 1)
            if len(parts) != 2:
                continue
            a, ts = parts
            if a == asin:
                try:
                    if datetime.fromisoformat(ts) > cutoff:
                        return False
                except Exception:
                    pass
    return True

def mark_posted(asin):
    with open(PUB_TS, "a", encoding="utf-8") as f:
        f.write(f"{asin};{datetime.utcnow().isoformat()}\n")
        f.flush()
        os.fsync(f.fileno())

def resetta_pubblicati():
    open(PUB_FILE, "w", encoding="utf-8").close()
    open(PUB_TS, "w", encoding="utf-8").close()

def get_kw_index():
    try:
        with open(KW_INDEX, "r", encoding="utf-8") as f:
            i = int(f.read().strip())
    except Exception:
        i = 0
    return i % len(KEYWORDS)

def bump_kw_index():
    i = get_kw_index()
    i = (i + 1) % len(KEYWORDS)
    with open(KW_INDEX, "w", encoding="utf-8") as f:
        f.write(str(i))

def pick_keyword():
    i = get_kw_index()
    kw = KEYWORDS[i]
    bump_kw_index()
    return kw

def _extract_title(item) -> str:
    title = getattr(getattr(getattr(item, "item_info", None), "title", None), "display_value", "") or ""
    return " ".join(title.split())

def _extract_image(item) -> str:
    url_img = getattr(
        getattr(getattr(getattr(item, "images", None), "primary", None), "large", None),
        "url",
        None,
    )
    if not url_img:
        url_img = "https://m.media-amazon.com/images/I/71bhWgQK-cL._AC_SL1500_.jpg"
    return url_img

def _extract_url(item, asin: str) -> str:
    url = getattr(item, "detail_page_url", None)
    if not url and asin:
        url = f"https://www.amazon.it/dp/{asin}?tag={AMAZON_ASSOCIATE_TAG}"
    return url

def _extract_price_from_item(item):
    """
    Prova OffersV2 prima, poi legacy Offers.
    Ritorna: (price_val, disc, old_val) oppure (None, 0, None)
    """
    # OffersV2
    offers_v2 = getattr(item, "offers_v2", None)
    if offers_v2:
        listings = getattr(offers_v2, "listings", None) or []
        if listings:
            listing = listings[0]
            price_obj = getattr(listing, "price", None)
            if price_obj:
                price_val = parse_eur_amount(getattr(price_obj, "display_amount", None))
                savings = getattr(price_obj, "savings", None)
                disc = int(getattr(savings, "percentage", 0) or 0) if savings else 0
                sav_amt = _float_from_amount(getattr(savings, "amount", None)) if savings else None
                old_val = (price_val + sav_amt) if (price_val is not None and sav_amt is not None) else None
                return price_val, disc, old_val

    # Legacy Offers
    offers = getattr(item, "offers", None)
    if offers:
        listings = getattr(offers, "listings", None) or []
        if listings:
            listing = listings[0]
            price_obj = getattr(listing, "price", None)
            if price_obj:
                price_val = parse_eur_amount(getattr(price_obj, "display_amount", None))
                savings = getattr(price_obj, "savings", None)
                disc = int(getattr(savings, "percentage", 0) or 0) if savings else 0
                sav_amt = _float_from_amount(getattr(savings, "amount", None)) if savings else None
                old_val = (price_val + sav_amt) if (price_val is not None and sav_amt is not None) else None
                return price_val, disc, old_val

    return None, 0, None

def _search_items_safe(kw: str, page: int, resources: list[str]):
    """
    Wrapper con gestione TooManyRequests e log utile.
    """
    try:
        res = amazon.search_items(
            keywords=kw,
            item_count=ITEMS_PER_PAGE,
            search_index=SEARCH_INDEX,
            item_page=page,
            resources=resources,
        )
        items = getattr(res, "items", []) or []
        if DEBUG_AMAZON:
            log(f"[DEBUG] kw={kw} page={page} items={len(items)}")
        return items, None
    except Exception as e:
        # amazon_paapi spesso mette dentro l'eccezione sia codice che messaggio
        if DEBUG_AMAZON:
            log(f"❌ ERRORE Amazon PA-API (kw='{kw}', page={page}): {repr(e)}")
        return [], e

def _first_valid_item_for_keyword(kw, pubblicati):
    reasons = Counter()
    resources = RESOURCES_V2  # la chiave: chiediamo OffersV2

    for page in range(1, PAGES + 1):
        items, err = _search_items_safe(kw, page, resources)
        if err is not None:
            # se è rate limit, fermati e riprova al giro successivo
            if "TooManyRequests" in repr(err):
                reasons["paapi_rate_limit"] += 1
                break
            reasons["paapi_error"] += 1
            continue

        for item in items:
            asin = (getattr(item, "asin", None) or "").strip().upper()
            if not asin:
                reasons["no_asin"] += 1
                continue
            if asin in pubblicati or not can_post(asin, hours=24):
                reasons["already_posted"] += 1
                continue

            title = _extract_title(item)
            url_img = _extract_image(item)
            url = _extract_url(item, asin)

            price_val, disc, old_val = _extract_price_from_item(item)
            if price_val is None:
                reasons["no_price_obj"] += 1
                continue

            if price_val < MIN_PRICE or price_val > MAX_PRICE:
                reasons["out_of_price_range"] += 1
                continue

            if disc < MIN_DISCOUNT:
                reasons["low_discount"] += 1
                continue

            if old_val is None:
                # se non abbiamo old price calcolabile, stimiamo
                old_val = price_val

            minimo = disc >= 30

            if DEBUG_AMAZON:
                log(f"[DEBUG] PICK asin={asin} price={price_val} disc={disc} old={old_val}")

            return {
                "asin": asin,
                "title": title[:80].strip() + ("…" if len(title) > 80 else ""),
                "price_new": price_val,
                "price_old": old_val,
                "discount": disc,
                "url_img": url_img,
                "url": url,
                "minimo": minimo,
            }

    if DEBUG_AMAZON:
        log(f"[DEBUG] kw={kw} reasons={dict(reasons)}")

    return None

def invia_offerta():
    pubblicati = load_pubblicati()
    kw = pick_keyword()

    payload = _first_valid_item_for_keyword(kw, pubblicati)
    if not payload:
        log(f"⚠️ Nessuna offerta valida trovata per keyword: {kw}")
        return False

    titolo = payload["title"]
    prezzo_nuovo_val = payload["price_new"]
    prezzo_vecchio_val = payload["price_old"]
    sconto = payload["discount"]
    url_img = payload["url_img"]
    url = payload["url"]
    minimo = payload["minimo"]
    asin = payload["asin"]

    immagine = genera_immagine_offerta(
        titolo, prezzo_nuovo_val, prezzo_vecchio_val, sconto, url_img, minimo
    )

    safe_title = html.escape(titolo)
    safe_url = html.escape(url, quote=True)

    caption_parts = [f"📌 <b>{safe_title}</b>"]
    if minimo and sconto >= 30:
        caption_parts.append("❗️🚨 <b>MINIMO STORICO</b> 🚨❗️")

    caption_parts.append(
        f"💶 A soli <b>{prezzo_nuovo_val:.2f}€</b> invece di "
        f"<s>{prezzo_vecchio_val:.2f}€</s> (<b>-{sconto}%</b>)"
    )
    caption_parts.append(f'👉 <a href="{safe_url}">Acquista ora</a>')
    caption = "\n\n".join(caption_parts)

    button = InlineKeyboardMarkup([[InlineKeyboardButton("🛒 Acquista ora", url=url)]])

    bot.send_photo(
        chat_id=TELEGRAM_CHAT_ID,
        photo=immagine,
        caption=caption,
        parse_mode="HTML",
        reply_markup=button,
    )

    save_pubblicati(asin)
    mark_posted(asin)
    log(f"✅ Pubblicata: {asin} | {kw}")
    return True

def is_in_italy_window(now_utc=None):
    if now_utc is None:
        now_utc = datetime.utcnow()
    month = now_utc.month
    offset_hours = 2 if 4 <= month <= 10 else 1
    italy_time = now_utc + timedelta(hours=offset_hours)
    in_window = 9 <= italy_time.hour < 21
    return in_window, italy_time

def run_if_in_fascia_oraria():
    now_utc = datetime.utcnow()
    in_window, italy_time = is_in_italy_window(now_utc)
    if in_window:
        invia_offerta()
    else:
        log(f"⏸ Fuori fascia oraria (Italia {italy_time.strftime('%H:%M')}), nessuna offerta pubblicata.")

def start_scheduler():
    schedule.clear()
    schedule.every().monday.at("06:59").do(resetta_pubblicati)
    schedule.every(14).minutes.do(run_if_in_fascia_oraria)
    while True:
        schedule.run_pending()
        time.sleep(5)

def _start_scheduler_once():
    # evita doppio scheduler se gunicorn fa più worker
    try:
        fd = os.open(SCHED_LOCK, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, str(os.getpid()).encode("utf-8"))
        os.close(fd)
    except FileExistsError:
        log("ℹ️ Scheduler già avviato (lock presente).")
        return

    t = threading.Thread(target=start_scheduler, daemon=True)
    t.start()
    log("✅ Scheduler avviato in background.")

# =========================
# FLASK ENDPOINTS (Render)
# =========================

@app.get("/")
def home():
    return Response("OK", mimetype="text/plain")

@app.get("/health")
def health():
    return Response("OK", mimetype="text/plain")

@app.get("/run")
def run_now():
    # trigger manuale per test
    run_if_in_fascia_oraria()
    return Response("Triggered", mimetype="text/plain")

# Avvio scheduler quando parte il processo web
_start_scheduler_once()
