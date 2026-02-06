import os
import time
import html
import schedule
import requests
import threading
from io import BytesIO
from pathlib import Path
from datetime import datetime, timedelta
from collections import Counter

from PIL import Image, ImageDraw, ImageFont
from telegram import Bot, InlineKeyboardMarkup, InlineKeyboardButton
from amazon_paapi import AmazonApi

from flask import Flask, jsonify

# =========================
# CONFIG (usa ENV VAR, niente hardcoded in produzione)
# =========================
AMAZON_ACCESS_KEY = os.environ.get("AMAZON_ACCESS_KEY", "")
AMAZON_SECRET_KEY = os.environ.get("AMAZON_SECRET_KEY", "")
AMAZON_ASSOCIATE_TAG = os.environ.get("AMAZON_ASSOCIATE_TAG", "itech00-21")
AMAZON_COUNTRY = os.environ.get("AMAZON_COUNTRY", "IT")

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

FONT_PATH = os.environ.get("FONT_PATH", "Montserrat-VariableFont_wght.ttf")
LOGO_PATH = os.environ.get("LOGO_PATH", "header_clean2.png")
BADGE_PATH = os.environ.get("BADGE_PATH", "minimo storico flat.png")

# Persistenza: se hai disk su Render usa /data
DATA_DIR = os.environ.get("DATA_DIR", "/tmp/botdata")
Path(DATA_DIR).mkdir(parents=True, exist_ok=True)

PUB_FILE = os.path.join(DATA_DIR, "pubblicati.txt")
PUB_TS = os.path.join(DATA_DIR, "pubblicati_ts.csv")
KW_INDEX = os.path.join(DATA_DIR, "kw_index.txt")

MIN_DISCOUNT = int(os.environ.get("MIN_DISCOUNT", "15"))
MIN_PRICE = float(os.environ.get("MIN_PRICE", "15"))
MAX_PRICE = float(os.environ.get("MAX_PRICE", "1900"))

DEBUG_AMAZON = os.environ.get("DEBUG_AMAZON", "0") == "1"
GETITEMS_FALLBACK_MAX = int(os.environ.get("GETITEMS_FALLBACK_MAX", "4"))

# riduci chiamate per non triggerare rate-limit
ITEMS_PER_PAGE = int(os.environ.get("ITEMS_PER_PAGE", "6"))
PAGES = int(os.environ.get("PAGES", "3"))
SEARCH_INDEX = os.environ.get("SEARCH_INDEX", "All")

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

if not (AMAZON_ACCESS_KEY and AMAZON_SECRET_KEY and TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
    # Non blocco l'app HTTP, ma ti avviso nei log
    print("⚠️ ENV mancanti: controlla AMAZON_ACCESS_KEY/SECRET e TELEGRAM_BOT_TOKEN/CHAT_ID")

bot = Bot(token=TELEGRAM_BOT_TOKEN) if TELEGRAM_BOT_TOKEN else None
amazon = AmazonApi(AMAZON_ACCESS_KEY, AMAZON_SECRET_KEY, AMAZON_ASSOCIATE_TAG, AMAZON_COUNTRY)

# =========================
# PA-API resources (robusti)
# =========================
RESOURCES_V2 = [
    "ItemInfo.Title",
    "Images.Primary.Large",
    "OffersV2.Listings.Price",
    "OffersV2.Listings.SavingBasis",
    "OffersV2.Summaries.LowestPrice",
    "OffersV2.Summaries.Savings",
]

RESOURCES_V1 = [
    "ItemInfo.Title",
    "Images.Primary.Large",
    "Offers.Listings.Price",
    "Offers.Listings.Savings",
    "Offers.Summaries.LowestPrice",
    "Offers.Summaries.Savings",
]

# =========================
# UTILS
# =========================
def parse_eur_amount(display_amount):
    """
    Gestisce bene:
    - "€ 19,99"
    - "19,99 €"
    - "1.299,00"
    - "1299.00"
    """
    if not display_amount:
        return None
    s = str(display_amount).replace("\u20ac", "").replace("€", "").replace("\xa0", " ").strip()

    # Caso IT: 1.299,00
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    # Caso: 1299,00
    elif "," in s and "." not in s:
        s = s.replace(",", ".")
    # Caso: 1299.00 -> ok
    s = s.strip()

    try:
        return float(s)
    except:
        return None


def get_attr(obj, *path, default=None):
    cur = obj
    try:
        for p in path:
            if cur is None:
                return default
            if isinstance(p, int):
                cur = (cur or [None])[p]
            else:
                cur = getattr(cur, p, None)
        return cur if cur is not None else default
    except:
        return default


def safe_first(lst):
    try:
        return (lst or [None])[0]
    except:
        return None


def draw_bold_text(draw, position, text, font, fill="black", offset=1):
    x, y = position
    for dx in (-offset, 0, offset):
        for dy in (-offset, 0, offset):
            draw.text((x + dx, y + dy), text, font=font, fill=fill)


def genera_immagine_offerta(titolo, prezzo_nuovo, prezzo_vecchio, sconto, url_img, minimo_storico):
    img = Image.new("RGB", (1080, 1080), "white")
    draw = ImageDraw.Draw(img)

    logo = Image.open(LOGO_PATH).resize((1080, 165))
    img.paste(logo, (0, 0))

    if minimo_storico and sconto >= 30 and os.path.exists(BADGE_PATH):
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
                except:
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
    except:
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


# =========================
# AMAZON CALLS (con fallback anti-MalformedRequest)
# =========================
def search_items_with_fallback(kw, page):
    # 1) prova V2
    try:
        return amazon.search_items(
            keywords=kw,
            item_count=ITEMS_PER_PAGE,
            search_index=SEARCH_INDEX,
            item_page=page,
            resources=RESOURCES_V2,
        )
    except Exception as e:
        if "MalformedRequest" not in repr(e):
            # throttling o altro: rilancio
            raise
        if DEBUG_AMAZON:
            print(f"[DEBUG] SearchItems V2 MalformedRequest -> fallback V1 ({kw} p{page})")

    # 2) prova V1
    try:
        return amazon.search_items(
            keywords=kw,
            item_count=ITEMS_PER_PAGE,
            search_index=SEARCH_INDEX,
            item_page=page,
            resources=RESOURCES_V1,
        )
    except Exception as e:
        if "MalformedRequest" not in repr(e):
            raise
        if DEBUG_AMAZON:
            print(f"[DEBUG] SearchItems V1 MalformedRequest -> fallback NO resources ({kw} p{page})")

    # 3) senza resources
    return amazon.search_items(
        keywords=kw,
        item_count=ITEMS_PER_PAGE,
        search_index=SEARCH_INDEX,
        item_page=page,
    )


def get_items_with_fallback(asins):
    # 1) prova V2
    try:
        return amazon.get_items(items=asins, resources=RESOURCES_V2)
    except Exception as e:
        if "MalformedRequest" not in repr(e):
            raise
        if DEBUG_AMAZON:
            print("[DEBUG] GetItems V2 MalformedRequest -> fallback V1")

    # 2) prova V1
    try:
        return amazon.get_items(items=asins, resources=RESOURCES_V1)
    except Exception as e:
        if "MalformedRequest" not in repr(e):
            raise
        if DEBUG_AMAZON:
            print("[DEBUG] GetItems V1 MalformedRequest -> fallback NO resources")

    # 3) senza resources
    return amazon.get_items(items=asins)


def extract_price_discount(item):
    """
    Estrae prezzo/sconto da OffersV2 o Offers.
    Ritorna (price_val, disc, old_val) oppure (None, None, None)
    """

    # ---- OffersV2 (varie grafie possibili nel wrapper) ----
    offersv2 = (
        getattr(item, "offers_v2", None)
        or getattr(item, "offersv2", None)
        or getattr(item, "offersV2", None)
    )

    try:
        l0 = safe_first(getattr(offersv2, "listings", None) or [])
        if l0:
            price_val = parse_eur_amount(get_attr(l0, "price", "display_amount"))
            old_val = parse_eur_amount(get_attr(l0, "saving_basis", "display_amount"))

            disc = 0
            s0 = safe_first(getattr(offersv2, "summaries", None) or [])
            if s0:
                disc = int(get_attr(s0, "savings", "percentage", default=0) or 0)

            # se non ho percentuale ma ho saving_basis, la calcolo
            if price_val is not None and old_val is not None and old_val > 0:
                disc = int(round((1 - (price_val / old_val)) * 100))

            if price_val is not None:
                return price_val, disc, (old_val if old_val is not None else price_val)
    except:
        pass

    # ---- Offers (vecchio) ----
    try:
        listing = safe_first(get_attr(item, "offers", "listings") or [])
        price_obj = getattr(listing, "price", None)
        if not price_obj:
            return None, None, None

        price_val = parse_eur_amount(getattr(price_obj, "display_amount", None))
        if price_val is None:
            return None, None, None

        savings = getattr(price_obj, "savings", None)
        disc = int(getattr(savings, "percentage", 0) or 0) if savings else 0
        old_val = price_val + float(getattr(savings, "amount", 0) or 0) if savings else price_val

        return price_val, disc, old_val
    except:
        return None, None, None


# =========================
# CORE LOGIC
# =========================
def _first_valid_item_for_keyword(kw, pubblicati):
    reasons = Counter()
    fallback_asins = []

    for page in range(1, PAGES + 1):
        try:
            results = search_items_with_fallback(kw, page)
            items = getattr(results, "items", []) or []
            if DEBUG_AMAZON:
                print(f"[DEBUG] kw={kw} page={page} items={len(items)}")
        except Exception as e:
            msg = repr(e)
            reasons["paapi_error"] += 1
            print(f"❌ ERRORE Amazon PA-API (kw='{kw}', page={page}): {msg}")
            if "TooManyRequests" in msg:
                print("⏳ Rate limit SearchItems: pausa 2s")
                time.sleep(2)
            continue

        for item in items:
            asin = (getattr(item, "asin", None) or "").strip().upper()
            if not asin:
                reasons["no_asin"] += 1
                continue
            if asin in pubblicati or not can_post(asin, hours=24):
                reasons["already_posted"] += 1
                continue

            title = get_attr(item, "item_info", "title", "display_value", default="") or ""
            title = " ".join(str(title).split())

            price_val, disc, old_val = extract_price_discount(item)
            if price_val is None:
                reasons["no_price_in_searchitems"] += 1
                if len(fallback_asins) < GETITEMS_FALLBACK_MAX:
                    fallback_asins.append(asin)
                if DEBUG_AMAZON:
                    has_offers = bool(getattr(item, "offers", None) or getattr(item, "offers_v2", None))
                    print(f"[DEBUG] asin={asin} no_price | has_offers={has_offers}")
                continue

            if price_val < MIN_PRICE or price_val > MAX_PRICE:
                reasons["price_out_range"] += 1
                continue
            if disc < MIN_DISCOUNT:
                reasons["disc_too_low"] += 1
                continue

            url_img = get_attr(item, "images", "primary", "large", "url", default=None)
            if not url_img:
                url_img = "https://m.media-amazon.com/images/I/71bhWgQK-cL._AC_SL1500_.jpg"

            url = getattr(item, "detail_page_url", None) or f"https://www.amazon.it/dp/{asin}?tag={AMAZON_ASSOCIATE_TAG}"
            minimo = disc >= 30

            if DEBUG_AMAZON:
                print(f"[DEBUG] FOUND SearchItems asin={asin} price={price_val} disc={disc}")

            return {
                "asin": asin,
                "title": title[:80].strip() + ("…" if len(title) > 80 else ""),
                "price_new": price_val,
                "price_old": old_val if old_val is not None else price_val,
                "discount": disc,
                "url_img": url_img,
                "url": url,
                "minimo": minimo,
            }

    # fallback: GetItems su pochi ASIN
    if fallback_asins:
        try:
            res = get_items_with_fallback(fallback_asins)
            items = getattr(res, "items", []) or []
            if DEBUG_AMAZON:
                print(f"[DEBUG] GetItems fallback asins={fallback_asins} items={len(items)}")

            for item in items:
                asin = (getattr(item, "asin", None) or "").strip().upper()
                if not asin or asin in pubblicati or not can_post(asin, hours=24):
                    continue

                title = get_attr(item, "item_info", "title", "display_value", default="") or ""
                title = " ".join(str(title).split())

                price_val, disc, old_val = extract_price_discount(item)
                if price_val is None:
                    continue
                if price_val < MIN_PRICE or price_val > MAX_PRICE:
                    continue
                if disc < MIN_DISCOUNT:
                    continue

                url_img = get_attr(item, "images", "primary", "large", "url", default=None)
                if not url_img:
                    url_img = "https://m.media-amazon.com/images/I/71bhWgQK-cL._AC_SL1500_.jpg"

                url = getattr(item, "detail_page_url", None) or f"https://www.amazon.it/dp/{asin}?tag={AMAZON_ASSOCIATE_TAG}"
                minimo = disc >= 30

                if DEBUG_AMAZON:
                    print(f"[DEBUG] FOUND GetItems asin={asin} price={price_val} disc={disc}")

                return {
                    "asin": asin,
                    "title": title[:80].strip() + ("…" if len(title) > 80 else ""),
                    "price_new": price_val,
                    "price_old": old_val if old_val is not None else price_val,
                    "discount": disc,
                    "url_img": url_img,
                    "url": url,
                    "minimo": minimo,
                }

        except Exception as e:
            msg = repr(e)
            print(f"❌ GetItems fallback error: {msg}")
            if "TooManyRequests" in msg:
                print("⏳ Rate limit GetItems: pausa 2s")
                time.sleep(2)

    if DEBUG_AMAZON:
        print(f"[DEBUG] kw={kw} reasons={dict(reasons)} fallback_asins={fallback_asins}")

    return None


def invia_offerta():
    if not bot:
        print("❌ Telegram bot non inizializzato (manca TELEGRAM_BOT_TOKEN).")
        return False

    pubblicati = load_pubblicati()
    kw = pick_keyword()

    payload = _first_valid_item_for_keyword(kw, pubblicati)
    if not payload:
        print(f"⚠️ Nessuna offerta valida trovata per keyword: {kw}")
        return False

    titolo = payload["title"]
    prezzo_nuovo = payload["price_new"]
    prezzo_vecchio = payload["price_old"]
    sconto = payload["discount"]
    url_img = payload["url_img"]
    url = payload["url"]
    minimo = payload["minimo"]
    asin = payload["asin"]

    immagine = genera_immagine_offerta(titolo, prezzo_nuovo, prezzo_vecchio, sconto, url_img, minimo)

    safe_title = html.escape(titolo)
    safe_url = html.escape(url, quote=True)

    caption_parts = [f"📌 <b>{safe_title}</b>"]
    if minimo and sconto >= 30:
        caption_parts.append("❗️🚨 <b>MINIMO STORICO</b> 🚨❗️")

    caption_parts.append(
        f"💶 A soli <b>{prezzo_nuovo:.2f}€</b> invece di <s>{prezzo_vecchio:.2f}€</s> (<b>-{sconto}%</b>)"
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

    print(f"✅ Pubblicata: {asin} | {kw}")
    return True


def is_in_italy_window(now_utc=None):
    if now_utc is None:
        now_utc = datetime.utcnow()
    month = now_utc.month
    offset_hours = 2 if 4 <= month <= 10 else 1
    italy_time = now_utc + timedelta(hours=offset_hours)
    return (9 <= italy_time.hour < 21), italy_time


def run_if_in_fascia_oraria():
    in_window, italy_time = is_in_italy_window()
    if in_window:
        return invia_offerta()
    print(f"⏸ Fuori fascia oraria (Italia {italy_time.strftime('%H:%M')}), nessuna offerta pubblicata.")
    return False


def start_scheduler():
    schedule.clear()
    schedule.every().monday.at("06:59").do(resetta_pubblicati)
    schedule.every(14).minutes.do(run_if_in_fascia_oraria)
    while True:
        schedule.run_pending()
        time.sleep(5)


# =========================
# FLASK APP (Render)
# =========================
app = Flask(__name__)

@app.get("/health")
def health():
    return jsonify({"ok": True})

@app.get("/run")
def run_once():
    try:
        ok = run_if_in_fascia_oraria()
        return jsonify({"ok": bool(ok)})
    except Exception as e:
        return jsonify({"ok": False, "error": repr(e)}), 500


# Avvio scheduler in thread (solo se abilitato)
# IMPORTANTISSIMO: su Render metti gunicorn con 1 worker oppure rischi doppie pubblicazioni.
SCHEDULER_ENABLED = os.environ.get("SCHEDULER_ENABLED", "1") == "1"
_started = False

def _boot_scheduler_once():
    global _started
    if _started or not SCHEDULER_ENABLED:
        return
    _started = True
    t = threading.Thread(target=start_scheduler, daemon=True)
    t.start()
    print("🟢 Scheduler avviato (thread).")

_boot_scheduler_once()
