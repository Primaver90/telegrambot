import os
import time
import html
import threading
from io import BytesIO
from pathlib import Path
from datetime import datetime, timedelta
from collections import Counter

import schedule
import requests
from PIL import Image, ImageDraw, ImageFont
from telegram import Bot, InlineKeyboardMarkup, InlineKeyboardButton
from amazon_paapi import AmazonApi
from flask import Flask, jsonify

AMAZON_ACCESS_KEY = os.environ.get("AMAZON_ACCESS_KEY", "AKPAZS2VGY1748024339")
AMAZON_SECRET_KEY = os.environ.get("AMAZON_SECRET_KEY", "yiA1TX0xWWVtW1HgKpkR2LWZpklQXaJ2k9D4HsiL")
AMAZON_ASSOCIATE_TAG = os.environ.get("AMAZON_ASSOCIATE_TAG", "itech00-21")
AMAZON_COUNTRY = os.environ.get("AMAZON_COUNTRY", "IT")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "7687135950:AAHfRV6b4RgAcVU6j71wDfZS-1RTMJ15ajg")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "-1001010781022")

FONT_PATH = os.environ.get("FONT_PATH", "Montserrat-VariableFont_wght.ttf")
LOGO_PATH = os.environ.get("LOGO_PATH", "header_clean2.png")
BADGE_PATH = os.environ.get("BADGE_PATH", "minimo storico flat.png")

DATA_DIR = os.environ.get("DATA_DIR", "/tmp/botdata")
Path(DATA_DIR).mkdir(parents=True, exist_ok=True)

PUB_FILE = os.path.join(DATA_DIR, "pubblicati.txt")
PUB_TS = os.path.join(DATA_DIR, "pubblicati_ts.csv")
KW_INDEX = os.path.join(DATA_DIR, "kw_index.txt")

MIN_DISCOUNT = int(os.environ.get("MIN_DISCOUNT", "15"))
MIN_PRICE = float(os.environ.get("MIN_PRICE", "15"))
MAX_PRICE = float(os.environ.get("MAX_PRICE", "1900"))

DEBUG_FILTERS = os.environ.get("DEBUG_FILTERS", "1") == "1"

# 1 = richiede sconto, 0 = (solo test) accetta anche prezzo senza sconto
REQUIRE_DISCOUNT = os.environ.get("REQUIRE_DISCOUNT", "1") == "1"

# Per test: metti PAGES=1 e GETITEMS_BATCH=3 per non triggerare rate limit
SEARCH_INDEX = os.environ.get("SEARCH_INDEX", "All")
ITEMS_PER_PAGE = int(os.environ.get("ITEMS_PER_PAGE", "8"))
PAGES = int(os.environ.get("PAGES", "4"))
GETITEMS_BATCH = int(os.environ.get("GETITEMS_BATCH", "5"))

# Risorse necessarie per avere prezzi/offerte in GetItems
GETITEMS_RESOURCES = [
    "ItemInfo.Title",
    "Images.Primary.Large",
    "Offers.Listings.Price",
    "Offers.Listings.Savings",
    "Offers.Summaries.LowestPrice",
    "Offers.Summaries.Savings",
]

KEYWORDS = [
    "Apple", "Android", "iPhone", "MacBook", "tablet", "smartwatch",
    "auricolari Bluetooth", "smart TV", "monitor PC", "notebook",
    "gaming mouse", "gaming tastiera", "console", "soundbar", "smart home",
    "aspirapolvere robot", "telecamere WiFi", "caricatore wireless",
    "accessori smartphone", "accessori iPhone",
]


# =========================
# INIT
# =========================
app = Flask(__name__)

if not AMAZON_ACCESS_KEY or not AMAZON_SECRET_KEY or not AMAZON_ASSOCIATE_TAG:
    raise RuntimeError("Mancano AMAZON_ACCESS_KEY / AMAZON_SECRET_KEY / AMAZON_ASSOCIATE_TAG (env vars).")

amazon = AmazonApi(
    AMAZON_ACCESS_KEY,
    AMAZON_SECRET_KEY,
    AMAZON_ASSOCIATE_TAG,
    AMAZON_COUNTRY,
)

bot = Bot(token=TELEGRAM_BOT_TOKEN) if TELEGRAM_BOT_TOKEN else None


# =========================
# DEBUG HELPERS
# =========================
def log_exc(prefix: str, e: Exception):
    print(f"❌ {prefix}: {type(e).__name__} -> {repr(e)}")


def _get(obj, *names, default=None):
    if obj is None:
        return default
    if isinstance(obj, dict):
        for n in names:
            if n in obj:
                return obj.get(n, default)
        return default
    for n in names:
        if hasattr(obj, n):
            return getattr(obj, n)
    return default


def parse_eur_amount(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)

    s = str(value)
    s = s.replace("\u20ac", "").replace("€", "")
    s = s.replace("\xa0", " ").strip()
    s = s.replace(".", "").replace(",", ".").strip()
    try:
        return float(s)
    except Exception:
        return None


# =========================
# IMAGE
# =========================
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


# =========================
# PERSISTENCE
# =========================
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
    i = (get_kw_index() + 1) % len(KEYWORDS)
    with open(KW_INDEX, "w", encoding="utf-8") as f:
        f.write(str(i))


def pick_keyword():
    i = get_kw_index()
    kw = KEYWORDS[i]
    bump_kw_index()
    return kw


# =========================
# AMAZON EXTRACTORS
# =========================
def _extract_title(item):
    info = _get(item, "item_info", "itemInfo")
    title = _get(info, "title", "Title")
    t = _get(title, "display_value", "displayValue", default="") or ""
    return " ".join(str(t).split())


def _extract_image_url(item):
    images = _get(item, "images", "Images")
    primary = _get(images, "primary", "Primary")
    large = _get(primary, "large", "Large")
    return _get(large, "url", "URL")


def _extract_price_data(item):
    offers = _get(item, "offers", "Offers")
    if not offers:
        return (None, None)

    listings = _get(offers, "listings", "Listings", default=[]) or []
    listing0 = listings[0] if listings else None
    if listing0 is not None:
        price_obj = _get(listing0, "price", "Price")
        savings_obj = _get(price_obj, "savings", "Savings")
        if price_obj is not None:
            return (price_obj, savings_obj)

    summaries = _get(offers, "summaries", "Summaries", default=[]) or []
    s0 = summaries[0] if summaries else None
    if s0 is not None:
        lowest = _get(s0, "lowest_price", "lowestPrice", "LowestPrice")
        sv = _get(s0, "savings", "Savings")
        if lowest is not None:
            return (lowest, sv)

    return (None, None)


def _price_value_from_priceobj(price_obj):
    display = _get(price_obj, "display_amount", "displayAmount")
    v = parse_eur_amount(display)
    if v is not None:
        return v
    amount = _get(price_obj, "amount", "Amount", "value", "Value")
    return parse_eur_amount(amount)


def _discount_from_savings(savings_obj):
    if not savings_obj:
        return 0
    perc = _get(savings_obj, "percentage", "Percentage", default=0) or 0
    try:
        return int(perc)
    except Exception:
        return 0


def _old_price_from_savings(price_val, savings_obj):
    if not savings_obj:
        return price_val
    amt = _get(savings_obj, "amount", "Amount", default=0) or 0
    extra = parse_eur_amount(amt)
    return float(price_val) + float(extra or 0)


def _normalize_items_response(resp):
    if resp is None:
        return []
    if isinstance(resp, list):
        return resp
    if hasattr(resp, "items"):
        it = getattr(resp, "items")
        return it or []
    if isinstance(resp, dict) and "items" in resp:
        return resp.get("items") or []
    return []


def safe_get_items_batch(asins):
    """
    IMPORTANTISSIMO: prima proviamo SEMPRE le chiamate con resources.
    Non usiamo inspect.signature perché spesso il wrapper espone *args/**kwargs e ci frega.
    """
    if not asins:
        return [], None

    fn = getattr(amazon, "get_items", None)
    if not fn:
        print("❌ amazon.get_items non disponibile.")
        return [], None

    # tentativi in ordine: con resources, senza, poi posizionale
    attempts = [
        ("items+resources", lambda: fn(items=asins, resources=GETITEMS_RESOURCES)),
        ("item_ids+resources", lambda: fn(item_ids=asins, resources=GETITEMS_RESOURCES)),
        ("itemIds+resources", lambda: fn(itemIds=asins, resources=GETITEMS_RESOURCES)),
        ("items", lambda: fn(items=asins)),
        ("item_ids", lambda: fn(item_ids=asins)),
        ("positional", lambda: fn(asins)),
    ]

    last_err = None
    for kind, call in attempts:
        try:
            resp = call()
            items = _normalize_items_response(resp)
            if DEBUG_FILTERS:
                print(f"[DEBUG] GetItems ok ({kind}). extracted_items={len(items)}")
            return items, None
        except Exception as e:
            last_err = e
            name = type(e).__name__
            if DEBUG_FILTERS:
                print(f"[DEBUG] GetItems fallito ({kind}): {name} -> {repr(e)}")

            # se rate limit: stop immediato
            if name in ("TooManyRequests", "TooManyRequestsException"):
                print("⏳ Rate limit su GetItems: stop batch e riprova più tardi.")
                return [], "rate_limited"

            # se malformed con resources: prova altri tentativi
            if name == "MalformedRequest":
                continue

    if last_err:
        log_exc("GetItems batch error", last_err)
    return [], "error"


def _search_items_page(kw, page):
    return amazon.search_items(
        keywords=kw,
        item_count=ITEMS_PER_PAGE,
        search_index=SEARCH_INDEX,
        item_page=page,
    )


def _first_valid_item_for_keyword(kw, pubblicati):
    reasons = Counter()

    for page in range(1, PAGES + 1):
        try:
            results = _search_items_page(kw, page)
            items = getattr(results, "items", []) or []
        except Exception as e:
            log_exc(f"SearchItems kw='{kw}' page={page}", e)
            reasons["paapi_error"] += 1
            items = []

        if DEBUG_FILTERS:
            print(f"[DEBUG] kw={kw} page={page} items={len(items)}")

        candidates = []
        for it in items:
            asin = (getattr(it, "asin", None) or "").strip().upper()
            if not asin:
                reasons["no_asin"] += 1
                continue
            if asin in pubblicati:
                reasons["dup_pub_file"] += 1
                continue
            if not can_post(asin, hours=24):
                reasons["dup_24h"] += 1
                continue

            candidates.append({
                "asin": asin,
                "title": _extract_title(it),
                "url_img": _extract_image_url(it),
                "url": getattr(it, "detail_page_url", None) or f"https://www.amazon.it/dp/{asin}?tag={AMAZON_ASSOCIATE_TAG}",
            })
            if len(candidates) >= GETITEMS_BATCH:
                break

        if not candidates:
            reasons["no_candidates"] += 1
            continue

        asins = [c["asin"] for c in candidates]
        details, status = safe_get_items_batch(asins)

        # se rate limit: stop tutto subito (non continuare pagine)
        if status == "rate_limited":
            reasons["rate_limited"] += 1
            break

        detail_by_asin = {}
        for d in details:
            a = (getattr(d, "asin", None) or "").strip().upper()
            if a:
                detail_by_asin[a] = d

        for c in candidates:
            asin = c["asin"]
            det = detail_by_asin.get(asin)
            if not det:
                reasons["getitems_empty_or_unmapped"] += 1
                continue

            price_obj, savings_obj = _extract_price_data(det)
            if not price_obj:
                if DEBUG_FILTERS:
                    has_offers = hasattr(det, "offers") or hasattr(det, "Offers")
                    print(f"[DEBUG] asin={asin} no_price_obj | has_offers={has_offers}")
                reasons["no_price_obj"] += 1
                continue

            price_val = _price_value_from_priceobj(price_obj)
            if price_val is None:
                reasons["bad_price_parse"] += 1
                continue

            if price_val < MIN_PRICE or price_val > MAX_PRICE:
                reasons["price_out_range"] += 1
                continue

            disc = _discount_from_savings(savings_obj)
            old_val = _old_price_from_savings(price_val, savings_obj)

            if REQUIRE_DISCOUNT and disc < MIN_DISCOUNT:
                reasons["disc_too_low"] += 1
                continue

            url_img = c["url_img"] or "https://m.media-amazon.com/images/I/71bhWgQK-cL._AC_SL1500_.jpg"
            url = c["url"] or f"https://www.amazon.it/dp/{asin}?tag={AMAZON_ASSOCIATE_TAG}"
            title = (c["title"] or asin).strip()
            minimo = disc >= 30

            if DEBUG_FILTERS:
                print(f"[DEBUG] ✅ scelto asin={asin} price={price_val} disc={disc}")

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

    if DEBUG_FILTERS:
        print(f"[DEBUG] kw={kw} reasons={dict(reasons)}")

    return None


def invia_offerta():
    if bot is None:
        print("❌ TELEGRAM_BOT_TOKEN mancante.")
        return False
    if not TELEGRAM_CHAT_ID:
        print("❌ TELEGRAM_CHAT_ID mancante.")
        return False

    pubblicati = load_pubblicati()
    kw = pick_keyword()

    payload = _first_valid_item_for_keyword(kw, pubblicati)
    if not payload:
        print(f"⚠️ Nessuna offerta valida trovata per keyword: {kw}")
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
    print(f"✅ Pubblicata: {asin} | {kw}")
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
        print(f"⏸ Fuori fascia oraria (Italia {italy_time.strftime('%H:%M')}), nessuna offerta pubblicata.")


def start_scheduler_background():
    def _loop():
        schedule.clear()
        schedule.every().monday.at("06:59").do(resetta_pubblicati)
        schedule.every(14).minutes.do(run_if_in_fascia_oraria)
        while True:
            schedule.run_pending()
            time.sleep(5)

    threading.Thread(target=_loop, daemon=True).start()


# =========================
# FLASK ROUTES
# =========================
@app.route("/", methods=["GET"])
def home():
    return ("ok", 200)


@app.route("/health", methods=["GET", "HEAD"])
def health():
    return ("ok", 200)


@app.route("/run", methods=["GET"])
def run_once():
    try:
        ok = invia_offerta()
        return jsonify({"ok": bool(ok)}), 200
    except Exception as e:
        log_exc("Errore /run", e)
        return jsonify({"ok": False, "error": str(e)}), 500


start_scheduler_background()
start_scheduler = app
