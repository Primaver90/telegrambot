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

# 1 = richiede sconto; 0 = per test accetta anche prezzo senza sconto (utile per capire se i prezzi arrivano)
REQUIRE_DISCOUNT = os.environ.get("REQUIRE_DISCOUNT", "1") == "1"

SEARCH_INDEX = os.environ.get("SEARCH_INDEX", "All")
ITEMS_PER_PAGE = int(os.environ.get("ITEMS_PER_PAGE", "8"))
PAGES = int(os.environ.get("PAGES", "4"))

# Quanti ASIN max per batch (fallback GetItems). Tienilo basso per evitare rate limit.
GETITEMS_BATCH = int(os.environ.get("GETITEMS_BATCH", "5"))

# Throttle per /run manuale (anti-spam)
MIN_SECONDS_BETWEEN_RUNS = int(os.environ.get("MIN_SECONDS_BETWEEN_RUNS", "25"))
_last_run_ts = 0.0

KEYWORDS = [
    "Apple", "Android", "iPhone", "MacBook", "tablet", "smartwatch",
    "auricolari Bluetooth", "smart TV", "monitor PC", "notebook",
    "gaming mouse", "gaming tastiera", "console", "soundbar", "smart home",
    "aspirapolvere robot", "telecamere WiFi", "caricatore wireless",
    "accessori smartphone", "accessori iPhone",
]

# RISORSE: qui sta la magia. Se Amazon/libreria ha cambiato default, senza queste i prezzi spesso spariscono.
PAAPI_RESOURCES = [
    "ItemInfo.Title",
    "Images.Primary.Large",
    "Offers.Listings.Price",
    "Offers.Listings.Savings",
    "Offers.Summaries.LowestPrice",
    "Offers.Summaries.Savings",
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
# HELPERS
# =========================
def log_exc(prefix: str, e: Exception):
    print(f"❌ {prefix}: {type(e).__name__} -> {repr(e)}")


def parse_eur_amount(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)

    s = str(value)
    s = s.replace("\u20ac", "").replace("€", "")
    s = s.replace("\xa0", " ").strip()
    # attenzione: Amazon spesso usa "1.234,56"
    s = s.replace(".", "").replace(",", ".").strip()
    try:
        return float(s)
    except Exception:
        return None


def _safe_attr(obj, name, default=None):
    try:
        return getattr(obj, name)
    except Exception:
        return default


def _normalize_items_response(resp):
    if resp is None:
        return []
    if isinstance(resp, list):
        return resp
    if hasattr(resp, "items"):
        return getattr(resp, "items") or []
    if isinstance(resp, dict) and "items" in resp:
        return resp.get("items") or []
    return []


def _extract_title(item):
    try:
        t = item.item_info.title.display_value
        return " ".join(str(t).split())
    except Exception:
        return ""


def _extract_image_url(item):
    try:
        return item.images.primary.large.url
    except Exception:
        return None


def _extract_price_obj_and_savings(item):
    """
    Prova Listings[0].Price, altrimenti Summaries[0].LowestPrice
    Ritorna (price_obj, savings_obj) oppure (None, None)
    """
    offers = _safe_attr(item, "offers", None)
    if not offers:
        return None, None

    # listings
    try:
        listings = offers.listings or []
        l0 = listings[0] if listings else None
        if l0 and getattr(l0, "price", None):
            price_obj = l0.price
            savings_obj = getattr(price_obj, "savings", None)
            return price_obj, savings_obj
    except Exception:
        pass

    # summaries
    try:
        sums = offers.summaries or []
        s0 = sums[0] if sums else None
        if s0:
            lowest = getattr(s0, "lowest_price", None) or getattr(s0, "lowestPrice", None)
            if lowest:
                savings_obj = getattr(s0, "savings", None)
                return lowest, savings_obj
    except Exception:
        pass

    return None, None


def _price_value_from_priceobj(price_obj):
    if not price_obj:
        return None
    disp = getattr(price_obj, "display_amount", None) or getattr(price_obj, "displayAmount", None)
    v = parse_eur_amount(disp)
    if v is not None:
        return v
    amt = getattr(price_obj, "amount", None) or getattr(price_obj, "value", None)
    return parse_eur_amount(amt)


def _discount_from_savings(savings_obj):
    if not savings_obj:
        return 0
    perc = getattr(savings_obj, "percentage", 0) or 0
    try:
        return int(perc)
    except Exception:
        return 0


def _old_price_from_savings(price_val, savings_obj):
    if price_val is None:
        return None
    if not savings_obj:
        return float(price_val)
    amt = getattr(savings_obj, "amount", 0) or 0
    extra = parse_eur_amount(amt)
    return float(price_val) + float(extra or 0)


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
# AMAZON CALLS (LOW RATE)
# =========================
def search_items_with_resources(kw, page):
    """
    Prima prova con resources (per far arrivare i prezzi),
    se il wrapper non lo supporta, fallback senza.
    """
    try:
        res = amazon.search_items(
            keywords=kw,
            item_count=ITEMS_PER_PAGE,
            search_index=SEARCH_INDEX,
            item_page=page,
            resources=PAAPI_RESOURCES,
        )
        return res, "with_resources"
    except TypeError as e:
        # wrapper non accetta resources
        if DEBUG_FILTERS:
            print(f"[DEBUG] SearchItems resources non supportato: {repr(e)}")
        res = amazon.search_items(
            keywords=kw,
            item_count=ITEMS_PER_PAGE,
            search_index=SEARCH_INDEX,
            item_page=page,
        )
        return res, "no_resources"
    except Exception as e:
        raise e


def safe_get_items_batch(asins):
    """
    Fallback: GetItems con resources se possibile.
    Se rate limit, stop subito.
    """
    if not asins:
        return [], None

    fn = getattr(amazon, "get_items", None)
    if not fn:
        return [], "no_get_items"

    attempts = [
        ("items+resources", lambda: fn(items=asins, resources=PAAPI_RESOURCES)),
        ("item_ids+resources", lambda: fn(item_ids=asins, resources=PAAPI_RESOURCES)),
        ("items", lambda: fn(items=asins)),
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
                print(f"[DEBUG] GetItems fail ({kind}): {name} -> {repr(e)}")
            if name in ("TooManyRequests", "TooManyRequestsException"):
                return [], "rate_limited"
            if name == "MalformedRequest":
                continue

    if last_err:
        log_exc("GetItems batch error", last_err)
    return [], "error"


def _first_valid_item_for_keyword(kw, pubblicati):
    reasons = Counter()

    for page in range(1, PAGES + 1):
        try:
            results, mode = search_items_with_resources(kw, page)
            items = getattr(results, "items", []) or []
        except Exception as e:
            log_exc(f"SearchItems kw='{kw}' page={page}", e)
            if type(e).__name__ in ("TooManyRequests", "TooManyRequestsException"):
                reasons["rate_limited_search"] += 1
                break
            reasons["paapi_error"] += 1
            items = []
            mode = "error"

        if DEBUG_FILTERS:
            print(f"[DEBUG] kw={kw} page={page} items={len(items)} mode={mode}")

        # 1) Prima prova a prendere prezzi direttamente da SearchItems (se resources hanno funzionato)
        for it in items:
            asin = (getattr(it, "asin", None) or "").strip().upper()
            if not asin:
                reasons["no_asin"] += 1
                continue
            if asin in pubblicati or not can_post(asin, hours=24):
                reasons["dup"] += 1
                continue

            title = _extract_title(it)
            url_img = _extract_image_url(it) or "https://m.media-amazon.com/images/I/71bhWgQK-cL._AC_SL1500_.jpg"
            url = getattr(it, "detail_page_url", None) or f"https://www.amazon.it/dp/{asin}?tag={AMAZON_ASSOCIATE_TAG}"

            price_obj, savings_obj = _extract_price_obj_and_savings(it)
            if not price_obj:
                reasons["no_price_in_searchitem"] += 1
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

            minimo = disc >= 30

            if DEBUG_FILTERS:
                print(f"[DEBUG] ✅ scelto da SearchItems asin={asin} price={price_val} disc={disc}")

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

        # 2) Se SearchItems non ha prezzi, fallback GetItems su un batch piccolo
        candidates = []
        for it in items:
            asin = (getattr(it, "asin", None) or "").strip().upper()
            if not asin:
                continue
            if asin in pubblicati or not can_post(asin, hours=24):
                continue
            candidates.append(it)
            if len(candidates) >= GETITEMS_BATCH:
                break

        if not candidates:
            reasons["no_candidates"] += 1
            continue

        asins = [(getattr(c, "asin", None) or "").strip().upper() for c in candidates]
        details, status = safe_get_items_batch(asins)
        if status == "rate_limited":
            reasons["rate_limited_getitems"] += 1
            break

        detail_by_asin = {}
        for d in details:
            a = (getattr(d, "asin", None) or "").strip().upper()
            if a:
                detail_by_asin[a] = d

        for c in candidates:
            asin = (getattr(c, "asin", None) or "").strip().upper()
            det = detail_by_asin.get(asin)
            if not det:
                reasons["getitems_empty_or_unmapped"] += 1
                continue

            price_obj, savings_obj = _extract_price_obj_and_savings(det)
            if not price_obj:
                if DEBUG_FILTERS:
                    has_offers = hasattr(det, "offers")
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

            title = _extract_title(c)
            url_img = _extract_image_url(c) or "https://m.media-amazon.com/images/I/71bhWgQK-cL._AC_SL1500_.jpg"
            url = getattr(c, "detail_page_url", None) or f"https://www.amazon.it/dp/{asin}?tag={AMAZON_ASSOCIATE_TAG}"
            minimo = disc >= 30

            if DEBUG_FILTERS:
                print(f"[DEBUG] ✅ scelto da GetItems asin={asin} price={price_val} disc={disc}")

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


# =========================
# SEND OFFER
# =========================
def invia_offerta():
    global _last_run_ts

    if bot is None:
        print("❌ TELEGRAM_BOT_TOKEN mancante.")
        return False
    if not TELEGRAM_CHAT_ID:
        print("❌ TELEGRAM_CHAT_ID mancante.")
        return False

    # anti-spam /run
    now = time.time()
    if now - _last_run_ts < MIN_SECONDS_BETWEEN_RUNS:
        wait = int(MIN_SECONDS_BETWEEN_RUNS - (now - _last_run_ts))
        print(f"⏳ /run chiamato troppo presto. Attendi {wait}s.")
        return False
    _last_run_ts = now

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


# =========================
# TIME WINDOW + SCHEDULER
# =========================
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
