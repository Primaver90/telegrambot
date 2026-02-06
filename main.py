import os
import time
import html
import base64
import threading
from io import BytesIO
from datetime import datetime, timedelta
from pathlib import Path
from collections import Counter

import requests
import schedule
from PIL import Image, ImageDraw, ImageFont
from telegram import Bot, InlineKeyboardMarkup, InlineKeyboardButton


# =========================
# ENV / CONFIG
# =========================

CREATORS_CREDENTIAL_ID = os.environ.get("CREATORS_CREDENTIAL_ID", "").strip()
CREATORS_CREDENTIAL_SECRET = os.environ.get("CREATORS_CREDENTIAL_SECRET", "").strip()
CREATORS_CREDENTIAL_VERSION = os.environ.get("CREATORS_CREDENTIAL_VERSION", "").strip()

CREATORS_MARKETPLACE = os.environ.get("CREATORS_MARKETPLACE", "www.amazon.it").strip()

CREATORS_TOKEN_URL = os.environ.get(
    "CREATORS_TOKEN_URL",
    "https://creatorsapi.auth.eu-south-2.amazoncognito.com/oauth2/token"
).strip()

CREATORS_API_BASE = os.environ.get("CREATORS_API_BASE", "https://creatorsapi.amazon").strip()
CREATORS_SEARCH_URL = f"{CREATORS_API_BASE}/catalog/v1/searchItems"

AMAZON_ASSOCIATE_TAG = os.environ.get("AMAZON_ASSOCIATE_TAG", "").strip()

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

FONT_PATH = os.environ.get("FONT_PATH", "Montserrat-VariableFont_wght.ttf")
LOGO_PATH = os.environ.get("LOGO_PATH", "header_clean2.png")
BADGE_PATH = os.environ.get("BADGE_PATH", "minimo storico flat.png")

DATA_DIR = "/tmp/botdata"
Path(DATA_DIR).mkdir(parents=True, exist_ok=True)

PUB_FILE = os.path.join(DATA_DIR, "pubblicati.txt")
PUB_TS = os.path.join(DATA_DIR, "pubblicati_ts.csv")
KW_INDEX = os.path.join(DATA_DIR, "kw_index.txt")

MIN_DISCOUNT = int(os.environ.get("MIN_DISCOUNT", "15"))
MIN_PRICE = float(os.environ.get("MIN_PRICE", "15"))
MAX_PRICE = float(os.environ.get("MAX_PRICE", "1900"))

DEBUG_AMAZON = os.environ.get("DEBUG_AMAZON", "0") == "1"

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

ITEMS_PER_PAGE = 8
PAGES = 4

bot = Bot(token=TELEGRAM_BOT_TOKEN) if TELEGRAM_BOT_TOKEN else None


# =========================
# DEBUG
# =========================

def _debug(msg: str):
    if DEBUG_AMAZON:
        print(f"[DEBUG] {msg}")


# =========================
# TOKEN CACHE (OAuth2)
# =========================

_token_lock = threading.Lock()
_cached_token = None
_cached_token_exp = None  # epoch seconds


def get_access_token():
    global _cached_token, _cached_token_exp

    if not CREATORS_CREDENTIAL_ID or not CREATORS_CREDENTIAL_SECRET or not CREATORS_CREDENTIAL_VERSION:
        raise RuntimeError("Creators API credentials mancanti (ID/SECRET/VERSION).")

    with _token_lock:
        now = time.time()
        if _cached_token and _cached_token_exp and now < (_cached_token_exp - 60):
            return _cached_token

        basic = base64.b64encode(
            f"{CREATORS_CREDENTIAL_ID}:{CREATORS_CREDENTIAL_SECRET}".encode("utf-8")
        ).decode("utf-8")

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {basic}",
        }
        data = "grant_type=client_credentials&scope=creatorsapi/default"

        _debug(f"Token refresh -> {CREATORS_TOKEN_URL}")
        r = requests.post(CREATORS_TOKEN_URL, headers=headers, data=data, timeout=20)
        if r.status_code != 200:
            raise RuntimeError(f"Token error {r.status_code}: {r.text}")

        payload = r.json()
        token = payload.get("access_token")
        expires_in = int(payload.get("expires_in", 3600))

        if not token:
            raise RuntimeError(f"Token response invalida: {payload}")

        _cached_token = token
        _cached_token_exp = time.time() + expires_in
        _debug(f"Token OK (expires_in={expires_in}s)")
        return token


# =========================
# IMAGE + PARSING UTILS
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


def parse_eur_amount(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            return float(value)
        except:
            return None

    s = str(value)
    s = s.replace("\u20ac", "").replace("€", "")
    s = s.replace("\xa0", " ").strip()
    s = s.replace(".", "").replace(",", ".").strip()
    try:
        return float(s)
    except:
        return None


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
# CREATORS API
# =========================
# Qui la magia: lista resources a livelli, con fallback se 400.
# Molti account/marketplace NON accettano alcune resources (da qui il tuo errore).
RESOURCES_LEVELS = [
    # Level 1: completo (se passa, ottimo)
    [
        "images.primary.large",
        "itemInfo.title",
        "offersV2.listings.price",
        "offersV2.listings.savingBasis",
    ],
    # Level 2: prezzi senza savingBasis (più permissivo)
    [
        "images.primary.large",
        "itemInfo.title",
        "offersV2.listings.price",
    ],
    # Level 3: minimo sicuro (quasi sempre accettato)
    [
        "images.primary.large",
        "itemInfo.title",
    ],
]


def creators_search_items(keyword: str, page: int, resources):
    token = get_access_token()

    headers = {
        "Authorization": f"Bearer {token}, Version {CREATORS_CREDENTIAL_VERSION}",
        "Content-Type": "application/json",
        "x-marketplace": CREATORS_MARKETPLACE,
    }

    body = {
        "keywords": keyword,
        "partnerTag": AMAZON_ASSOCIATE_TAG,
        "marketplace": CREATORS_MARKETPLACE,
        "resources": resources,
        "page": page,
        "itemCount": ITEMS_PER_PAGE,
    }

    r = requests.post(CREATORS_SEARCH_URL, headers=headers, json=body, timeout=25)

    if r.status_code == 429:
        time.sleep(2.0)

    if r.status_code != 200:
        raise RuntimeError(f"Creators API error {r.status_code}: {r.text}")

    return r.json()


def creators_search_items_with_fallback(keyword: str, page: int):
    last_err = None
    for i, res in enumerate(RESOURCES_LEVELS, start=1):
        try:
            _debug(f"SearchItems try L{i} resources={res}")
            data = creators_search_items(keyword, page, res)
            return data, res
        except Exception as e:
            last_err = e
            msg = str(e)
            # Se è proprio l’errore “resources constraint”, scalo livello
            if "validation error" in msg and "resources" in msg:
                _debug(f"SearchItems L{i} rejected by API (resources) -> fallback")
                continue
            # Altri errori: esco subito
            raise
    # se tutti falliscono
    raise last_err


def extract_from_item_dict(item: dict):
    asin = (item.get("asin") or "").strip().upper()
    if not asin:
        return None

    title = (((item.get("itemInfo") or {}).get("title") or {}).get("displayValue") or "").strip()
    title = " ".join(title.split())

    detail_url = item.get("detailPageURL")
    if not detail_url:
        detail_url = f"https://www.amazon.it/dp/{asin}?tag={AMAZON_ASSOCIATE_TAG}"

    # image
    img_url = None
    images = item.get("images") or {}
    primary = images.get("primary") or {}
    large = primary.get("large") or {}
    img_url = large.get("url") or "https://m.media-amazon.com/images/I/71bhWgQK-cL._AC_SL1500_.jpg"

    # offers
    offersv2 = item.get("offersV2") or {}
    listings = offersv2.get("listings") or []
    if not listings:
        return {
            "asin": asin,
            "title": title,
            "url": detail_url,
            "url_img": img_url,
            "price_new": None,
            "price_old": None,
            "discount": 0,
        }

    l0 = listings[0] or {}

    # price
    price_val = None
    price_obj = l0.get("price") or {}
    if isinstance(price_obj, dict):
        # preferisci money.amount se c’è
        money = price_obj.get("money") or {}
        price_val = parse_eur_amount(money.get("amount"))
        if price_val is None:
            price_val = parse_eur_amount(money.get("displayAmount"))
        if price_val is None:
            price_val = parse_eur_amount(price_obj.get("displayAmount"))
    else:
        price_val = parse_eur_amount(price_obj)

    if price_val is None:
        return {
            "asin": asin,
            "title": title,
            "url": detail_url,
            "url_img": img_url,
            "price_new": None,
            "price_old": None,
            "discount": 0,
        }

    # savingBasis (old price) se presente
    old_val = None
    sb = l0.get("savingBasis") or {}
    if isinstance(sb, dict):
        sb_money = sb.get("money") or {}
        old_val = parse_eur_amount(sb_money.get("amount"))
        if old_val is None:
            old_val = parse_eur_amount(sb_money.get("displayAmount"))

    # discount calcolata
    disc_pct = 0
    if old_val is not None and old_val > 0 and old_val >= price_val:
        try:
            disc_pct = int(round((1 - (price_val / old_val)) * 100))
        except:
            disc_pct = 0
    else:
        old_val = price_val
        disc_pct = 0

    return {
        "asin": asin,
        "title": title,
        "url": detail_url,
        "url_img": img_url,
        "price_new": price_val,
        "price_old": old_val,
        "discount": int(disc_pct or 0),
    }


# =========================
# CORE LOGIC
# =========================

def _first_valid_item_for_keyword(kw, pubblicati):
    reasons = Counter()

    for page in range(1, PAGES + 1):
        try:
            data, used_res = creators_search_items_with_fallback(kw, page)
            sr = data.get("searchResult") or {}
            items = sr.get("items") or []

            _debug(f"kw={kw} page={page} items={len(items)} used_resources={used_res}")

            if DEBUG_AMAZON and items:
                _debug(f"searchItems raw keys: {list((data.get('searchResult') or {}).keys())}")
                _debug(f"searchItems raw preview: {items[0]}")

        except Exception as e:
            reasons["api_error"] += 1
            print(f"❌ Creators searchItems error (kw='{kw}', page={page}): {repr(e)}")
            continue

        for item in items:
            payload = extract_from_item_dict(item)
            if not payload:
                reasons["no_payload"] += 1
                continue

            asin = payload["asin"]
            if asin in pubblicati or not can_post(asin, hours=24):
                reasons["already_posted"] += 1
                continue

            price_val = payload["price_new"]
            disc = payload["discount"]

            if price_val is None:
                reasons["no_price"] += 1
                if DEBUG_AMAZON:
                    offers = (item.get("offersV2") or {}).get("listings") or []
                    _debug(f"asin={asin} no_price | has_offers={bool(offers)}")
                continue

            if price_val < MIN_PRICE or price_val > MAX_PRICE:
                reasons["price_out_range"] += 1
                continue

            if disc < MIN_DISCOUNT:
                reasons["disc_too_low"] += 1
                continue

            payload["minimo"] = disc >= 30
            t = payload["title"] or ""
            payload["title"] = t[:80].strip() + ("…" if len(t) > 80 else "")

            _debug(f"FOUND asin={asin} price={price_val} disc={disc}")
            return payload

    _debug(f"kw={kw} reasons={dict(reasons)}")
    return None


def invia_offerta():
    if bot is None:
        raise RuntimeError("TELEGRAM_BOT_TOKEN mancante o bot non inizializzato.")

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
    minimo = payload.get("minimo", False)
    asin = payload["asin"]

    immagine = genera_immagine_offerta(
        titolo,
        prezzo_nuovo_val,
        prezzo_vecchio_val,
        sconto,
        url_img,
        minimo,
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


def start_scheduler():
    schedule.clear()
    schedule.every().monday.at("06:59").do(resetta_pubblicati)
    schedule.every(14).minutes.do(run_if_in_fascia_oraria)
    while True:
        schedule.run_pending()
        time.sleep(5)
