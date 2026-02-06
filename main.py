import os
import time
import html
import base64
import schedule
import requests

from io import BytesIO
from pathlib import Path
from datetime import datetime, timedelta
from collections import Counter

from PIL import Image, ImageDraw, ImageFont
from telegram import Bot, InlineKeyboardMarkup, InlineKeyboardButton


# =========================
# ENV / CONFIG
# =========================

# Creators API credentials (NO DEFAULTS: mettile su Render env vars)
CREATORS_CREDENTIAL_ID = os.environ.get("CREATORS_CREDENTIAL_ID", "").strip()
CREATORS_CREDENTIAL_SECRET = os.environ.get("CREATORS_CREDENTIAL_SECRET", "").strip()
CREATORS_CREDENTIAL_VERSION = os.environ.get("CREATORS_CREDENTIAL_VERSION", "").strip()
CREATORS_MARKETPLACE = os.environ.get("CREATORS_MARKETPLACE", "www.amazon.it").strip()

# Partner tag
AMAZON_ASSOCIATE_TAG = os.environ.get("AMAZON_ASSOCIATE_TAG", "").strip()

# Telegram
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

# Assets
FONT_PATH = os.environ.get("FONT_PATH", "Montserrat-VariableFont_wght.ttf")
LOGO_PATH = os.environ.get("LOGO_PATH", "header_clean2.png")
BADGE_PATH = os.environ.get("BADGE_PATH", "minimo storico flat.png")

# Filters
MIN_DISCOUNT = int(os.environ.get("MIN_DISCOUNT", "15"))
MIN_PRICE = float(os.environ.get("MIN_PRICE", "15"))
MAX_PRICE = float(os.environ.get("MAX_PRICE", "1900"))

# Debug
DEBUG_AMAZON = os.environ.get("DEBUG_AMAZON", "0") == "1"

# Paging
ITEMS_PER_PAGE = 8
PAGES = 4

# Your keyword rotation
KEYWORDS = [
    "Apple", "Android", "iPhone", "MacBook", "tablet", "smartwatch",
    "auricolari Bluetooth", "smart TV", "monitor PC", "notebook",
    "gaming mouse", "gaming tastiera", "console", "soundbar", "smart home",
    "aspirapolvere robot", "telecamere WiFi", "caricatore wireless",
    "accessori smartphone", "accessori iPhone",
]

# Storage
DATA_DIR = "/tmp/botdata"
Path(DATA_DIR).mkdir(parents=True, exist_ok=True)

PUB_FILE = os.path.join(DATA_DIR, "pubblicati.txt")
PUB_TS = os.path.join(DATA_DIR, "pubblicati_ts.csv")
KW_INDEX = os.path.join(DATA_DIR, "kw_index.txt")


# =========================
# HARD FAIL EARLY if missing env
# =========================
def _require_env():
    missing = []
    if not CREATORS_CREDENTIAL_ID: missing.append("CREATORS_CREDENTIAL_ID")
    if not CREATORS_CREDENTIAL_SECRET: missing.append("CREATORS_CREDENTIAL_SECRET")
    if not CREATORS_CREDENTIAL_VERSION: missing.append("CREATORS_CREDENTIAL_VERSION")
    if not AMAZON_ASSOCIATE_TAG: missing.append("AMAZON_ASSOCIATE_TAG")
    if not TELEGRAM_BOT_TOKEN: missing.append("TELEGRAM_BOT_TOKEN")
    if not TELEGRAM_CHAT_ID: missing.append("TELEGRAM_CHAT_ID")
    if missing:
        raise RuntimeError(f"Missing env vars: {', '.join(missing)}")

_require_env()

bot = Bot(token=TELEGRAM_BOT_TOKEN)


# =========================
# CREATORS API ENDPOINTS
# =========================

# In base alla guida Amazon, il token endpoint è regionale.
# Se il tuo dashboard/guida ti dà un token endpoint diverso, impostalo via env:
# CREATORS_TOKEN_URL="https://creatorsapi.auth.eu-south-2.amazoncognito.com/oauth2/token"
CREATORS_TOKEN_URL = os.environ.get(
    "CREATORS_TOKEN_URL",
    "https://creatorsapi.auth.eu-south-2.amazoncognito.com/oauth2/token"
).strip()

CREATORS_SEARCH_URL = "https://creatorsapi.amazon/catalog/v1/searchItems"

# Resources: usa SOLO quelli che risultano ammessi nei tuoi errori.
# Set MINIMO (titolo + immagine + prezzo). Niente summaries, niente savingBasis.
CREATORS_RESOURCES = [
    "itemInfo.title",
    "images.primary.large",
    "offersV2.listings.price",
    # opzionali ma ammessi e spesso utili:
    "offersV2.listings.dealDetails",
    "offersV2.listings.availability",
]


# =========================
# TOKEN CACHE (1h)
# =========================
_token_value = None
_token_expiry_utc = None  # datetime

def _get_access_token():
    global _token_value, _token_expiry_utc

    # token ancora valido? (margine 60s)
    if _token_value and _token_expiry_utc:
        if datetime.utcnow() < (_token_expiry_utc - timedelta(seconds=60)):
            return _token_value

    basic = base64.b64encode(
        f"{CREATORS_CREDENTIAL_ID}:{CREATORS_CREDENTIAL_SECRET}".encode("utf-8")
    ).decode("ascii")

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": f"Basic {basic}",
    }
    data = "grant_type=client_credentials&scope=creatorsapi/default"

    if DEBUG_AMAZON:
        print(f"[DEBUG] Token refresh -> {CREATORS_TOKEN_URL}")

    r = requests.post(CREATORS_TOKEN_URL, headers=headers, data=data, timeout=20)
    if r.status_code != 200:
        raise RuntimeError(f"Token error {r.status_code}: {r.text}")

    js = r.json()
    token = js.get("access_token")
    expires = int(js.get("expires_in", 3600))

    if not token:
        raise RuntimeError(f"Token missing in response: {js}")

    _token_value = token
    _token_expiry_utc = datetime.utcnow() + timedelta(seconds=expires)

    if DEBUG_AMAZON:
        print(f"[DEBUG] Token OK (expires_in={expires}s)")

    return _token_value


# =========================
# UTILS
# =========================
def parse_eur_amount(display_amount):
    if not display_amount:
        return None
    s = str(display_amount).replace("€", "").replace("\u20ac", "")
    s = s.replace("\xa0", " ").strip()
    # 1.299,00 -> 1299.00
    s = s.replace(".", "").replace(",", ".").strip()
    try:
        return float(s)
    except:
        return None

def deep_find_max_percent(obj):
    """
    Estrae un possibile "percent" da dealDetails (se presente).
    Cerca ricorsivamente chiavi tipo percent/percentage e ritorna max int trovato.
    """
    best = 0

    def walk(x):
        nonlocal best
        if isinstance(x, dict):
            for k, v in x.items():
                lk = str(k).lower()
                if "percent" in lk or "percentage" in lk:
                    try:
                        n = int(str(v).replace("%", "").strip())
                        if n > best:
                            best = n
                    except:
                        pass
                walk(v)
        elif isinstance(x, list):
            for it in x:
                walk(it)

    walk(obj)
    return best

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

    response = requests.get(url_img, timeout=20)
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
# PUBBLICATI tracking
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


# =========================
# KEYWORD rotation
# =========================
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
# CREATORS API CALL
# =========================
def creators_search_items(kw, page):
    token = _get_access_token()

    headers = {
        # Formato richiesto dalla guida: "Bearer <token>, Version <version>"
        "Authorization": f"Bearer {token}, Version {CREATORS_CREDENTIAL_VERSION}",
        "Content-Type": "application/json",
        "x-marketplace": CREATORS_MARKETPLACE,
    }

    body = {
        "keywords": kw,
        "partnerTag": AMAZON_ASSOCIATE_TAG,
        "marketplace": CREATORS_MARKETPLACE,
        "resources": CREATORS_RESOURCES,
        "itemCount": ITEMS_PER_PAGE,
        "itemPage": page,
    }

    r = requests.post(CREATORS_SEARCH_URL, headers=headers, json=body, timeout=25)

    # rate limit/backoff
    if r.status_code in (429, 503):
        if DEBUG_AMAZON:
            print(f"[DEBUG] Creators throttled {r.status_code}. Sleep 2s.")
        time.sleep(2.0)
        return None

    if r.status_code != 200:
        raise RuntimeError(f"Creators API error {r.status_code}: {r.text}")

    return r.json()


def _extract_from_creators_item(item):
    """
    Estrae asin, titolo, immagine, prezzo, sconto (se possibile).
    Struttura robusta: la risposta è JSON.
    """
    asin = (item.get("asin") or "").strip().upper()
    if not asin:
        return None

    # title
    title = ""
    try:
        title = item["itemInfo"]["title"]["displayValue"]
    except:
        title = ""
    title = " ".join(str(title).split()).strip()

    # image
    url_img = None
    try:
        url_img = item["images"]["primary"]["large"]["url"]
    except:
        url_img = None
    if not url_img:
        url_img = "https://m.media-amazon.com/images/I/71bhWgQK-cL._AC_SL1500_.jpg"

    # offersV2 listings
    price_val = None
    deal_details = None
    try:
        listings = item.get("offersV2", {}).get("listings", []) or []
        l0 = listings[0] if listings else None
        if l0:
            # price
            display_amount = (
                l0.get("price", {}).get("displayAmount")
                or l0.get("price", {}).get("display_amount")
                or l0.get("price", {}).get("amount")  # fallback
            )
            price_val = parse_eur_amount(display_amount)

            # dealDetails (per ricavare percentuale)
            deal_details = l0.get("dealDetails")
    except:
        pass

    if price_val is None:
        return None

    # discount (best effort)
    disc = deep_find_max_percent(deal_details) if deal_details else 0

    # se non troviamo sconto, per evitare “prodotti random” scartiamo (deal-bot ≠ catalogo)
    if disc <= 0:
        return None

    # old price stimato (non sempre disponibile)
    try:
        old_val = price_val / (1 - disc / 100.0)
    except:
        old_val = price_val

    return {
        "asin": asin,
        "title": title[:80].strip() + ("…" if len(title) > 80 else ""),
        "price_new": price_val,
        "price_old": old_val,
        "discount": disc,
        "url_img": url_img,
        "url": f"https://www.amazon.it/dp/{asin}?tag={AMAZON_ASSOCIATE_TAG}",
        "minimo": disc >= 30,
    }


def _first_valid_item_for_keyword(kw, pubblicati):
    reasons = Counter()

    for page in range(1, PAGES + 1):
        try:
            js = creators_search_items(kw, page)
            if js is None:
                reasons["throttled"] += 1
                continue

            items = js.get("items", []) or []
            if DEBUG_AMAZON:
                print(f"[DEBUG] kw={kw} page={page} items={len(items)}")

        except Exception as e:
            reasons["api_error"] += 1
            print(f"❌ Creators searchItems error (kw='{kw}', page={page}): {repr(e)}")
            continue

        for raw in items:
            payload = _extract_from_creators_item(raw)
            if not payload:
                reasons["no_price_or_no_discount"] += 1
                continue

            asin = payload["asin"]
            if asin in pubblicati or not can_post(asin, hours=24):
                reasons["already_posted"] += 1
                continue

            if payload["price_new"] < MIN_PRICE or payload["price_new"] > MAX_PRICE:
                reasons["price_out_range"] += 1
                continue

            if payload["discount"] < MIN_DISCOUNT:
                reasons["disc_too_low"] += 1
                continue

            if DEBUG_AMAZON:
                print(f"[DEBUG] FOUND asin={asin} price={payload['price_new']} disc={payload['discount']}")

            return payload

    if DEBUG_AMAZON:
        print(f"[DEBUG] kw={kw} reasons={dict(reasons)}")

    return None


# =========================
# SEND OFFER
# =========================
def invia_offerta():
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

    caption_parts = [f"<b>{safe_title}</b>"]
    if minimo and sconto >= 30:
        caption_parts.append("<b>MINIMO STORICO</b>")

    caption_parts.append(
        f"💶 <b>{prezzo_nuovo_val:.2f}€</b> (stimato da sconto <b>-{sconto}%</b>)"
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
# TIME WINDOW IT
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
