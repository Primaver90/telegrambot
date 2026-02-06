import os
import time
import html
import base64
import random
from io import BytesIO
from pathlib import Path
from datetime import datetime, timedelta
from collections import Counter

import schedule
import requests
from PIL import Image, ImageDraw, ImageFont
from telegram import Bot, InlineKeyboardMarkup, InlineKeyboardButton


# =========================
# ENV / CONFIG
# =========================
AMAZON_ASSOCIATE_TAG = os.environ.get("AMAZON_ASSOCIATE_TAG", "").strip()
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

CREATORS_CREDENTIAL_ID = os.environ.get("CREATORS_CREDENTIAL_ID", "").strip()
CREATORS_CREDENTIAL_SECRET = os.environ.get("CREATORS_CREDENTIAL_SECRET", "").strip()
CREATORS_CREDENTIAL_VERSION = os.environ.get("CREATORS_CREDENTIAL_VERSION", "").strip()
CREATORS_MARKETPLACE = os.environ.get("CREATORS_MARKETPLACE", "www.amazon.it").strip()

# Regione token endpoint (di solito: eu-south-2 o us-west-2 a seconda di cosa ti mostra Amazon)
CREATORS_REGION = os.environ.get("CREATORS_REGION", "eu-south-2").strip()

DEBUG_AMAZON = os.environ.get("DEBUG_AMAZON", "0") == "1"

MIN_DISCOUNT = int(os.environ.get("MIN_DISCOUNT", "15"))
MIN_PRICE = float(os.environ.get("MIN_PRICE", "15"))
MAX_PRICE = float(os.environ.get("MAX_PRICE", "1900"))

# Quanti ASIN max proviamo in fallback GetItems
GETITEMS_FALLBACK_MAX = int(os.environ.get("GETITEMS_FALLBACK_MAX", "4"))

# keyword rotation
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

FONT_PATH = os.environ.get("FONT_PATH", "Montserrat-VariableFont_wght.ttf")
LOGO_PATH = os.environ.get("LOGO_PATH", "header_clean2.png")
BADGE_PATH = os.environ.get("BADGE_PATH", "minimo storico flat.png")

# Persistenza: se /data esiste (disco Render), usalo. Altrimenti /tmp.
DEFAULT_DATA_DIR = "/data/botdata" if os.path.isdir("/data") else "/tmp/botdata"
DATA_DIR = os.environ.get("DATA_DIR", DEFAULT_DATA_DIR)
Path(DATA_DIR).mkdir(parents=True, exist_ok=True)

PUB_FILE = os.path.join(DATA_DIR, "pubblicati.txt")
PUB_TS = os.path.join(DATA_DIR, "pubblicati_ts.csv")
KW_INDEX = os.path.join(DATA_DIR, "kw_index.txt")


# =========================
# BASIC CHECKS
# =========================
if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    print("⚠️ TELEGRAM_BOT_TOKEN o TELEGRAM_CHAT_ID mancanti nelle env vars.")
if not AMAZON_ASSOCIATE_TAG:
    print("⚠️ AMAZON_ASSOCIATE_TAG mancante nelle env vars.")
if not (CREATORS_CREDENTIAL_ID and CREATORS_CREDENTIAL_SECRET and CREATORS_CREDENTIAL_VERSION):
    print("⚠️ Credenziali Creators API mancanti (ID/SECRET/VERSION).")


bot = Bot(token=TELEGRAM_BOT_TOKEN)

# =========================
# CREATORS API ENDPOINTS
# =========================
TOKEN_URL = f"https://creatorsapi.auth.{CREATORS_REGION}.amazoncognito.com/oauth2/token"
CATALOG_BASE = "https://creatorsapi.amazon/catalog/v1"
SEARCH_URL = f"{CATALOG_BASE}/searchItems"
GETITEMS_URL = f"{CATALOG_BASE}/getItems"

# Resources "sicure" (dal set accettato che si vede nel tuo errore)
SEARCH_RESOURCES = [
    "itemInfo.title",
    "images.primary.large",
    "offersV2.listings.price",
    "offersV2.listings.dealDetails",
    "offersV2.listings.availability",
]

GETITEMS_RESOURCES = [
    "itemInfo.title",
    "images.primary.large",
    "offersV2.listings.price",
    "offersV2.listings.dealDetails",
    "offersV2.listings.availability",
]

# Token cache in-memory
_token_cache = {"access_token": None, "expires_at": 0}


def _now():
    return int(time.time())


def _debug(msg: str):
    if DEBUG_AMAZON:
        print(f"[DEBUG] {msg}")


def get_access_token(force_refresh=False):
    """
    OAuth2 client_credentials con caching (1h).
    """
    if not force_refresh:
        tok = _token_cache.get("access_token")
        exp = _token_cache.get("expires_at", 0)
        if tok and _now() < (exp - 60):  # 60s di margine
            return tok

    if not (CREATORS_CREDENTIAL_ID and CREATORS_CREDENTIAL_SECRET):
        raise RuntimeError("Creators API credentials mancanti (CREATORS_CREDENTIAL_ID/SECRET).")

    data = {
        "grant_type": "client_credentials",
        "scope": "creatorsapi/default",
    }

    # requests gestisce Basic auth correttamente
    r = requests.post(
        TOKEN_URL,
        data=data,
        auth=(CREATORS_CREDENTIAL_ID, CREATORS_CREDENTIAL_SECRET),
        timeout=20,
    )

    if r.status_code != 200:
        raise RuntimeError(f"Token error {r.status_code}: {r.text}")

    payload = r.json()
    access_token = payload.get("access_token")
    expires_in = int(payload.get("expires_in", 3600) or 3600)

    if not access_token:
        raise RuntimeError(f"Token response senza access_token: {payload}")

    _token_cache["access_token"] = access_token
    _token_cache["expires_at"] = _now() + expires_in

    _debug(f"Token OK (expires_in={expires_in}s) url={TOKEN_URL}")
    return access_token


def creators_headers(token: str):
    # Formato richiesto: "Bearer <token>, Version <version>"
    return {
        "Authorization": f"Bearer {token}, Version {CREATORS_CREDENTIAL_VERSION}",
        "Content-Type": "application/json",
        "x-marketplace": CREATORS_MARKETPLACE,
    }


def _post_with_backoff(url, headers, json_body, max_retries=3):
    """
    Backoff semplice per 429/5xx.
    """
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.post(url, headers=headers, json=json_body, timeout=25)
            if r.status_code in (429, 500, 502, 503, 504):
                sleep_s = min(8, 1.5 * attempt) + random.random()
                _debug(f"{url} -> {r.status_code}, retry in {sleep_s:.2f}s (attempt {attempt})")
                time.sleep(sleep_s)
                last_err = RuntimeError(f"HTTP {r.status_code}: {r.text}")
                continue

            if r.status_code != 200:
                raise RuntimeError(f"HTTP {r.status_code}: {r.text}")

            return r.json()

        except Exception as e:
            last_err = e
            sleep_s = min(8, 1.2 * attempt) + random.random()
            _debug(f"{url} exception {repr(e)} retry in {sleep_s:.2f}s (attempt {attempt})")
            time.sleep(sleep_s)

    raise RuntimeError(f"Request failed after retries: {repr(last_err)}")


def creators_search_items(kw, page):
    token = get_access_token()
    body = {
        "keywords": kw,
        "partnerTag": AMAZON_ASSOCIATE_TAG,
        "marketplace": CREATORS_MARKETPLACE,
        "resources": SEARCH_RESOURCES,
        "itemCount": ITEMS_PER_PAGE,
        "itemPage": page,
    }
    return _post_with_backoff(SEARCH_URL, creators_headers(token), body)


def creators_get_items(asins):
    token = get_access_token()
    body = {
        "itemIds": asins,
        "partnerTag": AMAZON_ASSOCIATE_TAG,
        "marketplace": CREATORS_MARKETPLACE,
        "resources": GETITEMS_RESOURCES,
    }
    return _post_with_backoff(GETITEMS_URL, creators_headers(token), body)


# =========================
# PARSING HELPERS
# =========================
def parse_amount(obj):
    """
    Accetta:
    - numero
    - stringa tipo "€ 19,99"
    - dict con {amount: 19.99} o {displayAmount: "..."} ecc.
    """
    if obj is None:
        return None

    if isinstance(obj, (int, float)):
        return float(obj)

    if isinstance(obj, str):
        s = obj.replace("\u20ac", "").replace("€", "").replace("\xa0", " ").strip()
        s = s.replace(".", "").replace(",", ".").strip()
        try:
            return float(s)
        except:
            return None

    if isinstance(obj, dict):
        if "amount" in obj and isinstance(obj["amount"], (int, float, str)):
            return parse_amount(obj["amount"])
        if "displayAmount" in obj:
            return parse_amount(obj["displayAmount"])
        if "display_amount" in obj:
            return parse_amount(obj["display_amount"])
        if "value" in obj:
            return parse_amount(obj["value"])

    return None


def first_listing_offersv2(item):
    offers = item.get("offersV2") or {}
    listings = offers.get("listings") or []
    if listings and isinstance(listings, list):
        return listings[0]
    return None


def extract_title(item):
    info = item.get("itemInfo") or {}
    t = info.get("title") or {}
    # a volte può essere stringa diretta
    if isinstance(t, str):
        return " ".join(t.split())
    if isinstance(t, dict):
        return " ".join(str(t.get("displayValue") or t.get("value") or "").split())
    return ""


def extract_image(item):
    images = item.get("images") or {}
    primary = images.get("primary") or {}
    large = primary.get("large") or {}
    url = large.get("url")
    if not url:
        # fallback
        return "https://m.media-amazon.com/images/I/71bhWgQK-cL._AC_SL1500_.jpg"
    return url


def extract_price_discount(item):
    """
    Ritorna: (price_new, price_old, discount_percent)
    - price_new: da offersV2.listings.price
    - discount: prova da dealDetails (se presente)
    - price_old: se esiste savingBasis/listPrice dentro dealDetails
    Se non trovo sconto/vecchio, discount=0 e old=new.
    """
    listing = first_listing_offersv2(item)
    if not listing:
        return None, None, None

    price = listing.get("price") or {}
    price_new = parse_amount(price)

    if price_new is None:
        return None, None, None

    deal = listing.get("dealDetails") or {}

    # Tentativi robusti:
    # 1) percent esplicita
    disc = deal.get("percentage") or deal.get("percentOff") or deal.get("percentageOff")
    disc_val = None
    if disc is not None:
        try:
            disc_val = int(float(disc))
        except:
            disc_val = None

    # 2) savings amount + savingBasis / listPrice
    saving_basis = deal.get("savingBasis") or deal.get("listPrice") or deal.get("wasPrice")
    price_old = parse_amount(saving_basis)

    savings_amount = deal.get("savingsAmount") or deal.get("savings") or deal.get("amountSaved")
    savings_val = parse_amount(savings_amount)

    if price_old is None and savings_val is not None:
        price_old = price_new + savings_val

    if disc_val is None and price_old is not None and price_old > 0:
        disc_val = int(round((1 - (price_new / price_old)) * 100))

    if disc_val is None:
        disc_val = 0
    if price_old is None:
        price_old = price_new

    return price_new, price_old, disc_val


# =========================
# IMAGE GENERATION
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
# CORE LOGIC
# =========================
def _first_valid_item_for_keyword(kw, pubblicati):
    reasons = Counter()
    fallback_asins = []

    for page in range(1, PAGES + 1):
        try:
            data = creators_search_items(kw, page)
            items = data.get("items") or []
            _debug(f"kw={kw} page={page} items={len(items)}")
        except Exception as e:
            reasons["api_error"] += 1
            print(f"❌ Creators searchItems error (kw='{kw}', page={page}): {repr(e)}")
            items = []

        for item in items:
            asin = (item.get("asin") or item.get("itemId") or "").strip().upper()
            if not asin:
                reasons["no_asin"] += 1
                continue
            if asin in pubblicati or not can_post(asin, hours=24):
                reasons["already_posted"] += 1
                continue

            title = extract_title(item)
            if not title:
                reasons["no_title"] += 1
                continue

            price_new, price_old, disc = extract_price_discount(item)
            if price_new is None:
                reasons["no_price_in_search"] += 1
                if len(fallback_asins) < GETITEMS_FALLBACK_MAX:
                    fallback_asins.append(asin)
                continue

            if price_new < MIN_PRICE or price_new > MAX_PRICE:
                reasons["price_out_range"] += 1
                continue

            if disc < MIN_DISCOUNT:
                reasons["disc_too_low"] += 1
                continue

            url_img = extract_image(item)
            url = item.get("detailPageUrl") or item.get("detail_page_url") or f"https://www.amazon.it/dp/{asin}?tag={AMAZON_ASSOCIATE_TAG}"
            minimo = disc >= 30

            _debug(f"FOUND via SearchItems asin={asin} price={price_new} old={price_old} disc={disc}")

            return {
                "asin": asin,
                "title": title[:80].strip() + ("…" if len(title) > 80 else ""),
                "price_new": float(price_new),
                "price_old": float(price_old),
                "discount": int(disc),
                "url_img": url_img,
                "url": url,
                "minimo": minimo,
            }

    # Fallback: GetItems su pochi ASIN se SearchItems non dà prezzo
    if fallback_asins:
        try:
            data = creators_get_items(fallback_asins)
            items = data.get("items") or []
            _debug(f"GetItems fallback asins={fallback_asins} items={len(items)}")

            for item in items:
                asin = (item.get("asin") or item.get("itemId") or "").strip().upper()
                if not asin or asin in pubblicati or not can_post(asin, hours=24):
                    continue

                title = extract_title(item)
                price_new, price_old, disc = extract_price_discount(item)
                if price_new is None:
                    continue
                if price_new < MIN_PRICE or price_new > MAX_PRICE:
                    continue
                if disc < MIN_DISCOUNT:
                    continue

                url_img = extract_image(item)
                url = item.get("detailPageUrl") or item.get("detail_page_url") or f"https://www.amazon.it/dp/{asin}?tag={AMAZON_ASSOCIATE_TAG}"
                minimo = disc >= 30

                _debug(f"FOUND via GetItems asin={asin} price={price_new} old={price_old} disc={disc}")

                return {
                    "asin": asin,
                    "title": title[:80].strip() + ("…" if len(title) > 80 else ""),
                    "price_new": float(price_new),
                    "price_old": float(price_old),
                    "discount": int(disc),
                    "url_img": url_img,
                    "url": url,
                    "minimo": minimo,
                }

        except Exception as e:
            reasons["getitems_error"] += 1
            print(f"❌ Creators getItems fallback error: {repr(e)}")

    _debug(f"kw={kw} reasons={dict(reasons)} fallback_asins={fallback_asins}")
    return None


def invia_offerta():
    pubblicati = load_pubblicati()
    kw = pick_keyword()

    payload = _first_valid_item_for_keyword(kw, pubblicati)
    if not payload:
        print("⚠️ Nessuna offerta valida trovata (filtri/duplicati o prezzi non disponibili). Riprova tra poco.")
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


def start_scheduler():
    schedule.clear()
    schedule.every().monday.at("06:59").do(resetta_pubblicati)
    schedule.every(14).minutes.do(run_if_in_fascia_oraria)
    while True:
        schedule.run_pending()
        time.sleep(5)
