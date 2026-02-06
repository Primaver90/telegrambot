import os
import time
import html
import json
import base64
import threading
from io import BytesIO
from pathlib import Path
from datetime import datetime, timedelta

import schedule
import requests
from PIL import Image, ImageDraw, ImageFont
from telegram import Bot, InlineKeyboardMarkup, InlineKeyboardButton

# =========================
# ENV
# =========================
CREATORS_CREDENTIAL_ID = os.environ.get("CREATORS_CREDENTIAL_ID", "").strip()
CREATORS_CREDENTIAL_SECRET = os.environ.get("CREATORS_CREDENTIAL_SECRET", "").strip()
CREATORS_CREDENTIAL_VERSION = os.environ.get("CREATORS_CREDENTIAL_VERSION", "").strip()  # es: "2.2"
CREATORS_MARKETPLACE = os.environ.get("CREATORS_MARKETPLACE", "www.amazon.it").strip()

AMAZON_ASSOCIATE_TAG = os.environ.get("AMAZON_ASSOCIATE_TAG", "itech00-21").strip()

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

FONT_PATH = os.environ.get("FONT_PATH", "Montserrat-VariableFont_wght.ttf")
LOGO_PATH = os.environ.get("LOGO_PATH", "header_clean2.png")
BADGE_PATH = os.environ.get("BADGE_PATH", "minimo storico flat.png")

MIN_DISCOUNT = int(os.environ.get("MIN_DISCOUNT", "15"))
MIN_PRICE = float(os.environ.get("MIN_PRICE", "15"))
MAX_PRICE = float(os.environ.get("MAX_PRICE", "1900"))

DEBUG_AMAZON = os.environ.get("DEBUG_AMAZON", "0") == "1"

# =========================
# BOT DATA
# =========================
DATA_DIR = "/tmp/botdata"
Path(DATA_DIR).mkdir(parents=True, exist_ok=True)

PUB_FILE = os.path.join(DATA_DIR, "pubblicati.txt")
PUB_TS = os.path.join(DATA_DIR, "pubblicati_ts.csv")
KW_INDEX = os.path.join(DATA_DIR, "kw_index.txt")

KEYWORDS = [
    "Apple", "Android", "iPhone", "MacBook", "tablet", "smartwatch",
    "auricolari Bluetooth", "smart TV", "monitor PC", "notebook",
    "gaming mouse", "gaming tastiera", "console", "soundbar", "smart home",
    "aspirapolvere robot", "telecamere WiFi", "caricatore wireless",
    "accessori smartphone", "accessori iPhone",
]

SEARCH_INDEX = "All"
ITEMS_PER_PAGE = 8
PAGES = 4

bot = Bot(token=TELEGRAM_BOT_TOKEN)

# =========================
# Creators API constants
# =========================
CREATORS_BASE_URL = "https://creatorsapi.amazon"
CREATORS_SEARCH_ENDPOINT = f"{CREATORS_BASE_URL}/catalog/v1/searchItems"
CREATORS_GETITEMS_ENDPOINT = f"{CREATORS_BASE_URL}/catalog/v1/getItems"

# Token endpoint dipende dalla "credential version".
# Dal PDF: 2.1 -> us-east-1, 2.2 -> eu-south-2, 2.3 -> us-west-2
TOKEN_ENDPOINTS_BY_VERSION = {
    "2.1": "https://creatorsapi.auth.us-east-1.amazoncognito.com/oauth2/token",
    "2.2": "https://creatorsapi.auth.eu-south-2.amazoncognito.com/oauth2/token",
    "2.3": "https://creatorsapi.auth.us-west-2.amazoncognito.com/oauth2/token",
}

def _token_endpoint():
    # fallback prudente: se non c'è, prova EU (visto marketplace IT)
    return TOKEN_ENDPOINTS_BY_VERSION.get(CREATORS_CREDENTIAL_VERSION, TOKEN_ENDPOINTS_BY_VERSION["2.2"])

# Resources: qui c’è la “magia” per riavere i prezzi.
# Se non chiedi offers/price, spesso offersV2 ti arriva null.
CREATORS_RESOURCES = [
    "images.primary.large",
    "itemInfo.title",

    # offerte / prezzi (V2)
    "offersV2.listings.price",
    "offersV2.listings.savingBasis",
    "offersV2.summaries.lowestPrice",
    "offersV2.summaries.savings",
]

# =========================
# Token cache (thread-safe)
# =========================
_token_lock = threading.Lock()
_token_cache = {
    "access_token": None,
    "expires_at": 0.0,  # epoch seconds
}

def _debug(msg: str):
    if DEBUG_AMAZON:
        print(f"[DEBUG] {msg}", flush=True)

def creators_get_access_token() -> str:
    """
    OAuth2 client_credentials.
    Metodo "Authorization: Basic base64(client_id:client_secret)"
    scope: creatorsapi/default
    Cache 1h (3600s) - rinnovo con margine.
    """
    now = time.time()

    with _token_lock:
        if _token_cache["access_token"] and now < (_token_cache["expires_at"] - 60):
            return _token_cache["access_token"]

        if not CREATORS_CREDENTIAL_ID or not CREATORS_CREDENTIAL_SECRET:
            raise RuntimeError("CreatorsAPI: CREATORS_CREDENTIAL_ID/SECRET mancanti nelle env var.")

        token_url = _token_endpoint()
        _debug(f"Token refresh -> {token_url}")

        basic = base64.b64encode(f"{CREATORS_CREDENTIAL_ID}:{CREATORS_CREDENTIAL_SECRET}".encode("utf-8")).decode("utf-8")
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {basic}",
        }
        data = "grant_type=client_credentials&scope=creatorsapi/default"

        r = requests.post(token_url, headers=headers, data=data, timeout=20)
        if r.status_code != 200:
            # utile per capire invalid_client
            raise RuntimeError(f"Token error {r.status_code}: {r.text}")

        payload = r.json()
        token = payload.get("access_token")
        expires_in = int(payload.get("expires_in", 3600))

        if not token:
            raise RuntimeError(f"Token response senza access_token: {payload}")

        _token_cache["access_token"] = token
        _token_cache["expires_at"] = now + expires_in

        _debug(f"Token OK (expires_in={expires_in}s)")
        return token

def creators_headers() -> dict:
    token = creators_get_access_token()
    # Dal PDF: Authorization: Bearer <token>, Version <credential_version>
    return {
        "Authorization": f"Bearer {token}, Version {CREATORS_CREDENTIAL_VERSION}",
        "Content-Type": "application/json",
        "x-marketplace": CREATORS_MARKETPLACE,
    }

def _request_with_retries(url: str, payload: dict, max_tries: int = 4) -> dict:
    """
    Retry con backoff per 429/5xx + refresh token se serve.
    """
    backoff = 1.0
    last_err = None

    for attempt in range(1, max_tries + 1):
        try:
            h = creators_headers()
            r = requests.post(url, headers=h, json=payload, timeout=25)

            if r.status_code == 401:
                # token scaduto/invalid -> forza refresh
                with _token_lock:
                    _token_cache["access_token"] = None
                    _token_cache["expires_at"] = 0.0
                last_err = f"401 unauthorized: {r.text}"

            elif r.status_code in (429, 500, 502, 503, 504):
                last_err = f"{r.status_code}: {r.text}"

            elif r.status_code != 200:
                raise RuntimeError(f"Creators API error {r.status_code}: {r.text}")

            else:
                return r.json()

        except Exception as e:
            last_err = repr(e)

        _debug(f"Retry {attempt}/{max_tries} on {url} -> {last_err}")
        time.sleep(backoff)
        backoff = min(backoff * 2.0, 10.0)

    raise RuntimeError(f"Creators API request failed after retries: {last_err}")

def creators_search_items(keywords: str, page: int) -> list:
    payload = {
        "keywords": keywords,
        "partnerTag": AMAZON_ASSOCIATE_TAG,
        "marketplace": CREATORS_MARKETPLACE,
        "resources": CREATORS_RESOURCES,
        "itemCount": ITEMS_PER_PAGE,
        "itemPage": page,
        "searchIndex": SEARCH_INDEX,
    }
    data = _request_with_retries(CREATORS_SEARCH_ENDPOINT, payload)

    # struttura tipica: itemsResult.items
    items = (data or {}).get("itemsResult", {}).get("items", []) or []
    return items

def creators_get_items(asins: list) -> list:
    payload = {
        "itemIds": asins,
        "itemIdType": "ASIN",
        "marketplace": CREATORS_MARKETPLACE,
        "partnerTag": AMAZON_ASSOCIATE_TAG,
        "resources": CREATORS_RESOURCES,
    }
    data = _request_with_retries(CREATORS_GETITEMS_ENDPOINT, payload)
    items = (data or {}).get("itemsResult", {}).get("items", []) or []
    return items

# =========================
# Helpers: parsing prezzo/discount
# =========================
def parse_eur_amount(x):
    if not x:
        return None
    s = str(x)
    s = s.replace("\u20ac", "").replace("€", "")
    s = s.replace("\xa0", " ").strip()
    s = s.replace(".", "").replace(",", ".").strip()
    try:
        return float(s)
    except:
        return None

def first(lst):
    return (lst or [None])[0]

def extract_from_offersV2(item: dict):
    """
    Prova a prendere prezzo e sconto da offersV2.
    """
    offers = item.get("offersV2") or {}
    listings = offers.get("listings") or []
    l0 = first(listings)
    if not l0:
        return None, 0, None

    price_disp = ((l0.get("price") or {}).get("displayAmount"))
    price_val = parse_eur_amount(price_disp)

    saving_basis_disp = ((l0.get("savingBasis") or {}).get("displayAmount"))
    old_val = parse_eur_amount(saving_basis_disp)

    disc = 0
    summaries = offers.get("summaries") or []
    s0 = first(summaries)
    if s0:
        disc = int(((s0.get("savings") or {}).get("percentage")) or 0)

    # se non c'è disc ma ho old_val, calcolo
    if price_val is not None and old_val is not None and old_val > 0:
        disc = max(disc, int(round((old_val - price_val) / old_val * 100)))

    return price_val, disc, old_val

def extract_title(item: dict) -> str:
    # itemInfo.title.displayValue
    return (((item.get("itemInfo") or {}).get("title") or {}).get("displayValue")) or ""

def extract_image(item: dict) -> str:
    # images.primary.large.url
    imgs = item.get("images") or {}
    primary = imgs.get("primary") or {}
    large = primary.get("large") or {}
    return large.get("url") or ""

def extract_asin(item: dict) -> str:
    return (item.get("asin") or "").strip().upper()

def build_detail_url(asin: str) -> str:
    return f"https://www.amazon.it/dp/{asin}?tag={AMAZON_ASSOCIATE_TAG}"

# =========================
# Immagine offerta
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
# Pubblicati / anti-duplicati
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
# Core: trova prima offerta valida
# =========================
def _first_valid_item_for_keyword(kw, pubblicati):
    fallback_asins = []

    for page in range(1, PAGES + 1):
        try:
            items = creators_search_items(kw, page)
            _debug(f"kw={kw} page={page} items={len(items)}")
        except Exception as e:
            print(f"❌ Creators searchItems error (kw='{kw}', page={page}): {e}", flush=True)
            items = []

        for item in items:
            asin = extract_asin(item)
            if not asin or asin in pubblicati or not can_post(asin, hours=24):
                continue

            title = " ".join(extract_title(item).split())
            url_img = extract_image(item) or "https://m.media-amazon.com/images/I/71bhWgQK-cL._AC_SL1500_.jpg"

            price_val, disc, old_val = extract_from_offersV2(item)
            has_offers = bool(item.get("offersV2"))

            if price_val is None:
                _debug(f"asin={asin} no_price | has_offers={has_offers}")
                # candidati per getItems
                if len(fallback_asins) < 6:
                    fallback_asins.append(asin)
                continue

            if price_val < MIN_PRICE or price_val > MAX_PRICE:
                continue
            if disc < MIN_DISCOUNT:
                continue

            if old_val is None:
                old_val = price_val

            return {
                "asin": asin,
                "title": title[:80].strip() + ("…" if len(title) > 80 else ""),
                "price_new": price_val,
                "price_old": old_val,
                "discount": disc,
                "url_img": url_img,
                "url": build_detail_url(asin),
                "minimo": disc >= 30,
            }

    # fallback GetItems su pochi ASIN se searchItems non porta offers/price
    if fallback_asins:
        try:
            items = creators_get_items(fallback_asins)
            _debug(f"GetItems fallback asins={fallback_asins} items={len(items)}")
            for item in items:
                asin = extract_asin(item)
                if not asin or asin in pubblicati or not can_post(asin, hours=24):
                    continue

                title = " ".join(extract_title(item).split())
                url_img = extract_image(item) or "https://m.media-amazon.com/images/I/71bhWgQK-cL._AC_SL1500_.jpg"

                price_val, disc, old_val = extract_from_offersV2(item)
                if price_val is None:
                    continue

                if price_val < MIN_PRICE or price_val > MAX_PRICE:
                    continue
                if disc < MIN_DISCOUNT:
                    continue

                if old_val is None:
                    old_val = price_val

                return {
                    "asin": asin,
                    "title": title[:80].strip() + ("…" if len(title) > 80 else ""),
                    "price_new": price_val,
                    "price_old": old_val,
                    "discount": disc,
                    "url_img": url_img,
                    "url": build_detail_url(asin),
                    "minimo": disc >= 30,
                }
        except Exception as e:
            print(f"❌ Creators getItems fallback error: {e}", flush=True)

    return None

# =========================
# Invio su Telegram
# =========================
def invia_offerta():
    pubblicati = load_pubblicati()
    kw = pick_keyword()

    payload = _first_valid_item_for_keyword(kw, pubblicati)
    if not payload:
        print(f"⚠️ Nessuna offerta valida trovata per keyword: {kw}", flush=True)
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
        f"💶 A soli <b>{prezzo_nuovo_val:.2f}€</b> invece di "
        f"<s>{prezzo_vecchio_val:.2f}€</s> (<b>-{sconto}%</b>)"
    )
    caption_parts.append(f'<a href="{safe_url}">Acquista ora</a>')

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
    print(f"✅ Pubblicata: {asin} | {kw}", flush=True)
    return True

# =========================
# Fascia oraria IT
# =========================
def is_in_italy_window(now_utc=None):
    if now_utc is None:
        now_utc = datetime.utcnow()

    # CET/CEST "semplice" come avevi già
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
        print(f"⏸ Fuori fascia oraria (Italia {italy_time.strftime('%H:%M')}), nessuna offerta pubblicata.", flush=True)

def start_scheduler():
    schedule.clear()
    schedule.every().monday.at("06:59").do(resetta_pubblicati)
    schedule.every(14).minutes.do(run_if_in_fascia_oraria)
    while True:
        schedule.run_pending()
        time.sleep(5)
