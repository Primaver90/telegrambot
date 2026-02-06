import os
import time
import json
import html
import schedule
import requests

from io import BytesIO
from datetime import datetime, timedelta
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont
from telegram import Bot, InlineKeyboardMarkup, InlineKeyboardButton


# =========================
# ENV + CONFIG
# =========================

# Creators API credentials (metterle su Render -> Environment Variables)
CREATORS_CREDENTIAL_ID = os.environ.get("CREATORS_CREDENTIAL_ID", "").strip()
CREATORS_CREDENTIAL_SECRET = os.environ.get("CREATORS_CREDENTIAL_SECRET", "").strip()
CREATORS_CREDENTIAL_VERSION = os.environ.get("CREATORS_CREDENTIAL_VERSION", "").strip()

# Marketplace IT: www.amazon.it
CREATORS_MARKETPLACE = os.environ.get("CREATORS_MARKETPLACE", "www.amazon.it").strip()

# Endpoints (di default quelli che hai incollato; se Amazon ti dà regionali diversi, li sovrascrivi via env)
TOKEN_ENDPOINT = os.environ.get(
    "TOKEN_ENDPOINT",
    "https://creatorsapi.auth.us-west-2.amazoncognito.com/oauth2/token"
).strip()

CATALOG_ENDPOINT = os.environ.get(
    "CATALOG_ENDPOINT",
    "https://creatorsapi.amazon/catalog/v1"
).strip()

AMAZON_ASSOCIATE_TAG = os.environ.get("AMAZON_ASSOCIATE_TAG", "").strip()

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

FONT_PATH = os.environ.get("FONT_PATH", "Montserrat-VariableFont_wght.ttf")
LOGO_PATH = os.environ.get("LOGO_PATH", "header_clean2.png")
BADGE_PATH = os.environ.get("BADGE_PATH", "minimo storico flat.png")

MIN_DISCOUNT = int(os.environ.get("MIN_DISCOUNT", "15"))
MIN_PRICE = float(os.environ.get("MIN_PRICE", "15"))
MAX_PRICE = float(os.environ.get("MAX_PRICE", "1900"))

DEBUG_AMAZON = os.environ.get("DEBUG_AMAZON", "0") == "1"

# Quante pagine e quanti item per page
ITEMS_PER_PAGE = int(os.environ.get("ITEMS_PER_PAGE", "8"))
PAGES = int(os.environ.get("PAGES", "4"))

# Fallback: quanti itemIds prelevare da search e approfondire con getItems
GETITEMS_FALLBACK_MAX = int(os.environ.get("GETITEMS_FALLBACK_MAX", "6"))

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

# Resources (Creators API usa dot notation in lowerCamel, come da doc)
# Qui metto un set ragionevole per titolo + immagine + offerte/prezzi/sconti.
SEARCH_RESOURCES = [
    "itemInfo.title",
    "images.primary.large",
    "offers.listings.price",
    "offers.listings.savings",
    "offers.listings.savingBasis",
]

GETITEMS_RESOURCES = [
    "itemInfo.title",
    "images.primary.large",
    "offers.listings.price",
    "offers.listings.savings",
    "offers.listings.savingBasis",
]


# =========================
# STORAGE (pubblicati + token cache)
# =========================
DATA_DIR = "/tmp/botdata"
Path(DATA_DIR).mkdir(parents=True, exist_ok=True)

PUB_FILE = os.path.join(DATA_DIR, "pubblicati.txt")
PUB_TS = os.path.join(DATA_DIR, "pubblicati_ts.csv")
KW_INDEX = os.path.join(DATA_DIR, "kw_index.txt")

TOKEN_CACHE_FILE = os.path.join(DATA_DIR, "creators_token.json")


# =========================
# GUARDS
# =========================
def _require_env():
    missing = []
    if not CREATORS_CREDENTIAL_ID:
        missing.append("CREATORS_CREDENTIAL_ID")
    if not CREATORS_CREDENTIAL_SECRET:
        missing.append("CREATORS_CREDENTIAL_SECRET")
    if not CREATORS_CREDENTIAL_VERSION:
        missing.append("CREATORS_CREDENTIAL_VERSION")
    if not AMAZON_ASSOCIATE_TAG:
        missing.append("AMAZON_ASSOCIATE_TAG")
    if not TELEGRAM_BOT_TOKEN:
        missing.append("TELEGRAM_BOT_TOKEN")
    if not TELEGRAM_CHAT_ID:
        missing.append("TELEGRAM_CHAT_ID")

    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")


# =========================
# HELPERS: parsing + safe dict
# =========================
def parse_eur_amount(value):
    """
    Gestisce stringhe tipo:
    - "€ 19,99"
    - "19,99 €"
    - "1.299,00"
    - 1299.00
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)

    s = str(value)
    s = s.replace("\u20ac", "").replace("€", "").replace("\xa0", " ")
    s = s.strip()

    # "1.299,00" -> "1299.00"
    s = s.replace(".", "").replace(",", ".").strip()
    try:
        return float(s)
    except:
        return None


def pick_first(lst):
    try:
        return (lst or [None])[0]
    except:
        return None


# =========================
# TOKEN MANAGEMENT (OAuth2 client_credentials)
# =========================
_token_mem = {"access_token": None, "expires_at": 0}


def _load_token_cache():
    if not os.path.exists(TOKEN_CACHE_FILE):
        return
    try:
        with open(TOKEN_CACHE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        _token_mem["access_token"] = data.get("access_token")
        _token_mem["expires_at"] = int(data.get("expires_at") or 0)
    except:
        pass


def _save_token_cache():
    try:
        with open(TOKEN_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(_token_mem, f)
            f.flush()
            os.fsync(f.fileno())
    except:
        pass


def _basic_auth_header():
    # requests gestisce Basic Auth con auth=(id, secret),
    # ma qui restiamo espliciti.
    import base64
    raw = f"{CREATORS_CREDENTIAL_ID}:{CREATORS_CREDENTIAL_SECRET}".encode("utf-8")
    b64 = base64.b64encode(raw).decode("ascii")
    return f"Basic {b64}"


def get_access_token():
    """
    Token valido 3600s. Usiamo cache + rinnovo automatico.
    """
    now = int(time.time())

    # Carica cache da file una volta
    if _token_mem["access_token"] is None and _token_mem["expires_at"] == 0:
        _load_token_cache()

    # Se ho token valido (con margine 60s), lo uso
    if _token_mem["access_token"] and (_token_mem["expires_at"] - 60) > now:
        return _token_mem["access_token"]

    # Altrimenti richiedo nuovo token
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": _basic_auth_header(),
    }
    data = "grant_type=client_credentials&scope=creatorsapi/default"

    if DEBUG_AMAZON:
        print(f"[DEBUG] Token refresh -> {TOKEN_ENDPOINT}")

    r = requests.post(TOKEN_ENDPOINT, headers=headers, data=data, timeout=20)
    if r.status_code >= 400:
        raise RuntimeError(f"Token error {r.status_code}: {r.text[:500]}")

    payload = r.json()
    token = payload.get("access_token")
    expires_in = int(payload.get("expires_in") or 3600)

    if not token:
        raise RuntimeError(f"Token response missing access_token: {payload}")

    _token_mem["access_token"] = token
    _token_mem["expires_at"] = now + expires_in
    _save_token_cache()

    return token


# =========================
# CREATORS API CALLS
# =========================
def _auth_header(token):
    # Formato richiesto: "Bearer <token>, Version <version>"
    return f"Bearer {token}, Version {CREATORS_CREDENTIAL_VERSION}"


def creators_post(path, json_body, token=None, retries=3):
    """
    POST robusto con refresh token su 401 e backoff su 429/5xx.
    """
    if token is None:
        token = get_access_token()

    url = f"{CATALOG_ENDPOINT.rstrip('/')}/{path.lstrip('/')}"
    headers = {
        "Content-Type": "application/json",
        "Authorization": _auth_header(token),
        "x-marketplace": CREATORS_MARKETPLACE,
    }

    for attempt in range(retries):
        try:
            r = requests.post(url, headers=headers, json=json_body, timeout=25)
        except Exception as e:
            if attempt == retries - 1:
                raise
            time.sleep(1.5 + attempt)
            continue

        # 401: token scaduto/invalid -> refresh e retry
        if r.status_code == 401 and attempt < retries - 1:
            if DEBUG_AMAZON:
                print("[DEBUG] 401 -> refresh token")
            _token_mem["access_token"] = None
            _token_mem["expires_at"] = 0
            token = get_access_token()
            headers["Authorization"] = _auth_header(token)
            continue

        # 429 o 5xx -> backoff
        if (r.status_code == 429 or r.status_code >= 500) and attempt < retries - 1:
            wait = 2.0 * (attempt + 1)
            if DEBUG_AMAZON:
                print(f"[DEBUG] {r.status_code} -> backoff {wait}s")
            time.sleep(wait)
            continue

        # error definitivo
        if r.status_code >= 400:
            raise RuntimeError(f"Creators API error {r.status_code}: {r.text[:800]}")

        return r.json()

    raise RuntimeError("Creators API call failed after retries")


def creators_search_items(keywords, page=1):
    body = {
        "keywords": keywords,
        "partnerTag": AMAZON_ASSOCIATE_TAG,
        "marketplace": CREATORS_MARKETPLACE,
        "itemCount": ITEMS_PER_PAGE,
        "itemPage": page,
        "resources": SEARCH_RESOURCES,
    }
    return creators_post("/searchItems", body)


def creators_get_items(item_ids):
    body = {
        "itemIds": item_ids,
        "partnerTag": AMAZON_ASSOCIATE_TAG,
        "marketplace": CREATORS_MARKETPLACE,
        "resources": GETITEMS_RESOURCES,
    }
    return creators_post("/getItems", body)


# =========================
# EXTRACTION (Creators JSON)
# =========================
def extract_from_item(item):
    """
    Ritorna dict con: asin, title, url, url_img, price_new, price_old, discount
    Oppure None se mancano dati.
    """
    asin = (item.get("asin") or item.get("itemId") or "").strip().upper()
    if not asin:
        return None

    # title
    title = None
    try:
        title = item["itemInfo"]["title"]["displayValue"]
    except:
        pass
    if not title:
        title = item.get("title") or ""
    title = " ".join(str(title).split())

    # image
    url_img = None
    try:
        url_img = item["images"]["primary"]["large"]["url"]
    except:
        pass
    if not url_img:
        url_img = "https://m.media-amazon.com/images/I/71bhWgQK-cL._AC_SL1500_.jpg"

    # detail page url
    url = item.get("detailPageUrl") or item.get("detail_page_url")
    if not url and asin:
        url = f"https://www.amazon.it/dp/{asin}?tag={AMAZON_ASSOCIATE_TAG}"

    # offers -> listings[0]
    listings = []
    try:
        listings = item["offers"]["listings"] or []
    except:
        listings = []

    l0 = pick_first(listings)
    if not l0:
        return None

    # price
    price_disp = None
    try:
        price_disp = l0["price"]["displayAmount"]
    except:
        pass
    price_new = parse_eur_amount(price_disp)
    if price_new is None:
        return None

    # savings
    disc = 0
    old_val = None

    # savingBasis = prezzo originale
    try:
        old_disp = l0["savingBasis"]["displayAmount"]
        old_val = parse_eur_amount(old_disp)
    except:
        old_val = None

    try:
        disc = int(l0["savings"]["percentage"] or 0)
    except:
        disc = 0

    # se manca old_val ma c'è disc, stima
    if old_val is None and disc:
        try:
            old_val = price_new / (1 - disc / 100.0)
        except:
            old_val = None

    if old_val is None:
        old_val = price_new

    return {
        "asin": asin,
        "title": title[:80].strip() + ("…" if len(title) > 80 else ""),
        "url_img": url_img,
        "url": url,
        "price_new": float(price_new),
        "price_old": float(old_val),
        "discount": int(disc),
    }


# =========================
# PUBLISHED TRACKING
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
# KW ROTATION
# =========================
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
# IMAGE GEN
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
# CORE: find offer
# =========================
def _first_valid_item_for_keyword(kw, pubblicati):
    # 1) SearchItems: prendi itemIds e prova estrazione diretta se include offerte
    candidates = []

    for page in range(1, PAGES + 1):
        try:
            data = creators_search_items(kw, page=page)
        except Exception as e:
            print(f"❌ Creators searchItems error (kw='{kw}', page={page}): {e}")
            continue

        items = data.get("items") or []
        if DEBUG_AMAZON:
            print(f"[DEBUG] kw={kw} page={page} items={len(items)}")

        for it in items:
            asin = (it.get("asin") or it.get("itemId") or "").strip().upper()
            if not asin:
                continue
            if asin in pubblicati or not can_post(asin, hours=24):
                continue

            extracted = extract_from_item(it)
            if extracted:
                # Filtri
                price_new = extracted["price_new"]
                disc = extracted["discount"]
                if price_new < MIN_PRICE or price_new > MAX_PRICE:
                    continue
                if disc < MIN_DISCOUNT:
                    continue
                return extracted

            # Altrimenti lo metto tra i candidati per getItems
            if len(candidates) < GETITEMS_FALLBACK_MAX:
                candidates.append(asin)

    # 2) GetItems fallback: approfondisci pochi itemIds e prova a estrarre
    if candidates:
        try:
            data = creators_get_items(candidates)
        except Exception as e:
            print(f"❌ Creators getItems error (candidates={candidates}): {e}")
            return None

        items = data.get("items") or []
        if DEBUG_AMAZON:
            print(f"[DEBUG] getItems candidates={candidates} items={len(items)}")

        for it in items:
            asin = (it.get("asin") or it.get("itemId") or "").strip().upper()
            if not asin:
                continue
            if asin in pubblicati or not can_post(asin, hours=24):
                continue

            extracted = extract_from_item(it)
            if not extracted:
                continue

            price_new = extracted["price_new"]
            disc = extracted["discount"]

            if price_new < MIN_PRICE or price_new > MAX_PRICE:
                continue
            if disc < MIN_DISCOUNT:
                continue

            return extracted

    return None


# =========================
# SEND OFFER (Telegram)
# =========================
bot = None

def invia_offerta():
    global bot
    _require_env()

    if bot is None:
        bot = Bot(token=TELEGRAM_BOT_TOKEN)

    pubblicati = load_pubblicati()
    kw = pick_keyword()

    payload = _first_valid_item_for_keyword(kw, pubblicati)
    if not payload:
        print(f"⚠️ Nessuna offerta valida trovata per keyword: {kw}")
        return False

    asin = payload["asin"]
    titolo = payload["title"]
    prezzo_nuovo_val = payload["price_new"]
    prezzo_vecchio_val = payload["price_old"]
    sconto = payload["discount"]
    url_img = payload["url_img"]
    url = payload["url"]

    minimo = sconto >= 30

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
    if minimo:
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
    print(f"✅ Pubblicata: {asin} | kw={kw}")
    return True


# =========================
# TIME WINDOW
# =========================
def is_in_italy_window(now_utc=None):
    if now_utc is None:
        now_utc = datetime.utcnow()

    month = now_utc.month
    # CEST aprile-ottobre approx
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


# =========================
# SCHEDULER
# =========================
def start_scheduler():
    _require_env()
    schedule.clear()
    schedule.every().monday.at("06:59").do(resetta_pubblicati)
    schedule.every(14).minutes.do(run_if_in_fascia_oraria)
    while True:
        schedule.run_pending()
        time.sleep(5)
