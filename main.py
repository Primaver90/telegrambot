import os
import time
import html
import base64
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
AMAZON_ASSOCIATE_TAG = os.environ.get("AMAZON_ASSOCIATE_TAG")  # es: itech00-21
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

CREATORS_CREDENTIAL_ID = os.environ.get("CREATORS_CREDENTIAL_ID")
CREATORS_CREDENTIAL_SECRET = os.environ.get("CREATORS_CREDENTIAL_SECRET")
CREATORS_CREDENTIAL_VERSION = os.environ.get("CREATORS_CREDENTIAL_VERSION")
CREATORS_MARKETPLACE = os.environ.get("CREATORS_MARKETPLACE", "www.amazon.it")

# Token endpoint: deve includere https://  (nel PDF è fondamentale)
CREATORS_TOKEN_URL = os.environ.get(
    "CREATORS_TOKEN_URL",
    "https://creatorsapi.auth.eu-south-2.amazoncognito.com/oauth2/token",
)

# API base (come da PDF): https://creatorsapi.amazon/catalog/v1/*
CREATORS_API_BASE = os.environ.get(
    "CREATORS_API_BASE",
    "https://creatorsapi.amazon/catalog/v1",
)

DEBUG_AMAZON = os.environ.get("DEBUG_AMAZON", "0") == "1"
GETITEMS_FALLBACK_MAX = int(os.environ.get("GETITEMS_FALLBACK_MAX", "6"))

MIN_DISCOUNT = int(os.environ.get("MIN_DISCOUNT", "15"))
MIN_PRICE = float(os.environ.get("MIN_PRICE", "15"))
MAX_PRICE = float(os.environ.get("MAX_PRICE", "1900"))

FONT_PATH = os.environ.get("FONT_PATH", "Montserrat-VariableFont_wght.ttf")
LOGO_PATH = os.environ.get("LOGO_PATH", "header_clean2.png")
BADGE_PATH = os.environ.get("BADGE_PATH", "minimo storico flat.png")

DATA_DIR = os.environ.get("DATA_DIR", "/tmp/botdata")
Path(DATA_DIR).mkdir(parents=True, exist_ok=True)

PUB_FILE = os.path.join(DATA_DIR, "pubblicati.txt")
PUB_TS = os.path.join(DATA_DIR, "pubblicati_ts.csv")
KW_INDEX = os.path.join(DATA_DIR, "kw_index.txt")

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
# VALIDATION
# =========================
def _require_env():
    missing = []
    for k in [
        "AMAZON_ASSOCIATE_TAG",
        "TELEGRAM_BOT_TOKEN",
        "TELEGRAM_CHAT_ID",
        "CREATORS_CREDENTIAL_ID",
        "CREATORS_CREDENTIAL_SECRET",
        "CREATORS_CREDENTIAL_VERSION",
        "CREATORS_MARKETPLACE",
    ]:
        if not os.environ.get(k):
            missing.append(k)
    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")


# =========================
# JSON HELPERS
# =========================
def jget(obj, path, default=None):
    cur = obj
    try:
        for p in path:
            if cur is None:
                return default
            if isinstance(p, int):
                cur = cur[p]
            else:
                cur = cur.get(p)
        return cur if cur is not None else default
    except Exception:
        return default


def parse_eur_amount(display_amount):
    if not display_amount:
        return None
    s = str(display_amount)
    s = s.replace("\u20ac", "").replace("€", "").replace("\xa0", " ").strip()
    s = s.replace(".", "").replace(",", ".").strip()
    try:
        return float(s)
    except Exception:
        return None


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

    response = requests.get(url_img, timeout=25)
    prodotto = Image.open(BytesIO(response.content)).convert("RGBA").resize((600, 600))
    img.paste(prodotto, (240, 230), prodotto)

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
# STORAGE
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
    i = (get_kw_index() + 1) % len(KEYWORDS)
    with open(KW_INDEX, "w", encoding="utf-8") as f:
        f.write(str(i))


def pick_keyword():
    i = get_kw_index()
    kw = KEYWORDS[i]
    bump_kw_index()
    return kw


# =========================
# CREATORS API (OAuth2)
# =========================
_token_cache = {"token": None, "expires_at": 0}


def _basic_auth_header(client_id, client_secret):
    raw = f"{client_id}:{client_secret}".encode("utf-8")
    b64 = base64.b64encode(raw).decode("utf-8")
    return f"Basic {b64}"


def creators_get_token():
    now = time.time()
    if _token_cache["token"] and now < (_token_cache["expires_at"] - 60):
        return _token_cache["token"]

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": _basic_auth_header(CREATORS_CREDENTIAL_ID, CREATORS_CREDENTIAL_SECRET),
    }
    data = "grant_type=client_credentials&scope=creatorsapi/default"

    if DEBUG_AMAZON:
        print(f"[DEBUG] Token refresh -> {CREATORS_TOKEN_URL}")

    r = requests.post(CREATORS_TOKEN_URL, headers=headers, data=data, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"Token error {r.status_code}: {r.text}")

    js = r.json()
    token = js.get("access_token")
    expires_in = int(js.get("expires_in", 3600) or 3600)

    if not token:
        raise RuntimeError(f"Token response missing access_token: {js}")

    _token_cache["token"] = token
    _token_cache["expires_at"] = time.time() + expires_in

    if DEBUG_AMAZON:
        print(f"[DEBUG] Token OK (expires_in={expires_in}s)")

    return token


def creators_headers():
    token = creators_get_token()
    # Formato ESATTO (come da PDF): "Bearer <token>, Version <version>"
    return {
        "Authorization": f"Bearer {token}, Version {CREATORS_CREDENTIAL_VERSION}",
        "Content-Type": "application/json",
        "x-marketplace": CREATORS_MARKETPLACE,
    }


# SearchItems leggero (asin + title + image + url)
SEARCH_RESOURCES = [
    "itemInfo.title",
    "images.primary.large",
]

# Prezzi solo in GetItems (più affidabile)
GETITEMS_RESOURCES = [
    "itemInfo.title",
    "images.primary.large",
    "offersV2.listings.price",
    "offersV2.listings.savingBasis",
    "offersV2.summaries.savings",
    "offersV2.summaries.lowestPrice",
]


def creators_search_items(kw, page):
    url = f"{CREATORS_API_BASE}/searchItems"
    body = {
        "keywords": kw,
        "partnerTag": AMAZON_ASSOCIATE_TAG,
        "marketplace": CREATORS_MARKETPLACE,
        "resources": SEARCH_RESOURCES,
        "itemCount": ITEMS_PER_PAGE,
        "itemPage": page,
    }

    h = creators_headers()
    r = requests.post(url, headers=h, json=body, timeout=35)

    if r.status_code == 429:
        time.sleep(2)
        r = requests.post(url, headers=h, json=body, timeout=35)

    if r.status_code != 200:
        raise RuntimeError(f"Creators searchItems HTTP {r.status_code}: {r.text}")

    return r.json()


def creators_get_items(item_ids):
    url = f"{CREATORS_API_BASE}/getItems"
    body = {
        "itemIds": item_ids,
        "itemIdType": "ASIN",  # come da PDF
        "partnerTag": AMAZON_ASSOCIATE_TAG,
        "marketplace": CREATORS_MARKETPLACE,
        "resources": GETITEMS_RESOURCES,
    }

    h = creators_headers()
    r = requests.post(url, headers=h, json=body, timeout=35)

    if r.status_code == 429:
        time.sleep(2)
        r = requests.post(url, headers=h, json=body, timeout=35)

    if r.status_code != 200:
        raise RuntimeError(f"Creators getItems HTTP {r.status_code}: {r.text}")

    return r.json()


def extract_title_image_url(item):
    asin = (item.get("asin") or "").strip().upper()

    title = jget(item, ["itemInfo", "title", "displayValue"], "") or ""
    title = " ".join(str(title).split())

    url_img = jget(item, ["images", "primary", "large", "url"])
    if not url_img:
        url_img = "https://m.media-amazon.com/images/I/71bhWgQK-cL._AC_SL1500_.jpg"

    # Nota: nel log Creators è detailPageURL (URL maiuscolo)
    url = item.get("detailPageURL") or item.get("detailPageUrl") or item.get("detail_page_url")
    if not url and asin:
        url = f"https://www.amazon.it/dp/{asin}?tag={AMAZON_ASSOCIATE_TAG}"

    return asin, title, url_img, url


def extract_price_discount(item):
    # OffersV2
    price_disp = jget(item, ["offersV2", "listings", 0, "price", "displayAmount"])
    price_val = parse_eur_amount(price_disp)

    old_disp = jget(item, ["offersV2", "listings", 0, "savingBasis", "displayAmount"])
    old_val = parse_eur_amount(old_disp)

    disc = jget(item, ["offersV2", "summaries", 0, "savings", "percentage"], 0) or 0
    try:
        disc = int(disc)
    except Exception:
        disc = 0

    if price_val is not None and old_val is None and disc:
        try:
            old_val = price_val / (1 - disc / 100.0)
        except Exception:
            old_val = None

    if price_val is None:
        return None, None, None
    if old_val is None:
        old_val = price_val

    return price_val, disc, old_val


def _extract_items_list_from_search_response(data):
    # FIX: come nel tuo log, SearchItems torna dentro "searchResult"
    items = data.get("items")
    if isinstance(items, list):
        return items
    items = jget(data, ["searchResult", "items"], [])
    if isinstance(items, list):
        return items
    return []


def _extract_items_list_from_getitems_response(data):
    # GetItems in alcuni casi è "itemsResult.items"
    items = data.get("items")
    if isinstance(items, list):
        return items
    items = jget(data, ["itemsResult", "items"], [])
    if isinstance(items, list):
        return items
    return []


def _first_valid_item_for_keyword(kw, pubblicati):
    reasons = Counter()
    candidates = []

    # 1) SearchItems -> raccogli ASIN candidati
    for page in range(1, PAGES + 1):
        try:
            data = creators_search_items(kw, page)
            items = _extract_items_list_from_search_response(data)

            if DEBUG_AMAZON:
                print(f"[DEBUG] kw={kw} page={page} items={len(items)}")
                print(f"[DEBUG] searchItems raw keys: {list(data.keys())}")
                if items[:1]:
                    print(f"[DEBUG] searchItems raw preview: {items[:1]}")

        except Exception as e:
            reasons["api_error_search"] += 1
            if DEBUG_AMAZON:
                print(f"[DEBUG] Creators searchItems error (kw={kw}, page={page}): {repr(e)}")
            continue

        for it in items:
            asin = (it.get("asin") or "").strip().upper()
            if not asin:
                reasons["no_asin"] += 1
                continue
            if asin in pubblicati or not can_post(asin, hours=24):
                reasons["already_posted"] += 1
                continue

            candidates.append(asin)
            if len(candidates) >= GETITEMS_FALLBACK_MAX:
                break

        if len(candidates) >= GETITEMS_FALLBACK_MAX:
            break

    if DEBUG_AMAZON:
        print(f"[DEBUG] kw={kw} asin_candidates={candidates} reasons={dict(reasons)}")

    if not candidates:
        return None

    # 2) GetItems -> recupera prezzi e scegli il primo valido
    try:
        gdata = creators_get_items(candidates)
        gitems = _extract_items_list_from_getitems_response(gdata)

        if DEBUG_AMAZON:
            print(f"[DEBUG] getItems returned items={len(gitems)} for candidates={candidates}")

    except Exception as e:
        if DEBUG_AMAZON:
            print(f"[DEBUG] Creators getItems error: {repr(e)}")
        return None

    for item in gitems:
        asin, title, url_img, url = extract_title_image_url(item)
        if not asin:
            continue
        if asin in pubblicati or not can_post(asin, hours=24):
            continue

        price_val, disc, old_val = extract_price_discount(item)
        if price_val is None:
            continue

        if price_val < MIN_PRICE or price_val > MAX_PRICE:
            continue
        if (disc or 0) < MIN_DISCOUNT:
            continue

        minimo = (disc or 0) >= 30

        return {
            "asin": asin,
            "title": title[:80].strip() + ("…" if len(title) > 80 else ""),
            "price_new": price_val,
            "price_old": old_val if old_val is not None else price_val,
            "discount": int(disc or 0),
            "url_img": url_img,
            "url": url,
            "minimo": minimo,
        }

    return None


def invia_offerta():
    _require_env()

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
