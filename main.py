import os
import time
import base64
import json
import html
import threading
from io import BytesIO
from datetime import datetime, timedelta
from collections import Counter
from pathlib import Path

import schedule
import requests
from PIL import Image, ImageDraw, ImageFont
from telegram import Bot, InlineKeyboardMarkup, InlineKeyboardButton

# =========================
# ENV / CONFIG
# =========================
AMAZON_ASSOCIATE_TAG = os.environ.get("AMAZON_ASSOCIATE_TAG", "").strip()
CREATORS_CREDENTIAL_ID = os.environ.get("CREATORS_CREDENTIAL_ID", "").strip()
CREATORS_CREDENTIAL_SECRET = os.environ.get("CREATORS_CREDENTIAL_SECRET", "").strip()
CREATORS_CREDENTIAL_VERSION = os.environ.get("CREATORS_CREDENTIAL_VERSION", "").strip()

# Marketplace (confermato): www.amazon.it
CREATORS_MARKETPLACE = os.environ.get("CREATORS_MARKETPLACE", "www.amazon.it").strip()

# Endpoint token (EU south 2 nel tuo caso)
CREATORS_TOKEN_URL = os.environ.get(
    "CREATORS_TOKEN_URL",
    "https://creatorsapi.auth.eu-south-2.amazoncognito.com/oauth2/token"
).strip()

# Base API
CREATORS_API_BASE = os.environ.get(
    "CREATORS_API_BASE",
    "https://creatorsapi.amazon/catalog/v1"
).strip().rstrip("/")

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

FONT_PATH = os.environ.get("FONT_PATH", "Montserrat-VariableFont_wght.ttf")
LOGO_PATH = os.environ.get("LOGO_PATH", "header_clean2.png")
BADGE_PATH = os.environ.get("BADGE_PATH", "minimo storico flat.png")

DATA_DIR = os.environ.get("DATA_DIR", "/tmp/botdata")
Path(DATA_DIR).mkdir(parents=True, exist_ok=True)

PUB_FILE = os.path.join(DATA_DIR, "pubblicati.txt")
PUB_TS = os.path.join(DATA_DIR, "pubblicati_ts.csv")
KW_INDEX = os.path.join(DATA_DIR, "kw_index.txt")

# Filtri prezzo/sconto
MIN_DISCOUNT = int(os.environ.get("MIN_DISCOUNT", "15"))
# Apple di solito ha sconti più “timidi”: soglia dedicata
MIN_DISCOUNT_APPLE = int(os.environ.get("MIN_DISCOUNT_APPLE", "10"))

MIN_PRICE = float(os.environ.get("MIN_PRICE", "15"))
MAX_PRICE = float(os.environ.get("MAX_PRICE", "1900"))

ITEMS_PER_PAGE = int(os.environ.get("ITEMS_PER_PAGE", "8"))
PAGES = int(os.environ.get("PAGES", "4"))

DEBUG_AMAZON = os.environ.get("DEBUG_AMAZON", "0") == "1"

# Fallback GetItems su pochi ASIN quando serve
GETITEMS_FALLBACK_MAX = int(os.environ.get("GETITEMS_FALLBACK_MAX", "4"))

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

SEARCH_INDEX = os.environ.get("SEARCH_INDEX", "All")

# Telegram bot
bot = Bot(token=TELEGRAM_BOT_TOKEN)

# =========================
# CREATORS API (OAUTH2)
# =========================
_token_cache = {"access_token": None, "expires_at": 0}

def _require_env():
    missing = []
    if not AMAZON_ASSOCIATE_TAG:
        missing.append("AMAZON_ASSOCIATE_TAG")
    if not CREATORS_CREDENTIAL_ID:
        missing.append("CREATORS_CREDENTIAL_ID")
    if not CREATORS_CREDENTIAL_SECRET:
        missing.append("CREATORS_CREDENTIAL_SECRET")
    if not CREATORS_CREDENTIAL_VERSION:
        missing.append("CREATORS_CREDENTIAL_VERSION")
    if not TELEGRAM_BOT_TOKEN:
        missing.append("TELEGRAM_BOT_TOKEN")
    if not TELEGRAM_CHAT_ID:
        missing.append("TELEGRAM_CHAT_ID")
    if missing:
        raise RuntimeError(f"Variabili mancanti: {', '.join(missing)}")

def get_access_token(force_refresh=False):
    """
    OAuth2 client_credentials con caching (token valido 3600s).
    """
    now = int(time.time())
    if (not force_refresh) and _token_cache["access_token"] and now < (_token_cache["expires_at"] - 30):
        return _token_cache["access_token"]

    auth_raw = f"{CREATORS_CREDENTIAL_ID}:{CREATORS_CREDENTIAL_SECRET}".encode("utf-8")
    auth_b64 = base64.b64encode(auth_raw).decode("ascii")

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": f"Basic {auth_b64}",
    }
    data = "grant_type=client_credentials&scope=creatorsapi/default"

    if DEBUG_AMAZON:
        print(f"[DEBUG] Token refresh -> {CREATORS_TOKEN_URL}")

    r = requests.post(CREATORS_TOKEN_URL, headers=headers, data=data, timeout=20)
    if r.status_code != 200:
        raise RuntimeError(f"Token error {r.status_code}: {r.text}")

    payload = r.json()
    access_token = payload.get("access_token")
    expires_in = int(payload.get("expires_in", 3600))

    if not access_token:
        raise RuntimeError(f"Token payload senza access_token: {payload}")

    _token_cache["access_token"] = access_token
    _token_cache["expires_at"] = now + expires_in

    if DEBUG_AMAZON:
        print(f"[DEBUG] Token OK (expires_in={expires_in}s) url={CREATORS_TOKEN_URL}")

    return access_token

def creators_headers(token):
    """
    Header come da guida:
    Authorization: Bearer <token>, Version <version>
    + x-marketplace
    """
    return {
        "Authorization": f"Bearer {token}, Version {CREATORS_CREDENTIAL_VERSION}",
        "Content-Type": "application/json",
        "x-marketplace": CREATORS_MARKETPLACE,
    }

# =========================
# RESOURCES (con fallback automatico)
# =========================
# Dal tuo debug: alcuni resource set vengono rifiutati (400).
# Qui provo in cascata: L1 -> L2 -> L3
SEARCH_RESOURCES_TRIES = [
    # L1: completo (spesso rifiutato dall'API)
    ["images.primary.large", "itemInfo.title", "offersV2.listings.price", "offersV2.listings.savingBasis"],
    # L2: quello che ti ha funzionato spesso
    ["images.primary.large", "itemInfo.title", "offersV2.listings.price"],
    # L3: minimo (se proprio blocca offers)
    ["images.primary.large", "itemInfo.title"],
]

GETITEMS_RESOURCES_TRIES = [
    ["images.primary.large", "itemInfo.title", "offersV2.listings.price", "offersV2.listings.savingBasis"],
    ["images.primary.large", "itemInfo.title", "offersV2.listings.price"],
    ["images.primary.large", "itemInfo.title"],
]

# =========================
# UTILS
# =========================
def parse_money_amount(x):
    """
    Creators API: spesso money.amount è numero (float/int).
    In fallback gestisce stringhe tipo "€ 19,99".
    """
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).replace("\u20ac", "").replace("€", "").replace("\xa0", " ").strip()
    s = s.replace(".", "").replace(",", ".").strip()
    try:
        return float(s)
    except:
        return None

def safe_first_list(x):
    try:
        return (x or [None])[0]
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

    if os.path.exists(LOGO_PATH):
        logo = Image.open(LOGO_PATH).resize((1080, 165))
        img.paste(logo, (0, 0))

    if minimo_storico and sconto >= 30 and os.path.exists(BADGE_PATH):
        badge = Image.open(BADGE_PATH).resize((220, 96))
        img.paste(badge, (24, 140), badge.convert("RGBA"))

    font_perc = ImageFont.truetype(FONT_PATH, 88)
    draw.text((830, 230), f"-{sconto}%", font=font_perc, fill="black")

    response = requests.get(url_img, timeout=20)
    prodotto = Image.open(BytesIO(response.content)).convert("RGBA").resize((600, 600))
    img.paste(prodotto, (240, 230), prodotto if prodotto.mode == "RGBA" else None)

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

def min_discount_for_kw(kw: str) -> int:
    k = (kw or "").lower()
    if "apple" in k or "iphone" in k or "macbook" in k:
        return MIN_DISCOUNT_APPLE
    return MIN_DISCOUNT

# =========================
# CREATORS API CALLS
# =========================
def creators_post_json(path, body):
    token = get_access_token()
    url = f"{CREATORS_API_BASE}/{path.lstrip('/')}"
    r = requests.post(url, headers=creators_headers(token), json=body, timeout=25)
    # Token scaduto o invalidato? refresh una volta
    if r.status_code in (401, 403):
        token = get_access_token(force_refresh=True)
        r = requests.post(url, headers=creators_headers(token), json=body, timeout=25)
    return r

def creators_search_items(kw, page, resources):
    body = {
        "keywords": kw,
        "partnerTag": AMAZON_ASSOCIATE_TAG,
        "marketplace": CREATORS_MARKETPLACE,
        "resources": resources,
        "itemCount": ITEMS_PER_PAGE,
        "itemPage": page,
        "searchIndex": SEARCH_INDEX,
    }
    return creators_post_json("searchItems", body)

def creators_get_items(item_ids, resources):
    body = {
        "itemIds": item_ids,
        "partnerTag": AMAZON_ASSOCIATE_TAG,
        "marketplace": CREATORS_MARKETPLACE,
        "resources": resources,
    }
    return creators_post_json("getItems", body)

def _extract_from_item(item):
    """
    Legge i campi principali da un item Creators API (dict).
    Ritorna dict con price/old/discount se disponibili, altrimenti None per price.
    """
    asin = (item.get("asin") or "").strip().upper()
    detail_url = item.get("detailPageURL")

    # immagine
    img_url = None
    try:
        img_url = item["images"]["primary"]["large"]["url"]
    except:
        pass

    # titolo
    title = ""
    try:
        title = item["itemInfo"]["title"]["displayValue"]
    except:
        pass
    title = " ".join(str(title).split())

    # offersV2
    price_val = None
    old_val = None
    disc = 0

    offersv2 = item.get("offersV2") or {}
    listings = offersv2.get("listings") or []
    l0 = safe_first_list(listings)

    if l0:
        # prezzo nuovo
        try:
            price_val = parse_money_amount(l0["price"]["money"]["amount"])
        except:
            price_val = None

        # prezzo vecchio (savingBasis) se disponibile
        try:
            old_val = parse_money_amount(l0["savingBasis"]["money"]["amount"])
        except:
            old_val = None

        # savings/percent se presente
        try:
            savings = l0.get("savings") or {}
            disc = int(savings.get("percentage") or 0)
            if not old_val:
                # a volte c'è amount: ricostruisco old = new + savingAmount
                saving_amount = parse_money_amount((savings.get("money") or {}).get("amount"))
                if price_val is not None and saving_amount:
                    old_val = price_val + saving_amount
        except:
            disc = 0

        # se non ho percent ma ho old/new, calcolo
        if price_val is not None and old_val and old_val > 0 and disc == 0:
            try:
                disc = int(round((old_val - price_val) / old_val * 100))
            except:
                disc = 0

    return {
        "asin": asin,
        "title": title,
        "detail_url": detail_url,
        "img_url": img_url,
        "price": price_val,
        "old": old_val if old_val is not None else price_val,
        "discount": disc,
        "has_offers": True if (item.get("offersV2") is not None) else False,
    }

def _search_with_resource_fallback(kw, page):
    """
    Prova SEARCH_RESOURCES_TRIES in cascata.
    Se l'API rifiuta resources (400) passa al try successivo.
    Ritorna (items, used_resources) oppure ([], last_resources).
    """
    last_used = None
    for i, res in enumerate(SEARCH_RESOURCES_TRIES, start=1):
        if DEBUG_AMAZON:
            print(f"[DEBUG] SearchItems try L{i} resources={res}")
        r = creators_search_items(kw, page, res)
        if r.status_code == 200:
            data = r.json()
            items = data.get("items") or []
            return items, res
        # 400 su resources: fallback
        if r.status_code == 400 and "validation error" in (r.text or "").lower():
            if DEBUG_AMAZON:
                print(f"[DEBUG] SearchItems L{i} rejected by API (resources) -> fallback")
            last_used = res
            continue
        # altri errori: li faccio esplodere
        raise RuntimeError(f"Creators searchItems error {r.status_code}: {r.text}")

    return [], last_used or SEARCH_RESOURCES_TRIES[-1]

def _getitems_with_resource_fallback(asins):
    """
    GetItems fallback, provando risorse in cascata.
    """
    last_used = None
    for i, res in enumerate(GETITEMS_RESOURCES_TRIES, start=1):
        if DEBUG_AMAZON:
            print(f"[DEBUG] GetItems try L{i} resources={res}")
        r = creators_get_items(asins, res)
        if r.status_code == 200:
            data = r.json()
            items = data.get("items") or []
            return items, res
        if r.status_code == 400 and "validation error" in (r.text or "").lower():
            if DEBUG_AMAZON:
                print(f"[DEBUG] GetItems L{i} rejected by API (resources) -> fallback")
            last_used = res
            continue
        raise RuntimeError(f"Creators getItems error {r.status_code}: {r.text}")

    return [], last_used or GETITEMS_RESOURCES_TRIES[-1]

# =========================
# CORE LOGIC
# =========================
def _first_valid_item_for_keyword(kw, pubblicati):
    reasons = Counter()
    asin_candidates = []

    min_disc = min_discount_for_kw(kw)

    for page in range(1, PAGES + 1):
        try:
            items_raw, used_res = _search_with_resource_fallback(kw, page)
            if DEBUG_AMAZON:
                print(f"[DEBUG] kw={kw} page={page} items={len(items_raw)} used_resources={used_res}")
                if items_raw:
                    print(f"[DEBUG] searchItems raw keys: {list((items_raw[0] or {}).keys())}")
        except Exception as e:
            reasons["api_error"] += 1
            print(f"❌ Creators searchItems error (kw='{kw}', page={page}): {e}")
            continue

        for it in items_raw:
            parsed = _extract_from_item(it or {})
            asin = parsed["asin"]
            if not asin:
                reasons["no_asin"] += 1
                continue
            if asin in pubblicati or not can_post(asin, hours=24):
                reasons["already_posted"] += 1
                continue

            # se manca il prezzo, lo metto tra i candidati per GetItems fallback
            if parsed["price"] is None:
                reasons["no_price"] += 1
                if len(asin_candidates) < GETITEMS_FALLBACK_MAX:
                    asin_candidates.append(asin)
                if DEBUG_AMAZON:
                    print(f"[DEBUG] asin={asin} no_price | has_offers={parsed['has_offers']}")
                continue

            price_val = parsed["price"]
            old_val = parsed["old"] if parsed["old"] is not None else price_val
            disc = int(parsed["discount"] or 0)

            if price_val < MIN_PRICE or price_val > MAX_PRICE:
                reasons["price_out_range"] += 1
                if DEBUG_AMAZON:
                    print(f"[DEBUG] asin={asin} price_out_range price={price_val} old={old_val} disc={disc}")
                continue

            if disc < min_disc:
                reasons["disc_too_low"] += 1
                if DEBUG_AMAZON:
                    print(f"[DEBUG] asin={asin} disc_too_low price={price_val} old={old_val} disc={disc}")
                continue

            titolo = (parsed["title"] or "").strip()
            if len(titolo) > 80:
                titolo = titolo[:80].strip() + "…"

            url_img = parsed["img_url"] or "https://m.media-amazon.com/images/I/71bhWgQK-cL._AC_SL1500_.jpg"
            url = parsed["detail_url"] or f"https://www.amazon.it/dp/{asin}?tag={AMAZON_ASSOCIATE_TAG}"

            minimo = disc >= 30

            if DEBUG_AMAZON:
                print(f"[DEBUG] FOUND via SearchItems asin={asin} price={price_val} old={old_val} disc={disc}")

            return {
                "asin": asin,
                "title": titolo,
                "price_new": price_val,
                "price_old": old_val,
                "discount": disc,
                "url_img": url_img,
                "url": url,
                "minimo": minimo,
            }

    # Fallback GetItems su asin_candidates
    if asin_candidates:
        try:
            items_raw, used_res = _getitems_with_resource_fallback(asin_candidates)
            if DEBUG_AMAZON:
                print(f"[DEBUG] GetItems fallback candidates={asin_candidates} items={len(items_raw)} used_resources={used_res}")

            for it in items_raw:
                parsed = _extract_from_item(it or {})
                asin = parsed["asin"]
                if not asin or asin in pubblicati or not can_post(asin, hours=24):
                    continue

                if parsed["price"] is None:
                    continue

                price_val = parsed["price"]
                old_val = parsed["old"] if parsed["old"] is not None else price_val
                disc = int(parsed["discount"] or 0)

                if price_val < MIN_PRICE or price_val > MAX_PRICE:
                    continue
                if disc < min_disc:
                    continue

                titolo = (parsed["title"] or "").strip()
                if len(titolo) > 80:
                    titolo = titolo[:80].strip() + "…"

                url_img = parsed["img_url"] or "https://m.media-amazon.com/images/I/71bhWgQK-cL._AC_SL1500_.jpg"
                url = parsed["detail_url"] or f"https://www.amazon.it/dp/{asin}?tag={AMAZON_ASSOCIATE_TAG}"

                minimo = disc >= 30

                if DEBUG_AMAZON:
                    print(f"[DEBUG] FOUND via GetItems asin={asin} price={price_val} old={old_val} disc={disc}")

                return {
                    "asin": asin,
                    "title": titolo,
                    "price_new": price_val,
                    "price_old": old_val,
                    "discount": disc,
                    "url_img": url_img,
                    "url": url,
                    "minimo": minimo,
                }
        except Exception as e:
            reasons["getitems_error"] += 1
            if DEBUG_AMAZON:
                print(f"[DEBUG] GetItems fallback error: {e}")

    if DEBUG_AMAZON:
        print(f"[DEBUG] kw={kw} reasons={dict(reasons)} asin_candidates={asin_candidates}")

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
    print(f"✅ Pubblicata: {asin} | {kw} | -{sconto}%")
    return True

def is_in_italy_window(now_utc=None):
    if now_utc is None:
        now_utc = datetime.utcnow()
    month = now_utc.month
    offset_hours = 2 if 4 <= month <= 10 else 1  # CEST/CET approx
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
