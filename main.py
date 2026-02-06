import os
import time
import html
import base64
import json
import schedule
import requests

from io import BytesIO
from datetime import datetime, timedelta
from pathlib import Path
from collections import Counter

from PIL import Image, ImageDraw, ImageFont
from telegram import Bot, InlineKeyboardMarkup, InlineKeyboardButton


# ==========================================================
# ENV / CONFIG
# ==========================================================
# --- Telegram ---
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# --- Creators API (Amazon) ---
CREATORS_CREDENTIAL_ID = os.environ.get("CREATORS_CREDENTIAL_ID", "")
CREATORS_CREDENTIAL_SECRET = os.environ.get("CREATORS_CREDENTIAL_SECRET", "")
CREATORS_CREDENTIAL_VERSION = os.environ.get("CREATORS_CREDENTIAL_VERSION", "")
CREATORS_MARKETPLACE = os.environ.get("CREATORS_MARKETPLACE", "www.amazon.it")  # x-marketplace + marketplace

AMAZON_ASSOCIATE_TAG = os.environ.get("AMAZON_ASSOCIATE_TAG", "")  # es. itech00-21

# Token endpoint: scegli quello giusto per la tua credential version/regione.
# Dal tuo log stai usando eu-south-2 (ok).
CREATORS_AUTH_URL = os.environ.get(
    "CREATORS_AUTH_URL",
    "https://creatorsapi.auth.eu-south-2.amazoncognito.com/oauth2/token"
)

# Catalog endpoint (Creators API)
CREATORS_API_BASE = os.environ.get(
    "CREATORS_API_BASE",
    "https://creatorsapi.amazon/catalog/v1"
)

# Debug
DEBUG_AMAZON = os.environ.get("DEBUG_AMAZON", "0") == "1"
GETITEMS_FALLBACK_MAX = int(os.environ.get("GETITEMS_FALLBACK_MAX", "4"))

# Filtri prezzo/sconto
MIN_DISCOUNT = int(os.environ.get("MIN_DISCOUNT", "15"))
MIN_PRICE = float(os.environ.get("MIN_PRICE", "15"))
MAX_PRICE = float(os.environ.get("MAX_PRICE", "1900"))

# Keywords
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

SEARCH_INDEX = "All"
ITEMS_PER_PAGE = 8
PAGES = 4


# --- Assets grafici ---
FONT_PATH = os.environ.get("FONT_PATH", "Montserrat-VariableFont_wght.ttf")
LOGO_PATH = os.environ.get("LOGO_PATH", "header_clean2.png")
BADGE_PATH = os.environ.get("BADGE_PATH", "minimo storico flat.png")

# Persistenza su Render (nel tuo setup usavi /data; qui resto su /tmp come avevi in main stabile)
DATA_DIR = os.environ.get("DATA_DIR", "/tmp/botdata")
Path(DATA_DIR).mkdir(parents=True, exist_ok=True)

PUB_FILE = os.path.join(DATA_DIR, "pubblicati.txt")
PUB_TS = os.path.join(DATA_DIR, "pubblicati_ts.csv")
KW_INDEX = os.path.join(DATA_DIR, "kw_index.txt")


# Telegram Bot
bot = Bot(token=TELEGRAM_BOT_TOKEN) if TELEGRAM_BOT_TOKEN else None


# ==========================================================
# Creators API: Resources (dot notation)
# ==========================================================
SEARCH_RESOURCES = [
    "itemInfo.title",
    "images.primary.large",
    "offersV2.listings.price",
    "offersV2.listings.savingBasis",
    "offersV2.summaries.lowestPrice",
    "offersV2.summaries.savings",
]

GETITEMS_RESOURCES = [
    "itemInfo.title",
    "images.primary.large",
    "offersV2.listings.price",
    "offersV2.listings.savingBasis",
    "offersV2.summaries.lowestPrice",
    "offersV2.summaries.savings",
]


# ==========================================================
# Utils
# ==========================================================
def parse_eur_amount(display_amount):
    """
    Gestisce:
    - "€ 19,99"
    - "19,99 €"
    - "1.299,00"
    - "1299.00"
    """
    if not display_amount:
        return None
    s = str(display_amount)
    s = s.replace("\u20ac", "").replace("€", "")
    s = s.replace("\xa0", " ").strip()
    # "1.299,00" -> "1299.00"
    s = s.replace(".", "").replace(",", ".").strip()
    try:
        return float(s)
    except:
        return None


def get_nested(d, path, default=None):
    """
    path: lista di chiavi/indici, es: ["offersV2","listings",0,"price","displayAmount"]
    """
    cur = d
    try:
        for p in path:
            if cur is None:
                return default
            if isinstance(p, int):
                cur = cur[p]
            else:
                cur = cur.get(p)
        return cur if cur is not None else default
    except:
        return default


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


# ==========================================================
# Persistenza pubblicati
# ==========================================================
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


# ==========================================================
# Creators API Auth (OAuth2) con caching token
# ==========================================================
_token_cache = {
    "access_token": None,
    "expires_at": 0,  # epoch seconds
}

def _creators_get_access_token():
    """
    Token valido 3600s. Caching + refresh automatico.
    """
    now = int(time.time())
    if _token_cache["access_token"] and now < (_token_cache["expires_at"] - 60):
        return _token_cache["access_token"]

    if not (CREATORS_CREDENTIAL_ID and CREATORS_CREDENTIAL_SECRET and CREATORS_CREDENTIAL_VERSION):
        raise RuntimeError("Creators API: credenziali mancanti (CREATORS_CREDENTIAL_ID/SECRET/VERSION).")

    basic = base64.b64encode(
        f"{CREATORS_CREDENTIAL_ID}:{CREATORS_CREDENTIAL_SECRET}".encode("utf-8")
    ).decode("utf-8")

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": f"Basic {basic}",
    }

    data = "grant_type=client_credentials&scope=creatorsapi/default"
    r = requests.post(CREATORS_AUTH_URL, headers=headers, data=data, timeout=20)

    if r.status_code != 200:
        raise RuntimeError(f"Token error {r.status_code}: {r.text}")

    js = r.json()
    token = js.get("access_token")
    expires_in = int(js.get("expires_in", 3600))

    _token_cache["access_token"] = token
    _token_cache["expires_at"] = now + expires_in

    if DEBUG_AMAZON:
        print(f"[DEBUG] Token OK (expires_in={expires_in}s)")
    return token


def _creators_headers():
    token = _creators_get_access_token()
    # Formato richiesto da Amazon: "Bearer <token>, Version <version>"
    return {
        "Authorization": f"Bearer {token}, Version {CREATORS_CREDENTIAL_VERSION}",
        "Content-Type": "application/json",
        "x-marketplace": CREATORS_MARKETPLACE,
    }


def _creators_post(path, payload, retries=2):
    """
    POST robusto con mini backoff su 429/5xx.
    """
    url = f"{CREATORS_API_BASE}{path}"
    last_err = None
    for attempt in range(retries + 1):
        try:
            r = requests.post(url, headers=_creators_headers(), json=payload, timeout=25)

            if r.status_code == 401:
                # token scaduto/invalid -> force refresh una volta
                _token_cache["access_token"] = None
                _token_cache["expires_at"] = 0
                last_err = RuntimeError(f"401 Unauthorized: {r.text}")
                continue

            if r.status_code == 429:
                # rate limit
                wait = 2 + attempt * 2
                if DEBUG_AMAZON:
                    print(f"[DEBUG] 429 TooManyRequests -> sleep {wait}s")
                time.sleep(wait)
                last_err = RuntimeError(f"429 TooManyRequests: {r.text}")
                continue

            if 500 <= r.status_code < 600:
                wait = 1 + attempt * 2
                if DEBUG_AMAZON:
                    print(f"[DEBUG] {r.status_code} -> retry sleep {wait}s")
                time.sleep(wait)
                last_err = RuntimeError(f"{r.status_code} Server error: {r.text}")
                continue

            if r.status_code != 200:
                raise RuntimeError(f"Creators API error {r.status_code}: {r.text}")

            return r.json()

        except Exception as e:
            last_err = e
            time.sleep(1 + attempt)

    raise last_err


# ==========================================================
# Creators API: SearchItems / GetItems
# ==========================================================
def creators_search_items(kw, page):
    payload = {
        "keywords": kw,
        "partnerTag": AMAZON_ASSOCIATE_TAG,
        "marketplace": CREATORS_MARKETPLACE,
        "resources": SEARCH_RESOURCES,
        "itemCount": ITEMS_PER_PAGE,
        "itemPage": page,
        "searchIndex": SEARCH_INDEX,
    }
    return _creators_post("/searchItems", payload)


def creators_get_items(asins):
    payload = {
        "itemIds": asins,
        "partnerTag": AMAZON_ASSOCIATE_TAG,
        "marketplace": CREATORS_MARKETPLACE,
        "resources": GETITEMS_RESOURCES,
    }
    return _creators_post("/getItems", payload)


def extract_price_discount(item):
    """
    Estrae prezzo/sconto da OffersV2.
    Ritorna: (price_val, disc_percent, old_val) o (None, None, None)
    """
    # listings[0].price.displayAmount
    price_disp = get_nested(item, ["offersV2", "listings", 0, "price", "displayAmount"])
    price_val = parse_eur_amount(price_disp)

    # savingBasis (prezzo vecchio)
    old_disp = get_nested(item, ["offersV2", "listings", 0, "savingBasis", "displayAmount"])
    old_val = parse_eur_amount(old_disp)

    # sconto %
    disc = get_nested(item, ["offersV2", "summaries", 0, "savings", "percentage"], 0)
    try:
        disc = int(disc or 0)
    except:
        disc = 0

    if price_val is None:
        return None, None, None

    if old_val is None:
        # se non arriva base, prova stima da percent
        if disc > 0:
            try:
                old_val = price_val / (1 - disc / 100.0)
            except:
                old_val = price_val
        else:
            old_val = price_val

    return price_val, disc, old_val


def extract_title(item):
    # In Creators API spesso è itemInfo.title.displayValue oppure itemInfo.title
    t = get_nested(item, ["itemInfo", "title", "displayValue"])
    if not t:
        t = get_nested(item, ["itemInfo", "title"])
    if not t:
        t = ""
    return " ".join(str(t).split())


def extract_image(item):
    u = get_nested(item, ["images", "primary", "large", "url"])
    if not u:
        u = "https://m.media-amazon.com/images/I/71bhWgQK-cL._AC_SL1500_.jpg"
    return u


def extract_url(item, asin):
    u = item.get("detailPageUrl")
    if not u and asin:
        u = f"https://www.amazon.it/dp/{asin}?tag={AMAZON_ASSOCIATE_TAG}"
    return u


# ==========================================================
# Core: trova la prima offerta valida per keyword
# ==========================================================
def _first_valid_item_for_keyword(kw, pubblicati):
    reasons = Counter()
    fallback_candidates = []

    for page in range(1, PAGES + 1):
        try:
            js = creators_search_items(kw, page)
            items = js.get("items", []) or []
            if DEBUG_AMAZON:
                print(f"[DEBUG] kw={kw} page={page} items={len(items)}")
        except Exception as e:
            reasons["api_error"] += 1
            print(f"❌ Creators searchItems error (kw='{kw}', page={page}): {repr(e)}")
            items = []

        for item in items:
            asin = (item.get("asin") or "").strip().upper()
            if not asin:
                reasons["no_asin"] += 1
                continue
            if asin in pubblicati or not can_post(asin, hours=24):
                reasons["already_posted"] += 1
                continue

            title = extract_title(item)
            url_img = extract_image(item)
            url = extract_url(item, asin)

            price_val, disc, old_val = extract_price_discount(item)
            if price_val is None:
                reasons["no_price_in_searchitems"] += 1
                if len(fallback_candidates) < GETITEMS_FALLBACK_MAX:
                    fallback_candidates.append(asin)
                continue

            if price_val < MIN_PRICE or price_val > MAX_PRICE:
                reasons["price_out_range"] += 1
                continue

            if disc < MIN_DISCOUNT:
                reasons["disc_too_low"] += 1
                continue

            minimo = disc >= 30

            if DEBUG_AMAZON:
                print(f"[DEBUG] FOUND via searchItems asin={asin} price={price_val} disc={disc}")

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

    # fallback GetItems
    if fallback_candidates:
        try:
            js = creators_get_items(fallback_candidates)
            items = js.get("items", []) or []
            if DEBUG_AMAZON:
                print(f"[DEBUG] getItems fallback asins={fallback_candidates} items={len(items)}")

            for item in items:
                asin = (item.get("asin") or "").strip().upper()
                if not asin or asin in pubblicati or not can_post(asin, hours=24):
                    continue

                title = extract_title(item)
                url_img = extract_image(item)
                url = extract_url(item, asin)

                price_val, disc, old_val = extract_price_discount(item)
                if price_val is None:
                    continue
                if price_val < MIN_PRICE or price_val > MAX_PRICE:
                    continue
                if disc < MIN_DISCOUNT:
                    continue

                minimo = disc >= 30

                if DEBUG_AMAZON:
                    print(f"[DEBUG] FOUND via getItems asin={asin} price={price_val} disc={disc}")

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
            reasons["getitems_error"] += 1
            if DEBUG_AMAZON:
                print(f"[DEBUG] getItems fallback error: {repr(e)}")

    if DEBUG_AMAZON:
        print(f"[DEBUG] kw={kw} reasons={dict(reasons)} fallback_asins={fallback_candidates}")

    return None


# ==========================================================
# Pubblica su Telegram
# ==========================================================
def invia_offerta():
    if not bot:
        raise RuntimeError("TELEGRAM_BOT_TOKEN mancante.")
    if not TELEGRAM_CHAT_ID:
        raise RuntimeError("TELEGRAM_CHAT_ID mancante.")
    if not (CREATORS_CREDENTIAL_ID and CREATORS_CREDENTIAL_SECRET and CREATORS_CREDENTIAL_VERSION):
        raise RuntimeError("Creators API: credenziali mancanti (CREATORS_CREDENTIAL_ID/SECRET/VERSION).")
    if not AMAZON_ASSOCIATE_TAG:
        raise RuntimeError("AMAZON_ASSOCIATE_TAG mancante.")
    if not CREATORS_MARKETPLACE:
        raise RuntimeError("CREATORS_MARKETPLACE mancante (es: www.amazon.it).")

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


# ==========================================================
# Fascia oraria Italia
# ==========================================================
def is_in_italy_window(now_utc=None):
    if now_utc is None:
        now_utc = datetime.utcnow()

    month = now_utc.month
    # CET/CEST "approssimato" come nel tuo stabile
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


# ==========================================================
# Scheduler (richiamato da app.py)
# ==========================================================
def start_scheduler():
    schedule.clear()
    schedule.every().monday.at("06:59").do(resetta_pubblicati)
    schedule.every(14).minutes.do(run_if_in_fascia_oraria)

    while True:
        schedule.run_pending()
        time.sleep(5)
