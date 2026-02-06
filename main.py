import os
import time
import json
import base64
import html
from io import BytesIO
from datetime import datetime, timedelta
from pathlib import Path
from collections import Counter

import requests
import schedule
from PIL import Image, ImageDraw, ImageFont
from telegram import Bot, InlineKeyboardMarkup, InlineKeyboardButton


# =========================
# ENV VARS (Render)
# =========================
AMAZON_ASSOCIATE_TAG = os.environ.get("AMAZON_ASSOCIATE_TAG", "").strip()
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

CREATORS_CREDENTIAL_ID = os.environ.get("CREATORS_CREDENTIAL_ID", "").strip()
CREATORS_CREDENTIAL_SECRET = os.environ.get("CREATORS_CREDENTIAL_SECRET", "").strip()
CREATORS_CREDENTIAL_VERSION = os.environ.get("CREATORS_CREDENTIAL_VERSION", "").strip()
CREATORS_MARKETPLACE = os.environ.get("CREATORS_MARKETPLACE", "www.amazon.it").strip()

# Debug
DEBUG_AMAZON = os.environ.get("DEBUG_AMAZON", "0") == "1"
GETITEMS_FALLBACK_MAX = int(os.environ.get("GETITEMS_FALLBACK_MAX", "4"))

# Filtri
MIN_DISCOUNT = int(os.environ.get("MIN_DISCOUNT", "15"))
MIN_PRICE = float(os.environ.get("MIN_PRICE", "15"))
MAX_PRICE = float(os.environ.get("MAX_PRICE", "1900"))

# Assets
FONT_PATH = os.environ.get("FONT_PATH", "Montserrat-VariableFont_wght.ttf")
LOGO_PATH = os.environ.get("LOGO_PATH", "header_clean2.png")
BADGE_PATH = os.environ.get("BADGE_PATH", "minimo storico flat.png")

# Storage (Render)
DATA_DIR = "/tmp/botdata"
Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
PUB_FILE = os.path.join(DATA_DIR, "pubblicati.txt")
PUB_TS = os.path.join(DATA_DIR, "pubblicati_ts.csv")
KW_INDEX = os.path.join(DATA_DIR, "kw_index.txt")

# Scheduling
SEARCH_INDEX = "All"
ITEMS_PER_PAGE = 8
PAGES = 4

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

bot = Bot(token=TELEGRAM_BOT_TOKEN)


# =========================
# Creators API endpoints
# =========================
CATALOG_BASE_URL = "https://creatorsapi.amazon/catalog/v1"

# Token endpoint: in EU (version 2.2) è eu-south-2; US spesso us-west-2.
# Puoi forzarlo da env con CREATORS_TOKEN_URL se vuoi.
def _default_token_url(marketplace: str) -> str:
    forced = os.environ.get("CREATORS_TOKEN_URL", "").strip()
    if forced:
        return forced

    # euristica semplice: .com -> us-west-2, tutto il resto -> eu-south-2
    if marketplace.endswith(".com"):
        region = "us-west-2"
    else:
        region = "eu-south-2"
    return f"https://creatorsapi.auth.{region}.amazoncognito.com/oauth2/token"

TOKEN_URL = _default_token_url(CREATORS_MARKETPLACE)


# Resources "safe" (minime) per titolo + immagine + prezzi/sconti
# Evitiamo savingBasis perché spesso è la prima a far scattare la validation.
SEARCH_RESOURCES = [
    "itemInfo.title",
    "images.primary.large",
    "offersV2.listings.price",
    "offersV2.summaries.lowestPrice",
    "offersV2.summaries.savings",
]

GETITEMS_RESOURCES = [
    "itemInfo.title",
    "images.primary.large",
    "offersV2.listings.price",
    "offersV2.summaries.lowestPrice",
    "offersV2.summaries.savings",
]


# =========================
# Token cache (OAuth2)
# =========================
_token_cache = {
    "access_token": None,
    "expires_at": 0,  # epoch
}

def _basic_auth_header(client_id: str, client_secret: str) -> str:
    raw = f"{client_id}:{client_secret}".encode("utf-8")
    return "Basic " + base64.b64encode(raw).decode("utf-8")

def get_access_token() -> str:
    now = int(time.time())
    # refresh se scade entro 60s
    if _token_cache["access_token"] and now < (_token_cache["expires_at"] - 60):
        return _token_cache["access_token"]

    if DEBUG_AMAZON:
        print(f"[DEBUG] Token refresh -> {TOKEN_URL}")

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": _basic_auth_header(CREATORS_CREDENTIAL_ID, CREATORS_CREDENTIAL_SECRET),
    }
    data = "grant_type=client_credentials&scope=creatorsapi/default"

    r = requests.post(TOKEN_URL, headers=headers, data=data, timeout=20)
    if r.status_code != 200:
        raise RuntimeError(f"Token error {r.status_code}: {r.text}")

    payload = r.json()
    token = payload.get("access_token")
    expires_in = int(payload.get("expires_in", 3600))

    _token_cache["access_token"] = token
    _token_cache["expires_at"] = int(time.time()) + expires_in

    if DEBUG_AMAZON:
        print(f"[DEBUG] Token OK (expires_in={expires_in}s) url={TOKEN_URL}")

    return token


# =========================
# Helpers
# =========================
def parse_eur_amount(display_amount):
    if not display_amount:
        return None
    s = str(display_amount)
    s = s.replace("\u20ac", "").replace("€", "").replace("\xa0", " ").strip()
    # 1.299,00 -> 1299.00
    s = s.replace(".", "").replace(",", ".").strip()
    try:
        return float(s)
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
# Published tracking
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
# Creators API calls + retry
# =========================
def creators_post(path: str, payload: dict, max_retries: int = 4):
    token = get_access_token()
    url = f"{CATALOG_BASE_URL}{path}"

    headers = {
        "Content-Type": "application/json",
        "x-marketplace": CREATORS_MARKETPLACE,
        # formato richiesto dal PDF: "Bearer <token>, Version <version>"
        "Authorization": f"Bearer {token}, Version {CREATORS_CREDENTIAL_VERSION}",
    }

    backoff = 1.0
    last_err = None

    for attempt in range(1, max_retries + 1):
        try:
            r = requests.post(url, headers=headers, json=payload, timeout=25)

            # 401: token scaduto o invalido -> forziamo refresh e riproviamo
            if r.status_code == 401:
                _token_cache["access_token"] = None
                _token_cache["expires_at"] = 0
                token = get_access_token()
                headers["Authorization"] = f"Bearer {token}, Version {CREATORS_CREDENTIAL_VERSION}"
                last_err = RuntimeError(f"401 Unauthorized: {r.text}")

            # 429 / 5xx -> retry con backoff
            elif r.status_code in (429, 500, 502, 503, 504):
                last_err = RuntimeError(f"{r.status_code}: {r.text}")

            elif r.status_code != 200:
                # errore “reale” (validation ecc.) -> stop
                raise RuntimeError(f"Creators API error {r.status_code}: {r.text}")

            else:
                return r.json()

        except Exception as e:
            last_err = e

        if DEBUG_AMAZON:
            print(f"[DEBUG] Retry {attempt}/{max_retries} on {url} -> {repr(last_err)}")

        time.sleep(backoff)
        backoff *= 2

    raise RuntimeError(f"Creators API request failed after retries: {repr(last_err)}")


def creators_search_items(kw: str, page: int):
    payload = {
        "keywords": kw,
        "partnerTag": AMAZON_ASSOCIATE_TAG,
        "marketplace": CREATORS_MARKETPLACE,
        "resources": SEARCH_RESOURCES,
        "itemCount": ITEMS_PER_PAGE,
        "itemPage": page,
        "searchIndex": SEARCH_INDEX,
    }
    return creators_post("/searchItems", payload)


def creators_get_items(asins):
    payload = {
        "itemIds": asins,
        "partnerTag": AMAZON_ASSOCIATE_TAG,
        "marketplace": CREATORS_MARKETPLACE,
        "resources": GETITEMS_RESOURCES,
    }
    return creators_post("/getItems", payload)


# =========================
# Extract item (dict)
# =========================
def extract_from_item_dict(item: dict):
    asin = (item.get("asin") or "").strip().upper()
    if not asin:
        return None

    title = (
        (((item.get("itemInfo") or {}).get("title") or {}).get("displayValue"))
        or ""
    )
    title = " ".join(str(title).split())

    # URL
    url = item.get("detailPageURL")
    if not url and asin:
        url = f"https://www.amazon.it/dp/{asin}?tag={AMAZON_ASSOCIATE_TAG}"

    # Immagine
    img_url = None
    try:
        img_url = (((item.get("images") or {}).get("primary") or {}).get("large") or {}).get("url")
    except:
        img_url = None
    if not img_url:
        img_url = "https://m.media-amazon.com/images/I/71bhWgQK-cL._AC_SL1500_.jpg"

    # Prezzo: preferisci listings.price, altrimenti summaries.lowestPrice
    price_val = None
    savings_pct = 0
    savings_amount = 0.0

    offersv2 = item.get("offersV2") or {}
    listings = offersv2.get("listings") or []
    summaries = offersv2.get("summaries") or []

    def _price_from_obj(pobj):
        if not pobj:
            return None
        # prova displayAmount, poi amount
        if "displayAmount" in pobj:
            return parse_eur_amount(pobj.get("displayAmount"))
        if "amount" in pobj:
            try:
                return float(pobj.get("amount"))
            except:
                return None
        return None

    if listings:
        p = (listings[0] or {}).get("price")
        price_val = _price_from_obj(p)

    if price_val is None and summaries:
        lp = (summaries[0] or {}).get("lowestPrice")
        price_val = _price_from_obj(lp)

    if summaries:
        sav = (summaries[0] or {}).get("savings") or {}
        try:
            savings_pct = int(sav.get("percentage") or 0)
        except:
            savings_pct = 0
        # savings.amount può essere obj o numero
        amt = sav.get("amount")
        if isinstance(amt, dict):
            savings_amount = _price_from_obj(amt) or 0.0
        else:
            try:
                savings_amount = float(amt or 0)
            except:
                savings_amount = 0.0

    if price_val is None:
        return {
            "asin": asin,
            "title": title,
            "url": url,
            "url_img": img_url,
            "price_new": None,
            "price_old": None,
            "discount": 0,
        }

    # Old price: se ho amount -> new + amount; se ho solo percent -> stima
    old_val = None
    if savings_amount and savings_amount > 0:
        old_val = price_val + float(savings_amount)
    elif savings_pct and savings_pct > 0:
        try:
            old_val = price_val / (1 - savings_pct / 100.0)
        except:
            old_val = price_val
    else:
        old_val = price_val

    return {
        "asin": asin,
        "title": title,
        "url": url,
        "url_img": img_url,
        "price_new": price_val,
        "price_old": old_val,
        "discount": int(savings_pct or 0),
    }


# =========================
# Core logic
# =========================
def _first_valid_item_for_keyword(kw, pubblicati):
    reasons = Counter()
    fallback_asins = []

    for page in range(1, PAGES + 1):
        try:
            data = creators_search_items(kw, page)
            search_result = data.get("searchResult") or {}
            items = search_result.get("items") or []

            if DEBUG_AMAZON:
                print(f"[DEBUG] kw={kw} page={page} items={len(items)}")
                print(f"[DEBUG] searchItems raw keys: {list(data.keys())}")
                if items:
                    preview = json.dumps({"searchResult": {"items": items[:1]}}, ensure_ascii=False)[:900]
                    print(f"[DEBUG] searchItems raw preview: {preview}")

        except Exception as e:
            reasons["api_error"] += 1
            if DEBUG_AMAZON:
                print(f"[DEBUG] Creators searchItems error (kw='{kw}', page={page}): {repr(e)}")
            continue

        for it in items:
            extracted = extract_from_item_dict(it)
            if not extracted:
                reasons["extract_fail"] += 1
                continue

            asin = extracted["asin"]
            if asin in pubblicati or not can_post(asin, hours=24):
                reasons["already_posted"] += 1
                continue

            if extracted["price_new"] is None:
                reasons["no_price_in_searchitems"] += 1
                if len(fallback_asins) < GETITEMS_FALLBACK_MAX:
                    fallback_asins.append(asin)
                continue

            price_val = extracted["price_new"]
            disc = extracted["discount"] or 0

            if price_val < MIN_PRICE or price_val > MAX_PRICE:
                reasons["price_out_range"] += 1
                continue

            if disc < MIN_DISCOUNT:
                reasons["disc_too_low"] += 1
                continue

            titolo = extracted["title"][:80].strip() + ("…" if len(extracted["title"]) > 80 else "")
            minimo = disc >= 30

            if DEBUG_AMAZON:
                print(f"[DEBUG] FOUND via searchItems asin={asin} price={price_val} disc={disc}")

            return {
                "asin": asin,
                "title": titolo,
                "price_new": extracted["price_new"],
                "price_old": extracted["price_old"],
                "discount": disc,
                "url_img": extracted["url_img"],
                "url": extracted["url"],
                "minimo": minimo,
            }

    # Fallback: GetItems su pochi ASIN che avevano prezzo mancante
    if fallback_asins:
        try:
            data = creators_get_items(fallback_asins)
            result = data.get("getItemsResult") or {}
            items = result.get("items") or []

            if DEBUG_AMAZON:
                print(f"[DEBUG] GetItems fallback asins={fallback_asins} items={len(items)}")

            for it in items:
                extracted = extract_from_item_dict(it)
                if not extracted or extracted["price_new"] is None:
                    continue

                asin = extracted["asin"]
                if asin in pubblicati or not can_post(asin, hours=24):
                    continue

                price_val = extracted["price_new"]
                disc = extracted["discount"] or 0

                if price_val < MIN_PRICE or price_val > MAX_PRICE:
                    continue
                if disc < MIN_DISCOUNT:
                    continue

                titolo = extracted["title"][:80].strip() + ("…" if len(extracted["title"]) > 80 else "")
                minimo = disc >= 30

                if DEBUG_AMAZON:
                    print(f"[DEBUG] FOUND via getItems asin={asin} price={price_val} disc={disc}")

                return {
                    "asin": asin,
                    "title": titolo,
                    "price_new": extracted["price_new"],
                    "price_old": extracted["price_old"],
                    "discount": disc,
                    "url_img": extracted["url_img"],
                    "url": extracted["url"],
                    "minimo": minimo,
                }
        except Exception as e:
            reasons["getitems_error"] += 1
            if DEBUG_AMAZON:
                print(f"[DEBUG] GetItems fallback error: {repr(e)}")

    if DEBUG_AMAZON:
        print(f"[DEBUG] kw={kw} reasons={dict(reasons)} fallback_asins={fallback_asins}")

    return None


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
