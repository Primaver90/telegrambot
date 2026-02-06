import os
from io import BytesIO
from datetime import datetime, timedelta
import time
import schedule
import requests
import html
from collections import Counter
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont
from telegram import Bot, InlineKeyboardMarkup, InlineKeyboardButton

# =========================
# ENV
# =========================
AMAZON_ASSOCIATE_TAG = os.environ.get("AMAZON_ASSOCIATE_TAG", "itech00-21")

# Creators API creds (OAuth2)
CREATORS_CREDENTIAL_ID = os.environ.get("CREATORS_CREDENTIAL_ID", "")
CREATORS_CREDENTIAL_SECRET = os.environ.get("CREATORS_CREDENTIAL_SECRET", "")
CREATORS_CREDENTIAL_VERSION = os.environ.get("CREATORS_CREDENTIAL_VERSION", "")
CREATORS_MARKETPLACE = os.environ.get("CREATORS_MARKETPLACE", "www.amazon.it")  # confermato da te

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

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

ITEMS_PER_PAGE = 8
PAGES = 4

bot = Bot(token=TELEGRAM_BOT_TOKEN)

# =========================
# Creators API - endpoints
# (region token endpoint: dalla guida / PDF)
# Nel tuo log si vede eu-south-2 e path /oauth2/token
# =========================
CREATORS_TOKEN_URL = os.environ.get(
    "CREATORS_TOKEN_URL",
    "https://creatorsapi.auth.eu-south-2.amazoncognito.com/oauth2/token",
)
CREATORS_BASE_URL = os.environ.get(
    "CREATORS_BASE_URL",
    "https://creatorsapi.amazon/catalog/v1",
)

# Resources "whitelist-safe" (NO summaries, NO savingBasis)
SEARCH_RESOURCES = [
    "itemInfo.title",
    "images.primary.large",
    "offersV2.listings.price",
    "offersV2.listings.dealDetails",
    "offersV2.listings.isBuyBoxWinner",
    "offersV2.listings.availability",
]

GETITEMS_RESOURCES = [
    "itemInfo.title",
    "images.primary.large",
    "offersV2.listings.price",
    "offersV2.listings.dealDetails",
    "offersV2.listings.isBuyBoxWinner",
    "offersV2.listings.availability",
]

# =========================
# Token cache
# =========================
_token_cache = {"access_token": None, "expires_at": 0}

def _basic_auth_header(client_id: str, client_secret: str) -> str:
    import base64
    raw = f"{client_id}:{client_secret}".encode("utf-8")
    return "Basic " + base64.b64encode(raw).decode("utf-8")

def get_access_token():
    now = int(time.time())
    if _token_cache["access_token"] and now < (_token_cache["expires_at"] - 30):
        return _token_cache["access_token"]

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": _basic_auth_header(CREATORS_CREDENTIAL_ID, CREATORS_CREDENTIAL_SECRET),
    }
    data = "grant_type=client_credentials&scope=creatorsapi/default"

    if DEBUG_AMAZON:
        print(f"[DEBUG] Token refresh -> {CREATORS_TOKEN_URL}")

    r = requests.post(CREATORS_TOKEN_URL, headers=headers, data=data, timeout=20)
    if r.status_code != 200:
        raise RuntimeError(f"Token error {r.status_code}: {r.text}")

    payload = r.json()
    token = payload.get("access_token")
    expires_in = int(payload.get("expires_in", 3600))

    _token_cache["access_token"] = token
    _token_cache["expires_at"] = now + expires_in

    if DEBUG_AMAZON:
        print(f"[DEBUG] Token OK (expires_in={expires_in}s)")

    return token

def creators_headers(token: str):
    # Per guida: "Authorization: Bearer <token>, Version <version>"
    return {
        "Authorization": f"Bearer {token}, Version {CREATORS_CREDENTIAL_VERSION}",
        "Content-Type": "application/json",
        "x-marketplace": CREATORS_MARKETPLACE,
    }

# =========================
# Helpers
# =========================
def parse_eur_amount(display_amount):
    if not display_amount:
        return None
    s = str(display_amount)
    s = s.replace("\u20ac", "").replace("€", "")
    s = s.replace("\xa0", " ").strip()
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
# Creators API calls
# =========================
def creators_search_items(keyword: str, page: int):
    token = get_access_token()
    url = f"{CREATORS_BASE_URL}/searchItems"
    body = {
        "keywords": keyword,
        "partnerTag": AMAZON_ASSOCIATE_TAG,
        "marketplace": CREATORS_MARKETPLACE,
        "itemCount": ITEMS_PER_PAGE,
        "itemPage": page,
        "resources": SEARCH_RESOURCES,
    }
    r = requests.post(url, headers=creators_headers(token), json=body, timeout=25)
    if r.status_code != 200:
        raise RuntimeError(f"Creators API error {r.status_code}: {r.text}")
    return r.json()

def creators_get_items(item_ids):
    token = get_access_token()
    url = f"{CREATORS_BASE_URL}/getItems"
    body = {
        "itemIds": item_ids,
        "partnerTag": AMAZON_ASSOCIATE_TAG,
        "marketplace": CREATORS_MARKETPLACE,
        "resources": GETITEMS_RESOURCES,
    }
    r = requests.post(url, headers=creators_headers(token), json=body, timeout=25)
    if r.status_code != 200:
        raise RuntimeError(f"Creators API error {r.status_code}: {r.text}")
    return r.json()

# =========================
# Extraction from dict
# =========================
def extract_from_item_dict(item: dict):
    asin = (item.get("asin") or "").strip().upper()
    if not asin:
        return None

    title = (((item.get("itemInfo") or {}).get("title") or {}).get("displayValue") or "").strip()
    title = " ".join(title.split())

    detail_url = item.get("detailPageURL")
    if not detail_url:
        detail_url = f"https://www.amazon.it/dp/{asin}?tag={AMAZON_ASSOCIATE_TAG}"

    img_url = None
    images = item.get("images") or {}
    primary = images.get("primary") or {}
    large = primary.get("large") or {}
    img_url = large.get("url") or "https://m.media-amazon.com/images/I/71bhWgQK-cL._AC_SL1500_.jpg"

    # prezzo/sconto SOLO da listings + dealDetails
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
    price_obj = l0.get("price") or {}
    price_val = parse_eur_amount(price_obj.get("displayAmount")) if isinstance(price_obj, dict) else parse_eur_amount(price_obj)

    # dealDetails: percent/savings possono avere naming variabile
    dd = l0.get("dealDetails") or {}

    disc_pct = 0
    for k in ("savingsPercentage", "percentage", "percent", "savings_percent"):
        if k in dd and dd.get(k) is not None:
            try:
                disc_pct = int(dd.get(k))
                break
            except:
                pass

    savings_amount = 0.0
    for k in ("savings", "savingsAmount", "amountSaved"):
        if k in dd and dd.get(k) is not None:
            v = dd.get(k)
            if isinstance(v, dict):
                savings_amount = parse_eur_amount(v.get("displayAmount")) or 0.0
            else:
                try:
                    savings_amount = float(v)
                except:
                    savings_amount = 0.0
            break

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

    # calcolo old price
    if savings_amount and savings_amount > 0:
        old_val = price_val + float(savings_amount)
    elif disc_pct and disc_pct > 0:
        try:
            old_val = price_val / (1 - disc_pct / 100.0)
        except:
            old_val = price_val
    else:
        old_val = price_val

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
# Core logic
# =========================
def _first_valid_item_for_keyword(kw, pubblicati):
    reasons = Counter()
    fallback_asins = []

    for page in range(1, PAGES + 1):
        try:
            res = creators_search_items(kw, page)

            if DEBUG_AMAZON:
                print(f"[DEBUG] kw={kw} page={page} items=?")
                print(f"[DEBUG] searchItems raw keys: {list(res.keys())}")
                preview = str(res)[:900]
                print(f"[DEBUG] searchItems raw preview: {preview}...")

            search_result = res.get("searchResult") or {}
            items = search_result.get("items") or []
            if DEBUG_AMAZON:
                print(f"[DEBUG] kw={kw} page={page} items={len(items)}")

        except Exception as e:
            reasons["api_error"] += 1
            print(f"❌ Creators searchItems error (kw='{kw}', page={page}): {e}")
            items = []

        for raw in items:
            asin = (raw.get("asin") or "").strip().upper()
            if not asin:
                reasons["no_asin"] += 1
                continue
            if asin in pubblicati or not can_post(asin, hours=24):
                reasons["already_posted"] += 1
                continue

            data = extract_from_item_dict(raw)
            if not data:
                reasons["extract_fail"] += 1
                continue

            if data["price_new"] is None:
                reasons["no_price_in_searchitems"] += 1
                if len(fallback_asins) < GETITEMS_FALLBACK_MAX:
                    fallback_asins.append(asin)
                if DEBUG_AMAZON:
                    has_offers = (raw.get("offersV2") is not None)
                    print(f"[DEBUG] asin={asin} no_price | has_offers={has_offers}")
                continue

            price_val = data["price_new"]
            disc = data["discount"] or 0

            if price_val < MIN_PRICE or price_val > MAX_PRICE:
                reasons["price_out_range"] += 1
                continue
            if disc < MIN_DISCOUNT:
                reasons["disc_too_low"] += 1
                continue

            if DEBUG_AMAZON:
                print(f"[DEBUG] FOUND via SearchItems asin={asin} price={price_val} disc={disc}")

            return {
                "asin": asin,
                "title": (data["title"][:80].strip() + ("…" if len(data["title"]) > 80 else "")),
                "price_new": data["price_new"],
                "price_old": data["price_old"] if data["price_old"] is not None else data["price_new"],
                "discount": disc,
                "url_img": data["url_img"],
                "url": data["url"],
                "minimo": disc >= 30,
            }

    # Fallback GetItems su pochi ASIN (stessa whitelist resources)
    if fallback_asins:
        try:
            if DEBUG_AMAZON:
                print(f"[DEBUG] GetItems fallback asins={fallback_asins}")

            res = creators_get_items(fallback_asins)
            get_result = res.get("getResult") or {}
            items = get_result.get("items") or []

            for raw in items:
                asin = (raw.get("asin") or "").strip().upper()
                if not asin or asin in pubblicati or not can_post(asin, hours=24):
                    continue

                data = extract_from_item_dict(raw)
                if not data or data["price_new"] is None:
                    continue

                price_val = data["price_new"]
                disc = data["discount"] or 0

                if price_val < MIN_PRICE or price_val > MAX_PRICE:
                    continue
                if disc < MIN_DISCOUNT:
                    continue

                if DEBUG_AMAZON:
                    print(f"[DEBUG] FOUND via GetItems asin={asin} price={price_val} disc={disc}")

                return {
                    "asin": asin,
                    "title": (data["title"][:80].strip() + ("…" if len(data["title"]) > 80 else "")),
                    "price_new": data["price_new"],
                    "price_old": data["price_old"] if data["price_old"] is not None else data["price_new"],
                    "discount": disc,
                    "url_img": data["url_img"],
                    "url": data["url"],
                    "minimo": disc >= 30,
                }

        except Exception as e:
            reasons["getitems_error"] += 1
            if DEBUG_AMAZON:
                print(f"[DEBUG] GetItems fallback error: {e}")

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
