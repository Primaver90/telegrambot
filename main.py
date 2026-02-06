import os
import time
import html
import json
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
# ENV VARS (Render)
# =========================
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

AMAZON_ASSOCIATE_TAG = os.environ.get("AMAZON_ASSOCIATE_TAG", "").strip()  # es: itech00-21
CREATORS_CREDENTIAL_ID = os.environ.get("CREATORS_CREDENTIAL_ID", "").strip()
CREATORS_CREDENTIAL_SECRET = os.environ.get("CREATORS_CREDENTIAL_SECRET", "").strip()
CREATORS_CREDENTIAL_VERSION = os.environ.get("CREATORS_CREDENTIAL_VERSION", "").strip()  # es: 2
CREATORS_MARKETPLACE = os.environ.get("CREATORS_MARKETPLACE", "www.amazon.it").strip()

# Debug
DEBUG_AMAZON = os.environ.get("DEBUG_AMAZON", "0") == "1"
GETITEMS_FALLBACK_MAX = int(os.environ.get("GETITEMS_FALLBACK_MAX", "4"))

# Filtri offerta
MIN_DISCOUNT = int(os.environ.get("MIN_DISCOUNT", "15"))
MIN_PRICE = float(os.environ.get("MIN_PRICE", "15"))
MAX_PRICE = float(os.environ.get("MAX_PRICE", "1900"))

# Assets
FONT_PATH = os.environ.get("FONT_PATH", "Montserrat-VariableFont_wght.ttf")
LOGO_PATH = os.environ.get("LOGO_PATH", "header_clean2.png")
BADGE_PATH = os.environ.get("BADGE_PATH", "minimo storico flat.png")

# Storage
DATA_DIR = "/tmp/botdata"
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

# Creators API
CREATORS_API_BASE = "https://creatorsapi.amazon"
TOKEN_ENDPOINT_EU_V2 = "https://creatorsapi.auth.eu-south-2.amazoncognito.com/oauth2/token"
SCOPE = "creatorsapi/default"

# Resources (a livelli) – l’API a volte rifiuta certi set, quindi degradiamo.
# NOTA: qui usiamo naming come nei tuoi log: images.primary.large, itemInfo.title, offersV2...
RES_L1 = ["images.primary.large", "itemInfo.title", "offersV2.listings.price", "offersV2.listings.savingBasis"]
RES_L2 = ["images.primary.large", "itemInfo.title", "offersV2.listings.price"]
RES_L3 = ["images.primary.large", "itemInfo.title"]
RESOURCE_LEVELS_SEARCH = [RES_L1, RES_L2, RES_L3]

RES_GETITEMS = ["images.primary.large", "itemInfo.title", "offersV2.listings.price", "offersV2.listings.savingBasis"]

# Telegram
bot = Bot(token=TELEGRAM_BOT_TOKEN) if TELEGRAM_BOT_TOKEN else None

# =========================
# SAFETY CHECKS
# =========================
def _assert_env():
    missing = []
    if not TELEGRAM_BOT_TOKEN:
        missing.append("TELEGRAM_BOT_TOKEN")
    if not TELEGRAM_CHAT_ID:
        missing.append("TELEGRAM_CHAT_ID")
    if not AMAZON_ASSOCIATE_TAG:
        missing.append("AMAZON_ASSOCIATE_TAG")
    if not CREATORS_CREDENTIAL_ID:
        missing.append("CREATORS_CREDENTIAL_ID")
    if not CREATORS_CREDENTIAL_SECRET:
        missing.append("CREATORS_CREDENTIAL_SECRET")
    if not CREATORS_CREDENTIAL_VERSION:
        missing.append("CREATORS_CREDENTIAL_VERSION")

    if missing:
        raise RuntimeError(f"Env var mancanti: {', '.join(missing)}")


# =========================
# TOKEN CACHE
# =========================
_token_cache = {"access_token": None, "expires_at": 0}

def _get_access_token():
    """
    OAuth 2.0 (client_credentials) come da PDF: client_id e client_secret nel body.
    """
    now = int(time.time())
    if _token_cache["access_token"] and now < (_token_cache["expires_at"] - 60):
        return _token_cache["access_token"]

    data = {
        "grant_type": "client_credentials",
        "client_id": CREATORS_CREDENTIAL_ID,
        "client_secret": CREATORS_CREDENTIAL_SECRET,
        "scope": SCOPE,
    }

    r = requests.post(
        TOKEN_ENDPOINT_EU_V2,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data=data,
        timeout=20,
    )

    if r.status_code != 200:
        raise RuntimeError(f"Token error {r.status_code}: {r.text}")

    payload = r.json()
    token = payload.get("access_token")
    expires_in = int(payload.get("expires_in", 3600))
    if not token:
        raise RuntimeError(f"Token response senza access_token: {payload}")

    _token_cache["access_token"] = token
    _token_cache["expires_at"] = int(time.time()) + expires_in

    if DEBUG_AMAZON:
        print(f"[DEBUG] Token OK (expires_in={expires_in}s) url={TOKEN_ENDPOINT_EU_V2}")

    return token


def _creators_headers():
    token = _get_access_token()
    return {
        "Authorization": f"Bearer {token}, Version {CREATORS_CREDENTIAL_VERSION}",
        "Content-Type": "application/json",
        "x-marketplace": CREATORS_MARKETPLACE,
    }


def _creators_post(path, body):
    url = f"{CREATORS_API_BASE}{path}"
    h = _creators_headers()
    r = requests.post(url, headers=h, data=json.dumps(body), timeout=25)
    return r


# =========================
# PARSING HELPERS
# =========================
def _parse_eur_from_display_amount(s):
    """
    Gestisce "249,0 €", "249,00€", "1.299,00" ecc.
    """
    if not s:
        return None
    txt = str(s).replace("€", "").replace("\xa0", " ").strip()
    # "1.299,00" -> "1299.00"
    txt = txt.replace(".", "").replace(",", ".").strip()
    try:
        return float(txt)
    except:
        return None


def _extract_price_and_old(item):
    """
    Legge Creators API dict:
      offersV2: { listings: [ { price: { money: {amount}, displayAmount }, savingBasis: {...} } ] }
    """
    offers = item.get("offersV2") or {}
    listings = offers.get("listings") or []
    if not listings:
        return (None, None, None)

    l0 = listings[0] or {}

    price = l0.get("price") or {}
    price_amount = None

    # 1) money.amount (migliore)
    money = price.get("money") or {}
    if isinstance(money, dict):
        price_amount = money.get("amount")

    # 2) displayAmount fallback
    if price_amount is None:
        price_amount = _parse_eur_from_display_amount(price.get("displayAmount"))

    # savingBasis (prezzo “vecchio”)
    saving_basis = l0.get("savingBasis") or {}
    old_amount = None
    old_money = saving_basis.get("money") or {}
    if isinstance(old_money, dict):
        old_amount = old_money.get("amount")
    if old_amount is None:
        old_amount = _parse_eur_from_display_amount(saving_basis.get("displayAmount"))

    # se non ho old_amount, lo metto uguale al nuovo (niente sconto calcolabile)
    if price_amount is None:
        return (None, None, None)

    if old_amount is None:
        old_amount = float(price_amount)

    # calcolo sconto %
    try:
        if old_amount > 0 and old_amount >= price_amount:
            disc = int(round((1.0 - (float(price_amount) / float(old_amount))) * 100))
        else:
            disc = 0
    except:
        disc = 0

    return (float(price_amount), disc, float(old_amount))


def _extract_title(item):
    # itemInfo.title.displayValue
    info = item.get("itemInfo") or {}
    title_obj = info.get("title") or {}
    t = title_obj.get("displayValue") or ""
    return " ".join(str(t).split())


def _extract_image(item):
    # images.primary.large.url
    images = item.get("images") or {}
    primary = images.get("primary") or {}
    large = primary.get("large") or {}
    url = large.get("url")
    if not url:
        # fallback brutale
        url = "https://m.media-amazon.com/images/I/71bhWgQK-cL._AC_SL1500_.jpg"
    return url


def _extract_detail_url(item, asin):
    url = item.get("detailPageURL")
    if url:
        return url
    if asin:
        return f"https://www.amazon.it/dp/{asin}?tag={AMAZON_ASSOCIATE_TAG}"
    return "https://www.amazon.it"


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
# PERSISTENCE: PUBLISHED
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
# KEYWORD ROTATION
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
# CREATORS API CALLS
# =========================
def creators_search_items(kw, page, resources):
    body = {
        "keywords": kw,
        "partnerTag": AMAZON_ASSOCIATE_TAG,
        "marketplace": CREATORS_MARKETPLACE,
        "resources": resources,
        "page": page,
        "itemCount": ITEMS_PER_PAGE,
    }
    r = _creators_post("/catalog/v1/searchItems", body)
    return r


def creators_get_items(asins, resources):
    body = {
        "itemIds": asins,
        "partnerTag": AMAZON_ASSOCIATE_TAG,
        "marketplace": CREATORS_MARKETPLACE,
        "resources": resources,
    }
    r = _creators_post("/catalog/v1/getItems", body)
    return r


def _api_error_text(resp):
    try:
        return resp.text
    except:
        return "<no text>"


def _json_or_none(resp):
    try:
        return resp.json()
    except:
        return None


# =========================
# CORE: FIND FIRST VALID ITEM
# =========================
def _first_valid_item_for_keyword(kw, pubblicati):
    reasons = Counter()
    fallback_asins = []

    for page in range(1, PAGES + 1):
        items = []

        # Prova resources a livelli (per evitare errori "resources rejected")
        used_resources = None
        last_err = None
        for lvl, res in enumerate(RESOURCE_LEVELS_SEARCH, start=1):
            if DEBUG_AMAZON:
                print(f"[DEBUG] SearchItems try L{lvl} resources={res}")

            resp = creators_search_items(kw, page, res)
            if resp.status_code == 200:
                data = _json_or_none(resp) or {}
                # compatibilità: alcuni response hanno searchResult, altri items direttamente
                sr = data.get("searchResult") or data
                items = sr.get("items") or []
                used_resources = res
                if DEBUG_AMAZON:
                    keys = list(sr.keys()) if isinstance(sr, dict) else []
                    print(f"[DEBUG] kw={kw} page={page} items={len(items)} used_resources={used_resources}")
                    if keys:
                        print(f"[DEBUG] searchItems raw keys: {keys[:10]}")
                break

            # Se 400: spesso è resources invalid -> degrada
            last_err = _api_error_text(resp)
            if DEBUG_AMAZON:
                print(f"[DEBUG] SearchItems L{lvl} rejected ({resp.status_code}) -> {last_err[:240]}")
            reasons["paapi_error"] += 1
            continue

        if used_resources is None:
            # nessun livello ha funzionato
            if DEBUG_AMAZON and last_err:
                print(f"[DEBUG] SearchItems failed all levels kw={kw} page={page}: {last_err[:400]}")
            continue

        # Debug preview (solo primo item)
        if DEBUG_AMAZON and items:
            preview = {"asin": items[0].get("asin"), "detailPageURL": items[0].get("detailPageURL")}
            print(f"[DEBUG] searchItems raw preview: {json.dumps(preview, ensure_ascii=False)[:300]}")

        for item in items:
            asin = (item.get("asin") or "").strip().upper()
            if not asin:
                reasons["no_asin"] += 1
                continue
            if asin in pubblicati or not can_post(asin, hours=24):
                reasons["already_posted"] += 1
                continue

            # MAP? (se presente e True, spesso niente prezzi reali)
            if (item.get("offersV2") or {}).get("violatesMAP") is True:
                reasons["violates_map"] += 1
                continue

            title = _extract_title(item)
            url_img = _extract_image(item)
            url = _extract_detail_url(item, asin)

            price_new, disc, price_old = _extract_price_and_old(item)

            has_offers = bool((item.get("offersV2") or {}).get("listings"))
            if price_new is None:
                reasons["no_price_in_searchitems"] += 1
                if DEBUG_AMAZON:
                    print(f"[DEBUG] asin={asin} no_price | has_offers={has_offers}")
                if len(fallback_asins) < GETITEMS_FALLBACK_MAX:
                    fallback_asins.append(asin)
                continue

            if DEBUG_AMAZON:
                print(f"[DEBUG] asin={asin} price={price_new} old={price_old} disc={disc} has_offers={has_offers}")

            if price_new < MIN_PRICE or price_new > MAX_PRICE:
                reasons["price_out_range"] += 1
                continue

            if disc < MIN_DISCOUNT:
                reasons["disc_too_low"] += 1
                continue

            minimo = disc >= 30

            return {
                "asin": asin,
                "title": title[:80].strip() + ("…" if len(title) > 80 else ""),
                "price_new": float(price_new),
                "price_old": float(price_old) if price_old is not None else float(price_new),
                "discount": int(disc),
                "url_img": url_img,
                "url": url,
                "minimo": minimo,
            }

    # Fallback: GetItems su pochi ASIN per cercare prezzi
    if fallback_asins:
        if DEBUG_AMAZON:
            print(f"[DEBUG] GetItems fallback asins={fallback_asins}")

        resp = creators_get_items(fallback_asins, RES_GETITEMS)
        if resp.status_code == 200:
            data = _json_or_none(resp) or {}
            ir = data.get("itemsResult") or data
            items = ir.get("items") or []
            if DEBUG_AMAZON:
                print(f"[DEBUG] GetItems items={len(items)}")

            for item in items:
                asin = (item.get("asin") or "").strip().upper()
                if not asin or asin in pubblicati or not can_post(asin, hours=24):
                    continue

                title = _extract_title(item)
                url_img = _extract_image(item)
                url = _extract_detail_url(item, asin)

                price_new, disc, price_old = _extract_price_and_old(item)
                if price_new is None:
                    continue

                if price_new < MIN_PRICE or price_new > MAX_PRICE:
                    continue
                if disc < MIN_DISCOUNT:
                    continue

                minimo = disc >= 30
                if DEBUG_AMAZON:
                    print(f"[DEBUG] FOUND via GetItems asin={asin} price={price_new} old={price_old} disc={disc}")

                return {
                    "asin": asin,
                    "title": title[:80].strip() + ("…" if len(title) > 80 else ""),
                    "price_new": float(price_new),
                    "price_old": float(price_old) if price_old is not None else float(price_new),
                    "discount": int(disc),
                    "url_img": url_img,
                    "url": url,
                    "minimo": minimo,
                }
        else:
            if DEBUG_AMAZON:
                print(f"[DEBUG] GetItems error {resp.status_code}: {_api_error_text(resp)[:400]}")
            reasons["getitems_error"] += 1

    if DEBUG_AMAZON:
        print(f"[DEBUG] kw={kw} reasons={dict(reasons)} fallback_asins={fallback_asins}")

    return None


# =========================
# SEND OFFER
# =========================
def invia_offerta():
    _assert_env()

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
# TIME WINDOW ITALY
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
