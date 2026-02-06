import os
import time
import html
import json
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

AMAZON_ASSOCIATE_TAG = os.environ.get("AMAZON_ASSOCIATE_TAG", "").strip()
CREATORS_CREDENTIAL_ID = os.environ.get("CREATORS_CREDENTIAL_ID", "").strip()
CREATORS_CREDENTIAL_SECRET = os.environ.get("CREATORS_CREDENTIAL_SECRET", "").strip()
CREATORS_CREDENTIAL_VERSION = os.environ.get("CREATORS_CREDENTIAL_VERSION", "").strip()
CREATORS_MARKETPLACE = os.environ.get("CREATORS_MARKETPLACE", "www.amazon.it").strip()

DEBUG_AMAZON = os.environ.get("DEBUG_AMAZON", "0") == "1"
GETITEMS_FALLBACK_MAX = int(os.environ.get("GETITEMS_FALLBACK_MAX", "4"))

MIN_DISCOUNT = int(os.environ.get("MIN_DISCOUNT", "15"))
MIN_PRICE = float(os.environ.get("MIN_PRICE", "15"))
MAX_PRICE = float(os.environ.get("MAX_PRICE", "1900"))

FONT_PATH = os.environ.get("FONT_PATH", "Montserrat-VariableFont_wght.ttf")
LOGO_PATH = os.environ.get("LOGO_PATH", "header_clean2.png")
BADGE_PATH = os.environ.get("BADGE_PATH", "minimo storico flat.png")

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

ITEMS_PER_PAGE = 8
PAGES = 4

# Creators API
CREATORS_API_BASE = "https://creatorsapi.amazon"
TOKEN_ENDPOINT = "https://creatorsapi.auth.eu-south-2.amazoncognito.com/oauth2/token"
SCOPE = "creatorsapi/default"

# ✅ Resources: niente savingBasis in SearchItems L1 (ti fa 400), usiamo summaries per lo sconto
RES_L1 = [
    "images.primary.large",
    "itemInfo.title",
    "offersV2.listings.price",
    "offersV2.summaries.savings",
    "offersV2.summaries.lowestPrice",
]
RES_L2 = [
    "images.primary.large",
    "itemInfo.title",
    "offersV2.listings.price",
    "offersV2.summaries.lowestPrice",
]
RES_L3 = [
    "images.primary.large",
    "itemInfo.title",
    "offersV2.listings.price",
]
RES_L4 = [
    "images.primary.large",
    "itemInfo.title",
]
RESOURCE_LEVELS_SEARCH = [RES_L1, RES_L2, RES_L3, RES_L4]

# GetItems può essere più ricco (spesso accetta savingBasis, ma se non lo accetta non muore)
RES_GETITEMS = [
    "images.primary.large",
    "itemInfo.title",
    "offersV2.listings.price",
    "offersV2.summaries.savings",
    "offersV2.summaries.lowestPrice",
    # tentiamo anche savingBasis ma se rifiuta gestiamo
    "offersV2.listings.savingBasis",
]

bot = Bot(token=TELEGRAM_BOT_TOKEN) if TELEGRAM_BOT_TOKEN else None

# =========================
# SAFETY CHECKS
# =========================
def _assert_env():
    missing = []
    if not TELEGRAM_BOT_TOKEN: missing.append("TELEGRAM_BOT_TOKEN")
    if not TELEGRAM_CHAT_ID: missing.append("TELEGRAM_CHAT_ID")
    if not AMAZON_ASSOCIATE_TAG: missing.append("AMAZON_ASSOCIATE_TAG")
    if not CREATORS_CREDENTIAL_ID: missing.append("CREATORS_CREDENTIAL_ID")
    if not CREATORS_CREDENTIAL_SECRET: missing.append("CREATORS_CREDENTIAL_SECRET")
    if not CREATORS_CREDENTIAL_VERSION: missing.append("CREATORS_CREDENTIAL_VERSION")
    if missing:
        raise RuntimeError(f"Env var mancanti: {', '.join(missing)}")

# =========================
# TOKEN CACHE
# =========================
_token_cache = {"access_token": None, "expires_at": 0}

def _get_access_token():
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
        TOKEN_ENDPOINT,
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
        print(f"[DEBUG] Token OK (expires_in={expires_in}s) url={TOKEN_ENDPOINT}")

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
    r = requests.post(url, headers=_creators_headers(), data=json.dumps(body), timeout=25)
    return r

# =========================
# HELPERS
# =========================
def _extract_title(item):
    info = item.get("itemInfo") or {}
    title_obj = info.get("title") or {}
    t = title_obj.get("displayValue") or ""
    return " ".join(str(t).split())

def _extract_image(item):
    images = item.get("images") or {}
    primary = images.get("primary") or {}
    large = primary.get("large") or {}
    url = large.get("url")
    if not url:
        url = "https://m.media-amazon.com/images/I/71bhWgQK-cL._AC_SL1500_.jpg"
    return url

def _extract_detail_url(item, asin):
    url = item.get("detailPageURL")
    if url:
        return url
    if asin:
        return f"https://www.amazon.it/dp/{asin}?tag={AMAZON_ASSOCIATE_TAG}"
    return "https://www.amazon.it"

def _money_amount(obj):
    """Ritorna float se trova money.amount oppure displayAmount convertibile."""
    if not obj:
        return None
    money = obj.get("money")
    if isinstance(money, dict) and money.get("amount") is not None:
        try:
            return float(money["amount"])
        except:
            pass
    disp = obj.get("displayAmount")
    if disp:
        s = str(disp).replace("€", "").replace("\xa0", " ").strip()
        s = s.replace(".", "").replace(",", ".")
        try:
            return float(s)
        except:
            return None
    return None

def _extract_price_old_disc(item):
    """
    Prezzo nuovo: offersV2.listings[0].price
    Sconto: preferisci offersV2.summaries[0].savings.percentage
    Old price:
      - se savingBasis disponibile => quello
      - altrimenti prova summaries[0].lowestPrice come base (quando sensato)
      - altrimenti old = new
    """
    offers = item.get("offersV2") or {}
    listings = offers.get("listings") or []
    l0 = listings[0] if listings else None

    price_new = None
    if l0:
        price_new = _money_amount(l0.get("price") or {})

    # summaries
    summaries = offers.get("summaries") or []
    s0 = summaries[0] if summaries else {}

    # 1) sconto diretto
    disc = 0
    savings = s0.get("savings") or {}
    perc = savings.get("percentage")
    if perc is not None:
        try:
            disc = int(perc)
        except:
            disc = 0

    # 2) old price: savingBasis se presente
    old_price = None
    if l0:
        old_price = _money_amount(l0.get("savingBasis") or {})

    # 3) old price: lowestPrice (se maggiore del new)
    if old_price is None:
        lp = _money_amount(s0.get("lowestPrice") or {})
        if price_new is not None and lp is not None and lp >= price_new:
            old_price = lp

    # fallback old=new
    if price_new is None:
        return None, None, None

    if old_price is None:
        old_price = float(price_new)

    # se disc non c’è ma ho old/new, calcolo io
    if disc == 0 and old_price and old_price > 0 and old_price >= price_new:
        try:
            disc = int(round((1 - (float(price_new) / float(old_price))) * 100))
        except:
            disc = 0

    return float(price_new), int(disc), float(old_price)

# =========================
# IMAGE
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
# PUBLISHED
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
# API CALLS
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
    return _creators_post("/catalog/v1/searchItems", body)

def creators_get_items(asins, resources):
    body = {
        "itemIds": asins,
        "partnerTag": AMAZON_ASSOCIATE_TAG,
        "marketplace": CREATORS_MARKETPLACE,
        "resources": resources,
    }
    return _creators_post("/catalog/v1/getItems", body)

def _json_or_none(resp):
    try:
        return resp.json()
    except:
        return None

# =========================
# CORE
# =========================
def _first_valid_item_for_keyword(kw, pubblicati):
    reasons = Counter()
    fallback_asins = []

    for page in range(1, PAGES + 1):
        items = []
        used_resources = None

        for lvl, res in enumerate(RESOURCE_LEVELS_SEARCH, start=1):
            if DEBUG_AMAZON:
                print(f"[DEBUG] SearchItems try L{lvl} resources={res}")

            resp = creators_search_items(kw, page, res)
            if resp.status_code == 200:
                data = _json_or_none(resp) or {}
                sr = data.get("searchResult") or data
                items = sr.get("items") or []
                used_resources = res
                if DEBUG_AMAZON:
                    print(f"[DEBUG] kw={kw} page={page} items={len(items)} used_resources={used_resources}")
                    print(f"[DEBUG] searchItems raw keys: {list(sr.keys()) if isinstance(sr, dict) else []}")
                break
            else:
                if DEBUG_AMAZON:
                    print(f"[DEBUG] SearchItems L{lvl} rejected ({resp.status_code}) -> {resp.text[:240]}")
                reasons["api_error"] += 1

        if used_resources is None:
            continue

        if DEBUG_AMAZON and items:
            print(f"[DEBUG] searchItems raw preview: {json.dumps({'asin': items[0].get('asin'), 'detailPageURL': items[0].get('detailPageURL')}, ensure_ascii=False)}")

        for item in items:
            asin = (item.get("asin") or "").strip().upper()
            if not asin:
                reasons["no_asin"] += 1
                continue
            if asin in pubblicati or not can_post(asin, hours=24):
                reasons["already_posted"] += 1
                continue

            title = _extract_title(item)
            url_img = _extract_image(item)
            url = _extract_detail_url(item, asin)

            price_new, disc, price_old = _extract_price_old_disc(item)
            has_offers = bool((item.get("offersV2") or {}).get("listings"))

            if price_new is None:
                reasons["no_price"] += 1
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
                "price_old": float(price_old),
                "discount": int(disc),
                "url_img": url_img,
                "url": url,
                "minimo": minimo,
            }

    # Fallback GetItems (se serve)
    if fallback_asins:
        if DEBUG_AMAZON:
            print(f"[DEBUG] GetItems fallback asins={fallback_asins}")

        # tentiamo 2 volte: prima con RES_GETITEMS completo, poi senza savingBasis se rifiuta
        for attempt, res in enumerate([RES_GETITEMS, [r for r in RES_GETITEMS if r != "offersV2.listings.savingBasis"]], start=1):
            resp = creators_get_items(fallback_asins, res)
            if resp.status_code != 200:
                if DEBUG_AMAZON:
                    print(f"[DEBUG] GetItems attempt {attempt} error {resp.status_code}: {resp.text[:240]}")
                continue

            data = _json_or_none(resp) or {}
            ir = data.get("itemsResult") or data
            items = ir.get("items") or []
            if DEBUG_AMAZON:
                print(f"[DEBUG] GetItems attempt {attempt} items={len(items)} used_resources={res}")

            for item in items:
                asin = (item.get("asin") or "").strip().upper()
                if not asin or asin in pubblicati or not can_post(asin, hours=24):
                    continue

                title = _extract_title(item)
                url_img = _extract_image(item)
                url = _extract_detail_url(item, asin)

                price_new, disc, price_old = _extract_price_old_disc(item)
                if price_new is None:
                    continue
                if price_new < MIN_PRICE or price_new > MAX_PRICE:
                    continue
                if disc < MIN_DISCOUNT:
                    continue

                minimo = disc >= 30
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

    if DEBUG_AMAZON:
        print(f"[DEBUG] kw={kw} reasons={dict(reasons)} fallback_asins={fallback_asins}")

    return None

# =========================
# SEND
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
