import os
import time
import json
import html
import base64
from io import BytesIO
from datetime import datetime, timedelta
from pathlib import Path
from collections import Counter

import requests
import schedule
from PIL import Image, ImageDraw, ImageFont
from telegram import Bot, InlineKeyboardMarkup, InlineKeyboardButton


# =========================
# ENV / CONFIG
# =========================
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

CREATORS_CREDENTIAL_ID = os.environ.get("CREATORS_CREDENTIAL_ID", "")
CREATORS_CREDENTIAL_SECRET = os.environ.get("CREATORS_CREDENTIAL_SECRET", "")
CREATORS_CREDENTIAL_VERSION = os.environ.get("CREATORS_CREDENTIAL_VERSION", "")
CREATORS_MARKETPLACE = os.environ.get("CREATORS_MARKETPLACE", "www.amazon.it")

CREATORS_TOKEN_URL = os.environ.get(
    "CREATORS_TOKEN_URL",
    "https://creatorsapi.auth.eu-south-2.amazoncognito.com/oauth2/token",
)

CREATORS_API_BASE = os.environ.get(
    "CREATORS_API_BASE",
    "https://creatorsapi.amazon/catalog/v1",
)

AMAZON_ASSOCIATE_TAG = os.environ.get("AMAZON_ASSOCIATE_TAG", "itech00-21")

DATA_DIR = os.environ.get("DATA_DIR", "/tmp/botdata")
Path(DATA_DIR).mkdir(parents=True, exist_ok=True)

PUB_FILE = os.path.join(DATA_DIR, "pubblicati.txt")
PUB_TS = os.path.join(DATA_DIR, "pubblicati_ts.csv")
KW_INDEX = os.path.join(DATA_DIR, "kw_index.txt")

MIN_DISCOUNT = int(os.environ.get("MIN_DISCOUNT", "15"))
MIN_PRICE = float(os.environ.get("MIN_PRICE", "15"))
MAX_PRICE = float(os.environ.get("MAX_PRICE", "1900"))

DEBUG_AMAZON = os.environ.get("DEBUG_AMAZON", "0") == "1"

RUN_EVERY_MINUTES = int(os.environ.get("RUN_EVERY_MINUTES", "14"))

FONT_PATH = os.environ.get("FONT_PATH", "Montserrat-VariableFont_wght.ttf")
LOGO_PATH = os.environ.get("LOGO_PATH", "header_clean2.png")
BADGE_PATH = os.environ.get("BADGE_PATH", "minimo storico flat.png")

SEARCH_INDEX = os.environ.get("SEARCH_INDEX", "All")
ITEMS_PER_PAGE = int(os.environ.get("ITEMS_PER_PAGE", "8"))
PAGES = int(os.environ.get("PAGES", "4"))

KEYWORDS = [
    "Apple", "Android", "iPhone", "MacBook", "tablet", "smartwatch",
    "auricolari Bluetooth", "smart TV", "monitor PC", "notebook",
    "gaming mouse", "gaming tastiera", "console", "soundbar", "smart home",
    "aspirapolvere robot", "telecamere WiFi", "caricatore wireless",
    "accessori smartphone", "accessori iPhone",
]

bot = Bot(token=TELEGRAM_BOT_TOKEN)


# =========================
# RESOURCES (Creators API)
# =========================
# SearchItems: proviamo set progressivi.
SEARCH_RESOURCES_TRIES = [
    # L1 (ricco) – spesso passa, ma se rifiuta scendiamo
    [
        "images.primary.large",
        "itemInfo.title",
        "offersV2.listings.price",
        "offersV2.listings.savingBasis",
        "offersV2.listings.savings",
    ],
    # L2
    [
        "images.primary.large",
        "itemInfo.title",
        "offersV2.listings.price",
        "offersV2.listings.savings",
    ],
    # L3
    [
        "images.primary.large",
        "itemInfo.title",
        "offersV2.listings.price",
    ],
    # L4
    [
        "images.primary.large",
        "itemInfo.title",
    ],
]

# GetItems: qui l’API spesso “rompe” se chiedi savingBasis/savings.
# Quindi partiamo già con set più conservativi e usiamo GetItems SOLO se SearchItems non aveva prezzo.
GETITEMS_RESOURCES_TRIES = [
    [
        "images.primary.large",
        "itemInfo.title",
        "offersV2.listings.price",
    ],
    [
        "images.primary.large",
        "itemInfo.title",
    ],
]


# =========================
# UTILS
# =========================
def parse_money_amount(x):
    if x is None:
        return None
    try:
        return float(x)
    except:
        try:
            s = str(x).replace("\u20ac", "").replace("€", "").strip()
            s = s.replace(".", "").replace(",", ".")
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

    resp = requests.get(url_img, timeout=15)
    prodotto = Image.open(BytesIO(resp.content)).resize((600, 600))
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

    out = BytesIO()
    img.save(out, format="PNG")
    out.seek(0)
    return out


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


def is_in_italy_window(now_utc=None):
    if now_utc is None:
        now_utc = datetime.utcnow()
    month = now_utc.month
    offset_hours = 2 if 4 <= month <= 10 else 1
    italy_time = now_utc + timedelta(hours=offset_hours)
    in_window = 9 <= italy_time.hour < 21
    return in_window, italy_time


# =========================
# OAuth token caching
# =========================
_token_cache = {"access_token": None, "expires_at": 0}


def _basic_auth_header(client_id, client_secret):
    raw = f"{client_id}:{client_secret}".encode("utf-8")
    return "Basic " + base64.b64encode(raw).decode("utf-8")


def get_access_token():
    now = int(time.time())
    if _token_cache["access_token"] and now < (_token_cache["expires_at"] - 60):
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
        print(f"[DEBUG] Token OK (expires_in={expires_in}s) url={CREATORS_TOKEN_URL}")

    return token


def creators_post(path, json_body):
    token = get_access_token()
    url = f"{CREATORS_API_BASE}{path}"

    headers = {
        "Content-Type": "application/json",
        "x-marketplace": CREATORS_MARKETPLACE,
        "Authorization": f"Bearer {token}, Version {CREATORS_CREDENTIAL_VERSION}",
    }

    r = requests.post(url, headers=headers, json=json_body, timeout=25)
    if r.status_code != 200:
        raise RuntimeError(f"Creators API error {r.status_code}: {r.text}")
    return r.json()


# =========================
# Extractors
# =========================
def _safe_get(d, *keys, default=None):
    cur = d
    for k in keys:
        if cur is None:
            return default
        if isinstance(cur, dict):
            cur = cur.get(k)
        elif isinstance(cur, list):
            try:
                cur = cur[k]
            except:
                return default
        else:
            return default
    return cur if cur is not None else default


def _extract_from_item(item):
    asin = (item.get("asin") or "").strip().upper()
    if not asin:
        return None

    title = _safe_get(item, "itemInfo", "title", "displayValue", default="") or ""
    title = " ".join(str(title).split())

    url_img = _safe_get(item, "images", "primary", "large", "url", default=None)

    url = item.get("detailPageURL") or f"https://www.amazon.it/dp/{asin}?tag={AMAZON_ASSOCIATE_TAG}"

    listings = _safe_get(item, "offersV2", "listings", default=[]) or []
    l0 = listings[0] if listings else None
    has_offers = bool(l0)

    price_val = None
    old_val = None
    disc = 0

    if l0:
        price_amount = _safe_get(l0, "price", "money", "amount")
        price_val = parse_money_amount(price_amount)

        old_amount = _safe_get(l0, "savingBasis", "money", "amount")
        old_val = parse_money_amount(old_amount)

        savings = _safe_get(l0, "savings", default={}) or {}
        disc = int(savings.get("percentage") or 0)

        saving_amount = parse_money_amount(_safe_get(savings, "money", "amount"))

        # se manca old ma abbiamo saving_amount
        if old_val is None and price_val is not None and saving_amount:
            old_val = price_val + saving_amount

        # se percent manca ma abbiamo old/new
        if price_val is not None and old_val and old_val > 0 and disc == 0:
            try:
                disc = int(round((old_val - price_val) / old_val * 100))
            except:
                disc = 0

        # normalizza
        if price_val is not None and (old_val is None or old_val <= 0):
            old_val = price_val

    return {
        "asin": asin,
        "title": title,
        "url_img": url_img,
        "url": url,
        "price": price_val,
        "old": old_val,
        "disc": disc,
        "has_offers": has_offers,
        "listing": l0,
    }


# =========================
# Creators API ops
# =========================
def creators_search_items(kw, page, resources):
    body = {
        "keywords": kw,
        "partnerTag": AMAZON_ASSOCIATE_TAG,
        "marketplace": CREATORS_MARKETPLACE,
        "searchIndex": SEARCH_INDEX,
        "itemCount": ITEMS_PER_PAGE,
        "itemPage": page,
        "resources": resources,
    }
    return creators_post("/searchItems", body)


def creators_get_items(asins, resources):
    body = {
        "itemIds": asins,
        "partnerTag": AMAZON_ASSOCIATE_TAG,
        "marketplace": CREATORS_MARKETPLACE,
        "resources": resources,
    }
    return creators_post("/getItems", body)


def creators_search_items_with_fallback(kw, page):
    last_err = None
    for i, res_list in enumerate(SEARCH_RESOURCES_TRIES, start=1):
        try:
            if DEBUG_AMAZON:
                print(f"[DEBUG] SearchItems try L{i} resources={res_list}")
            data = creators_search_items(kw, page, res_list)
            return data, res_list
        except Exception as e:
            last_err = e
            if DEBUG_AMAZON:
                print(f"[DEBUG] SearchItems L{i} rejected by API (resources) -> fallback")
            continue
    raise last_err


def creators_get_items_with_fallback(asins):
    last_err = None
    for i, res_list in enumerate(GETITEMS_RESOURCES_TRIES, start=1):
        try:
            if DEBUG_AMAZON:
                print(f"[DEBUG] GetItems try L{i} resources={res_list}")
            data = creators_get_items(asins, res_list)
            return data, res_list
        except Exception as e:
            last_err = e
            if DEBUG_AMAZON:
                print(f"[DEBUG] GetItems L{i} rejected by API (resources) -> fallback")
            continue
    raise last_err


# =========================
# Core selection logic
# =========================
def _build_payload_from_extracted(x):
    asin = x["asin"]
    price = x["price"]
    old = x["old"] if x["old"] is not None else price
    disc = int(x["disc"] or 0)

    # Se per qualche motivo old==price ma in listing esiste savingBasis/savings,
    # proviamo a ricostruire (difesa extra)
    if x.get("listing") and (disc == 0 or old == price):
        l0 = x["listing"]
        sb = parse_money_amount(_safe_get(l0, "savingBasis", "money", "amount"))
        if sb and price and sb > price:
            old = sb
            try:
                disc = int(round((old - price) / old * 100))
            except:
                pass

    url_img = x["url_img"] or "https://m.media-amazon.com/images/I/71bhWgQK-cL._AC_SL1500_.jpg"
    title = (x["title"] or "").strip() or "Offerta Amazon"
    minimo = disc >= 30

    return {
        "asin": asin,
        "title": title[:80].strip() + ("…" if len(title) > 80 else ""),
        "price_new": float(price),
        "price_old": float(old),
        "discount": int(disc),
        "url_img": url_img,
        "url": x["url"],
        "minimo": minimo,
    }


def _first_valid_item_for_keyword(kw, pubblicati):
    reasons = Counter()
    fallback_asins = []

    for page in range(1, PAGES + 1):
        try:
            data, used_res = creators_search_items_with_fallback(kw, page)
            search_result = data.get("searchResult") or {}
            items = search_result.get("items") or []

            if DEBUG_AMAZON:
                print(f"[DEBUG] kw={kw} page={page} items={len(items)} used_resources={used_res}")
                if items:
                    print(f"[DEBUG] searchItems raw preview: {json.dumps(items[0], ensure_ascii=False)[:900]}")
        except Exception as e:
            reasons["api_error"] += 1
            print(f"❌ Creators searchItems error (kw='{kw}', page={page}): {repr(e)}")
            continue

        for it in items:
            x = _extract_from_item(it)
            if not x:
                reasons["no_asin"] += 1
                continue

            asin = x["asin"]
            if asin in pubblicati or not can_post(asin, hours=24):
                reasons["already_posted"] += 1
                continue

            # Se manca price: lo mettiamo in fallback per GetItems
            if x["price"] is None:
                reasons["no_price"] += 1
                if len(fallback_asins) < 6:
                    fallback_asins.append(asin)
                if DEBUG_AMAZON:
                    print(f"[DEBUG] asin={asin} no_price | has_offers={x['has_offers']}")
                continue

            # QUI È IL PUNTO CRUCIALE:
            # Se SearchItems ci ha già dato savingBasis/savings, NON facciamo GetItems.
            price = x["price"]
            old = x["old"] if x["old"] is not None else price
            disc = int(x["disc"] or 0)

            if DEBUG_AMAZON and x.get("listing"):
                try:
                    print("[DEBUG] listing snapshot:", json.dumps(x["listing"], ensure_ascii=False)[:700])
                except:
                    pass

            if price < MIN_PRICE or price > MAX_PRICE:
                reasons["price_out_range"] += 1
                continue

            # calcolo “robusto” disc/old
            payload = _build_payload_from_extracted(x)
            disc = payload["discount"]

            if disc < MIN_DISCOUNT:
                reasons["disc_too_low"] += 1
                continue

            if DEBUG_AMAZON:
                print(f"[DEBUG] FOUND via SearchItems asin={payload['asin']} price={payload['price_new']} old={payload['price_old']} disc={payload['discount']}")

            return payload

    # Fallback GetItems SOLO per gli asin che non avevano price su SearchItems
    if fallback_asins:
        try:
            data, used_res = creators_get_items_with_fallback(fallback_asins)
            items = data.get("items") or []
            if DEBUG_AMAZON:
                print(f"[DEBUG] GetItems fallback asins={fallback_asins} items={len(items)} used_resources={used_res}")
        except Exception as e:
            reasons["getitems_api_error"] += 1
            if DEBUG_AMAZON:
                print(f"[DEBUG] GetItems fallback error: {repr(e)}")
            items = []

        for it in items:
            x = _extract_from_item(it)
            if not x:
                continue

            asin = x["asin"]
            if asin in pubblicati or not can_post(asin, hours=24):
                continue

            if x["price"] is None:
                continue

            if x["price"] < MIN_PRICE or x["price"] > MAX_PRICE:
                continue

            payload = _build_payload_from_extracted(x)

            # GetItems con resources conservative spesso non avrà old/savings:
            # in quel caso disc diventa 0 e quindi non pubblichiamo (corretto).
            if payload["discount"] < MIN_DISCOUNT:
                continue

            if DEBUG_AMAZON:
                print(f"[DEBUG] FOUND via GetItems asin={payload['asin']} price={payload['price_new']} old={payload['price_old']} disc={payload['discount']}")

            return payload

    if DEBUG_AMAZON:
        print(f"[DEBUG] kw={kw} reasons={dict(reasons)} fallback_asins={fallback_asins}")

    return None


# =========================
# Telegram publishing
# =========================
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

    caption_parts = [f"<b>{safe_title}</b>"]
    if minimo and sconto >= 30:
        caption_parts.append("<b>MINIMO STORICO</b>")

    caption_parts.append(
        f"💶 <b>{prezzo_nuovo_val:.2f}€</b> invece di <s>{prezzo_vecchio_val:.2f}€</s> "
        f"(<b>-{sconto}%</b>)"
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


def run_if_in_fascia_oraria():
    now_utc = datetime.utcnow()
    in_window, italy_time = is_in_italy_window(now_utc)
    if in_window:
        return invia_offerta()
    print(f"⏸ Fuori fascia oraria (Italia {italy_time.strftime('%H:%M')}), nessuna offerta pubblicata.")
    return False


def start_scheduler():
    schedule.clear()
    schedule.every().monday.at("06:59").do(resetta_pubblicati)
    schedule.every(RUN_EVERY_MINUTES).minutes.do(run_if_in_fascia_oraria)
    while True:
        schedule.run_pending()
        time.sleep(5)
