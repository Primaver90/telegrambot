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
MIN_SAVING_EUR = float(os.environ.get("MIN_SAVING_EUR", "50"))

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

# Resources (a livelli)
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

RES_GETITEMS = [
    "images.primary.large",
    "itemInfo.title",
    "offersV2.listings.price",
    "offersV2.summaries.savings",
    "offersV2.summaries.lowestPrice",
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


def _json_or_none(resp):
    try:
        return resp.json()
    except:
        return None


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
# PARSING HELPERS
# =========================
def _money_amount(obj):
    if not obj:
        return None
    money = obj.get("money") if isinstance(obj, dict) else None
    if isinstance(money, dict) and money.get("amount") is not None:
        try:
            return float(money["amount"])
        except:
            return None
    disp = obj.get("displayAmount") if isinstance(obj, dict) else None
    if disp:
        s = str(disp).replace("€", "").replace("\xa0", " ").strip()
        s = s.replace(".", "").replace(",", ".")
        try:
            return float(s)
        except:
            return None
    return None


def extract_from_item(item):
    asin = (item.get("asin") or "").strip().upper()
    title = (item.get("itemInfo") or {}).get("title", {}).get("displayValue", "") or ""
    title = " ".join(str(title).split())

    url_img = (((item.get("images") or {}).get("primary") or {}).get("large") or {}).get("url")
    url = item.get("detailPageURL") or (f"https://www.amazon.it/dp/{asin}?tag={AMAZON_ASSOCIATE_TAG}" if asin else "https://www.amazon.it")

    offers = item.get("offersV2") or {}
    listings = offers.get("listings") or []
    l0 = listings[0] if listings else None

    price_val = None
    old_val = None
    disc = 0

    if l0:
        price_val = _money_amount(l0.get("price") or {})
        saving_basis = _money_amount(l0.get("savingBasis") or {})
        savings = l0.get("savings") or {}
        saving_amount = _money_amount(savings)  # savings.money.amount
        perc = savings.get("percentage") if isinstance(savings, dict) else None
        if perc is not None:
            try:
                disc = int(perc)
            except:
                disc = 0

        # old price: savingBasis se presente
        if saving_basis is not None:
            old_val = saving_basis

        # se manca savingBasis ma ho saving_amount, ricostruisco old
        if old_val is None and price_val is not None and saving_amount is not None:
            old_val = price_val + saving_amount

        # se manca percent ma ho old/new
        if disc == 0 and price_val is not None and old_val is not None and old_val > price_val:
            try:
                disc = int(round((old_val - price_val) / old_val * 100))
            except:
                disc = 0

    return {
        "asin": asin,
        "title": title,
        "url_img": url_img,
        "url": url,
        "price": price_val,
        "old": old_val,
        "discount": int(disc or 0),
        "has_offers": bool(l0),
    }


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
    resp = _creators_post("/catalog/v1/searchItems", body)
    if resp.status_code != 200:
        raise RuntimeError(resp.text)
    data = _json_or_none(resp) or {}
    sr = data.get("searchResult") or data
    return sr, resources


def creators_get_items(asins):
    # tentiamo 2 volte: con savingBasis e senza (se rifiutato)
    for attempt, res in enumerate([RES_GETITEMS, [r for r in RES_GETITEMS if r != "offersV2.listings.savingBasis"]], start=1):
        body = {
            "itemIds": asins,
            "partnerTag": AMAZON_ASSOCIATE_TAG,
            "marketplace": CREATORS_MARKETPLACE,
            "resources": res,
        }
        resp = _creators_post("/catalog/v1/getItems", body)
        if resp.status_code == 200:
            data = _json_or_none(resp) or {}
            ir = data.get("itemsResult") or data
            return ir, res
        if DEBUG_AMAZON:
            print(f"[DEBUG] GetItems attempt {attempt} error {resp.status_code}: {resp.text[:240]}")
    raise RuntimeError("GetItems failed")


# ============================================================
# Core: trova BEST offerta valida (Premium)
# ============================================================
def _first_valid_item_for_keyword(kw, pubblicati):
    """
    Premium: scegliamo la migliore (saving€ più alto) tra i candidati validi.
    Requisiti:
      - old_price reale e > new_price
      - saving_eur >= MIN_SAVING_EUR
      - discount >= MIN_DISCOUNT
      - price in range
    """
    reasons = Counter()
    asin_candidates = []

    best = None
    best_saving = 0.0

    def consider_candidate(parsed, source):
        nonlocal best, best_saving

        asin = parsed["asin"]
        price_val = parsed["price"]
        disc = int(parsed["discount"] or 0)
        old_val = parsed["old"]

        if price_val is None:
            reasons["no_price"] += 1
            return

        if old_val is None or old_val <= price_val:
            reasons["old_missing_or_equal"] += 1
            return

        saving_eur = float(old_val - price_val)

        if price_val < MIN_PRICE or price_val > MAX_PRICE:
            reasons["price_out_range"] += 1
            return

        if disc < MIN_DISCOUNT:
            reasons["disc_too_low"] += 1
            return

        if saving_eur < MIN_SAVING_EUR:
            reasons["saving_too_low"] += 1
            return

        url_img = parsed["url_img"] or "https://m.media-amazon.com/images/I/71bhWgQK-cL._AC_SL1500_.jpg"
        title = (parsed["title"] or "")[:80].strip()
        if len(parsed["title"] or "") > 80:
            title += "…"

        cand = {
            "asin": asin,
            "title": title,
            "price_new": float(price_val),
            "price_old": float(old_val),
            "discount": int(disc),
            "saving_eur": float(saving_eur),
            "url_img": url_img,
            "url": parsed["url"],
            "minimo": int(disc) >= 30,
            "source": source,
        }

        if DEBUG_AMAZON:
            print(f"[DEBUG] CANDIDATE({source}) asin={asin} saving={saving_eur:.2f}€ disc={disc}% new={price_val} old={old_val}")

        if saving_eur > best_saving:
            best = cand
            best_saving = saving_eur

    # ---- SearchItems pages ----
    for page in range(1, PAGES + 1):
        # Try resources levels
        items = []
        used_resources = None
        last_err = None
        for lvl, res in enumerate(RESOURCE_LEVELS_SEARCH, start=1):
            try:
                if DEBUG_AMAZON:
                    print(f"[DEBUG] SearchItems try L{lvl} resources={res}")
                sr, used_resources = creators_search_items(kw, page, res)
                items = sr.get("items") or []
                break
            except Exception as e:
                last_err = e
                continue

        if used_resources is None:
            reasons["api_error"] += 1
            if DEBUG_AMAZON:
                print(f"[DEBUG] SearchItems failed all levels kw={kw} page={page}: {repr(last_err)}")
            continue

        if DEBUG_AMAZON:
            print(f"[DEBUG] kw={kw} page={page} items={len(items)} used_resources={used_resources}")

        for item in items:
            parsed = extract_from_item(item)
            asin = parsed["asin"]
            if not asin:
                reasons["no_asin"] += 1
                continue
            if asin in pubblicati or not can_post(asin, hours=24):
                reasons["already_posted"] += 1
                continue

            # se manca old o discount in searchitems, mettiamo asin per getItems
            if parsed["price"] is None or parsed["old"] is None or int(parsed["discount"] or 0) == 0:
                reasons["incomplete_in_searchitems"] += 1
                if len(asin_candidates) < GETITEMS_FALLBACK_MAX:
                    asin_candidates.append(asin)
                continue

            consider_candidate(parsed, "SearchItems")

    # ---- GetItems fallback ----
    if asin_candidates:
        try:
            ir, used_res = creators_get_items(asin_candidates)
            items = ir.get("items") or []
            if DEBUG_AMAZON:
                print(f"[DEBUG] GetItems fallback asins={asin_candidates} items={len(items)} used_resources={used_res}")

            for item in items:
                parsed = extract_from_item(item)
                asin = parsed["asin"]
                if not asin or asin in pubblicati or not can_post(asin, hours=24):
                    continue

                if parsed["price"] is None or parsed["old"] is None:
                    reasons["no_price_or_old_in_getitems"] += 1
                    continue

                consider_candidate(parsed, "GetItems")

        except Exception as e:
            reasons["getitems_error"] += 1
            if DEBUG_AMAZON:
                print(f"[DEBUG] GetItems fallback error: {e}")

    if DEBUG_AMAZON:
        print(f"[DEBUG] kw={kw} reasons={dict(reasons)} asin_candidates={asin_candidates} best_saving={best_saving:.2f}€")

    return best


# ============================================================
# SEND OFFER
# ============================================================
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

    # Risparmio in euro (premium)
    saving_eur = float(prezzo_vecchio_val - prezzo_nuovo_val)
    caption_parts.append(f"💰 Risparmi <b>{saving_eur:.0f}€</b>")

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
    print(f"✅ Pubblicata: {asin} | {kw} | Risparmi {saving_eur:.0f}€")
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
