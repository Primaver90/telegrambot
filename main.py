import os
import time
import json
import base64
import html
import threading
from io import BytesIO
from datetime import datetime, timedelta
from pathlib import Path
from collections import Counter

import requests
import schedule
from PIL import Image, ImageDraw, ImageFont
from telegram import Bot, InlineKeyboardMarkup, InlineKeyboardButton


# ============================================================
# ENV (NO DEFAULT SECRETS: mettili su Render -> Environment)
# ============================================================
AMAZON_ASSOCIATE_TAG = os.environ.get("AMAZON_ASSOCIATE_TAG", "").strip()

CREATORS_CREDENTIAL_ID = os.environ.get("CREATORS_CREDENTIAL_ID", "").strip()
CREATORS_CREDENTIAL_SECRET = os.environ.get("CREATORS_CREDENTIAL_SECRET", "").strip()
CREATORS_CREDENTIAL_VERSION = os.environ.get("CREATORS_CREDENTIAL_VERSION", "").strip()

# Marketplace Creators API: es. "www.amazon.it"
CREATORS_MARKETPLACE = os.environ.get("CREATORS_MARKETPLACE", "www.amazon.it").strip()

# Regione Cognito per token endpoint (da PDF/guida: auth.<region>.amazoncognito.com)
# Per te: eu-south-2
CREATORS_AUTH_REGION = os.environ.get("CREATORS_AUTH_REGION", "eu-south-2").strip()

# Se vuoi forzare un token URL completo, puoi settare questa env var:
# CREATORS_TOKEN_URL="https://creatorsapi.auth.eu-south-2.amazoncognito.com/oauth2/token"
CREATORS_TOKEN_URL = os.environ.get("CREATORS_TOKEN_URL", "").strip()

# Endpoint base Creators API (catalog)
CREATORS_API_BASE = os.environ.get("CREATORS_API_BASE", "https://creatorsapi.amazon/catalog/v1").strip().rstrip("/")

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

FONT_PATH = os.environ.get("FONT_PATH", "Montserrat-VariableFont_wght.ttf").strip()
LOGO_PATH = os.environ.get("LOGO_PATH", "header_clean2.png").strip()
BADGE_PATH = os.environ.get("BADGE_PATH", "minimo storico flat.png").strip()

# Filtri offerta
MIN_DISCOUNT = int(os.environ.get("MIN_DISCOUNT", "15"))
MIN_PRICE = float(os.environ.get("MIN_PRICE", "15"))
MAX_PRICE = float(os.environ.get("MAX_PRICE", "3000"))
MIN_SAVING_EUR = float(os.environ.get("MIN_SAVING_EUR", "40"))

SEARCH_INDEX = os.environ.get("SEARCH_INDEX", "All").strip()
ITEMS_PER_PAGE = int(os.environ.get("ITEMS_PER_PAGE", "8"))
PAGES = int(os.environ.get("PAGES", "4"))

# Debug
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

# ============================================================
# STORAGE (Render: meglio /tmp, o /data se hai persistent disk)
# ============================================================
DATA_DIR = os.environ.get("DATA_DIR", "/tmp/botdata")
Path(DATA_DIR).mkdir(parents=True, exist_ok=True)

PUB_FILE = os.path.join(DATA_DIR, "pubblicati.txt")
PUB_TS = os.path.join(DATA_DIR, "pubblicati_ts.csv")
KW_INDEX = os.path.join(DATA_DIR, "kw_index.txt")

# ============================================================
# Telegram
# ============================================================
bot = Bot(token=TELEGRAM_BOT_TOKEN)


# ============================================================
# Helpers generali
# ============================================================
def _require_env():
    missing = []
    for k in [
        "AMAZON_ASSOCIATE_TAG",
        "CREATORS_CREDENTIAL_ID",
        "CREATORS_CREDENTIAL_SECRET",
        "CREATORS_CREDENTIAL_VERSION",
        "CREATORS_MARKETPLACE",
        "TELEGRAM_BOT_TOKEN",
        "TELEGRAM_CHAT_ID",
    ]:
        if not os.environ.get(k, "").strip():
            missing.append(k)
    if missing:
        raise RuntimeError(f"Missing env vars: {', '.join(missing)}")


def parse_eur_amount(v):
    """
    Accetta:
    - money.amount (float/int)
    - displayAmount tipo "249,00 €" o "1.299,00"
    """
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v)
    s = s.replace("\u20ac", "").replace("€", "")
    s = s.replace("\xa0", " ").strip()
    s = s.replace(".", "").replace(",", ".").strip()
    try:
        return float(s)
    except:
        return None


def safe_get(d, *path, default=None):
    cur = d
    for p in path:
        if cur is None:
            return default
        if isinstance(p, int):
            if not isinstance(cur, list) or len(cur) <= p:
                return default
            cur = cur[p]
        else:
            if not isinstance(cur, dict) or p not in cur:
                return default
            cur = cur[p]
    return cur if cur is not None else default


def draw_bold_text(draw, position, text, font, fill="black", offset=1):
    x, y = position
    for dx in (-offset, 0, offset):
        for dy in (-offset, 0, offset):
            draw.text((x + dx, y + dy), text, font=font, fill=fill)


# ============================================================
# Immagine offerta
# ============================================================
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

    out = BytesIO()
    img.save(out, format="PNG")
    out.seek(0)
    return out


# ============================================================
# Pubblicati / Rotazione keyword
# ============================================================
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


# ============================================================
# Creators API: OAuth token + caching
# ============================================================
_token_lock = threading.Lock()
_access_token = None
_token_expiry_epoch = 0


def _build_token_url():
    if CREATORS_TOKEN_URL:
        url = CREATORS_TOKEN_URL.strip()
        if not url.startswith("http"):
            url = "https://" + url.lstrip("/")
        return url
    return f"https://creatorsapi.auth.{CREATORS_AUTH_REGION}.amazoncognito.com/oauth2/token"


def _get_access_token():
    global _access_token, _token_expiry_epoch

    with _token_lock:
        now = time.time()
        # refresh 60s prima della scadenza
        if _access_token and now < (_token_expiry_epoch - 60):
            return _access_token

        token_url = _build_token_url()
        basic = base64.b64encode(
            f"{CREATORS_CREDENTIAL_ID}:{CREATORS_CREDENTIAL_SECRET}".encode("utf-8")
        ).decode("utf-8")

        data = "grant_type=client_credentials&scope=creatorsapi/default"
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {basic}",
        }

        if DEBUG_AMAZON:
            print(f"[DEBUG] Token refresh -> {token_url}")

        r = requests.post(token_url, headers=headers, data=data, timeout=20)
        if r.status_code != 200:
            raise RuntimeError(f"Token error {r.status_code}: {r.text}")

        j = r.json()
        _access_token = j.get("access_token")
        expires_in = int(j.get("expires_in", 3600) or 3600)
        _token_expiry_epoch = time.time() + expires_in

        if DEBUG_AMAZON:
            print(f"[DEBUG] Token OK (expires_in={expires_in}s)")

        return _access_token


def _auth_header():
    # Da guida: Authorization: Bearer <token>, Version <version>
    token = _get_access_token()
    return f"Bearer {token}, Version {CREATORS_CREDENTIAL_VERSION}"


def _creators_post(path, payload):
    url = f"{CREATORS_API_BASE}/{path.lstrip('/')}"
    headers = {
        "Authorization": _auth_header(),
        "Content-Type": "application/json",
        "x-marketplace": CREATORS_MARKETPLACE,
    }
    r = requests.post(url, headers=headers, json=payload, timeout=25)
    if r.status_code == 200:
        return r.json()

    # Log super utile
    raise RuntimeError(f"Creators API error {r.status_code}: {r.text}")


def _is_resources_validation_error(err_text: str) -> bool:
    t = (err_text or "").lower()
    return ("validation error detected" in t) and ("'resources'" in t or "resources" in t)


# ============================================================
# Creators API: searchItems / getItems con fallback resources
# ============================================================
SEARCH_RES_LEVELS = [
    ["images.primary.large", "itemInfo.title", "offersV2.listings.price", "offersV2.listings.savings"],
    ["images.primary.large", "itemInfo.title", "offersV2.listings.price"],
    ["images.primary.large", "itemInfo.title"],
]

GET_RES_LEVELS = [
    [
        "images.primary.large",
        "itemInfo.title",
        "offersV2.listings.price",
        "offersV2.listings.savings",
        "offersV2.listings.dealDetails",
    ],
    ["images.primary.large", "itemInfo.title", "offersV2.listings.price", "offersV2.listings.savings"],
    ["images.primary.large", "itemInfo.title", "offersV2.listings.price"],
    ["images.primary.large", "itemInfo.title"],
]


def creators_search_items(kw, page):
    last_err = None
    used_resources = None

    for level, resources in enumerate(SEARCH_RES_LEVELS, start=1):
        payload = {
            "keywords": kw,
            "partnerTag": AMAZON_ASSOCIATE_TAG,
            "marketplace": CREATORS_MARKETPLACE,
            "searchIndex": SEARCH_INDEX,
            "itemCount": ITEMS_PER_PAGE,
            "itemPage": page,
            "resources": resources,
        }

        try:
            if DEBUG_AMAZON:
                print(f"[DEBUG] SearchItems try L{level} resources={resources}")
            j = _creators_post("searchItems", payload)
            used_resources = resources
            return j, used_resources
        except Exception as e:
            last_err = e
            msg = str(e)
            if DEBUG_AMAZON:
                print(f"[DEBUG] SearchItems L{level} rejected -> {msg[:180]}")

            # Se è un errore di validazione sui resources, fallback al prossimo livello
            if _is_resources_validation_error(msg):
                continue

            # Altri errori: non insistere troppo
            raise

    # Se abbiamo solo errori di resources
    raise last_err


def creators_get_items(asins):
    last_err = None
    used_resources = None

    for level, resources in enumerate(GET_RES_LEVELS, start=1):
        payload = {
            "itemIds": asins,
            "partnerTag": AMAZON_ASSOCIATE_TAG,
            "marketplace": CREATORS_MARKETPLACE,
            "resources": resources,
        }
        try:
            if DEBUG_AMAZON:
                print(f"[DEBUG] GetItems try L{level} resources={resources} asins={asins}")
            j = _creators_post("getItems", payload)
            used_resources = resources
            return j, used_resources
        except Exception as e:
            last_err = e
            msg = str(e)
            if DEBUG_AMAZON:
                print(f"[DEBUG] GetItems L{level} rejected -> {msg[:180]}")
            if _is_resources_validation_error(msg):
                continue
            raise

    raise last_err


# ============================================================
# Parsing item -> prezzo/sconto
# ============================================================
def extract_from_item(item: dict):
    """
    Ritorna dict con:
    asin, title, url_img, url, price, old, discount, has_offers
    """
    asin = (item.get("asin") or "").strip().upper()
    url = item.get("detailPageURL") or item.get("detailPageUrl") or None

    title = safe_get(item, "itemInfo", "title", "displayValue", default="") or ""
    title = " ".join(str(title).split()).strip()

    url_img = safe_get(item, "images", "primary", "large", "url", default=None)

    listings = safe_get(item, "offersV2", "listings", default=[]) or []
    l0 = listings[0] if listings else None

    has_offers = bool(l0)

    price_val = None
    old_val = None
    disc = 0

    def _money_amount(obj):
        if obj is None:
            return None
        if isinstance(obj, (int, float)):
            return float(obj)
        if isinstance(obj, dict):
            if "money" in obj and isinstance(obj.get("money"), dict):
                obj = obj.get("money")
            if "amount" in obj:
                try:
                    return float(obj.get("amount"))
                except:
                    return None
            if "displayAmount" in obj:
                return parse_eur_amount(obj.get("displayAmount"))
        return None

    if l0:
        # prezzo nuovo
        price_val = _money_amount(l0.get("price"))

        # savings amount / % (da listings o price.savings)
        savings_obj = l0.get("savings") or safe_get(l0, "price", "savings", default=None)
        savings_amt = _money_amount(savings_obj)
        disc = (
            safe_get(l0, "savings", "percentage", default=0)
            or safe_get(l0, "price", "savings", "percentage", default=0)
            or safe_get(l0, "savings", "percentOff", default=0)
            or safe_get(l0, "price", "savings", "percentOff", default=0)
            or 0
        )

        # deal details (se presenti)
        deal = l0.get("dealDetails")
        if isinstance(deal, list) and deal:
            deal = deal[0]
        deal_list = _money_amount(deal.get("listPrice") or deal.get("wasPrice")) if isinstance(deal, dict) else None
        deal_amt = _money_amount(deal.get("amountSaved") or deal.get("amountOff") or deal.get("savings")) if isinstance(deal, dict) else None

        # savingBasis (se arriva)
        saving_basis = _money_amount(l0.get("savingBasis"))

        if saving_basis:
            old_val = saving_basis
        elif deal_list:
            old_val = deal_list
        elif savings_amt and price_val:
            old_val = price_val + savings_amt
        elif deal_amt and price_val:
            old_val = price_val + deal_amt

        # calcola sconto se manca
        if (not disc or disc == 0) and old_val and price_val and old_val > 0:
            disc = int(round((1 - (price_val / old_val)) * 100))

    # fallback: se old mancante, prova summaries.savings (quando disponibile)
    if (old_val is None or old_val == 0) and price_val is not None:
        summ_perc = safe_get(item, "offersV2", "summaries", 0, "savings", "percentage", default=None)
        summ_amt = safe_get(item, "offersV2", "summaries", 0, "savings", "money", "amount", default=None)
        if isinstance(summ_perc, (int, float)) and disc == 0:
            disc = int(summ_perc)
        if summ_amt is not None:
            sv = parse_eur_amount(summ_amt)
            if sv is not None:
                old_val = price_val + sv

    # ✅ filtro premium: risparmio minimo in euro
        if price_val is not None and old_val is not None and old_val > price_val:
            saving_eur = old_val - price_val
            if saving_eur < MIN_SAVING_EUR:
                return None  # oppure: continue (dipende dalla tua funzione)

    # URL fallback
    if not url and asin:
        url = f"https://www.amazon.it/dp/{asin}?tag={AMAZON_ASSOCIATE_TAG}"

    return {
        "asin": asin,
        "title": title,
        "url_img": url_img,
        "url": url,
        "price": price_val,
        "old": old_val,
        "discount": int(disc or 0),
        "has_offers": has_offers,
    }


# ============================================================
# Core: trova prima offerta valida
# ============================================================
def _first_valid_item_for_keyword(kw, pubblicati):
    reasons = Counter()
    asin_candidates = []

    for page in range(1, PAGES + 1):
        try:
            j, used_res = creators_search_items(kw, page)
            # response shape: { "searchResult": { "items": [...] } } oppure { "items": [...] }
            items = safe_get(j, "searchResult", "items", default=None)
            if items is None:
                items = j.get("items", []) or []

            if DEBUG_AMAZON:
                print(f"[DEBUG] kw={kw} page={page} items={len(items)} used_resources={used_res}")
                print(f"[DEBUG] searchItems raw keys: {list(j.keys())}")
                if items:
                    prev = items[0]
                    print(f"[DEBUG] searchItems raw preview: {json.dumps(prev)[:550]}")

        except Exception as e:
            reasons["api_error"] += 1
            print(f"❌ Creators searchItems error (kw='{kw}', page={page}): {e}")
            continue

        for item in items:
            parsed = extract_from_item(item)
            asin = parsed["asin"]
            if not asin:
                reasons["no_asin"] += 1
                continue
            if asin in pubblicati or not can_post(asin, hours=24):
                reasons["already_posted"] += 1
                continue

            # Se searchItems non include abbastanza info, salva asin per getItems
            if parsed["price"] is None or parsed["discount"] == 0:
                reasons["no_price_or_disc_in_searchitems"] += 1
                if len(asin_candidates) < GETITEMS_FALLBACK_MAX:
                    asin_candidates.append(asin)
                continue

            price_val = parsed["price"]
            disc = parsed["discount"]
            old_val = parsed["old"] if parsed["old"] else price_val

            if price_val < MIN_PRICE or price_val > MAX_PRICE:
                reasons["price_out_range"] += 1
                continue

            if disc < MIN_DISCOUNT:
                reasons["disc_too_low"] += 1
                continue

            url_img = parsed["url_img"] or "https://m.media-amazon.com/images/I/71bhWgQK-cL._AC_SL1500_.jpg"
            title = (parsed["title"] or "")[:80].strip()
            if len(parsed["title"] or "") > 80:
                title += "…"

            if DEBUG_AMAZON:
                print(f"[DEBUG] FOUND via SearchItems asin={asin} price={price_val} old={old_val} disc={disc}")

            return {
                "asin": asin,
                "title": title,
                "price_new": price_val,
                "price_old": old_val,
                "discount": disc,
                "url_img": url_img,
                "url": parsed["url"],
                "minimo": disc >= 30,
            }

    # Fallback getItems su pochi candidati (molto spesso qui arrivano old/savings meglio)
    if asin_candidates:
        try:
            j, used_res = creators_get_items(asin_candidates)
            items = safe_get(j, "items", default=[]) or []
            if DEBUG_AMAZON:
                print(f"[DEBUG] GetItems fallback asins={asin_candidates} items={len(items)} used_resources={used_res}")

            for item in items:
                parsed = extract_from_item(item)
                asin = parsed["asin"]
                if not asin or asin in pubblicati or not can_post(asin, hours=24):
                    continue

                price_val = parsed["price"]
                disc = parsed["discount"]
                old_val = parsed["old"] if parsed["old"] else price_val

                if price_val is None or disc == 0:
                    continue
                if price_val < MIN_PRICE or price_val > MAX_PRICE:
                    continue
                if disc < MIN_DISCOUNT:
                    continue

                url_img = parsed["url_img"] or "https://m.media-amazon.com/images/I/71bhWgQK-cL._AC_SL1500_.jpg"
                title = (parsed["title"] or "")[:80].strip()
                if len(parsed["title"] or "") > 80:
                    title += "…"

                if DEBUG_AMAZON:
                    print(f"[DEBUG] FOUND via GetItems asin={asin} price={price_val} old={old_val} disc={disc}")

                return {
                    "asin": asin,
                    "title": title,
                    "price_new": price_val,
                    "price_old": old_val,
                    "discount": disc,
                    "url_img": url_img,
                    "url": parsed["url"],
                    "minimo": disc >= 30,
                }

        except Exception as e:
            reasons["getitems_error"] += 1
            if DEBUG_AMAZON:
                print(f"[DEBUG] GetItems fallback error: {e}")

    if DEBUG_AMAZON:
        print(f"[DEBUG] kw={kw} reasons={dict(reasons)} asin_candidates={asin_candidates}")

    return None


# ============================================================
# Pubblica offerta
# ============================================================
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

    if prezzo_vecchio_val and prezzo_vecchio_val > prezzo_nuovo_val:
        caption_parts.append(
            f"💶 A soli <b>{prezzo_nuovo_val:.2f}€</b> invece di "
            f"<s>{prezzo_vecchio_val:.2f}€</s> (<b>-{sconto}%</b>)"
        )
    else:
        caption_parts.append(f"💶 A soli <b>{prezzo_nuovo_val:.2f}€</b>")

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


# ============================================================
# Fascia oraria Italia (semplice CET/CEST)
# ============================================================
def is_in_italy_window(now_utc=None):
    if now_utc is None:
        now_utc = datetime.utcnow()
    month = now_utc.month
    offset_hours = 2 if 4 <= month <= 10 else 1  # CEST approx / CET approx
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
    # Reset pubblicati ogni lunedì (ATTENZIONE: schedule usa timezone della macchina, spesso UTC su Render)
    schedule.every().monday.at("06:59").do(resetta_pubblicati)
    schedule.every(14).minutes.do(run_if_in_fascia_oraria)

    while True:
        schedule.run_pending()
        time.sleep(5)


if __name__ == "__main__":
    # utile per test manuale locale
    print("Running main.py directly -> test invia_offerta()")
    invia_offerta()
