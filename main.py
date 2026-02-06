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

# Creators API (python-amazon-paapi >= 6.0.0)
from amazon_creatorsapi import AmazonCreatorsApi, Country
from amazon_creatorsapi.models import GetItemsResource, SearchItemsResource


# =========================
# ENV
# =========================
CREATORS_CREDENTIAL_ID = os.environ.get("CREATORS_CREDENTIAL_ID")
CREATORS_CREDENTIAL_SECRET = os.environ.get("CREATORS_CREDENTIAL_SECRET")
CREATORS_CREDENTIAL_VERSION = os.environ.get("CREATORS_CREDENTIAL_VERSION", "2.2")

AMAZON_ASSOCIATE_TAG = os.environ.get("AMAZON_ASSOCIATE_TAG")
AMAZON_COUNTRY = os.environ.get("AMAZON_COUNTRY", "IT")

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

FONT_PATH = os.environ.get("FONT_PATH", "Montserrat-VariableFont_wght.ttf")
LOGO_PATH = os.environ.get("LOGO_PATH", "header_clean2.png")
BADGE_PATH = os.environ.get("BADGE_PATH", "minimo storico flat.png")

MIN_DISCOUNT = int(os.environ.get("MIN_DISCOUNT", "15"))
MIN_PRICE = float(os.environ.get("MIN_PRICE", "15"))
MAX_PRICE = float(os.environ.get("MAX_PRICE", "1900"))

DEBUG_AMAZON = os.environ.get("DEBUG_AMAZON", "0") == "1"
GETITEMS_FALLBACK_MAX = int(os.environ.get("GETITEMS_FALLBACK_MAX", "4"))

# Throttling (secondi di attesa tra chiamate API) – aiuta con rate-limit
AMAZON_THROTTLING = float(os.environ.get("AMAZON_THROTTLING", "1"))

# Persistenza: su Render meglio /data se c’è, altrimenti /tmp
DATA_DIR = "/data" if Path("/data").exists() else "/tmp/botdata"
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


# =========================
# VALIDAZIONI BASE
# =========================
def _require_env():
    missing = []
    for k, v in [
        ("CREATORS_CREDENTIAL_ID", CREATORS_CREDENTIAL_ID),
        ("CREATORS_CREDENTIAL_SECRET", CREATORS_CREDENTIAL_SECRET),
        ("CREATORS_CREDENTIAL_VERSION", CREATORS_CREDENTIAL_VERSION),
        ("AMAZON_ASSOCIATE_TAG", AMAZON_ASSOCIATE_TAG),
        ("TELEGRAM_BOT_TOKEN", TELEGRAM_BOT_TOKEN),
        ("TELEGRAM_CHAT_ID", TELEGRAM_CHAT_ID),
    ]:
        if not v:
            missing.append(k)
    if missing:
        raise RuntimeError(f"Missing env vars: {', '.join(missing)}")


def _country_from_code(code: str) -> Country:
    code = (code or "IT").strip().upper()
    if code == "IT":
        return Country.IT
    if code == "US":
        return Country.US
    if code == "UK" or code == "GB":
        return Country.UK
    if code == "DE":
        return Country.DE
    if code == "FR":
        return Country.FR
    if code == "ES":
        return Country.ES
    # fallback
    return Country.IT


_require_env()

bot = Bot(token=TELEGRAM_BOT_TOKEN)

amazon = AmazonCreatorsApi(
    credential_id=CREATORS_CREDENTIAL_ID,
    credential_secret=CREATORS_CREDENTIAL_SECRET,
    version=CREATORS_CREDENTIAL_VERSION,
    tag=AMAZON_ASSOCIATE_TAG,
    country=_country_from_code(AMAZON_COUNTRY),
    throttling=AMAZON_THROTTLING,
)


# =========================
# RESOURCES (IMPORTANTE)
# =========================
SEARCH_RESOURCES = [
    SearchItemsResource.ITEMINFO_TITLE,
    SearchItemsResource.IMAGES_PRIMARY_LARGE,

    # OffersV2 (prezzi)
    SearchItemsResource.OFFERSV2_LISTINGS_PRICE,
    SearchItemsResource.OFFERSV2_LISTINGS_SAVINGBASIS,
    SearchItemsResource.OFFERSV2_SUMMARIES_SAVINGS,
]

GETITEMS_RESOURCES = [
    GetItemsResource.ITEMINFO_TITLE,
    GetItemsResource.IMAGES_PRIMARY_LARGE,

    # OffersV2 (prezzi)
    GetItemsResource.OFFERSV2_LISTINGS_PRICE,
    GetItemsResource.OFFERSV2_LISTINGS_SAVINGBASIS,
    GetItemsResource.OFFERSV2_SUMMARIES_SAVINGS,
]


# =========================
# UTILS
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
# AMAZON (ESTRAZIONE PREZZO)
# =========================
def extract_price_discount(item):
    """
    Creators API: preferisci OffersV2.
    ritorna (price_new, discount_percent, price_old) oppure (None, None, None)
    """
    try:
        if not item.offers_v2 or not item.offers_v2.listings:
            return None, None, None

        listing = item.offers_v2.listings[0]

        # prezzo nuovo
        price_new = None
        if listing.price and listing.price.money and listing.price.money.amount is not None:
            price_new = float(listing.price.money.amount)

        if price_new is None:
            return None, None, None

        # prezzo vecchio (savingBasis)
        price_old = None
        if listing.saving_basis and listing.saving_basis.money and listing.saving_basis.money.amount is not None:
            price_old = float(listing.saving_basis.money.amount)

        # sconto % (se presente)
        disc = 0
        try:
            if item.offers_v2.summaries and item.offers_v2.summaries[0].savings:
                disc = int(item.offers_v2.summaries[0].savings.percentage or 0)
        except:
            disc = 0

        # se manca price_old ma ho disc, stimo
        if price_old is None and disc:
            try:
                price_old = price_new / (1 - disc / 100.0)
            except:
                price_old = price_new

        if price_old is None:
            price_old = price_new

        return price_new, disc, price_old
    except:
        return None, None, None


def _first_valid_item_for_keyword(kw, pubblicati):
    reasons = Counter()
    fallback_asins = []

    # 1) SearchItems
    for page in range(1, PAGES + 1):
        try:
            results = amazon.search_items(
                keywords=kw,
                item_count=ITEMS_PER_PAGE,
                item_page=page,
                resources=SEARCH_RESOURCES,
            )
            items = results.items or []
            if DEBUG_AMAZON:
                print(f"[DEBUG] kw={kw} page={page} items={len(items)}")
        except TypeError:
            # Se la signature differisce in qualche minor version
            results = amazon.search_items(keywords=kw)
            items = results.items or []
        except Exception as e:
            reasons["search_error"] += 1
            print(f"❌ Creators searchItems error (kw='{kw}', page={page}): {repr(e)}")
            items = []

        for item in items:
            asin = (getattr(item, "asin", None) or "").strip().upper()
            if not asin:
                reasons["no_asin"] += 1
                continue
            if asin in pubblicati or not can_post(asin, hours=24):
                reasons["already_posted"] += 1
                continue

            title = ""
            try:
                title = item.item_info.title.display_value or ""
            except:
                title = ""
            title = " ".join(str(title).split())

            price_new, disc, price_old = extract_price_discount(item)
            if price_new is None:
                reasons["no_price_in_search"] += 1
                if len(fallback_asins) < GETITEMS_FALLBACK_MAX:
                    fallback_asins.append(asin)
                continue

            if price_new < MIN_PRICE or price_new > MAX_PRICE:
                reasons["price_out_range"] += 1
                continue
            if (disc or 0) < MIN_DISCOUNT:
                reasons["disc_too_low"] += 1
                continue

            url_img = None
            try:
                url_img = item.images.primary.large.url
            except:
                url_img = None
            if not url_img:
                url_img = "https://m.media-amazon.com/images/I/71bhWgQK-cL._AC_SL1500_.jpg"

            url = f"https://www.amazon.it/dp/{asin}?tag={AMAZON_ASSOCIATE_TAG}"
            minimo = (disc or 0) >= 30

            if DEBUG_AMAZON:
                print(f"[DEBUG] FOUND via SearchItems asin={asin} price={price_new} disc={disc}")

            return {
                "asin": asin,
                "title": title[:80].strip() + ("…" if len(title) > 80 else ""),
                "price_new": price_new,
                "price_old": price_old,
                "discount": int(disc or 0),
                "url_img": url_img,
                "url": url,
                "minimo": minimo,
            }

    # 2) Fallback GetItems su pochi ASIN
    if fallback_asins:
        try:
            items = amazon.get_items(
                fallback_asins,
                resources=GETITEMS_RESOURCES,
            )
            items = items or []
            if DEBUG_AMAZON:
                print(f"[DEBUG] GetItems fallback asins={fallback_asins} items={len(items)}")

            for item in items:
                asin = (getattr(item, "asin", None) or "").strip().upper()
                if not asin or asin in pubblicati or not can_post(asin, hours=24):
                    continue

                title = ""
                try:
                    title = item.item_info.title.display_value or ""
                except:
                    title = ""
                title = " ".join(str(title).split())

                price_new, disc, price_old = extract_price_discount(item)
                if price_new is None:
                    continue
                if price_new < MIN_PRICE or price_new > MAX_PRICE:
                    continue
                if (disc or 0) < MIN_DISCOUNT:
                    continue

                url_img = None
                try:
                    url_img = item.images.primary.large.url
                except:
                    url_img = None
                if not url_img:
                    url_img = "https://m.media-amazon.com/images/I/71bhWgQK-cL._AC_SL1500_.jpg"

                url = f"https://www.amazon.it/dp/{asin}?tag={AMAZON_ASSOCIATE_TAG}"
                minimo = (disc or 0) >= 30

                if DEBUG_AMAZON:
                    print(f"[DEBUG] FOUND via GetItems asin={asin} price={price_new} disc={disc}")

                return {
                    "asin": asin,
                    "title": title[:80].strip() + ("…" if len(title) > 80 else ""),
                    "price_new": price_new,
                    "price_old": price_old,
                    "discount": int(disc or 0),
                    "url_img": url_img,
                    "url": url,
                    "minimo": minimo,
                }
        except Exception as e:
            print(f"❌ Creators getItems fallback error: {repr(e)}")

    if DEBUG_AMAZON:
        print(f"[DEBUG] kw={kw} reasons={dict(reasons)} fallback_asins={fallback_asins}")

    return None


# =========================
# TELEGRAM SEND
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
# FASCIA ORARIA ITALIA
# =========================
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
