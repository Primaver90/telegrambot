import os
from io import BytesIO
from datetime import datetime, timedelta
import time
import schedule
import requests
import html
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont
from telegram import Bot, InlineKeyboardMarkup, InlineKeyboardButton
from amazon_paapi import AmazonApi

AMAZON_ACCESS_KEY = os.environ.get("AMAZON_ACCESS_KEY", "AKPAZS2VGY1748024339")
AMAZON_SECRET_KEY = os.environ.get("AMAZON_SECRET_KEY", "yiA1TX0xWWVtW1HgKpkR2LWZpklQXaJ2k9D4HsiL")
AMAZON_ASSOCIATE_TAG = os.environ.get("AMAZON_ASSOCIATE_TAG", "itech00-21")
AMAZON_COUNTRY = os.environ.get("AMAZON_COUNTRY", "IT")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "7687135950:AAHfRV6b4RgAcVU6j71wDfZS-1RTMJ15ajg")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "-1001010781022")

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

# Debug Amazon (attiva con env var DEBUG_AMAZON=1)
DEBUG_AMAZON = os.environ.get("DEBUG_AMAZON", "0") == "1"
# Quanti ASIN al massimo testare via GetItems quando SearchItems non restituisce prezzo
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

SEARCH_INDEX = "All"
ITEMS_PER_PAGE = 8
PAGES = 4

bot = Bot(token=TELEGRAM_BOT_TOKEN)
amazon = AmazonApi(AMAZON_ACCESS_KEY, AMAZON_SECRET_KEY, AMAZON_ASSOCIATE_TAG, AMAZON_COUNTRY)

# Resources esplicite: fondamentale se Amazon/wrapper non include più prezzi di default
# (sia OffersV2 che Offers vecchio, così copriamo entrambe le strade)
PAAPI_RESOURCES = [
    "ItemInfo.Title",
    "Images.Primary.Large",

    # OffersV2 (nuovo)
    "OffersV2.Listings.Price",
    "OffersV2.Listings.SavingBasis",
    "OffersV2.Summaries.LowestPrice",
    "OffersV2.Summaries.Savings",

    # Offers (vecchio, fallback)
    "Offers.Listings.Price",
    "Offers.Listings.Savings",
    "Offers.Summaries.LowestPrice",
    "Offers.Summaries.Savings",
]


# =========================
# UTILS
# =========================
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


def get_attr_chain(obj, chain, default=None):
    """chain: lista di attributi, es. ['offers', 'listings', 0, 'price', 'display_amount']"""
    cur = obj
    try:
        for key in chain:
            if cur is None:
                return default
            if isinstance(key, int):
                cur = (cur or [])[key]
            else:
                cur = getattr(cur, key, None)
        return cur if cur is not None else default
    except:
        return default


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
# AMAZON EXTRACTION
# =========================
def extract_price_discount_old(item):
    """
    Prova:
    1) OffersV2 (nuovo)
    2) Offers (vecchio)
    Ritorna: (price_val, disc_percent, old_val) oppure (None, None, None)
    """

    # ---- 1) OffersV2 ----
    # Nome attributo può variare nel wrapper: offers_v2 / offersv2 / offersV2
    offersv2_obj = (
        getattr(item, "offers_v2", None)
        or getattr(item, "offersv2", None)
        or getattr(item, "offersV2", None)
        or getattr(item, "offers_v2", None)
    )

    try:
        listings = getattr(offersv2_obj, "listings", None) or []
        l0 = safe_first_list(listings)
        if l0:
            price_disp = get_attr_chain(l0, ["price", "display_amount"])
            price_val = parse_eur_amount(price_disp)

            # SavingBasis (prezzo vecchio) se presente
            old_disp = get_attr_chain(l0, ["saving_basis", "display_amount"])
            old_val = parse_eur_amount(old_disp)

            # Summary savings/percent: spesso è nelle summaries
            disc = 0
            try:
                # prova summaries
                summaries = getattr(offersv2_obj, "summaries", None) or []
                s0 = safe_first_list(summaries)
                # alcuni wrapper espongono savings.percentage o savings.amount
                disc = int(get_attr_chain(s0, ["savings", "percentage"], 0) or 0)
            except:
                disc = 0

            # se old_val non c'è ma ho disc, provo a stimare
            if price_val is not None and (old_val is None) and disc:
                try:
                    old_val = price_val / (1 - disc / 100.0)
                except:
                    old_val = None

            if price_val is not None:
                return price_val, disc, (old_val if old_val is not None else price_val)
    except:
        pass

    # ---- 2) Offers (vecchio) ----
    try:
        listing = safe_first_list(getattr(getattr(item, "offers", None), "listings", None) or [])
        price_obj = getattr(listing, "price", None)
        if not price_obj:
            return None, None, None

        price_val = parse_eur_amount(getattr(price_obj, "display_amount", None))
        if price_val is None:
            return None, None, None

        savings = getattr(price_obj, "savings", None)
        disc = int(getattr(savings, "percentage", 0) or 0) if savings else 0
        old_val = price_val + float(getattr(savings, "amount", 0) or 0) if savings else price_val

        return price_val, disc, old_val
    except:
        return None, None, None


def amazon_search_items(kw, page):
    """
    Wrapper robusto: prova con resources (prezzi), fallback senza se non supportato,
    e mini-backoff su throttling.
    """
    try:
        return amazon.search_items(
            keywords=kw,
            item_count=ITEMS_PER_PAGE,
            search_index=SEARCH_INDEX,
            item_page=page,
            resources=PAAPI_RESOURCES,
        )
    except TypeError:
        # Se il wrapper non accetta resources
        return amazon.search_items(
            keywords=kw,
            item_count=ITEMS_PER_PAGE,
            search_index=SEARCH_INDEX,
            item_page=page,
        )
    except Exception as e:
        msg = repr(e)
        if "TooManyRequests" in msg:
            time.sleep(2.0)
        raise


def amazon_get_items(asins):
    """
    GetItems su pochi ASIN per recuperare prezzi quando SearchItems non li include.
    """
    try:
        return amazon.get_items(items=asins, resources=PAAPI_RESOURCES)
    except TypeError:
        return amazon.get_items(items=asins)
    except Exception as e:
        msg = repr(e)
        if "TooManyRequests" in msg:
            time.sleep(2.0)
        raise


# =========================
# CORE LOGIC
# =========================
def _first_valid_item_for_keyword(kw, pubblicati):
    reasons = Counter()
    fallback_candidates = []

    for page in range(1, PAGES + 1):
        try:
            results = amazon_search_items(kw, page)
            items = getattr(results, "items", []) or []
            if DEBUG_AMAZON:
                print(f"[DEBUG] kw={kw} page={page} items={len(items)}")
        except Exception as e:
            reasons["paapi_error"] += 1
            print(f"❌ ERRORE Amazon PA-API (kw='{kw}', page={page}): {repr(e)}")
            items = []

        for item in items:
            asin = (getattr(item, "asin", None) or "").strip().upper()
            if not asin:
                reasons["no_asin"] += 1
                continue
            if asin in pubblicati or not can_post(asin, hours=24):
                reasons["already_posted"] += 1
                continue

            title = get_attr_chain(item, ["item_info", "title", "display_value"], "") or ""
            title = " ".join(str(title).split())

            # prova a estrarre prezzo/sconto
            price_val, disc, old_val = extract_price_discount_old(item)
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

            url_img = get_attr_chain(item, ["images", "primary", "large", "url"], None)
            if not url_img:
                url_img = "https://m.media-amazon.com/images/I/71bhWgQK-cL._AC_SL1500_.jpg"

            url = getattr(item, "detail_page_url", None)
            if not url and asin:
                url = f"https://www.amazon.it/dp/{asin}?tag={AMAZON_ASSOCIATE_TAG}"

            minimo = disc >= 30

            if DEBUG_AMAZON:
                print(f"[DEBUG] FOUND via SearchItems asin={asin} price={price_val} disc={disc}")

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

    # Fallback GetItems: se SearchItems non porta prezzi, proviamo su pochi ASIN candidati
    if fallback_candidates:
        try:
            res = amazon_get_items(fallback_candidates)
            items = getattr(res, "items", []) or []
            if DEBUG_AMAZON:
                print(f"[DEBUG] GetItems fallback candidates={fallback_candidates} items={len(items)}")
            for item in items:
                asin = (getattr(item, "asin", None) or "").strip().upper()
                if not asin or asin in pubblicati or not can_post(asin, hours=24):
                    continue

                title = get_attr_chain(item, ["item_info", "title", "display_value"], "") or ""
                title = " ".join(str(title).split())

                price_val, disc, old_val = extract_price_discount_old(item)
                if price_val is None:
                    continue

                if price_val < MIN_PRICE or price_val > MAX_PRICE:
                    continue
                if disc < MIN_DISCOUNT:
                    continue

                url_img = get_attr_chain(item, ["images", "primary", "large", "url"], None)
                if not url_img:
                    url_img = "https://m.media-amazon.com/images/I/71bhWgQK-cL._AC_SL1500_.jpg"

                url = getattr(item, "detail_page_url", None)
                if not url and asin:
                    url = f"https://www.amazon.it/dp/{asin}?tag={AMAZON_ASSOCIATE_TAG}"

                minimo = disc >= 30

                if DEBUG_AMAZON:
                    print(f"[DEBUG] FOUND via GetItems asin={asin} price={price_val} disc={disc}")

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
            if DEBUG_AMAZON:
                print(f"[DEBUG] GetItems fallback error: {repr(e)}")

    if DEBUG_AMAZON:
        print(f"[DEBUG] kw={kw} reasons={dict(reasons)} fallback_candidates={fallback_candidates}")

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

    caption_parts = [
        f"📌 <b>{safe_title}</b>",
    ]
    if minimo and sconto >= 30:
        caption_parts.append("❗️🚨 <b>MINIMO STORICO</b> 🚨❗️")

    caption_parts.append(
        f"💶 A soli <b>{prezzo_nuovo_val:.2f}€</b> invece di "
        f"<s>{prezzo_vecchio_val:.2f}€</s> (<b>-{sconto}%</b>)"
    )
    caption_parts.append(f'👉 <a href="{safe_url}">Acquista ora</a>')

    caption = "\n\n".join(caption_parts)

    button = InlineKeyboardMarkup(
        [[InlineKeyboardButton("🛒 Acquista ora", url=url)]]
    )

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
    if 4 <= month <= 10:
        offset_hours = 2  # CEST (circa aprile–ottobre)
    else:
        offset_hours = 1  # CET (circa novembre–marzo)

    italy_time = now_utc + timedelta(hours=offset_hours)
    in_window = 9 <= italy_time.hour < 21
    return in_window, italy_time


def run_if_in_fascia_oraria():
    now_utc = datetime.utcnow()
    in_window, italy_time = is_in_italy_window(now_utc)
    if in_window:
        invia_offerta()
    else:
        print(
            f"⏸ Fuori fascia oraria (Italia {italy_time.strftime('%H:%M')}), nessuna offerta pubblicata."
        )


def start_scheduler():
    schedule.clear()
    schedule.every().monday.at("06:59").do(resetta_pubblicati)
    schedule.every(14).minutes.do(run_if_in_fascia_oraria)
    while True:
        schedule.run_pending()
        time.sleep(5)
