import os
from io import BytesIO
from datetime import datetime, timedelta
import time
import schedule
import requests
import html
import threading
from collections import Counter
from pathlib import Path
from flask import Flask, jsonify
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
DEBUG_FILTERS = os.environ.get("DEBUG_FILTERS", "1") == "1"
STRICT_DISCOUNT = os.environ.get("STRICT_DISCOUNT", "0") == "1"

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

# Risorse per GetItems (fallback quando SearchItems non include offers/price)
GETITEMS_RESOURCES = [
    "ItemInfo.Title",
    "Images.Primary.Large",
    "Offers.Listings.Price",
]


# =========================
# INIT
# =========================
app = Flask(__name__)

if not AMAZON_ACCESS_KEY or not AMAZON_SECRET_KEY:
    raise RuntimeError("Mancano AMAZON_ACCESS_KEY / AMAZON_SECRET_KEY (env vars).")

amazon = AmazonApi(AMAZON_ACCESS_KEY, AMAZON_SECRET_KEY, AMAZON_ASSOCIATE_TAG, AMAZON_COUNTRY)

bot = None
if TELEGRAM_BOT_TOKEN:
    bot = Bot(token=TELEGRAM_BOT_TOKEN)


# =========================
# HELPERS
# =========================
def parse_eur_amount(display_amount: str):
    """Supporta anche: 1.299,00 € -> 1299.00"""
    if not display_amount:
        return None
    s = str(display_amount)
    s = s.replace("\u20ac", "").replace("€", "")
    s = s.replace("\xa0", " ").strip()
    s = s.replace(".", "").replace(",", ".").strip()
    try:
        return float(s)
    except Exception:
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


def _get_item_with_price_by_asin(asin: str):
    """
    Fallback: quando SearchItems non include offers/price.
    Proviamo diverse firme perché amazon_paapi cambia tra versioni.
    """
    if not asin:
        return None

    # 1) get_items(item_ids=[...], resources=[...])
    try:
        r = amazon.get_items(item_ids=[asin], resources=GETITEMS_RESOURCES)
        items = getattr(r, "items", None) or []
        return items[0] if items else None
    except Exception as e1:
        # 2) get_items(item_ids=[...]) senza resources
        try:
            r = amazon.get_items(item_ids=[asin])
            items = getattr(r, "items", None) or []
            return items[0] if items else None
        except Exception as e2:
            # 3) alcune versioni accettano asin singolo
            try:
                r = amazon.get_items(asin)
                items = getattr(r, "items", None) or []
                return items[0] if items else None
            except Exception as e3:
                print(f"❌ GetItems fallito asin={asin}: {repr(e1)} | {repr(e2)} | {repr(e3)}")
                return None


def _first_valid_item_for_keyword(kw, pubblicati):
    reasons = Counter()

    for page in range(1, PAGES + 1):
        # SearchItems "come prima" (senza resources) per evitare MalformedRequest()
        try:
            results = amazon.search_items(
                keywords=kw,
                item_count=ITEMS_PER_PAGE,
                search_index=SEARCH_INDEX,
                item_page=page,
            )
            items = getattr(results, "items", []) or []
        except Exception as e:
            print(f"❌ ERRORE Amazon PA-API SearchItems (kw='{kw}', page={page}): {repr(e)}")
            reasons["paapi_error"] += 1
            items = []

        if DEBUG_FILTERS:
            print(f"[DEBUG] kw={kw} page={page} items={len(items)}")

        for item in items:
            asin = (getattr(item, "asin", None) or "").strip().upper()
            if not asin:
                reasons["no_asin"] += 1
                continue

            if asin in pubblicati:
                reasons["dup_pub_file"] += 1
                continue

            if not can_post(asin, hours=24):
                reasons["dup_24h"] += 1
                continue

            # titolo
            title = getattr(
                getattr(getattr(item, "item_info", None), "title", None),
                "display_value",
                "",
            ) or ""
            title = " ".join(title.split())

            # prova prezzo da SearchItems
            listing = None
            try:
                listing = getattr(getattr(item, "offers", None), "listings", [None])[0]
            except Exception:
                listing = None

            price_obj = getattr(listing, "price", None) if listing else None

            # fallback GetItems se manca price
            item_for_images = item
            if not price_obj:
                reasons["no_price_obj"] += 1

                full_item = _get_item_with_price_by_asin(asin)
                if not full_item:
                    reasons["getitems_failed"] += 1
                    continue

                try:
                    listing = getattr(getattr(full_item, "offers", None), "listings", [None])[0]
                except Exception:
                    listing = None

                price_obj = getattr(listing, "price", None) if listing else None
                if not price_obj:
                    reasons["getitems_no_price"] += 1
                    continue

                # prendi titolo migliore se disponibile
                title2 = getattr(
                    getattr(getattr(full_item, "item_info", None), "title", None),
                    "display_value",
                    "",
                ) or ""
                title2 = " ".join(title2.split())
                if title2:
                    title = title2

                item_for_images = full_item

            # parsing prezzo
            price_val = parse_eur_amount(getattr(price_obj, "display_amount", ""))
            if price_val is None:
                reasons["bad_price_parse"] += 1
                continue

            if price_val < MIN_PRICE or price_val > MAX_PRICE:
                reasons["price_out_range"] += 1
                continue

            # sconti
            savings = getattr(price_obj, "savings", None)
            disc = int(getattr(savings, "percentage", 0) or 0)

            old_val = price_val
            try:
                old_val = price_val + float(getattr(savings, "amount", 0) or 0)
            except Exception:
                old_val = price_val

            if STRICT_DISCOUNT:
                if disc < MIN_DISCOUNT:
                    reasons["disc_too_low"] += 1
                    continue
            else:
                if savings and disc < MIN_DISCOUNT:
                    reasons["disc_too_low"] += 1
                    continue

            # immagine
            url_img = getattr(
                getattr(
                    getattr(getattr(item_for_images, "images", None), "primary", None),
                    "large",
                    None,
                ),
                "url",
                None,
            )
            if not url_img:
                reasons["no_img"] += 1
                url_img = "https://m.media-amazon.com/images/I/71bhWgQK-cL._AC_SL1500_.jpg"

            # url
            url = getattr(item, "detail_page_url", None)
            if not url:
                url = f"https://www.amazon.it/dp/{asin}?tag={AMAZON_ASSOCIATE_TAG}"

            minimo = disc >= 30

            if DEBUG_FILTERS:
                print(f"[DEBUG] ✅ scelto asin={asin} price={price_val} disc={disc} (fallback={'Y' if item_for_images is not item else 'N'})")

            return {
                "asin": asin,
                "title": title[:80].strip() + ("…" if len(title) > 80 else ""),
                "price_new": price_val,
                "price_old": old_val,
                "discount": disc,
                "url_img": url_img,
                "url": url,
                "minimo": minimo,
            }

    if DEBUG_FILTERS:
        print(f"[DEBUG] kw={kw} reasons={dict(reasons)}")
    return None


def invia_offerta():
    if bot is None:
        print("❌ TELEGRAM_BOT_TOKEN mancante.")
        return False
    if not TELEGRAM_CHAT_ID:
        print("❌ TELEGRAM_CHAT_ID mancante.")
        return False

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

    # se non abbiamo disc/old coerenti, evita testo brutto
    if sconto > 0 and prezzo_vecchio_val > prezzo_nuovo_val:
        caption_parts.append(
            f"💶 A soli <b>{prezzo_nuovo_val:.2f}€</b> invece di "
            f"<s>{prezzo_vecchio_val:.2f}€</s> (<b>-{sconto}%</b>)"
        )
    else:
        caption_parts.append(f"💶 Prezzo attuale: <b>{prezzo_nuovo_val:.2f}€</b>")

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


def start_scheduler_background():
    def _loop():
        schedule.clear()
        schedule.every().monday.at("06:59").do(resetta_pubblicati)
        schedule.every(14).minutes.do(run_if_in_fascia_oraria)
        while True:
            schedule.run_pending()
            time.sleep(5)

    t = threading.Thread(target=_loop, daemon=True)
    t.start()


# =========================
# FLASK ROUTES
# =========================
@app.route("/health", methods=["GET", "HEAD"])
def health():
    return ("ok", 200)


@app.route("/run", methods=["GET"])
def run_once():
    try:
        ok = invia_offerta()
        return jsonify({"ok": bool(ok)}), 200
    except Exception as e:
        print(f"❌ Errore /run: {repr(e)}")
        return jsonify({"ok": False, "error": str(e)}), 500


# Avvia lo scheduler in background appena il modulo viene caricato da gunicorn
start_scheduler_background()
