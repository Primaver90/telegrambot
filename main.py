import os
from telegram import Bot
from telegram import InlineKeyboardMarkup, InlineKeyboardButton
from amazon_paapi import AmazonApi
from PIL import Image, ImageDraw, ImageFont
import requests

def _autocrop_nonblack(im, tol=12):
    if im.mode != "RGB":
        im = im.convert("RGB")
    px = im.load()
    w,h = im.size
    top, bottom, left, right = 0, h-1, 0, w-1

    # top
    for y in range(h):
        if any(px[x,y][0]>tol or px[x,y][1]>tol or px[x,y][2]>tol for x in range(w)):
            top = y; break
    # bottom
    for y in range(h-1, -1, -1):
        if any(px[x,y][0]>tol or px[x,y][1]>tol or px[x,y][2]>tol for x in range(w)):
            bottom = y; break
    # left
    for x in range(w):
        if any(px[x,y][0]>tol or px[x,y][1]>tol or px[x,y][2]>tol for y in range(h)):
            left = x; break
    # right
    for x in range(w-1, -1, -1):
        if any(px[x,y][0]>tol or px[x,y][1]>tol or px[x,y][2]>tol for y in range(h)):
            right = x; break

    if right<=left or bottom<=top:
        return im
    return im.crop((left, top, right+1, bottom+1))


def _crop_bottom_black(im, tol=12):
    if im.mode != "RGB":
        im = im.convert("RGB")
    px = im.load()
    w, h = im.size
    cut = h
    for y in range(h-1, -1, -1):
        row_ok = False
        for x in range(w):
            r,g,b = px[x,y]
            if r>tol or g>tol or b>tol:
                row_ok = True
                break
        if row_ok:
            cut = y+1
            break
    return im.crop((0,0,w,cut))


def _fmt_euro(x):
    s = f"{x:,.2f}"
    s = s.replace(",", "X").replace(".", ",").replace("X", ".")
    return s
def _wrap(draw, text, font, max_w):
    words = text.split()
    lines=[]; cur=""
    for w in words:
        test = (cur+" "+w).strip()
        if draw.textlength(test, font=font) <= max_w or not cur:
            cur = test
        else:
            lines.append(cur); cur=w
    if cur: lines.append(cur)
    return lines[:4]

from io import BytesIO
import schedule
import time
from datetime import datetime

def is_real_deal(offer):
    try:
        price = getattr(offer, "price", None)
        savings = getattr(price, "savings", None)
        perc = int(getattr(savings, "percentage", 0) or 0)
        amount_save = float(getattr(savings, "amount", 0) or 0)
        prime = bool(getattr(offer, "is_eligible_for_prime", False))
        fulfilled = bool(getattr(getattr(offer, "delivery_info", None), "is_amazon_fulfilled", False))
        merchant = (getattr(getattr(offer, "merchant_info", None), "name", "") or "").lower()
        if perc < 15 or amount_save < 5:
            return False
        if not (prime or fulfilled):
            return False
        if merchant and "amazon" not in merchant:
            return False
        return True
    except Exception:
        return False

def draw_bold_text(draw, position, text, font, fill="black", offset=1):
    x, y = position
    # Disegna più volte il testo attorno al punto centrale
    for dx in (-offset, 0, offset):
        for dy in (-offset, 0, offset):
            draw.text((x + dx, y + dy), text, font=font, fill=fill)

# Chiavi di accesso Amazon e Telegram
AMAZON_ACCESS_KEY = "AKPAZS2VGY1748024339"
AMAZON_SECRET_KEY = "yiA1TX0xWWVtW1HgKpkR2LWZpklQXaJ2k9D4HsiL"
AMAZON_ASSOCIATE_TAG = "itech00-21"
AMAZON_COUNTRY = "IT"
TELEGRAM_BOT_TOKEN = "7687135950:AAHfRV6b4RgAcVU6j71wDfZS-1RTMJ15ajg"
TELEGRAM_CHAT_ID = "-1001010781022"

bot = Bot(token=TELEGRAM_BOT_TOKEN)
amazon = AmazonApi(AMAZON_ACCESS_KEY, AMAZON_SECRET_KEY, AMAZON_ASSOCIATE_TAG, AMAZON_COUNTRY)

font_path = "Montserrat-VariableFont_wght.ttf"
logo_path = "header_clean2.png"
badge_path = "minimo storico flat.png"

def resetta_pubblicati():
    with open("pubblicati.txt", "w") as f:
        f.write("")
    print("✅ File pubblicati.txt azzerato alle 8:59")
schedule.every().day.at("08:59").do(resetta_pubblicati)
def genera_immagine_offerta(titolo, prezzo_nuovo, prezzo_vecchio, sconto, url_img, minimo_storico):
    img = Image.new("RGB", (1080, 1080), "white")
    draw = ImageDraw.Draw(img)

    # Intestazione
    logo = Image.open(logo_path).resize((1080, 165))
    img.paste(logo, (0, 0))

    # Badge minimo storico
    if minimo_storico and sconto >= 30:
        badge = Image.open(badge_path).convert('RGBA')
        badge.thumbnail((250, 250), Image.LANCZOS)
        img.paste(badge, (60, 300), badge)

    # Percentuale sconto
    font_perc = ImageFont.truetype(font_path, 88)
    draw.text((850, 230), f"-{sconto}%", font=font_perc, fill="black")

    # Immagine prodotto
    response = requests.get(url_img, timeout=10)
    prodotto = Image.open(BytesIO(response.content)).resize((600, 600))
    img.paste(prodotto, (240, 230))

    # Prezzi
    font_old = ImageFont.truetype(font_path, 72)
    font_new = ImageFont.truetype(font_path, 120)
    prezzo_old_str = f"€ {prezzo_vecchio}"
    prezzo_new_str = f"€ {prezzo_nuovo}"

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

def invia_offerta():
    posted = False
    print("🕵️ Cerco offerte tech su Amazon...")
    try:
        with open("pubblicati.txt", "r") as f:
            pubblicati = {line.strip().upper() for line in f}
    except FileNotFoundError:
        pubblicati = set()

    KEYWORDS = ["apple","android","home","smartwatch","monitor","tv","soundbar","ssd","gaming","router","echo","kindle"]
    IDX_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "kw_idx.txt")
    try:
        with open(IDX_PATH, "r") as _f:
            _kw_idx = int(_f.read().strip()) % len(KEYWORDS)
    except Exception:
        _kw_idx = 0
    kw_order = KEYWORDS[_kw_idx:] + KEYWORDS[:_kw_idx]
    for kw in kw_order:
        time.sleep(1.2)
        for page in range(1, 5):
            print(f"chiamo PA-API '{kw}' pagina {page}")
            risultati = amazon.search_items(keywords=kw, item_count=5, search_index="All", item_page=page)
            print("ok", kw, page)

            for item in risultati.items:
                asin = getattr(item, 'asin', None)
                if not asin:
                    continue
                asin = asin.strip().upper()
                if asin in pubblicati:
                    continue

                titolo = item.item_info.title.display_value
                titolo = titolo[:80].strip() + "…" if len(titolo) > 80 else titolo

                offers = getattr(item, 'offers', None)
                listings = getattr(offers, 'listings', []) if offers else []
                offer = listings[0] if listings else None
                if not offer:
                    continue
                if not is_real_deal(offer):
                    continue

                if not listings:
                    continue

                price = getattr(listings[0].price, 'amount', None)
                savings = getattr(listings[0].price, 'savings', None)

                if price is None:
                    continue

                prezzo_nuovo_valore = float(price)
                prezzo_vecchio = prezzo_nuovo_valore + float(getattr(savings, 'amount', 0) or 0)
                sconto = int(getattr(savings, 'percentage', 0) or 0)

                if sconto < 5:
                    continue

                url_img = item.images.primary.large.url
                url = item.detail_page_url
                minimo = sconto >= 30

                immagine = genera_immagine_offerta(titolo, prezzo_nuovo_valore, prezzo_vecchio, sconto, url_img, minimo)

                caption = f"📌 *{titolo}*\n\n"
                if minimo:
                    caption += "❗️🚨 *MINIMO STORICO* 🚨❗️\n"
                caption += f"💶 A soli *{prezzo_nuovo_valore:.2f}€* invece di *{prezzo_vecchio:.2f}€* (*-{sconto}%*)\n\n👉 [Acquista ora]({url})\n"

                button = InlineKeyboardMarkup([[InlineKeyboardButton("🛒 Acquista ora", url=url)]])

                bot.send_photo(chat_id=TELEGRAM_CHAT_ID, photo=immagine, caption=caption, parse_mode="Markdown", reply_markup=button)

                with open("pubblicati.txt", "a") as f:
                    f.write(asin + "\n")

                try:
                    with open(IDX_PATH, "w") as _f:
                        _f.write(str((_kw_idx + 1) % len(KEYWORDS)))
                except Exception:
                    pass
                posted = True
                return
    if not posted:
        print("Nessuna offerta idonea")


invia_offerta()
import schedule
import time
from datetime import datetime

def is_real_deal(offer):
    try:
        price = getattr(offer, "price", None)
        savings = getattr(price, "savings", None)
        perc = int(getattr(savings, "percentage", 0) or 0)
        amount_save = float(getattr(savings, "amount", 0) or 0)
        prime = bool(getattr(offer, "is_eligible_for_prime", False))
        fulfilled = bool(getattr(getattr(offer, "delivery_info", None), "is_amazon_fulfilled", False))
        merchant = (getattr(getattr(offer, "merchant_info", None), "name", "") or "").lower()
        if perc < 15 or amount_save < 5:
            return False
        if not (prime or fulfilled):
            return False
        if merchant and "amazon" not in merchant:
            return False
        return True
    except Exception:
        return False


def run_if_in_fascia_oraria():
    now = datetime.now().time()
    if now >= datetime.strptime("09:00", "%H:%M").time() and now <= datetime.strptime("21:00", "%H:%M").time():
        invia_offerta()

# Ogni 14 minuti pubblica 1 offerta, solo se tra le 9:00 e le 21:00
schedule.every(14).minutes.do(run_if_in_fascia_oraria)

while True:
    schedule.run_pending()
    time.sleep(30)