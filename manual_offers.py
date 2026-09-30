"""Comandi privati, anteprima e conferma. Nessuna pubblicazione alla ricezione di un link."""
import os
import re
import secrets
import time
from urllib.parse import urlparse, urljoin

import requests
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
import main

ADMIN_IDS = {int(v.strip()) for v in os.environ.get('TELEGRAM_ADMIN_IDS', '').split(',') if v.strip()}
DRAFT_TTL = 600
_drafts = {}
HELP = ('Invia un link Amazon.it o /offerta ASIN.\n'
        'Poi puoi usare /testo descrizione e /coupon codice o istruzioni.\n'
        'Usa /testo - o /coupon - per cancellarli.\n'
        'Ogni modifica crea una nuova anteprima. Premi Pubblica solo quando è pronta.\n'
        '/annulla elimina la bozza. Il prezzo mostrato non sottrae automaticamente il coupon.')


def _allowed_url(url):
    p = urlparse(url)
    return (p.scheme == 'https' and p.hostname in
            {'amazon.it', 'www.amazon.it', 'm.amazon.it', 'amzn.eu', 'amzn.to'}
            and not p.username and not p.password and p.port in (None, 443))


def extract_asin(value):
    value = value.strip()
    if re.fullmatch(r'[A-Za-z0-9]{10}', value):
        return value.upper()
    if not _allowed_url(value):
        raise ValueError('Invia un link HTTPS Amazon.it, amzn.eu, amzn.to oppure un ASIN di 10 caratteri.')
    for _ in range(6):
        p = urlparse(value)
        if p.hostname in {'amazon.it', 'www.amazon.it', 'm.amazon.it'}:
            found = re.search(r'/(?:dp|gp/product|gp/aw/d)/([A-Za-z0-9]{10})(?:/|$)', p.path)
            if found:
                return found.group(1).upper()
        # Nessun redirect automatico: ogni destinazione è verificata prima della richiesta.
        with requests.get(value, allow_redirects=False, stream=True, timeout=10) as response:
            if response.status_code not in (301, 302, 303, 307, 308):
                break
            value = urljoin(value, response.headers.get('Location', ''))
            if not _allowed_url(value):
                raise ValueError('Il link rimanda fuori da Amazon.it: incolla il link completo del prodotto.')
    raise ValueError('Prodotto non riconosciuto: invia il link completo Amazon.it o il suo ASIN.')


def load_offer(asin):
    response, _ = main.creators_get_items([asin])
    items = main.safe_get(response, 'itemsResult', 'items', default=None)
    if items is None:
        items = response.get('items', [])
    for item in items or []:
        if item.get('asin', '').upper() != asin:
            continue
        p = main.extract_from_item(item)
        if not p or p['price'] is None or p['price'] <= 0 or not p['url_img']:
            break
        return dict(asin=asin, title=p['title'][:120], price_new=p['price'],
                    price_old=p['old'] or p['price'], discount=p['discount'],
                    url_img=p['url_img'],
                    url=f'https://www.amazon.it/dp/{asin}?tag={main.AMAZON_ASSOCIATE_TAG}',
                    minimo=False)
    raise ValueError('Amazon non restituisce prezzo e immagine validi per questo prodotto.')


def preview(uid, offer):
    # Sostituzione immediata: i pulsanti delle anteprime precedenti non possono pubblicare.
    nonce = secrets.token_hex(8)
    draft = dict(payload=offer, nonce=nonce, expires=time.time() + DRAFT_TTL)
    _drafts[uid] = draft
    buttons = InlineKeyboardMarkup([[
        InlineKeyboardButton('✅ Pubblica', callback_data='publish:' + nonce),
        InlineKeyboardButton('❌ Annulla', callback_data='cancel:' + nonce),
    ]])
    try:
        main.send_payload(offer, uid, reply_markup=buttons)
    except Exception:
        _drafts.pop(uid, None)
        raise


def handle_update(update):
    uid = update.effective_user.id if update.effective_user else None
    chat = update.effective_chat
    if not chat or chat.type != 'private' or uid is None:
        return
    query = update.callback_query
    message = update.effective_message
    text = (getattr(message, 'text', None) or '').strip()
    # L'ID personale è mostrato soltanto al suo proprietario, senza abilitare comandi.
    if not query and text.split('@', 1)[0] == '/id':
        main.bot.send_message(uid, f'Il tuo ID Telegram: {uid}')
        return
    if uid not in ADMIN_IDS:
        if query:
            query.answer('Non autorizzato.', show_alert=True)
        return
    if query:
        action, _, nonce = (query.data or '').partition(':')
        if action not in ('publish', 'cancel'):
            return
        draft = _drafts.get(uid)
        if not draft or nonce != draft['nonce'] or time.time() > draft['expires']:
            query.answer('Anteprima scaduta o sostituita. Invia di nuovo il link.', show_alert=True)
            return
        query.answer()
        if action == 'cancel':
            _drafts.pop(uid, None)
            main.bot.send_message(uid, 'Bozza annullata.')
            return
        # Consuma prima dell'invio: anche un timeout ambiguo non causa un reinvio automatico.
        _drafts.pop(uid, None)
        offer = draft['payload']
        fresh = load_offer(offer['asin'])
        fresh.update(note=offer.get('note', ''), coupon=offer.get('coupon', ''))
        if any(fresh[k] != offer[k] for k in ('price_new', 'price_old', 'discount', 'title', 'url_img', 'url')):
            main.bot.send_message(uid, 'I dati Amazon sono cambiati: controlla la nuova anteprima e conferma di nuovo.')
            preview(uid, fresh)
            return
        with main.publication_lock:
            if not main.can_post(offer['asin']):
                main.bot.send_message(uid, 'Questo prodotto è già stato pubblicato nelle ultime 24 ore.')
                return
            main.send_payload(fresh, main.TELEGRAM_CHAT_ID)
            main.save_pubblicati(offer['asin'])
            main.mark_posted(offer['asin'])
        main.bot.send_message(uid, '✅ Offerta pubblicata nel canale.')
        return
    command, _, arg = text.partition(' ')
    command = command.split('@', 1)[0].lower()
    arg = arg.strip()
    if command in ('/start', '/help', '/aiuto'):
        main.bot.send_message(uid, HELP)
    elif command == '/annulla':
        _drafts.pop(uid, None)
        main.bot.send_message(uid, 'Bozza annullata.')
    elif command in ('/testo', '/coupon'):
        draft = _drafts.get(uid)
        if not draft or time.time() > draft['expires']:
            raise ValueError('Prima invia il link del prodotto.')
        limit = 200 if command == '/testo' else 80
        if not arg or len(arg) > limit:
            raise ValueError(f'Inserisci un testo di massimo {limit} caratteri, oppure - per rimuoverlo.')
        offer = dict(draft['payload'])
        offer['note' if command == '/testo' else 'coupon'] = '' if arg == '-' else arg
        preview(uid, offer)
    elif command == '/offerta' or text.startswith('https://') or re.fullmatch(r'[A-Za-z0-9]{10}', text):
        asin = extract_asin(arg if command == '/offerta' else text)
        preview(uid, load_offer(asin))
    else:
        main.bot.send_message(uid, HELP)


def run_polling():
    # Non cancella eventuali webhook esistenti: in quel caso segnala il conflitto nei log.
    offset = None
    print("Comandi privati in ascolto; amministratori configurati: " + str(len(ADMIN_IDS)), flush=True)
    while True:
        try:
            updates = main.bot.get_updates(offset=offset, timeout=20,
                                           allowed_updates=['message', 'callback_query'])
            for update in updates:
                offset = update.update_id + 1
                try:
                    handle_update(update)
                except Exception as exc:
                    print(f'Comando manuale: {type(exc).__name__}')
                    if update.effective_user and update.effective_user.id in ADMIN_IDS:
                        try:
                            msg = str(exc) if isinstance(exc, ValueError) else (
                                'Operazione non completata. Controlla il canale prima di riprovare '
                                'se avevi premuto Pubblica; poi invia nuovamente il link.')
                            main.bot.send_message(update.effective_user.id, msg)
                        except Exception:
                            pass
            for uid, draft in list(_drafts.items()):
                if time.time() > draft['expires']:
                    _drafts.pop(uid, None)
        except Exception as exc:
            print(f'Ricezione Telegram: {type(exc).__name__}; nuovo tentativo tra 5 secondi')
            time.sleep(5)
