import os
import sys
import tempfile
import unittest
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import Mock, patch

os.environ.update(TELEGRAM_BOT_TOKEN='123456789:TEST_ONLY_NOT_A_REAL_TOKEN',
                  TELEGRAM_CHAT_ID='-1000000000000', TELEGRAM_ADMIN_IDS='42',
                  AMAZON_ASSOCIATE_TAG='test-21', CREATORS_CREDENTIAL_ID='test',
                  CREATORS_CREDENTIAL_SECRET='test', CREATORS_CREDENTIAL_VERSION='3.2',
                  CREATORS_MARKETPLACE='www.amazon.it', BOT_DISABLE_BACKGROUND='1')
_tmp = tempfile.TemporaryDirectory()
os.environ['DATA_DIR'] = _tmp.name
import main
import manual_offers as manual
import app

OFFER = dict(asin='B012345678', title='Prodotto', price_new=100, price_old=150,
             discount=33, url_img='https://m.media-amazon.com/product.jpg',
             url='https://www.amazon.it/dp/B012345678?tag=test-21', minimo=False)


def update(text='', uid=42, chat_type='private', action=None):
    q = SimpleNamespace(data=action, answer=Mock()) if action else None
    return SimpleNamespace(effective_user=SimpleNamespace(id=uid),
                           effective_chat=SimpleNamespace(type=chat_type),
                           effective_message=SimpleNamespace(text=text), callback_query=q)


class Tests(unittest.TestCase):
    def setUp(self):
        manual._drafts.clear()

    def test_lwa_endpoint_and_payload(self):
        main._access_token = None
        r = Mock(status_code=200)
        r.json.return_value = {'access_token': 'token', 'expires_in': 3600}
        with patch.object(main.requests, 'post', return_value=r) as post:
            self.assertEqual(main._auth_header(), 'Bearer token')
            self.assertEqual(main._auth_header(), 'Bearer token')
            post.assert_called_once()
            self.assertEqual(post.call_args.args[0], 'https://api.amazon.co.uk/auth/o2/token')
            self.assertEqual(post.call_args.kwargs['json']['scope'], 'creatorsapi::default')
            self.assertNotIn('Authorization', post.call_args.kwargs['headers'])

    def test_version_and_stale_override(self):
        with patch.object(main, 'CREATORS_TOKEN_URL', 'https://old.example.com'):
            self.assertEqual(main._build_token_url(), 'https://api.amazon.co.uk/auth/o2/token')
        with patch.object(main, 'CREATORS_CREDENTIAL_VERSION', '3.0'):
            self.assertRaises(RuntimeError, main._build_token_url)

    def test_auth_error_does_not_leak_body(self):
        main._access_token = None
        with patch.object(main.requests, 'post', return_value=Mock(status_code=401, text='secret')):
            with self.assertRaises(RuntimeError) as c:
                main._get_access_token()
            self.assertNotIn('secret', str(c.exception))

    def test_asin_and_url(self):
        self.assertEqual(manual.extract_asin('B012345678'), 'B012345678')
        self.assertEqual(manual.extract_asin('https://www.amazon.it/title/dp/B012345678?tag=other'), 'B012345678')
        for url in ['https://amazon.it.evil.com/dp/B012345678', 'http://127.0.0.1/', 'https://user@amazon.it/dp/B012345678']:
            self.assertRaises(ValueError, manual.extract_asin, url)

    def test_short_redirect_rejects_private_address(self):
        response = Mock(status_code=302, headers={'Location': 'http://127.0.0.1/private'})
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=False)
        with patch.object(manual.requests, 'get', return_value=response) as get:
            self.assertRaises(ValueError, manual.extract_asin, 'https://amzn.eu/example')
            get.assert_called_once()

    def test_nested_getitems_and_manual_bypasses_filters(self):
        item = {'asin': 'B012345678', 'itemInfo': {'title': {'displayValue': 'Prodotto'}},
                'images': {'primary': {'large': {'url': OFFER['url_img']}}},
                'offersV2': {'listings': [{'price': {'money': {'amount': 10},
                                          'savings': {'money': {'amount': 1}, 'percentage': 9}}}]}}
        with patch.object(main, 'creators_get_items', return_value=({'itemsResult': {'items': [item]}}, [])):
            offer = manual.load_offer('B012345678')
            self.assertEqual(offer['price_new'], 10)
            self.assertFalse(offer['minimo'])
            self.assertIn('tag=test-21', offer['url'])

    def test_unauthorized_and_group_do_not_fetch_or_send(self):
        with patch.object(main, 'bot') as bot, patch.object(manual, 'load_offer') as load:
            manual.handle_update(update('/offerta B012345678', uid=7))
            manual.handle_update(update('/offerta B012345678', chat_type='group'))
            load.assert_not_called()
            bot.send_message.assert_not_called()

    def test_link_only_previews_privately(self):
        with patch.object(manual, 'load_offer', return_value=dict(OFFER)), patch.object(main, 'send_payload') as send:
            manual.handle_update(update('/offerta B012345678'))
            self.assertEqual(send.call_args.args[1], 42)
            self.assertEqual(len(manual._drafts), 1)

    def test_confirm_once(self):
        with patch.object(main, 'send_payload') as send, patch.object(main, 'bot'), patch.object(manual, 'load_offer', return_value=dict(OFFER)), patch.object(main, 'can_post', return_value=True), patch.object(main, 'save_pubblicati'), patch.object(main, 'mark_posted'):
            manual.preview(42, dict(OFFER))
            nonce = manual._drafts[42]['nonce']
            send.reset_mock()
            manual.handle_update(update(action='publish:' + nonce))
            manual.handle_update(update(action='publish:' + nonce))
            send.assert_called_once()
            self.assertEqual(send.call_args.args[1], main.TELEGRAM_CHAT_ID)

    def test_changed_price_requires_new_confirmation(self):
        with patch.object(main, 'send_payload') as send, patch.object(main, 'bot'), patch.object(manual, 'load_offer', return_value=dict(OFFER, price_new=110)):
            manual.preview(42, dict(OFFER))
            nonce = manual._drafts[42]['nonce']
            send.reset_mock()
            manual.handle_update(update(action='publish:' + nonce))
            self.assertEqual(send.call_args.args[1], 42)
            self.assertNotEqual(manual._drafts[42]['nonce'], nonce)

    def test_old_cancel_and_expiry(self):
        with patch.object(main, 'send_payload'), patch.object(main, 'bot'), patch.object(manual, 'load_offer') as load:
            manual.preview(42, dict(OFFER))
            old = manual._drafts[42]['nonce']
            manual.preview(42, dict(OFFER))
            manual.handle_update(update(action='publish:' + old))
            nonce = manual._drafts[42]['nonce']
            manual._drafts[42]['expires'] = 0
            manual.handle_update(update(action='publish:' + nonce))
            load.assert_not_called()

    def test_cancellation(self):
        with patch.object(main, 'send_payload'), patch.object(main, 'bot'), patch.object(manual, 'load_offer') as load:
            manual.preview(42, dict(OFFER))
            nonce = manual._drafts[42]['nonce']
            manual.handle_update(update(action='cancel:' + nonce))
            self.assertNotIn(42, manual._drafts)
            load.assert_not_called()

    def test_caption_escapes_notes_and_coupon(self):
        with patch.object(main, 'genera_immagine_offerta', return_value=b'image'), patch.object(main, 'bot') as bot:
            main.send_payload(dict(OFFER, note='<b>test</b>', coupon='A&B'), 42)
            caption = bot.send_photo.call_args.kwargs['caption']
            self.assertIn('&lt;b&gt;', caption)
            self.assertIn('A&amp;B', caption)
            self.assertNotIn('MINIMO STORICO', caption)

    def test_italy_dst_boundaries(self):
        self.assertTrue(main.is_in_italy_window(datetime(2026, 3, 29, 7))[0])
        self.assertFalse(main.is_in_italy_window(datetime(2026, 10, 25, 7))[0])
        self.assertTrue(main.is_in_italy_window(datetime(2026, 10, 25, 8))[0])

    def test_public_run_disabled(self):
        self.assertEqual(app.app.test_client().get('/run').status_code, 403)


if __name__ == '__main__':
    unittest.main()
