import importlib
import os
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from aiogram.exceptions import TelegramNetworkError, TelegramRetryAfter, TelegramForbiddenError
from aiogram.methods import SendMessage
from monitor_delivery import PermanentDelivery, RetryDelivery, CancelDelivery


with patch.dict(os.environ, {'API_ID': '123', 'API_HASH': 'test',
        'BOT_TOKEN': '123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghi',
        'DATABASE_URL': 'postgresql://unused/test', 'ADMIN_ID': '55'}):
    main = importlib.import_module('main')


class ControllerTests(unittest.IsolatedAsyncioTestCase):
    def test_exact_screenshot_queries_choose_only_correct_notification(self):
        queries = [
            'iPhone 18 Pro Max eSIM 512GB Silver',
            'iPhone 18 Pro Max eSIM 512Gb Glasier',
            'iPhone 18 Pro eSIM 512Gb Silver',
            'iPhone 17 Pro Max SIM 512Gb Blue',
            'iPhone 17 Pro Max SIM 1Tb Deep Blue',
            'iPhone 17 Pro SIM 512Gb Silver',
        ]
        rows = [{'id': index + 1, 'query': query} for index, query in enumerate(queries)]
        correct = main.find_best_monitor_match(rows, 'iPhone 17 Pro Max 1tb deep Blue sim ?')
        self.assertEqual(correct[1], 5)
        self.assertIsNone(main.find_best_monitor_match(rows,
            'Телефон Apple iPhone 17 Pro Max 512 ГБ, цвет: Серебристый (Silver) сим'))
        self.assertIsNone(main.find_best_monitor_match(rows, '18 Pro Max 512GB Silver 1Sim+Esim'))

    async def test_live_handler_queues_and_does_not_send_before_delivery_worker(self):
        event = SimpleNamespace(raw_text='Куплю iPhone 17 256 Black', chat_id=-100123, id=9, out=False,
                                get_chat=AsyncMock(return_value=SimpleNamespace(title='Group')))
        outbox = SimpleNamespace(enqueue=AsyncMock(return_value=1))
        bot = SimpleNamespace(send_message=AsyncMock())
        with patch.object(main, 'get_owner_id', AsyncMock(return_value=55)), \
             patch.object(main, 'get_selected_chat_ids', AsyncMock(return_value={-100123})), \
             patch.object(main, 'get_queries', AsyncMock(return_value=[{'id': 2, 'query': 'iPhone 17 256GB Black'}])), \
             patch.object(main, 'seller_info', AsyncMock(return_value=('@buyer', 'buyer', 'user:77'))), \
             patch.object(main, 'notification_outbox', outbox), patch.object(main, 'bot', bot):
            await main.monitor_handler(event, 1)
        bot.send_message.assert_not_awaited()
        outbox.enqueue.assert_awaited_once()
        payload = outbox.enqueue.await_args.args[5]
        self.assertIn('Куплю iPhone 17 256 Black', payload['notification'])

    async def test_test_command_and_live_matcher_use_same_segmentation(self):
        sample = 'iPhone 17 256GB Black\nSamsung S26 Ultra 512GB Blue'
        rows = [{'id': 2, 'query': 'iPhone 17 256GB Black'}]
        message = SimpleNamespace(text='/test ' + sample, answer=AsyncMock(), from_user=SimpleNamespace(id=55))
        with patch.object(main, 'guard_message', AsyncMock(return_value=True)), \
             patch.object(main, 'get_queries', AsyncMock(return_value=rows)):
            await main.cmd_test(message)
        self.assertIn('✅', message.answer.await_args.args[0])
        self.assertEqual(main.find_best_monitor_match(rows, sample)[0], 100)

    async def test_wrong_brands_and_seller_posts_never_queue(self):
        outbox = SimpleNamespace(enqueue=AsyncMock())
        for text in ('Xiaomi 17 Pro Max 512GB Black eSIM', 'Продам iPhone 17 Pro Max 512GB Black eSIM'):
            event = SimpleNamespace(raw_text=text, chat_id=-100123, id=9, out=False)
            with patch.object(main, 'get_owner_id', AsyncMock(return_value=55)), \
                 patch.object(main, 'get_selected_chat_ids', AsyncMock(return_value={-100123})), \
                 patch.object(main, 'get_queries', AsyncMock(return_value=[{'id': 2, 'query': 'iPhone 17 Pro Max 512GB Black eSIM'}])), \
                 patch.object(main, 'notification_outbox', outbox):
                await main.monitor_handler(event, 1)
        outbox.enqueue.assert_not_awaited()

    async def test_screenshot_priced_sale_is_not_queued_as_a_buyer(self):
        query = 'iPhone 17 Pro Max SIM 512Gb Cosmic Orange'
        event = SimpleNamespace(raw_text='iPhone 17 Pro Max 512 Cosmic Orange (1 Sim) 🇪🇺 - 124000',
                                chat_id=-100123, id=99, out=False)
        outbox = SimpleNamespace(enqueue=AsyncMock())
        with patch.object(main, 'get_owner_id', AsyncMock(return_value=55)), \
             patch.object(main, 'get_selected_chat_ids', AsyncMock(return_value={-100123})), \
             patch.object(main, 'get_queries', AsyncMock(return_value=[{'id': 2, 'query': query}])), \
             patch.object(main, 'notification_outbox', outbox):
            await main.monitor_handler(event, 1)
        outbox.enqueue.assert_not_awaited()

    async def test_buy_header_and_price_survive_live_and_delivery_recheck(self):
        query = 'iPhone 17 Pro Max SIM 512Gb Cosmic Orange'
        text = 'Куплю:\niPhone 17 Pro Max 512 Cosmic Orange (1 Sim) 🇪🇺 - 124000'
        event = SimpleNamespace(raw_text=text, chat_id=-100123, id=99, out=False,
                                get_chat=AsyncMock(return_value=SimpleNamespace(title='Group')))
        outbox = SimpleNamespace(enqueue=AsyncMock(return_value=1))
        bot = SimpleNamespace(send_message=AsyncMock(return_value=SimpleNamespace(message_id=202)))
        with patch.object(main, 'get_owner_id', AsyncMock(return_value=55)), \
             patch.object(main, 'get_selected_chat_ids', AsyncMock(return_value={-100123})), \
             patch.object(main, 'get_queries', AsyncMock(return_value=[{'id': 2, 'query': query}])), \
             patch.object(main, 'seller_info', AsyncMock(return_value=('@buyer', 'buyer', 'user:77'))), \
             patch.object(main, 'notification_outbox', outbox), patch.object(main, 'bot', bot):
            await main.monitor_handler(event, 1)
            payload = outbox.enqueue.await_args.args[5]
            self.assertIn('Куплю:', payload['match_text'])
            result = await main.send_queued_notification({'owner_id': 55, 'payload': payload})
        self.assertEqual(result, 202)
        bot.send_message.assert_awaited_once()

    async def test_network_rate_limit_and_forbidden_are_classified(self):
        method = SendMessage(chat_id=55, text='test')
        row = {'owner_id': 55, 'payload': {'notification': 'test', 'found_request': 'iPhone 17',
               'monitor_query': 'iPhone 17', 'match_text': 'Куплю iPhone 17'}}
        for error, expected in [(TelegramNetworkError(method=method, message='test'), RetryDelivery),
                                (TelegramRetryAfter(method=method, message='test', retry_after=30), RetryDelivery),
                                (TelegramForbiddenError(method=method, message='test'), PermanentDelivery)]:
            with patch.object(main, 'bot', SimpleNamespace(send_message=AsyncMock(side_effect=error))), \
                 patch.object(main, 'get_owner_id', AsyncMock(return_value=55)):
                with self.assertRaises(expected):
                    await main.send_queued_notification(row)

    async def test_pending_old_match_is_rechecked_with_current_rules(self):
        for query, source in [
            ('iPhone 18 Pro Max eSIM 512GB Silver', '18 Pro Max 512GB Silver 1Sim+Esim'),
            ('iPhone 17 Pro SIM 512Gb Silver', 'Телефон Apple iPhone 17 Pro Max 512 ГБ, цвет: Серебристый (Silver) сим'),
            ('iPhone 17 Pro Max SIM 512Gb Cosmic Orange', 'iPhone 17 Pro Max 512 Cosmic Orange (1 Sim) 🇪🇺 - 124000'),
            ('iPhone 17 Pro Max eSIM 512Gb Cosmic Orange', 'iPhone 17 Pro Max 512 Cosmic Orange Global eSIM'),
        ]:
            for old_payload in (False, True):
                payload = dict(monitor_query=query, found_request=source,
                               notification='🔥 ЗАПРОС: ' + query + '\n\n💬 Group\n👤 Buyer\n\n' + source)
                if not old_payload:
                    payload['match_text'] = source
                bot = SimpleNamespace(send_message=AsyncMock())
                with patch.object(main, 'get_owner_id', AsyncMock(return_value=55)), patch.object(main, 'bot', bot):
                    with self.assertRaises(CancelDelivery):
                        await main.send_queued_notification({'owner_id': 55, 'payload': payload})
                bot.send_message.assert_not_awaited()

    async def test_management_requires_owner_in_private_chat(self):
        for user, chat, expected in [(55, 'private', True), (66, 'private', False), (55, 'group', False)]:
            message = SimpleNamespace(from_user=SimpleNamespace(id=user), chat=SimpleNamespace(type=chat), answer=AsyncMock())
            with patch.object(main, 'get_owner_id', AsyncMock(return_value=55)):
                self.assertEqual(await main.guard_message(message), expected)


if __name__ == '__main__':
    unittest.main()
