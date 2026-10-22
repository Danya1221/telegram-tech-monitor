import importlib
import os
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from aiogram.exceptions import TelegramNetworkError, TelegramRetryAfter, TelegramForbiddenError
from aiogram.methods import SendMessage
from monitor_delivery import PermanentDelivery, RetryDelivery


with patch.dict(os.environ, {'API_ID': '123', 'API_HASH': 'test',
        'BOT_TOKEN': '123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghi',
        'DATABASE_URL': 'postgresql://unused/test', 'ADMIN_ID': '55'}):
    main = importlib.import_module('main')


class ControllerTests(unittest.IsolatedAsyncioTestCase):
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

    async def test_network_rate_limit_and_forbidden_are_classified(self):
        method = SendMessage(chat_id=55, text='test')
        row = {'owner_id': 55, 'payload': {'notification': 'test', 'found_request': 'iPhone 17'}}
        for error, expected in [(TelegramNetworkError(method=method, message='test'), RetryDelivery),
                                (TelegramRetryAfter(method=method, message='test', retry_after=30), RetryDelivery),
                                (TelegramForbiddenError(method=method, message='test'), PermanentDelivery)]:
            with patch.object(main, 'bot', SimpleNamespace(send_message=AsyncMock(side_effect=error))), \
                 patch.object(main, 'get_owner_id', AsyncMock(return_value=55)):
                with self.assertRaises(expected):
                    await main.send_queued_notification(row)

    async def test_management_requires_owner_in_private_chat(self):
        for user, chat, expected in [(55, 'private', True), (66, 'private', False), (55, 'group', False)]:
            message = SimpleNamespace(from_user=SimpleNamespace(id=user), chat=SimpleNamespace(type=chat), answer=AsyncMock())
            with patch.object(main, 'get_owner_id', AsyncMock(return_value=55)):
                self.assertEqual(await main.guard_message(message), expected)


if __name__ == '__main__':
    unittest.main()
