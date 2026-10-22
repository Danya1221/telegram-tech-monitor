"""Run against an isolated schema when TEST_DATABASE_URL is provided."""
import asyncio
import os
import unittest
import uuid
from unittest.mock import AsyncMock, patch

import asyncpg

from monitor_delivery import DeliveryWorker, Outbox, RetryDelivery
from test_app import main


@unittest.skipUnless(os.environ.get('TEST_DATABASE_URL'), 'Set TEST_DATABASE_URL to run PostgreSQL integration tests')
class PostgreSQLTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.dsn = os.environ['TEST_DATABASE_URL']
        self.schema = 'monitor_test_' + uuid.uuid4().hex
        self.admin = await asyncpg.connect(self.dsn)
        await self.admin.execute(f'CREATE SCHEMA {self.schema}')
        self.pool = await asyncpg.create_pool(self.dsn, min_size=1, max_size=3,
                                            server_settings={'search_path': self.schema})
        self.old_pool = main.db_pool
        with patch.object(main.asyncpg, 'create_pool', AsyncMock(return_value=self.pool)):
            await main.init_db()
        self.repo = Outbox(self.pool)
        await self.repo.initialize()
        async with self.pool.acquire() as conn:
            self.query = await conn.fetchval("INSERT INTO queries(owner_id,query) VALUES(55,'iPhone 17') RETURNING id")
            self.account = await conn.fetchval("INSERT INTO telegram_accounts(owner_id,label,session) VALUES(55,'test','synthetic') RETURNING id")
            await conn.execute('INSERT INTO selected_chats(owner_id,chat_id,title,account_id) VALUES(55,-100123,$1,$2)', 'Test', self.account)
        self.payload = {'notification': 'test', 'chat_title': 'Test', 'monitor_query': 'iPhone 17',
                        'found_request': 'iPhone 17', 'brand': 'Apple',
                        'seller_key': 'user:77', 'request_key': 'iphone 17'}
        self.send = AsyncMock(return_value=111)
        self.worker = DeliveryWorker(self.repo, self.send)

    async def asyncTearDown(self):
        main.db_pool = self.old_pool
        await self.pool.close()
        await self.admin.execute(f'DROP SCHEMA {self.schema} CASCADE')
        await self.admin.close()

    async def enqueue(self, message=9, chat=-100123):
        return await self.repo.enqueue(55, chat, message, self.account, self.query, self.payload, 0)

    async def scalar(self, sql):
        async with self.pool.acquire() as conn:
            return await conn.fetchval(sql)

    async def test_enqueue_is_durable_and_idempotent_including_legacy_seen(self):
        self.assertIsNotNone(await self.enqueue())
        self.assertIsNone(await self.enqueue())
        new_repo = Outbox(self.pool)
        await new_repo.recover()
        self.assertEqual((await new_repo.claim())['message_id'], 9)
        async with self.pool.acquire() as conn:
            await conn.execute('INSERT INTO seen_messages(owner_id,chat_id,message_id) VALUES(55,-100123,10)')
        self.assertIsNone(await self.enqueue(10))

    async def test_concurrent_workers_cannot_claim_same_job(self):
        await self.enqueue()
        rows = await asyncio.gather(self.repo.claim(), self.repo.claim())
        self.assertEqual(sum(row is not None for row in rows), 1)

    async def test_network_error_does_not_mark_seen_or_analytics_and_later_retries(self):
        await self.enqueue()
        self.send.side_effect = RetryDelivery('network', 3)
        await self.worker.step()
        self.assertEqual(await self.scalar('SELECT COUNT(*) FROM seen_messages'), 0)
        self.assertEqual(await self.scalar('SELECT COUNT(*) FROM analytics_events'), 0)
        self.assertEqual(await self.scalar('SELECT COUNT(*) FROM recent_request_dedup'), 0)
        self.assertEqual(await self.scalar('SELECT status FROM notification_outbox'), 'pending')
        self.assertFalse(await self.worker.step())
        async with self.pool.acquire() as conn:
            await conn.execute('UPDATE notification_outbox SET due_at=NOW()')
        self.send.side_effect = None
        await DeliveryWorker(Outbox(self.pool), self.send).step()
        self.assertEqual(await self.scalar('SELECT status FROM notification_outbox'), 'sent')
        self.assertEqual(await self.scalar('SELECT COUNT(*) FROM analytics_events'), 1)
        self.assertEqual(await self.scalar('SELECT COUNT(*) FROM seen_messages'), 1)

    async def test_chat_or_query_removed_after_enqueue_cancels_delivery(self):
        await self.enqueue()
        async with self.pool.acquire() as conn:
            await conn.execute('DELETE FROM selected_chats')
        await self.worker.step()
        self.send.assert_not_awaited()
        self.assertEqual(await self.scalar('SELECT status FROM notification_outbox'), 'cancelled')
        self.assertEqual(await self.scalar('SELECT COUNT(*) FROM analytics_events'), 0)

    async def test_crosschat_dedup_and_counts_after_success_only(self):
        async with self.pool.acquire() as conn:
            await conn.execute('INSERT INTO selected_chats(owner_id,chat_id,title,account_id) VALUES(55,-100124,$1,$2)', 'Other', self.account)
        await self.enqueue()
        await self.enqueue(10, -100124)
        await self.worker.step()
        await self.worker.step()
        self.send.assert_awaited_once()
        self.assertEqual(await self.scalar('SELECT COUNT(*) FROM analytics_events'), 1)
        self.assertEqual((await self.repo.counts(55)).get('duplicate'), 1)

    async def test_delivery_completion_transaction_rolls_back_on_analytics_error(self):
        self.payload['monitor_query'] = 'invalid'
        async with self.pool.acquire() as conn:
            await conn.execute("ALTER TABLE analytics_events ADD CONSTRAINT test_reject CHECK(monitor_query<>'invalid')")
        await self.enqueue()
        await self.worker.step()
        self.send.assert_awaited_once()
        self.assertEqual(await self.scalar('SELECT status FROM notification_outbox'), 'uncertain')
        self.assertEqual(await self.scalar('SELECT COUNT(*) FROM seen_messages'), 0)
        self.assertEqual(await self.scalar('SELECT COUNT(*) FROM recent_request_dedup'), 0)
        self.assertFalse(await self.worker.step())

    async def test_restart_and_explicit_review_of_uncertain_jobs(self):
        await self.enqueue()
        await self.repo.claim()
        await Outbox(self.pool).recover()
        self.assertEqual(await self.scalar('SELECT status FROM notification_outbox'), 'uncertain')
        self.assertEqual(await self.repo.retry_failed(55), 0)
        self.assertEqual(await self.repo.retry_failed(55, True), 1)
        await self.worker.step()
        self.send.assert_awaited_once()


if __name__ == '__main__':
    unittest.main()
