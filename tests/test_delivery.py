import asyncio
import copy
import unittest
from unittest.mock import AsyncMock, patch

from monitor_delivery import DeliveryWorker, PermanentDelivery, RetryDelivery, CancelDelivery


class MemoryOutbox:
    def __init__(self):
        self.row = {'id': 1, 'owner_id': 55, 'chat_id': -100123, 'account_id': 1,
                    'query_id': 1, 'message_id': 9, 'attempts': 0, 'status': 'pending',
                    'payload': {'notification': 'test'}}
        self.current, self.duplicate = True, False
        self.seen, self.analytics, self.completed = [], [], []
        self.states = []
        self.complete_error = False

    async def claim(self):
        if self.row['status'] != 'pending':
            return None
        self.row['status'] = 'sending'
        self.row['attempts'] += 1
        return copy.deepcopy(self.row)

    async def is_current(self, row): return self.current
    async def is_duplicate(self, row): return self.duplicate

    async def state(self, row, status, error='', delay=0):
        self.row['status'] = status
        self.states.append((status, error, delay))

    async def complete(self, row, message_id):
        if self.complete_error:
            raise OSError('database temporarily unavailable')
        self.row['status'] = 'sent'
        self.completed.append(message_id)
        self.seen.append(row['message_id'])
        self.analytics.append(row['message_id'])


class DeliveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.repo = MemoryOutbox()
        self.sent = []
        async def send(row):
            self.sent.append(row)
            return 100
        self.worker = DeliveryWorker(self.repo, send)

    async def test_success_counts_only_after_confirmed_send_and_does_not_repeat(self):
        self.assertTrue(await self.worker.step())
        self.assertEqual(self.repo.completed, [100])
        self.assertEqual(self.repo.seen, [9])
        self.assertEqual(self.repo.analytics, [9])
        self.assertFalse(await self.worker.step())
        self.assertEqual(len(self.sent), 1)

    async def test_transient_send_failure_remains_pending_and_retries(self):
        calls = []
        async def send(row):
            calls.append(row)
            if len(calls) == 1:
                raise RetryDelivery('temporary network error', 7)
            return 101
        self.worker.send = send
        await self.worker.step()
        self.assertEqual(self.repo.row['status'], 'pending')
        self.assertEqual(self.repo.analytics, [])
        self.assertEqual(self.repo.seen, [])
        self.assertGreaterEqual(self.repo.states[-1][2], 7)
        # A new worker after a restart uses the same persisted pending job.
        await DeliveryWorker(self.repo, send).step()
        self.assertEqual(self.repo.completed, [101])
        self.assertEqual(len(calls), 2)

    async def test_rate_limit_wait_is_respected(self):
        async def send(row): raise RetryDelivery('429', 51)
        self.worker.send = send
        await self.worker.step()
        self.assertGreaterEqual(self.repo.states[-1][2], 51)
        self.assertEqual(self.repo.analytics, [])

    async def test_permanent_error_stays_visible_without_blocking_other_jobs(self):
        async def send(row): raise PermanentDelivery('bot blocked')
        self.worker.send = send
        await self.worker.step()
        self.assertEqual(self.repo.row['status'], 'failed')
        self.assertEqual(self.repo.seen, [])
        self.assertFalse(await self.worker.step())

    async def test_deleted_query_or_deselected_chat_cancels_queued_send(self):
        self.repo.current = False
        await self.worker.step()
        self.assertEqual(self.repo.row['status'], 'cancelled')
        self.assertEqual(self.sent, [])
        self.assertEqual(self.repo.analytics, [])

    async def test_current_selection_is_checked_after_dedup_lookup(self):
        async def duplicate(row):
            self.repo.current = False
            return False
        self.repo.is_duplicate = duplicate
        await self.worker.step()
        self.assertEqual(self.sent, [])

    async def test_crosschat_duplicate_is_not_sent_or_counted(self):
        self.repo.duplicate = True
        await self.worker.step()
        self.assertEqual(self.repo.row['status'], 'duplicate')
        self.assertEqual(self.sent, [])
        self.assertEqual(self.repo.analytics, [])

    async def test_updated_matching_rules_cancel_stale_queued_job(self):
        async def send(row): raise CancelDelivery('rules_updated')
        self.worker.send = send
        await self.worker.step()
        self.assertEqual(self.repo.row['status'], 'cancelled')
        self.assertEqual(self.repo.analytics, [])
        self.assertEqual(self.repo.seen, [])

    async def test_unknown_outcome_is_visible_and_never_blindly_retried(self):
        async def send(row): raise RuntimeError('unknown outcome')
        self.worker.send = send
        await self.worker.step()
        self.assertEqual(self.repo.row['status'], 'uncertain')
        self.assertFalse(await self.worker.step())
        self.assertEqual(self.repo.analytics, [])

    async def test_database_failure_after_send_does_not_resend(self):
        self.repo.complete_error = True
        await self.worker.step()
        self.assertEqual(len(self.sent), 1)
        self.assertEqual(self.repo.row['status'], 'uncertain')
        self.assertFalse(await self.worker.step())
        self.assertEqual(self.repo.analytics, [])

    async def test_shutdown_during_send_leaves_reviewable_state(self):
        async def send(row): raise asyncio.CancelledError()
        self.worker.send = send
        with self.assertRaises(asyncio.CancelledError):
            await self.worker.step()
        self.assertEqual(self.repo.row['status'], 'uncertain')

    async def test_worker_recovers_inflight_jobs_after_database_reconnect(self):
        self.repo.recover = AsyncMock()
        self.worker.step = AsyncMock(side_effect=[OSError('database down'), asyncio.CancelledError()])
        with patch('monitor_delivery.asyncio.sleep', AsyncMock()):
            with self.assertRaises(asyncio.CancelledError):
                await self.worker.run()
        self.repo.recover.assert_awaited_once()
