"""Durable notification queue. Analytics and dedup are committed after delivery."""
import asyncio
import json


class RetryDelivery(Exception):
    def __init__(self, reason, delay=5):
        super().__init__(reason)
        self.delay = max(1, delay)


class PermanentDelivery(Exception):
    pass


class Outbox:
    def __init__(self, pool, dedup_seconds=120):
        self.pool, self.dedup_seconds = pool, dedup_seconds

    async def initialize(self):
        async with self.pool.acquire() as conn:
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS notification_outbox (
                    id BIGSERIAL PRIMARY KEY,
                    owner_id BIGINT NOT NULL, chat_id BIGINT NOT NULL,
                    message_id BIGINT NOT NULL, account_id BIGINT NOT NULL,
                    query_id BIGINT NOT NULL, payload JSONB NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    attempts INTEGER NOT NULL DEFAULT 0,
                    due_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    last_error TEXT NOT NULL DEFAULT '', bot_message_id BIGINT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (owner_id, chat_id, message_id)
                );
                CREATE INDEX IF NOT EXISTS notification_outbox_pending
                    ON notification_outbox (due_at, id) WHERE status='pending';
            ''')

    async def recover(self):
        # A crashed send may already have reached Telegram. Keep it visible
        # for owner review rather than silently sending a second copy.
        async with self.pool.acquire() as conn:
            await conn.execute("""UPDATE notification_outbox SET status='uncertain',
                last_error='restart_during_send', updated_at=NOW() WHERE status='sending'""")

    async def enqueue(self, owner_id, chat_id, message_id, account_id, query_id, payload, delay=15):
        async with self.pool.acquire() as conn:
            return await conn.fetchval('''
                INSERT INTO notification_outbox
                    (owner_id, chat_id, message_id, account_id, query_id, payload, due_at)
                SELECT $1, $2, $3, $4, $5, $6::jsonb, NOW() + $7 * INTERVAL '1 second'
                WHERE NOT EXISTS (SELECT 1 FROM seen_messages
                    WHERE owner_id=$1 AND chat_id=$2 AND message_id=$3)
                ON CONFLICT (owner_id, chat_id, message_id) DO NOTHING RETURNING id
            ''', owner_id, chat_id, message_id, account_id, query_id,
                json.dumps(payload, ensure_ascii=False), float(delay))

    async def claim(self):
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('''
                UPDATE notification_outbox SET status='sending', attempts=attempts+1, updated_at=NOW()
                WHERE id=(SELECT id FROM notification_outbox WHERE status='pending' AND due_at<=NOW()
                    ORDER BY due_at, id FOR UPDATE SKIP LOCKED LIMIT 1) RETURNING *
            ''')
        if row is None:
            return None
        row = dict(row)
        if isinstance(row['payload'], str):
            row['payload'] = json.loads(row['payload'])
        return row

    async def is_current(self, row):
        async with self.pool.acquire() as conn:
            return bool(await conn.fetchval('''
                SELECT EXISTS (SELECT 1 FROM queries WHERE owner_id=$1 AND id=$2)
                    AND EXISTS (SELECT 1 FROM selected_chats WHERE owner_id=$1 AND chat_id=$3 AND account_id=$4)
                    AND EXISTS (SELECT 1 FROM telegram_accounts WHERE id=$4 AND owner_id=$1 AND active=TRUE)
            ''', row['owner_id'], row['query_id'], row['chat_id'], row['account_id']))

    async def is_duplicate(self, row):
        payload = row['payload']
        if not payload.get('seller_key') or not payload.get('request_key'):
            return False
        async with self.pool.acquire() as conn:
            return bool(await conn.fetchval('''SELECT EXISTS (SELECT 1 FROM recent_request_dedup
                WHERE owner_id=$1 AND seller_key=$2 AND request_key=$3
                    AND last_seen>=NOW()-$4*INTERVAL '1 second')''',
                row['owner_id'], payload['seller_key'], payload['request_key'], float(self.dedup_seconds)))

    async def state(self, row, status, error='', delay=0):
        async with self.pool.acquire() as conn:
            await conn.execute('''UPDATE notification_outbox SET status=$2, last_error=$3,
                due_at=NOW()+$4*INTERVAL '1 second', updated_at=NOW() WHERE id=$1''',
                row['id'], status, error[:300], float(delay))

    async def complete(self, row, bot_message_id):
        payload = row['payload']
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute('''UPDATE notification_outbox SET status='sent', bot_message_id=$2,
                    last_error='', updated_at=NOW() WHERE id=$1''', row['id'], bot_message_id)
                await conn.execute('''INSERT INTO seen_messages(owner_id,chat_id,message_id)
                    VALUES($1,$2,$3) ON CONFLICT DO NOTHING''',
                    row['owner_id'], row['chat_id'], row['message_id'])
                await conn.execute('''INSERT INTO analytics_events
                    (owner_id,chat_id,message_id,chat_title,monitor_query,found_request,brand)
                    VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING''',
                    row['owner_id'], row['chat_id'], row['message_id'], payload['chat_title'],
                    payload['monitor_query'], payload['found_request'], payload.get('brand'))
                if payload.get('seller_key') and payload.get('request_key'):
                    await conn.execute('''INSERT INTO recent_request_dedup
                        (owner_id,seller_key,request_key,last_seen) VALUES($1,$2,$3,NOW())
                        ON CONFLICT(owner_id,seller_key,request_key) DO UPDATE SET last_seen=EXCLUDED.last_seen''',
                        row['owner_id'], payload['seller_key'], payload['request_key'])

    async def counts(self, owner_id):
        async with self.pool.acquire() as conn:
            return {row['status']: row['n'] for row in await conn.fetch('''
                SELECT status,COUNT(*) AS n FROM notification_outbox WHERE owner_id=$1 GROUP BY status
            ''', owner_id)}

    async def retry_failed(self, owner_id, include_uncertain=False):
        statuses = ['failed', 'uncertain'] if include_uncertain else ['failed']
        async with self.pool.acquire() as conn:
            result = await conn.execute('''UPDATE notification_outbox SET status='pending', due_at=NOW(),
                last_error='', updated_at=NOW() WHERE owner_id=$1 AND status=ANY($2::text[])''', owner_id, statuses)
        return int(result.split()[-1])


class DeliveryWorker:
    def __init__(self, outbox, send):
        self.outbox, self.send = outbox, send

    async def step(self):
        row = await self.outbox.claim()
        if row is None:
            return False
        started = False
        try:
            if await self.outbox.is_duplicate(row):
                await self.outbox.state(row, 'duplicate')
                return True
            if not await self.outbox.is_current(row):
                await self.outbox.state(row, 'cancelled', 'query_chat_or_account_removed')
                return True
            started = True
            try:
                message_id = await self.send(row)
            except RetryDelivery as exc:
                await self.outbox.state(row, 'pending', str(exc),
                    max(exc.delay, min(300, 2 ** min(row['attempts'], 8))))
                return True
            except PermanentDelivery as exc:
                await self.outbox.state(row, 'failed', str(exc))
                return True
            await self.outbox.complete(row, message_id)
        except asyncio.CancelledError:
            await asyncio.shield(self.outbox.state(row, 'uncertain' if started else 'pending', 'worker_cancelled'))
            raise
        except Exception as exc:
            # Unknown send outcome or post-send DB failure requires review.
            await self.outbox.state(row, 'uncertain' if started else 'pending', type(exc).__name__, 10)
        return True

    async def run(self):
        while True:
            try:
                await self.step()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print('DELIVERY WORKER ERROR:', type(exc).__name__)
            # Private Bot API notifications are deliberately paced.
            await asyncio.sleep(1.1)
