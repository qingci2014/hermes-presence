"""Local, content-free accounting. Reservations survive failures and restarts."""
import math
import time
import uuid

from .common import check_fence, conversation, policy_of

USAGE_SCHEMA = '''
CREATE TABLE IF NOT EXISTS presence_review_usage (
    request_id TEXT PRIMARY KEY,
    profile_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    started_at REAL NOT NULL,
    finished_at REAL,
    state TEXT NOT NULL,
    model TEXT,
    input_tokens INTEGER,
    output_tokens INTEGER,
    total_tokens INTEGER,
    cache_read_tokens INTEGER,
    cache_write_tokens INTEGER,
    cost_usd REAL,
    FOREIGN KEY (profile_id, conversation_id)
      REFERENCES temporal_conversations(profile_id, conversation_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_presence_review_usage
ON presence_review_usage(profile_id, started_at);
'''


def review_budget(conn, profile_id, limit, now):
    row = conn.execute('SELECT count(*),min(started_at) FROM presence_review_usage '
                       'WHERE profile_id=? AND started_at>?', (profile_id, now-86400)).fetchone()
    return dict(attempts=row[0], limit=limit, remaining=max(0, limit-row[0]),
                next_available_at=(row[1]+86400 if row[1] is not None else now))


class UsageMixin:
    def reserve_review(self, scope, fence, *, now=None):
        now = time.time() if now is None else now
        request_id = uuid.uuid4().hex
        def write(conn):
            conv = conversation(conn, scope)
            check_fence(conv, fence)
            budget = review_budget(conn, scope.profile_id, policy_of(conv).max_daily_reviews, now)
            if not budget['remaining']:
                return None, budget['next_available_at']
            conn.execute('INSERT INTO presence_review_usage '
                '(request_id,profile_id,conversation_id,started_at,state) VALUES (?,?,?,?,?)',
                (request_id, *scope.sql, now, 'reserved'))
            return request_id, None
        return self._execute_write(write)

    def finish_review_usage(self, request_id, response=None, *, state='completed'):
        usage = getattr(response, 'usage', None)
        def get(key):
            return usage.get(key) if isinstance(usage, dict) else getattr(usage, key, None)
        values = {key: get(key) for key in ('input_tokens', 'output_tokens', 'total_tokens',
                                           'cache_read_tokens', 'cache_write_tokens')}
        values = {k: v if type(v) is int and v >= 0 else None for k, v in values.items()}
        # Hermes supplies an all-zero default when a provider omits usage.
        if not any(v for v in values.values()):
            values = dict.fromkeys(values)
        cost = get('cost_usd')
        if type(cost) not in (int, float) or not math.isfinite(cost) or cost < 0:
            cost = None
        model = getattr(response, 'model', None)
        self._execute_write(lambda conn: conn.execute(
            "UPDATE presence_review_usage SET finished_at=?,state=CASE WHEN state LIKE 'revoked%' THEN 'revoked_'||? ELSE ? END,model=?,input_tokens=?,output_tokens=?,"
            'total_tokens=?,cache_read_tokens=?,cache_write_tokens=?,cost_usd=? WHERE request_id=?',
            (time.time(), state, state, str(model)[:200] if model else None, *values.values(), cost, request_id)))

    def revoke_review_usage(self, request_id):
        self._execute_write(lambda conn: conn.execute("UPDATE presence_review_usage SET state='revoked_'||state "
            "WHERE request_id=? AND state NOT LIKE 'revoked%'", (request_id,)))

    def review_usage(self, limit, *, now=None):
        now = time.time() if now is None else now
        with self._read_ctx() as conn:
            budget = review_budget(conn, self.profile_id, limit, now)
            row = conn.execute('SELECT count(*) AS requests, sum(total_tokens IS NULL) AS unknown_usage_requests, '
                'sum(input_tokens) AS input_tokens, sum(output_tokens) AS output_tokens, '
                'sum(total_tokens) AS total_tokens, sum(cache_read_tokens) AS cache_read_tokens, '
                'sum(cost_usd) AS known_cost_usd FROM presence_review_usage WHERE profile_id=? AND started_at>?',
                (self.profile_id, now-86400)).fetchone()
            return dict(window_hours=24, **budget, **dict(row), scope='background_reviews_only')
