"""Transactional admission, claims and all-or-nothing review commits."""
import time
import uuid

from .protocol import (
    TemporalClaim, TemporalError, TemporalReviewFence, TemporalScope, TemporalStale,
    validate_decisions,
)
from .common import (
    SCOPE, budget_available, changed, check_fence, conversation, dumps, enabled,
    item, item_row, normalize, notice_budget_available, notice_window, policy_of, save_item,
)
from .usage import review_budget


def backoff(conv, obj, now, delay, kind):
    """New activity resets the streak; a known future event remains a boundary."""
    p, body = policy_of(conv), obj['body']
    signature = [conv['activity_version'], body.get('observation_event_id'), body.get('observed_at')]
    previous = body.get('review_backoff', {})
    count = previous.get('count', 0) + 1 if previous.get('signature') == signature and previous.get('kind') == kind else 1
    body['review_backoff'] = dict(signature=signature, kind=kind, count=min(count, 16))
    floor = p.failure_backoff_seconds if kind == 'failure' else p.min_review_seconds
    extra = min(p.no_progress_backoff_max_seconds, floor * 2**min(count-1, 16))
    boundary = body.get('expected_by')
    if boundary is not None and boundary > now:
        extra = min(extra, max(p.min_review_seconds, int(boundary-now)))
    return max(delay, extra)


class TemporalReviewsMixin:
    def temporal_contact_budget(self, scope, *, now=None):
        now = time.time() if now is None else now
        with self._read_ctx() as conn:
            conv = notice_window(conn, scope, conversation(conn, scope), now)
            reserved = 0 if policy_of(conv).dry_run else conv['notice_reservations']
            return {'remaining': max(0, conv['max_notice_attempts'] - conv['_window_attempts'] - reserved),
                    'attempts': conv['_window_attempts'], 'window_seconds': policy_of(conv).notification_window_seconds}

    def temporal_defer_busy(self, scope, *, now=None):
        now = time.time() if now is None else now
        def write(conn):
            conv = conversation(conn, scope)
            delay = policy_of(conv).user_activity_backoff_seconds
            return conn.execute(f"UPDATE temporal_items SET review_at=?, revision=revision+1 "
                f"WHERE {SCOPE} AND status='active' AND review_at<=? AND lease_token IS NULL AND pending_notice_id IS NULL",
                (now + delay, *scope.sql, now)).rowcount
        return self._execute_write(write)

    def temporal_admit(self, scope, work_token):
        if not work_token:
            raise TemporalError("work token required")
        def write(conn):
            conv = conversation(conn, scope)
            if not enabled(conv) or conv["work_token"]:
                return None
            conn.execute(f"UPDATE temporal_conversations SET work_token=? WHERE {SCOPE}", (work_token, *scope.sql))
            return TemporalReviewFence(conv["activity_version"], conv["route_version"], conv["policy_version"], work_token)
        return self._execute_write(write)

    def temporal_revoke_work(self, scope, work_token):
        return self._execute_write(lambda conn: conn.execute(
            f"UPDATE temporal_conversations SET work_token=NULL WHERE {SCOPE} AND work_token=?",
            (*scope.sql, work_token)).rowcount)

    def temporal_list_due_scopes(self, profile_id, *, now=None, limit=100):
        now = time.time() if now is None else now
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise TemporalError("invalid scan limit")
        rows = self._read_all("SELECT conversation_id,min(review_at) AS due FROM temporal_items "
            "WHERE profile_id=? AND status='active' AND review_at IS NOT NULL AND review_at<=? "
            "AND pending_notice_id IS NULL GROUP BY conversation_id ORDER BY due,conversation_id LIMIT ?",
            (profile_id, now, limit))
        return [TemporalScope(profile_id, row[0]) for row in rows]

    def temporal_claim_due(self, scope, fence, *, now=None):
        now = time.time() if now is None else now
        # Tokens are generated outside the retryable transaction.
        tokens = [uuid.uuid4().hex for _ in range(10)]
        def write(conn):
            conv = notice_window(conn, scope, conversation(conn, scope), now)
            check_fence(conv, fence)
            p = policy_of(conv)
            review_limit = review_budget(conn, scope.profile_id, p.max_daily_reviews, now)
            if conn.execute(f"SELECT 1 FROM temporal_notifications WHERE {SCOPE} AND state IN ('ready','sending')", scope.sql).fetchone():
                return [], []
            quiet = p.quiet_until(now)
            rows = conn.execute(f"SELECT * FROM temporal_items WHERE {SCOPE} AND status='active' "
                "AND review_at IS NOT NULL AND review_at<=? AND pending_notice_id IS NULL "
                "AND (lease_until IS NULL OR lease_until<=?) ORDER BY review_at,item_key LIMIT ?",
                (*scope.sql, now, now, p.batch_size)).fetchall()
            claims, snapshots = [], []
            for idx, row in enumerate(rows):
                obj = item_row(row)
                if obj['body'].get('care_expires_at', now+1) <= now:
                    changed(obj, 'care_window_expired')
                    obj['status'], obj['review_at'] = 'cancelled', None
                    obj['body'].setdefault('care_retired_at', now)
                elif not budget_available(conv, obj):
                    changed(obj, "budget_exhausted")
                    obj["status"], obj["review_at"] = "paused", None
                elif conv['_window_attempts'] >= conv['max_notice_attempts']:
                    changed(obj, 'conversation_frequency_limit')
                    obj['review_at'] = max(now + p.min_review_seconds, conv['_window_next'])
                elif quiet:
                    changed(obj, "quiet_hours")
                    obj["review_at"] = quiet
                elif not review_limit['remaining']:
                    changed(obj, 'daily_review_budget')
                    obj['review_at'] = max(now+p.min_review_seconds, review_limit['next_available_at'])
                else:
                    obj["revision"] += 1
                    obj["lease_token"], obj["lease_until"] = tokens[idx], now + p.lease_seconds
                    obj["body"]["review_attempts"] += 1
                    claims.append(TemporalClaim(scope, obj["item_key"], tokens[idx], obj["revision"], obj["lease_until"]))
                    snapshots.append(obj)
                save_item(conn, obj)
            return claims, snapshots
        return self._execute_write(write)

    def temporal_commit_batch(self, scope, fence, claims, output, *, now=None, commit_guard=None):
        now, notice_id = time.time() if now is None else now, uuid.uuid4().hex
        p = policy_of(self.temporal_conversation(scope))
        output = validate_decisions(output, claims, p)
        if not claims or any(c.scope != scope for c in claims):
            raise TemporalError("invalid claim scope")
        def write(conn):
            conv = notice_window(conn, scope, conversation(conn, scope), now)
            check_fence(conv, fence)
            p = policy_of(conv)
            objects = {}
            for claim in claims:
                obj = item(conn, scope, claim.key)
                if (obj["status"] != "active" or obj["pending_notice_id"] or
                        (obj["lease_token"], obj["revision"]) != (claim.token, claim.revision)
                        or obj["lease_until"] is None or obj["lease_until"] <= now):
                    raise TemporalStale("claim expired or changed; whole batch rejected")
                objects[claim.key] = obj
            notice = output["notification"]
            notify_keys = notice["item_keys"] if notice else []
            slot_free = not conn.execute(f"SELECT 1 FROM temporal_notifications WHERE {SCOPE} "
                                         "AND state IN ('ready','sending')", scope.sql).fetchone()
            can_notify = (slot_free and not p.quiet_until(now)
                          and all(notice_budget_available(conv, objects[k]) for k in notify_keys))
            after = {}
            for decision in output["decisions"]:
                obj, action = objects[decision["key"]], decision["action"]
                changed(obj, decision["reason"])
                if action == "notify" and can_notify:
                    obj['body'].pop('review_backoff', None)
                    after[obj["item_key"]] = decision["after_seconds"]
                    if p.dry_run:
                        obj["body"]["dry_run_notification_attempts"] += 1
                    else:
                        obj["body"]["notification_reservations"] += 1
                        obj["pending_notice_id"], obj["review_at"] = notice_id, None
                elif action in ("wait", "notify", "pause"):
                    # Durable pause is owned by explicit user controls and hard
                    # budgets. A model's topic/uncertainty pause is only a delay.
                    delay = max(decision["after_seconds"], p.user_activity_backoff_seconds) if action == "pause" else decision["after_seconds"]
                    delay = backoff(conv, obj, now, delay, 'wait')
                    normalize(conv, obj, now, delay)
                else:
                    obj['body'].pop('review_backoff', None)
                    obj["status"] = {"pause": "paused", "resolve": "resolved", "cancel": "cancelled"}[action]
                    obj["review_at"] = None
                if obj['body'].get('purpose') == 'care' and obj['status'] != 'active':
                    obj['body'].setdefault('care_retired_at', now)
                save_item(conn, obj)
            if notify_keys and can_notify:
                counter = "dry_run_notice_attempts" if p.dry_run else "notice_reservations"
                conn.execute(f"UPDATE temporal_conversations SET {counter}={counter}+1 WHERE {SCOPE}", scope.sql)
                conn.execute("INSERT INTO temporal_notifications (notice_id,profile_id,conversation_id,state,mode,"
                    "item_keys_json,item_revisions_json,next_review_json,activity_version,route_version,policy_version,"
                    "route_snapshot_json,reservation_state,message,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (notice_id, *scope.sql, "dry_run" if p.dry_run else "ready", p.mode, dumps(notify_keys),
                     dumps({k: objects[k]["revision"] for k in notify_keys}), dumps(after), conv["activity_version"],
                     conv["route_version"], conv["policy_version"], conv["route_json"],
                     "none" if p.dry_run else "reserved", notice["message"], now))
                if p.dry_run:
                    current = notice_window(conn, scope, conversation(conn, scope), now)
                    for key in notify_keys:
                        normalize(current, objects[key], now, after[key])
                        save_item(conn, objects[key])
            conn.execute(f"UPDATE temporal_conversations SET work_token=NULL WHERE {SCOPE} AND work_token=?", (*scope.sql, fence.work_token))
            return notice_id if notify_keys and can_notify else None
        return self._execute_write(write, commit_guard=commit_guard)

    def temporal_release_claims(self, scope, claims, *, now=None, user_activity=False):
        now = time.time() if now is None else now
        def write(conn):
            conv = conversation(conn, scope)
            p = policy_of(conv)
            for claim in claims:
                if claim.scope != scope:
                    raise TemporalError("claim scope mismatch")
                obj = item(conn, scope, claim.key)
                if (obj["lease_token"], obj["revision"]) != (claim.token, claim.revision):
                    continue
                changed(obj, "review_stale" if user_activity else "review_failed")
                delay = p.user_activity_backoff_seconds if user_activity else backoff(conv, obj, now, p.failure_backoff_seconds, 'failure')
                normalize(conv, obj, now, delay)
                save_item(conn, obj)
        return self._execute_write(write)

    def temporal_defer_unreviewed(self, scope, claims, *, now, until, reason):
        """Unsubmitted items spend neither model nor per-item evaluation budget."""
        def write(conn):
            conv = conversation(conn, scope)
            for claim in claims:
                obj = item(conn, scope, claim.key)
                if (obj['lease_token'], obj['revision']) != (claim.token, claim.revision):
                    continue
                obj['body']['review_attempts'] = max(0, obj['body']['review_attempts']-1)
                changed(obj, reason)
                normalize(conv, obj, now, max(policy_of(conv).min_review_seconds, int(until-now)))
                save_item(conn, obj)
        return self._execute_write(write)
