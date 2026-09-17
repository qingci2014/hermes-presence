"""Connection-local temporal primitives. Call only inside PresenceStore transactions."""
from dataclasses import asdict
import json
import logging

from .protocol import TemporalError, TemporalPolicy, TemporalStale

log = logging.getLogger("hermes.temporal")
SCOPE = "profile_id = ? AND conversation_id = ?"
TABLES = ("temporal_conversations", "temporal_session_bindings", "temporal_items",
          "temporal_notifications", "temporal_events", "temporal_records", "temporal_topics")


def dumps(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def conversation(conn, scope):
    row = conn.execute(f"SELECT * FROM temporal_conversations WHERE {SCOPE}", scope.sql).fetchone()
    if row is None:
        raise TemporalStale("conversation missing or deleted")
    return dict(row)


def policy_of(conv):
    return TemporalPolicy.from_dict(json.loads(conv["policy_json"]))


def item_row(row):
    if row is None:
        raise TemporalError("commitment not found")
    result = dict(row)
    result["body"] = json.loads(result.pop("body_json"))
    return result


def item(conn, scope, key):
    return item_row(conn.execute(f"SELECT * FROM temporal_items WHERE {SCOPE} AND item_key = ?",
                                 (*scope.sql, key)).fetchone())


def save_item(conn, obj):
    conn.execute("UPDATE temporal_items SET status=?, review_at=?, lease_until=?, lease_token=?, "
                 "revision=?, pending_notice_id=?, body_json=? "
                 f"WHERE {SCOPE} AND item_key=?",
                 (obj["status"], obj["review_at"], obj["lease_until"], obj["lease_token"],
                  obj["revision"], obj["pending_notice_id"], dumps(obj["body"]),
                  obj["profile_id"], obj["conversation_id"], obj["item_key"]))


def changed(obj, reason):
    obj["revision"] += 1
    obj["lease_token"] = obj["lease_until"] = None
    obj["body"]["reason"] = reason


def route_valid(conv):
    route = json.loads(conv["route_json"] or "null")
    return bool(route and route.get("authorized") is True and route.get("owner_id")
                and route.get("platform") and route.get("account_id") and route.get("chat_id")
                and conv["session_key"])


def enabled(conv):
    return conv["status"] == "active" and bool(conv["enabled"]) and policy_of(conv).enabled and route_valid(conv)


def budget_available(conv, obj):
    b = obj["body"]
    if b["review_attempts"] >= b["max_review_attempts"]:
        return False
    if policy_of(conv).dry_run:
        return b['dry_run_notification_attempts'] < b['max_notification_attempts']
    return b['notification_attempts'] + b['notification_reservations'] < b['max_notification_attempts']


def notice_window(conn, scope, conv, now):
    """Count attempted sends in a rolling window; keep lifetime audit counters intact."""
    p = policy_of(conv)
    if p.dry_run:
        predicate, timestamp = "state='dry_run'", 'created_at'
    else:
        predicate, timestamp = 'send_started_at IS NOT NULL', 'send_started_at'
    row = conn.execute(f'SELECT count(*),min({timestamp}) FROM temporal_notifications WHERE {SCOPE} '
        f'AND {predicate} AND {timestamp}>?', (*scope.sql, now-p.notification_window_seconds)).fetchone()
    conv['_window_attempts'] = row[0]
    conv['_window_next'] = (row[1] + p.notification_window_seconds) if row[1] is not None else now
    return conv


def notice_budget_available(conv, obj):
    b = obj["body"]
    if policy_of(conv).dry_run:
        return (b["dry_run_notification_attempts"] < b["max_notification_attempts"]
                and conv.get('_window_attempts', 0) < conv["max_notice_attempts"])
    return (b["notification_attempts"] + b["notification_reservations"] < b["max_notification_attempts"]
            and conv.get('_window_attempts', 0) + conv["notice_reservations"] < conv["max_notice_attempts"])


def normalize(conv, obj, now, delay=None, reason="waiting"):
    if obj["status"] != "active" or obj["pending_notice_id"]:
        return
    if not enabled(conv) or not budget_available(conv, obj):
        obj["status"], obj["review_at"] = "paused", None
        obj["body"]["reason"] = "permission_or_budget_exhausted"
    elif conv.get('_window_attempts', 0) >= conv['max_notice_attempts']:
        obj['review_at'] = max(now + policy_of(conv).min_review_seconds, conv['_window_next'])
        obj['body']['reason'] = 'conversation_frequency_limit'
    elif delay is not None:
        obj["review_at"] = now + policy_of(conv).delay(delay)
        obj["body"]["reason"] = reason
    elif obj["review_at"] is None:
        obj["status"] = "paused"
        obj["body"]["reason"] = "no_verified_schedule"


def cancel_ready(conn, scope, now, reason, delay=None):
    notice = conn.execute(f"SELECT * FROM temporal_notifications WHERE {SCOPE} AND state='ready'",
                          scope.sql).fetchone()
    if notice is None:
        return False
    if notice["reservation_state"] != "reserved":
        raise TemporalError("invalid notification reservation")
    conn.execute("UPDATE temporal_notifications SET state='cancelled', reservation_state='released', "
                 "completed_at=?, error_text=? WHERE notice_id=?", (now, reason, notice["notice_id"]))
    conn.execute(f"UPDATE temporal_conversations SET notice_reservations=notice_reservations-1 WHERE {SCOPE}", scope.sql)
    conv = conversation(conn, scope)
    for key in json.loads(notice["item_keys_json"]):
        obj = item(conn, scope, key)
        obj["body"]["notification_reservations"] -= 1
        if obj["body"]["notification_reservations"] < 0:
            raise TemporalError("reservation underflow")
        if obj["pending_notice_id"] == notice["notice_id"]:
            obj["pending_notice_id"] = None
            changed(obj, reason)
            normalize(conv, obj, now, delay, reason)
        save_item(conn, obj)
    return True


def revoke_scope(conn, scope, now, reason, *, suspend=False, disable=False, pause=True):
    updates = "work_token=NULL, updated_at=?"
    if suspend:
        updates += ", status='suspended', session_key=NULL, route_json=NULL, route_version=route_version+1"
    if disable:
        updates += ", enabled=0, policy_version=policy_version+1"
    conn.execute(f"UPDATE temporal_conversations SET {updates} WHERE {SCOPE}", (now, *scope.sql))
    delay = None if pause else policy_of(conversation(conn, scope)).user_activity_backoff_seconds
    cancel_ready(conn, scope, now, reason, delay)
    for row in conn.execute(f"SELECT * FROM temporal_items WHERE {SCOPE} AND status IN ('draft','active')", scope.sql).fetchall():
        obj = item_row(row)
        if obj["status"] == "draft":
            obj["status"] = "cancelled"
            obj["review_at"] = None
        elif pause:
            obj["status"], obj["review_at"] = "paused", None
        elif obj["lease_token"]:
            obj["review_at"] = now + delay
        else:
            continue
        changed(obj, reason)
        save_item(conn, obj)


def append_event(conn, scope, *, event_id, source, source_event_id, kind, now, body, occurred_at=None):
    old = conn.execute("SELECT * FROM temporal_events WHERE profile_id=? AND source=? AND source_event_id=?",
                       (scope.profile_id, source, source_event_id)).fetchone()
    if old:
        if old["conversation_id"] != scope.conversation_id or old["kind"] != kind:
            raise TemporalError("event identity conflict")
        return dict(old), False
    conv = conversation(conn, scope)
    seq = conv["event_seq"] + 1
    conn.execute(f"UPDATE temporal_conversations SET event_seq=?, updated_at=? WHERE {SCOPE}", (seq, now, *scope.sql))
    conn.execute("INSERT INTO temporal_events VALUES (?,?,?,?,?,?,?,?,?,?)",
                 (event_id, *scope.sql, seq, kind, source, source_event_id, now, occurred_at, dumps(body)))
    return dict(conn.execute("SELECT * FROM temporal_events WHERE event_id=?", (event_id,)).fetchone()), True


def check_fence(conv, fence):
    if not enabled(conv) or not fence.work_token or conv["work_token"] != fence.work_token:
        raise TemporalStale("work permission revoked")
    for key in ("activity_version", "route_version", "policy_version"):
        if conv[key] != getattr(fence, key):
            raise TemporalStale(f"{key} changed")


def policy_json(policy):
    return dumps(asdict(policy))
