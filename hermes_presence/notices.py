"""Reservation consumption and idempotent delivery accounting; never dispatches."""
import json
import time
import uuid

from .protocol import TemporalError
from .common import (
    SCOPE, append_event, cancel_ready, changed, conversation, dumps, enabled,
    item, normalize, notice_window, policy_of, save_item,
)


class TemporalNoticesMixin:
    def temporal_cancel_ready(self, scope, reason, *, now=None):
        now = time.time() if now is None else now
        return self._execute_write(lambda conn: cancel_ready(conn, scope, now, reason))

    def temporal_pending_notices(self, profile_id, *, limit=100):
        return [dict(row) for row in self._read_all("SELECT * FROM temporal_notifications WHERE profile_id=? "
                "AND state='ready' ORDER BY created_at LIMIT ?", (profile_id, limit))]

    def temporal_get_notice(self, scope, notice_id):
        row = self._read_one(f"SELECT * FROM temporal_notifications WHERE {SCOPE} AND notice_id=?", (*scope.sql, notice_id))
        return dict(row) if row else None

    def temporal_begin_send(self, scope, notice_id, *, owner_id, now=None, commit_guard=None):
        now, token = time.time() if now is None else now, uuid.uuid4().hex
        def write(conn):
            conv = notice_window(conn, scope, conversation(conn, scope), now)
            row = conn.execute(f"SELECT * FROM temporal_notifications WHERE {SCOPE} AND notice_id=?", (*scope.sql, notice_id)).fetchone()
            if row is None or row["state"] != "ready":
                return None
            n, p = dict(row), policy_of(conv)
            route = json.loads(conv["route_json"] or "{}")
            versions = ("activity_version", "route_version", "policy_version")
            valid = (enabled(conv) and not p.dry_run and n["mode"] == "live"
                     and n["reservation_state"] == "reserved" and conv["notice_reservations"] == 1
                     and all(conv[k] == n[k] for k in versions)
                     and conv["route_json"] == n["route_snapshot_json"]
                     and route.get("owner_id") == owner_id and route.get("live_capable") is True
                     and now - n["created_at"] < p.ready_expiry_seconds
                     and not p.quiet_until(now) and len(n["message"]) <= p.notification_max_chars)
            objects = [item(conn, scope, k) for k in json.loads(n["item_keys_json"])]
            revisions = json.loads(n["item_revisions_json"])
            valid = valid and all(o["status"] == "active" and o["pending_notice_id"] == notice_id
                                  and o["revision"] == revisions[o["item_key"]]
                                  and o["body"]["notification_reservations"] == 1
                                  and o['body'].get('care_expires_at', now+1) > now
                                  and o["body"]["notification_attempts"] + 1 <= o["body"]["max_notification_attempts"]
                                  for o in objects)
            valid = valid and conv['_window_attempts'] + 1 <= conv["max_notice_attempts"]
            if not valid:
                cancel_ready(conn, scope, now, "send_fence_rejected")
                return None
            conn.execute(f"UPDATE temporal_conversations SET notice_reservations=notice_reservations-1,"
                         f"notice_attempts=notice_attempts+1 WHERE {SCOPE}", scope.sql)
            for obj in objects:
                obj["body"]["notification_reservations"] -= 1
                obj["body"]["notification_attempts"] += 1
                changed(obj, "sending")
                revisions[obj["item_key"]] = obj["revision"]
                save_item(conn, obj)
            conn.execute("UPDATE temporal_notifications SET state='sending',reservation_state='consumed',"
                         "send_token=?,send_started_at=?,item_revisions_json=? WHERE notice_id=?",
                         (token, now, dumps(revisions), notice_id))
            n.update(state="sending", send_token=token, send_started_at=now)
            return n
        return self._execute_write(write, commit_guard=commit_guard)

    def temporal_finish_send(self, scope, notice_id, send_token, outcome, *, message_id=None, error=None, now=None):
        now, event_id = time.time() if now is None else now, uuid.uuid4().hex
        if outcome not in ("sent", "failed", "unknown") or (outcome == "sent" and not message_id):
            raise TemporalError("invalid delivery result")
        def write(conn):
            row = conn.execute(f"SELECT * FROM temporal_notifications WHERE {SCOPE} AND notice_id=?", (*scope.sql, notice_id)).fetchone()
            if row is None:  # A deleted scope must never be recreated by a late callback.
                return False
            n = dict(row)
            if n["send_token"] != send_token or n["state"] not in ("sending", "unknown"):
                return False
            if n["state"] == "unknown" and outcome == "unknown":
                return False
            late = n["state"] == "unknown"
            conn.execute("UPDATE temporal_notifications SET state=?,completed_at=?,transport_message_id=?,"
                         "error_text=?,late_receipt_json=? WHERE notice_id=?",
                         (outcome, now, message_id, str(error)[:500] if error else None,
                          dumps({"outcome": outcome, "at": now}) if late else None, notice_id))
            conv = conversation(conn, scope)
            p = policy_of(conv)
            revisions, after = json.loads(n["item_revisions_json"]), json.loads(n["next_review_json"])
            versions_match = all(conv[k] == n[k] for k in ("activity_version", "route_version", "policy_version"))
            route_policy_match = all(conv[k] == n[k] for k in ("route_version", "policy_version"))
            for key in json.loads(n["item_keys_json"]):
                obj = item(conn, scope, key)
                same = obj["revision"] == revisions[key]
                if outcome == "sent":
                    obj["body"]["delivered_notifications"] += 1
                if obj["pending_notice_id"] == notice_id:
                    obj["pending_notice_id"] = None
                if obj["status"] == "active":
                    if same and outcome in ("failed", "unknown"):
                        obj["status"], obj["review_at"] = "paused", None
                    elif same and outcome == "sent" and versions_match and not late:
                        obj["review_at"] = now + after[key]
                    elif same and route_policy_match and not late:
                        obj["review_at"] = now + p.user_activity_backoff_seconds
                    elif same:
                        obj["status"], obj["review_at"] = "paused", None
                    normalize(conv, obj, now)
                if obj['body'].get('purpose') == 'care' and obj['status'] != 'active':
                    obj['body'].setdefault('care_retired_at', now)
                # Always advance the revision for accounting, preserving newer evidence/status.
                obj["revision"] += 1
                save_item(conn, obj)
            if outcome == "sent":
                append_event(conn, scope, event_id=event_id, source="temporal_notice", source_event_id=notice_id,
                    kind="notice_delivery", now=now, body={"notice_id": notice_id, "message_id": message_id,
                    "text": n["message"], "delivery": "sent"})
            return True
        return self._execute_write(write)
