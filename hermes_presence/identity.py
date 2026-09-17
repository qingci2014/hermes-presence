"""Logical conversation identity and durable input acceptance."""
import json
import time
import uuid

from .protocol import TemporalError, TemporalPolicy, TemporalScope, text_field
from .common import (
    SCOPE, append_event, changed, conversation, dumps, item_row, policy_json, policy_of, revoke_scope, save_item,
)


class TemporalIdentityMixin:
    def temporal_begin_new(self, scope, *, now=None):
        """Fence delivery while the host rotates the transcript; keep task state."""
        now = time.time() if now is None else now
        def write(conn):
            conv = conversation(conn, scope)
            if conv["status"] != "active":
                raise TemporalError("only an active conversation can continue")
            revoke_scope(conn, scope, now, "new_transcript", pause=False)
            conn.execute(f"UPDATE temporal_conversations SET status='suspended',route_version=route_version+1 "
                         f"WHERE {SCOPE}", scope.sql)
            return conv["current_session_id"]
        return self._execute_write(write)

    def temporal_finish_new(self, scope, old_session_id, new_session_id, route, *, now=None):
        now = time.time() if now is None else now
        def write(conn):
            conv = conversation(conn, scope)
            if (conv["status"] != "suspended" or conv["current_session_id"] != old_session_id
                    or conv["route_json"] != dumps(route) or not conv["session_key"]):
                raise TemporalError("new transcript transition superseded")
            if not conn.execute("SELECT 1 FROM presence_host_sessions WHERE id=?", (new_session_id,)).fetchone():
                raise TemporalError("new host session missing")
            conn.execute("INSERT INTO temporal_session_bindings VALUES (?,?,?,?,?)",
                         (scope.profile_id, new_session_id, scope.conversation_id, "resume", now))
            conn.execute(f"UPDATE temporal_conversations SET current_session_id=?,status='active',"
                         f"route_version=route_version+1,updated_at=? WHERE {SCOPE}",
                         (new_session_id, now, *scope.sql))
        return self._execute_write(write)

    def temporal_bind_origin(self, profile_id, session_id, session_key, route, policy, *, now=None, resume=False):
        if profile_id != self.profile_id:
            raise TemporalError("profile does not belong to this Presence store")
        now = time.time() if now is None else now
        cid = uuid.uuid4().hex
        text_field(profile_id, "profile", 256)
        allowed = {"platform", "account_id", "chat_id", "thread_id", "owner_id", "authorized", "live_capable",
                   "user_id", "user_id_alt", "chat_type"}
        if not isinstance(route, dict) or set(route) - allowed:
            raise TemporalError("invalid host route fields")
        if not policy.dry_run and route.get("live_capable") is not True:
            raise TemporalError("platform has no verified temporal delivery capability")
        def write(conn):
            if not conn.execute("SELECT 1 FROM presence_host_sessions WHERE id=?", (session_id,)).fetchone():
                raise TemporalError("host session does not exist")
            binding = conn.execute("SELECT * FROM temporal_session_bindings WHERE profile_id=? AND session_id=?",
                                   (profile_id, session_id)).fetchone()
            if binding:
                scope = TemporalScope(profile_id, binding["conversation_id"])
                conv = conversation(conn, scope)
                if conv["current_session_id"] != session_id:
                    raise TemporalError("cannot rebind a historical session; resolve canonical tip")
                if conv["status"] != "active" and not resume:
                    raise TemporalError("suspended conversation requires explicit resume")
                if conv["session_key"] == session_key and conv["route_json"] == dumps(route):
                    return scope
            else:
                scope = TemporalScope(profile_id, cid)
            owner = conn.execute("SELECT conversation_id FROM temporal_conversations WHERE profile_id=? "
                                 "AND session_key=? AND status='active'", (profile_id, session_key)).fetchone()
            if owner and owner[0] != scope.conversation_id:
                revoke_scope(conn, TemporalScope(profile_id, owner[0]), now, "route_reassigned", suspend=True)
            if binding:
                revoke_scope(conn, scope, now, "route_changed")
                conn.execute(f"UPDATE temporal_conversations SET status='active', session_key=?, route_json=?, "
                             f"route_version=route_version+1 WHERE {SCOPE}", (session_key, dumps(route), *scope.sql))
            else:
                conn.execute("INSERT INTO temporal_conversations (profile_id,conversation_id,current_session_id,"
                             "session_key,max_notice_attempts,max_items,route_json,policy_json,created_at,updated_at) "
                             "VALUES (?,?,?,?,?,?,?,?,?,?)", (*scope.sql, session_id, session_key,
                             policy.default_max_conversation_notification_attempts, policy.max_items_per_conversation,
                             dumps(route), policy_json(policy), now, now))
                conn.execute("INSERT INTO temporal_session_bindings VALUES (?,?,?,?,?)",
                             (profile_id, session_id, cid, "origin", now))
            return scope
        return self._execute_write(write)

    @staticmethod
    def _temporal_bind_compression(conn, parent_session_id, child_session_id, now):
        for binding in conn.execute("SELECT * FROM temporal_session_bindings WHERE session_id=?", (parent_session_id,)).fetchall():
            scope = TemporalScope(binding["profile_id"], binding["conversation_id"])
            conv = conversation(conn, scope)
            if conv["current_session_id"] != parent_session_id:
                raise TemporalError("temporal compression parent is not current")
            conn.execute("INSERT INTO temporal_session_bindings VALUES (?,?,?,?,?)",
                         (scope.profile_id, child_session_id, scope.conversation_id, "compression", now))
            conn.execute(f"UPDATE temporal_conversations SET current_session_id=?,updated_at=? WHERE {SCOPE}",
                         (child_session_id, now, *scope.sql))

    def temporal_scope_for_session(self, profile_id, session_id):
        row = self._read_one("SELECT conversation_id FROM temporal_session_bindings WHERE profile_id=? AND session_id=?",
                             (profile_id, session_id))
        return TemporalScope(profile_id, row[0]) if row else None

    def temporal_conversation(self, scope):
        with self._read_ctx() as conn:
            return conversation(conn, scope)

    def temporal_suspend(self, scope, *, now=None, expected_route_version=None):
        now = time.time() if now is None else now
        def write(conn):
            if expected_route_version is not None and conversation(conn, scope)["route_version"] != expected_route_version:
                return
            revoke_scope(conn, scope, now, "conversation_suspended", suspend=True)
        return self._execute_write(write)

    def temporal_accept_user_event(self, scope, source, source_event_id, text, *, now=None):
        now, event_id = time.time() if now is None else now, uuid.uuid4().hex
        text_field(source, "source", 512)
        text_field(source_event_id, "source event ID", 512)
        def write(conn):
            conv = conversation(conn, scope)
            policy = TemporalPolicy.from_dict(json.loads(conv["policy_json"]))
            event, fresh = append_event(conn, scope, event_id=event_id, source=source,
                source_event_id=source_event_id, kind="user_accepted", now=now,
                body={"text": str(text)[:policy.event_text_max_chars]})
            if fresh:
                conn.execute(f"UPDATE temporal_conversations SET activity_version=activity_version+1 WHERE {SCOPE}", scope.sql)
                revoke_scope(conn, scope, now, "user_input_accepted", pause=False)
            return event
        return self._execute_write(write)

    def temporal_set_enabled(self, scope, value, *, now=None):
        now = time.time() if now is None else now
        audit_id = uuid.uuid4().hex
        if type(value) is not bool:
            raise TemporalError("enabled must be boolean")
        def write(conn):
            conv = conversation(conn, scope)
            if value and not TemporalPolicy.from_dict(json.loads(conv["policy_json"])).enabled:
                raise TemporalError("global temporal feature is disabled")
            if bool(conv["enabled"]) == value:
                return
            revoke_scope(conn, scope, now, "conversation_switch_changed")
            conn.execute(f"UPDATE temporal_conversations SET enabled=?,policy_version=policy_version+1 WHERE {SCOPE}",
                         (int(value), *scope.sql))
            append_event(conn, scope, event_id=audit_id, source="temporal_control", source_event_id=audit_id,
                         kind="control", now=now, body={"action": "on" if value else "off"})
        return self._execute_write(write)

    def temporal_apply_policy(self, scope, policy, *, now=None):
        now = time.time() if now is None else now
        audit_id = uuid.uuid4().hex
        def write(conn):
            conv = conversation(conn, scope)
            if policy_of(conv) == policy:
                return
            if (policy.enabled and not policy.dry_run and conv["status"] == "active"
                    and json.loads(conv["route_json"] or "{}").get("live_capable") is not True):
                raise TemporalError("live policy requires a verified delivery route")
            if policy.max_items_per_conversation < conn.execute(
                    f"SELECT count(*) FROM temporal_items WHERE {SCOPE} AND status IN ('draft','active','paused')", scope.sql).fetchone()[0]:
                raise TemporalError("new item cap is below existing items")
            revoke_scope(conn, scope, now, "policy_changed")
            for row in conn.execute(f"SELECT * FROM temporal_items WHERE {SCOPE}", scope.sql).fetchall():
                obj = item_row(row)
                for cap, consumed, limit in (
                        ("max_review_attempts", "review_attempts", policy.default_max_review_attempts),
                        ("max_notification_attempts", "notification_attempts", policy.default_max_notification_attempts)):
                    if cap == "max_notification_attempts" and obj["body"].get("purpose") == "care":
                        limit = 1
                    if obj["body"][consumed] > limit:
                        raise TemporalError("new item budget is below consumed attempts")
                    obj["body"][cap] = limit
                changed(obj, "policy_changed")
                save_item(conn, obj)
            conn.execute(f"UPDATE temporal_conversations SET policy_json=?,policy_version=policy_version+1,"
                         f"max_notice_attempts=?,max_items=? WHERE {SCOPE}", (policy_json(policy),
                         policy.default_max_conversation_notification_attempts, policy.max_items_per_conversation, *scope.sql))
            append_event(conn, scope, event_id=audit_id, source="temporal_control", source_event_id=audit_id,
                         kind="control", now=now, body={"action": "apply_policy", "policy": json.loads(policy_json(policy))})
        return self._execute_write(write)
