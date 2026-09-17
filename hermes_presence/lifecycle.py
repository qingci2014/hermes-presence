"""Recovery and user controls. Recovery never retries a transport operation."""
import json
import time
import uuid

from .protocol import TemporalError, TemporalScope
from .common import (
    SCOPE, append_event, cancel_ready, changed, conversation, dumps, enabled, item_row,
    normalize, policy_json, policy_of, revoke_scope, save_item,
)


class TemporalLifecycleMixin:
    def temporal_checkpoint(self, scope, *, now=None, expected_event_seq=None, commit_guard=None):
        """Host calls only after stopping ingress and draining its work."""
        now, audit_id = time.time() if now is None else now, uuid.uuid4().hex
        def write(conn):
            conv = conversation(conn, scope)
            if expected_event_seq is not None and conv["event_seq"] != expected_event_seq:
                return False
            if conv["status"] != "active" or conv["work_token"]:
                return False
            if conn.execute(f"SELECT 1 FROM temporal_items WHERE {SCOPE} AND lease_token IS NOT NULL", scope.sql).fetchone():
                return False
            if conn.execute(f"SELECT 1 FROM temporal_notifications WHERE {SCOPE} AND state='sending'", scope.sql).fetchone():
                return False
            append_event(conn, scope, event_id=audit_id, source="temporal_control", source_event_id=audit_id,
                kind="control", now=now, body={"action": "clean_shutdown",
                    **{k: conv[k] for k in ("activity_version", "route_version", "policy_version")}})
            return True
        return self._execute_write(write, commit_guard=commit_guard)

    def temporal_expire_drafts(self, profile_id, *, now=None, limit=100):
        """Bounded housekeeping; no model, route permission or evidence is invented."""
        now = time.time() if now is None else now
        def write(conn):
            rows = conn.execute("SELECT i.* FROM temporal_items i JOIN temporal_conversations c "
                "USING(profile_id,conversation_id) WHERE i.profile_id=? AND i.status='draft' "
                "AND json_extract(i.body_json,'$.created_at') + json_extract(c.policy_json,'$.draft_expiry_seconds')<=? "
                "ORDER BY json_extract(i.body_json,'$.created_at') LIMIT ?", (profile_id, now, min(100, limit))).fetchall()
            for row in rows:
                obj = item_row(row)
                obj["status"], obj["review_at"] = "cancelled", None
                changed(obj, "handoff_expired")
                save_item(conn, obj)
            return len(rows)
        return self._execute_write(write)

    def temporal_restore_checkpoint(self, scope, new_owner, policy, *, now=None):
        """Requires a host-verified current route, account ownership and canonical session."""
        now, audit_id = time.time() if now is None else now, uuid.uuid4().hex
        def write(conn):
            conv = conversation(conn, scope)
            last = conn.execute(f"SELECT kind,body_json FROM temporal_events WHERE {SCOPE} ORDER BY seq DESC LIMIT 1", scope.sql).fetchone()
            if not last or conv["status"] != "active" or not conv["route_json"] or policy_of(conv) != policy:
                return False
            body = json.loads(last["body_json"])
            if (last["kind"] != "control" or body.get("action") != "clean_shutdown"
                    or any(body.get(k) != conv[k] for k in ("activity_version", "route_version", "policy_version"))):
                return False
            route = json.loads(conv["route_json"])
            route["owner_id"] = new_owner
            conn.execute(f"UPDATE temporal_conversations SET route_json=?,route_version=route_version+1,work_token=NULL WHERE {SCOPE}",
                         (dumps(route), *scope.sql))
            cancel_ready(conn, scope, now, "clean_restart_new_owner", policy.failure_backoff_seconds)
            append_event(conn, scope, event_id=audit_id, source="temporal_control", source_event_id=audit_id,
                         kind="control", now=now, body={"action": "clean_restart"})
            return True
        return self._execute_write(write)

    def temporal_process_subscriptions(self, profile_id, *, after=("", "")):
        return self._read_all("SELECT conversation_id,item_key,body_json FROM temporal_items WHERE profile_id=? "
            "AND status IN ('active','paused') AND json_extract(body_json,'$.external_reference.adapter')='process_registry' "
            "AND json_extract(body_json,'$.external_reference.completed')=0 AND (conversation_id,item_key)>(?,?) "
            "ORDER BY conversation_id,item_key LIMIT 100", (profile_id, *after))

    def temporal_complete_process_subscription(self, scope, key, reference):
        from .common import item
        def write(conn):
            obj = item(conn, scope, key)
            if obj["body"].get("external_reference") != reference:
                return
            obj["body"]["external_reference"]["completed"] = True
            save_item(conn, obj)
        return self._execute_write(write)

    def temporal_scope_for_route(self, profile_id, session_key):
        row = self._read_one("SELECT conversation_id FROM temporal_conversations WHERE profile_id=? "
                             "AND session_key=? AND status='active'", (profile_id, session_key))
        return TemporalScope(profile_id, row[0]) if row else None

    def temporal_scopes(self, profile_id, *, after="", limit=100):
        return [TemporalScope(profile_id, r[0]) for r in self._read_all(
            "SELECT conversation_id FROM temporal_conversations WHERE profile_id=? AND conversation_id>? "
            "ORDER BY conversation_id LIMIT ?", (profile_id, after, min(100, limit)))]

    def temporal_recover(self, scope, *, inputs_verified=False, route_verified=False,
                         previous_owner_stopped=False, now=None):
        """Run with scanning stopped. Host assertions require recovered input/route evidence."""
        now = time.time() if now is None else now
        audit_id = uuid.uuid4().hex
        def write(conn):
            conv = conversation(conn, scope)
            p = policy_of(conv)
            problems = []
            conn.execute(f"UPDATE temporal_conversations SET work_token=NULL WHERE {SCOPE}", scope.sql)
            if not inputs_verified or not route_verified:
                revoke_scope(conn, scope, now, "recovery_requires_verified_input_and_route", suspend=True)
            ready = conn.execute(f"SELECT * FROM temporal_notifications WHERE {SCOPE} AND state='ready'", scope.sql).fetchone()
            conv = conversation(conn, scope)
            if ready and (not enabled(conv) or p.dry_run or ready["mode"] != "live"
                          or now - ready["created_at"] >= p.ready_expiry_seconds
                          or any(ready[k] != conv[k] for k in ("activity_version", "route_version", "policy_version"))):
                cancel_ready(conn, scope, now, "recovery_ready_stale")
            for row in conn.execute(f"SELECT * FROM temporal_items WHERE {SCOPE}", scope.sql).fetchall():
                obj = item_row(row)
                if obj["status"] == "draft" and now >= obj["body"]["created_at"] + p.draft_expiry_seconds:
                    obj["status"], obj["review_at"] = "cancelled", None
                    changed(obj, "handoff_expired")
                if obj["lease_until"] is not None and obj["lease_until"] <= now:
                    changed(obj, "recovered_expired_review")
                    normalize(conv, obj, now, p.failure_backoff_seconds)
                pending = obj["pending_notice_id"]
                notice = conn.execute("SELECT state,item_keys_json FROM temporal_notifications WHERE notice_id=? "
                                      f"AND {SCOPE}", (pending, *scope.sql)).fetchone() if pending else None
                if pending and (not notice or notice["state"] not in ("ready", "sending")
                                or obj["item_key"] not in json.loads(notice["item_keys_json"])):
                    obj["pending_notice_id"] = None
                    if obj["status"] == "active":
                        obj["status"], obj["review_at"] = "paused", None
                    changed(obj, "recovery_orphan_notice")
                    problems.append("orphan_notice:" + obj["item_key"])
                if obj["status"] == "active" and not obj["pending_notice_id"] and obj["review_at"] is None:
                    problems.append("orphan_schedule:" + obj["item_key"])
                normalize(conv, obj, now)
                save_item(conn, obj)
            reservations = conn.execute(f"SELECT count(*) FROM temporal_notifications WHERE {SCOPE} "
                                        "AND state='ready' AND reservation_state='reserved'", scope.sql).fetchone()[0]
            current = conversation(conn, scope)
            if reservations != current["notice_reservations"]:
                problems.append("reservation_mismatch")
                revoke_scope(conn, scope, now, "recovery_budget_inconsistent", suspend=True)
            append_event(conn, scope, event_id=audit_id, source="temporal_control",
                         source_event_id=audit_id, kind="control", now=now,
                         body={"action": "recover", "inputs_verified": inputs_verified,
                               "route_verified": route_verified, "problems": problems})
            return problems
        problems = self._execute_write(write)
        # Sending already consumed its attempt. Unknown is accounting, never a resend.
        for row in self._read_all(f"SELECT * FROM temporal_notifications WHERE {SCOPE} AND state='sending'", scope.sql):
            if previous_owner_stopped or now - row["send_started_at"] >= policy_of(self.temporal_conversation(scope)).sending_unknown_after_seconds:
                self.temporal_finish_send(scope, row["notice_id"], row["send_token"], "unknown", error="recovered_unconfirmed_send", now=now)
        return problems

    def temporal_control_all(self, scope, action, *, after_seconds=None, now=None):
        if action not in ("pause", "resume"):
            raise TemporalError("unsupported bulk action")
        now = time.time() if now is None else now
        results, offset = [], 0
        while True:
            page = self.temporal_list_items(scope, offset=offset)
            for obj in page["items"]:
                if action == "resume" and obj["status"] != "paused":
                    continue
                try:
                    self.temporal_mutate_item(scope, obj["item_key"], action,
                        after_seconds=after_seconds, resume_authorized=True, now=now)
                    results.append({"key": obj["item_key"], "ok": True})
                except TemporalError as exc:
                    results.append({"key": obj["item_key"], "ok": False, "error": str(exc)})
            if page["next_offset"] is None:
                return results
            offset = page["next_offset"]
