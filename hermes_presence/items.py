"""Commitment registration, complete handoff receipts and evidence transitions."""
import json
import time
import uuid

from .protocol import TemporalError, item_key, item_view, text_field, timestamp
from .common import (
    SCOPE, append_event, budget_available, cancel_ready, changed, conversation, dumps,
    enabled, item, item_row, normalize, policy_of, save_item,
)


class TemporalItemsMixin:
    def temporal_create_draft(self, scope, *, key, summary, awaited_event, owner,
                              review_after_seconds, turn_id, handoff_id, activity_version,
                              policy_version, expected_after_seconds=None, external_reference=None,
                              purpose="progress", quoted_text=None, event_id=None, topic_key=None, now=None):
        now = time.time() if now is None else now
        key = item_key(key)
        summary = text_field(summary, "summary", 500)
        awaited_event = text_field(awaited_event, "awaited event", 1000)
        text_field(turn_id, "turn ID", 256)
        text_field(handoff_id, "handoff ID", 256)
        if owner not in ("user", "external", "agent"):
            raise TemporalError("invalid waiting owner")
        if purpose not in ("progress", "care"):
            raise TemporalError("invalid temporal purpose")
        if topic_key is not None:
            topic_key = item_key(topic_key)
            if purpose != 'care':
                raise TemporalError('topic links belong to care cues')
        if purpose == "care":
            text_field(quoted_text, "care source quote", 500)
            if owner != "user" or expected_after_seconds is not None or external_reference is not None:
                raise TemporalError("care invites conversation; it has no completion deadline or external job")
        def write(conn):
            conv = conversation(conn, scope)
            policy = policy_of(conv)
            if not enabled(conv):
                raise TemporalError("temporal creation is disabled or route unauthorized")
            if (activity_version, policy_version) != (conv["activity_version"], conv["policy_version"]):
                raise TemporalError("turn was superseded")
            if purpose == "care":
                evidence = conn.execute(f"SELECT body_json FROM temporal_events WHERE {SCOPE} AND event_id=? AND kind='user_accepted'",
                                        (*scope.sql, event_id)).fetchone()
                if evidence is None or quoted_text not in json.loads(evidence[0])["text"]:
                    raise TemporalError("care quote must occur in the host-bound user message")
            delay = policy.delay(review_after_seconds)
            expected = expected_after_seconds
            if expected is not None:
                if type(expected) is not int or expected < 1:
                    raise TemporalError("invalid expected duration")
                expected = min(expected, policy.max_review_seconds)
            spec = dict(summary=summary, awaited_event=awaited_event, owner=owner,
                        review_after_seconds=delay, expected_after_seconds=expected, external_reference=external_reference,
                        purpose=purpose, care_source_quote=quoted_text if purpose == "care" else None, topic_key=topic_key)
            old = conn.execute(f"SELECT * FROM temporal_items WHERE {SCOPE} AND item_key=?", (*scope.sql, key)).fetchone()
            if old:
                obj = item_row(old)
                def registration(value):
                    # Completion is host-observed state, not a registration parameter.
                    return {k: v for k, v in value.items() if k != "completed"} if isinstance(value, dict) else value
                if any((registration(obj["body"].get(k)) != registration(v) if k == "external_reference"
                        else obj["body"].get(k, "progress" if k == "purpose" else None) != v) for k, v in spec.items()):
                    raise TemporalError("commitment key already has different parameters")
                return obj
            topic_body = None
            if topic_key is not None:
                topic = conn.execute(f'SELECT * FROM temporal_topics WHERE {SCOPE} AND topic_key=?', (*scope.sql, topic_key)).fetchone()
                if topic is None or topic['status'] != 'active':
                    raise TemporalError('care topic must exist and be active')
                topic_body = json.loads(topic['body_json'])
                if topic_body['source_event_id'] != event_id or topic_body['source_seq'] <= topic_body['last_care_seq']:
                    raise TemporalError('a new care opportunity requires a fresh user development in this topic')
            count = conn.execute(f"SELECT count(*) FROM temporal_items WHERE {SCOPE} AND status IN ('draft','active','paused')", scope.sql).fetchone()[0]
            if count >= conv["max_items"]:
                raise TemporalError("conversation item limit reached")
            body = dict(**spec, created_at=now, created_turn_id=turn_id,
                created_activity_version=activity_version, created_policy_version=policy_version,
                handoff_id=handoff_id, handoff_state="pending", origin_delivery_ids=[],
                armed_at=None, expected_by=None, observation=None, observation_event_id=None,
                observation_seq=0, observed_at=None, occurred_at=None, source_watermarks={},
                fresh_for_seconds=None, review_attempts=0, max_review_attempts=policy.default_max_review_attempts,
                notification_reservations=0, notification_attempts=0,
                max_notification_attempts=1 if purpose == "care" else policy.default_max_notification_attempts,
                delivered_notifications=0, dry_run_notification_attempts=0,
                reason="awaiting handoff receipt", metadata={})
            if purpose == "care":
                body["care_source_event_id"] = event_id
                body['care_expires_at'] = now + delay + policy.care_retention_seconds
            conn.execute("INSERT INTO temporal_items VALUES (?,?,?,'draft',NULL,NULL,NULL,1,NULL,?)", (*scope.sql, key, dumps(body)))
            if topic_body is not None:
                topic_body['last_care_seq'] = topic_body['source_seq']
                conn.execute(f'UPDATE temporal_topics SET body_json=? WHERE {SCOPE} AND topic_key=?', (dumps(topic_body), *scope.sql, topic_key))
            return item(conn, scope, key)
        return self._execute_write(write)

    def temporal_list_items(self, scope, *, offset=0, limit=20, include_terminal=False):
        if type(offset) is not int or offset < 0 or type(limit) is not int or not 1 <= limit <= 20:
            raise TemporalError("invalid pagination")
        status = "" if include_terminal else " AND status IN ('active','paused')"
        with self._read_ctx() as conn:
            conversation(conn, scope)
            total = conn.execute(f"SELECT count(*) FROM temporal_items WHERE {SCOPE}{status}", scope.sql).fetchone()[0]
            rows = conn.execute(f"SELECT * FROM temporal_items WHERE {SCOPE}{status} "
                                "ORDER BY json_extract(body_json,'$.created_at'),item_key LIMIT ? OFFSET ?",
                                (*scope.sql, limit, offset)).fetchall()
        return {"items": [item_row(r) for r in rows], "total": total,
                "next_offset": offset + len(rows) if offset + len(rows) < total else None}

    def temporal_get_item(self, scope, key):
        with self._read_ctx() as conn:
            return item(conn, scope, item_key(key))

    def temporal_handoff_receipt(self, scope, *, handoff_id, turn_id, outcome, delivery_ids,
                                  acknowledged_at, now=None):
        now, event_id = time.time() if now is None else now, uuid.uuid4().hex
        if outcome not in ("accepted", "failed", "unknown"):
            raise TemporalError("invalid handoff outcome")
        if not isinstance(delivery_ids, (list, tuple)) or any(not isinstance(v, str) or not v for v in delivery_ids):
            raise TemporalError("invalid handoff delivery IDs")
        if acknowledged_at is not None:
            acknowledged_at = timestamp(acknowledged_at)
        if outcome == "accepted" and (not delivery_ids or acknowledged_at is None or acknowledged_at > now):
            raise TemporalError("complete accepted handoff requires IDs and a valid receipt time")
        def write(conn):
            conv = conversation(conn, scope)
            receipt, fresh = append_event(conn, scope, event_id=event_id, source="handoff",
                source_event_id=handoff_id + ":" + outcome, kind="handoff_receipt", now=now,
                body=dict(handoff_id=handoff_id, turn_id=turn_id, outcome=outcome,
                          delivery_ids=delivery_ids, acknowledged_at=acknowledged_at))
            if not fresh or outcome == "unknown":
                return receipt
            rows = conn.execute(f"SELECT * FROM temporal_items WHERE {SCOPE} AND status='draft'", scope.sql).fetchall()
            for row in rows:
                obj = item_row(row)
                b = obj["body"]
                if b["handoff_id"] != handoff_id or b["created_turn_id"] != turn_id:
                    continue
                valid = (enabled(conv) and b["created_activity_version"] == conv["activity_version"]
                         and b["created_policy_version"] == conv["policy_version"]
                         and now < b["created_at"] + policy_of(conv).draft_expiry_seconds)
                if outcome == "accepted" and valid:
                    b.update(handoff_state="accepted", origin_delivery_ids=delivery_ids, armed_at=acknowledged_at,
                             expected_by=None if b["expected_after_seconds"] is None else acknowledged_at + b["expected_after_seconds"])
                    obj["status"], obj["review_at"] = "active", acknowledged_at + b["review_after_seconds"]
                    if b.get('purpose') == 'care':
                        b['care_expires_at'] = obj['review_at'] + policy_of(conv).care_retention_seconds
                    normalize(conv, obj, now)
                else:
                    b["handoff_state"] = outcome
                    obj["status"] = "cancelled"
                changed(obj, "handoff_" + outcome if valid else "handoff_stale")
                save_item(conn, obj)
            return receipt
        return self._execute_write(write)

    def temporal_mutate_item(self, scope, key, action, *, after_seconds=None, reason="user control",
                              resume_authorized=False, event_id=None, review_after_seconds=None,
                              expected_after_seconds=None, expected_activity=None, expected_policy=None, now=None):
        now = time.time() if now is None else now
        audit_id = uuid.uuid4().hex
        key = item_key(key)
        actions = {"pause": "paused", "resolve": "resolved", "cancel": "cancelled",
                   "resume": "active", "snooze": "active", "observe": None}
        if action not in actions:
            raise TemporalError("unknown commitment action")
        text_field(reason, "reason", 1000)
        def write(conn):
            conv = conversation(conn, scope)
            policy, obj = policy_of(conv), item(conn, scope, key)
            if ((expected_activity is not None and conv["activity_version"] != expected_activity)
                    or (expected_policy is not None and conv["policy_version"] != expected_policy)):
                raise TemporalError("turn superseded")
            if obj["status"] in ("resolved", "cancelled"):
                if actions[action] == obj["status"]:
                    return obj
                raise TemporalError("terminal commitment cannot be reopened")
            if action in ("resume", "snooze"):
                delay = policy.delay(after_seconds)
                if not enabled(conv):
                    raise TemporalError("permission unavailable")
                if action == "resume" and (not resume_authorized or obj["status"] != "paused"):
                    raise TemporalError("resume requires explicit user authorization and paused item")
                if action == "snooze" and obj["status"] != "active":
                    raise TemporalError("snooze cannot resume a paused item")
                if not budget_available(conv, obj):
                    raise TemporalError("budget_exhausted")
            event = None
            if action == "observe" or event_id is not None:
                event = conn.execute(f"SELECT * FROM temporal_events WHERE {SCOPE} AND event_id=? AND kind='user_accepted'",
                                     (*scope.sql, event_id)).fetchone()
                if not event:
                    raise TemporalError("observe requires the accepted user event of this turn")
                if event["seq"] < obj["body"]["observation_seq"]:
                    raise TemporalError("user evidence is older than the latest observation")
            if action == "observe":
                if event["seq"] == obj["body"]["observation_seq"]:
                    return obj
                # Validate before cancelling a merged ready notice.
                delay = policy.delay(obj["body"]["review_after_seconds"] if review_after_seconds is None else review_after_seconds)
                if expected_after_seconds is not None and (type(expected_after_seconds) is not int or expected_after_seconds < 1):
                    raise TemporalError("invalid expected duration")
            cancel_ready(conn, scope, now, "item_updated", policy.user_activity_backoff_seconds)
            conv, obj = conversation(conn, scope), item(conn, scope, key)
            b = obj["body"]
            if event is not None:
                b.update(observation=json.loads(event["body_json"])["text"], observation_event_id=event["event_id"],
                         observation_seq=event["seq"], observed_at=event["received_at"], occurred_at=event["occurred_at"])
            if action == "observe":
                if expected_after_seconds is not None:
                    b["expected_by"] = event["received_at"] + min(expected_after_seconds, policy.max_review_seconds)
                if obj["status"] == "active":
                    obj["review_at"] = max(now + policy.min_review_seconds, event["received_at"] + delay)
            elif action in ("resume", "snooze"):
                obj["status"], obj["review_at"] = "active", now + delay
            else:
                obj["status"], obj["review_at"] = actions[action], None
            changed(obj, reason)
            normalize(conv, obj, now)
            if b.get('purpose') == 'care':
                if obj['status'] == 'active':
                    b.pop('care_retired_at', None)
                else:
                    b.setdefault('care_retired_at', now)
            save_item(conn, obj)
            if action != "observe":
                append_event(conn, scope, event_id=audit_id, source="temporal_control", source_event_id=audit_id,
                             kind="control", now=now, body={"action": action, "key": key, "reason": reason,
                                                         "evidence_event_id": event_id})
            return obj
        return self._execute_write(write)

    def temporal_observe_external(self, scope, key, *, source, source_event_id, text,
                                  source_sequence=None, occurred_at=None, resolve=False, now=None):
        now, event_id = time.time() if now is None else now, uuid.uuid4().hex
        key = item_key(key)
        text_field(source, "source", 512)
        text_field(source_event_id, "source event ID", 512)
        if source_sequence is not None and (type(source_sequence) is not int or source_sequence < 0):
            raise TemporalError("invalid producer sequence")
        if resolve and source_sequence is None:
            raise TemporalError("unordered external events cannot automatically resolve")
        def write(conn):
            conv, obj = conversation(conn, scope), item(conn, scope, key)
            p = policy_of(conv)
            event, fresh = append_event(conn, scope, event_id=event_id, source=source,
                source_event_id=source_event_id, kind="external_observation", now=now, occurred_at=occurred_at,
                body={"key": key, "text": str(text)[:p.event_text_max_chars], "source_sequence": source_sequence})
            watermark = obj["body"]["source_watermarks"].get(source, -1)
            if not fresh or (source_sequence is not None and source_sequence <= watermark) or obj["status"] in ("resolved", "cancelled"):
                return obj
            cancel_ready(conn, scope, now, "external_observation", p.user_activity_backoff_seconds)
            obj = item(conn, scope, key)
            b = obj["body"]
            if source_sequence is not None:
                b["source_watermarks"][source] = source_sequence
            b.update(observation=json.loads(event["body_json"])["text"], observation_event_id=event_id,
                     observation_seq=event["seq"], observed_at=now, occurred_at=occurred_at)
            if resolve:
                obj["status"], obj["review_at"] = "resolved", None
            elif obj["status"] == "active":
                obj["review_at"] = now + b["review_after_seconds"]
            changed(obj, "verified_external_event")
            normalize(conversation(conn, scope), obj, now)
            save_item(conn, obj)
            return obj
        return self._execute_write(write)

    def temporal_context(self, scope, *, now=None, on_demand=False):
        now = time.time() if now is None else now
        conv = self.temporal_conversation(scope)
        p = policy_of(conv)
        page = self.temporal_list_items(scope, limit=p.context_max_items)
        page["items"] = [item_view(obj, now) for obj in page["items"]]
        events = self._read_all(f"SELECT kind,received_at,body_json,seq FROM temporal_events WHERE {SCOPE} "
                               "AND kind='notice_delivery' ORDER BY seq DESC LIMIT ?", (*scope.sql, p.review_message_limit))
        page["delivered_notices"] = [{"seq": r["seq"], "timestamp": r["received_at"],
                                      **json.loads(r["body_json"])} for r in reversed(events)]
        if on_demand:
            page = {"retrieval": "list: items; recall: records; topics: life threads.",
                    "delivered_notices": [n for n in page["delivered_notices"]
                                          if now - n["timestamp"] <= p.user_activity_backoff_seconds][-1:]}
        # Drop whole records, never truncate a JSON string into invalid context.
        while len(dumps(page)) > p.context_text_max_chars and (page.get("items") or page["delivered_notices"]):
            if page["delivered_notices"]:
                page["delivered_notices"].pop(0)
            else:
                page["items"].pop()
                page["next_offset"] = len(page["items"])
        return page
