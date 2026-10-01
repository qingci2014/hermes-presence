"""Explicit future responsibilities; identity/authority come only from the host."""
import json

from .protocol import TemporalError, item_view
from .snapshot import record_view, topic_view


TEMPORAL_SCHEMA = {
    "name": "temporal_commitment",
    "description": "Personal records, life topics, optional care and unfinished work; work_* measures same-turn execution. Drafts require delivery confirmation.",
    "parameters": {"type": "object", "additionalProperties": False, "required": ["action"],
        "properties": {
            "action": {"type": "string", "enum": ["create", "list", "observe", "snooze", "pause", "resume", "resolve", "cancel", "remember", "recall", "forget", "topics", "topic_update", "work_history", "work_start", "work_finish"]},
            "task_type": {"type": "string", "maxLength": 80, "description": "Stable narrow work category."},
            "criteria": {"type": "string", "maxLength": 500, "description": "Acceptance criteria fixed before execution."},
            "predicted_seconds": {"type": "integer", "minimum": 1, "maximum": 604800},
            "outcome": {"type": "string", "enum": ["completed", "failed", "cancelled", "waiting"]},
            "evidence_quote": {"type": "string", "maxLength": 400, "description": "Exact successful tool-result quote AFTER work_start."},
            "topic_key": {"type": "string", "description": "Stable life-topic key."},
            "care_decision": {"type": "string", "enum": ["create", "skip"], "description": "Required for topic_update; create adds care, skip retains the topic without scheduling contact."},
            "care_reason": {"type": "string", "maxLength": 500, "description": "Required for topic_update: why contact is worthwhile or skipped for now."},
            "topic_status": {"type": "string", "enum": ["active", "paused", "closed"], "description": "Omit to keep status; close requires evidence."},
            "kind": {"type": "string", "enum": ["preference", "view", "relationship"]},
            "content": {"type": "string", "maxLength": 500},
            "quoted_text": {"type": "string", "description": "Exact CURRENT user quote.", "maxLength": 500},
            "epistemic_status": {"type": "string", "enum": ["explicit", "observed", "inferred"]},
            "previous_id": {"type": "string", "description": "Record explicitly corrected by user."},
            "purpose": {"type": "string", "enum": ["progress", "care"], "description": "care: optional contact, one attempt, no deadline."},
            "record_id": {"type": "string"},
            "query": {"type": "string", "description": "Literal keyword for recall/topics; omit for recent entries."},
            "include_history": {"type": "boolean"},
            "use_for_followup": {"type": "boolean", "description": "Explicit contact preference, not send permission."},
            "key": {"type": "string"}, "summary": {"type": "string"}, "awaited_event": {"type": "string"},
            "owner": {"type": "string", "enum": ["user", "agent", "external"]},
            "review_after_seconds": {"type": "integer", "minimum": 60},
            "expected_after_seconds": {"type": "integer", "minimum": 1},
            "process_id": {"type": "string", "description": "Host-verified owned background process."},
            "after_seconds": {"type": "integer", "minimum": 60},
            "offset": {"type": "integer", "minimum": 0},
            "limit": {"type": "integer", "minimum": 1, "maximum": 20}, "reason": {"type": "string"},
        }},
}

_FIELDS = {
    "work_history": {"task_type"},
    "work_start": {"task_type", "criteria", "predicted_seconds"},
    "work_finish": {"outcome", "evidence_quote"},
    "topics": {"topic_key", "query", "limit"},
    "topic_update": {"topic_key", "summary", "quoted_text", "topic_status", "care_decision", "care_reason", "review_after_seconds", "awaited_event"},
    "remember": {"kind", "content", "quoted_text", "epistemic_status", "previous_id", "use_for_followup"},
    "recall": {"query", "kind", "include_history", "limit", "record_id"}, "forget": {"record_id"},
    "create": {"key", "summary", "awaited_event", "owner", "review_after_seconds", "expected_after_seconds", "process_id", "purpose", "quoted_text", "topic_key"},
    "list": {"offset", "limit"}, "observe": {"key", "review_after_seconds", "expected_after_seconds", "reason"},
    "snooze": {"key", "after_seconds", "reason"}, "resume": {"key", "after_seconds", "reason"},
    "pause": {"key", "reason"}, "resolve": {"key", "reason"}, "cancel": {"key", "reason"},
}


def temporal_tool(args, *, turn=None, process_reference=None):
    from .common import policy_of
    import time
    try:
        if turn is None:
            raise TemporalError("temporal tool requires an authorized Gateway turn")
        action = args.get("action")
        if action not in _FIELDS or set(args) - (_FIELDS[action] | {"action"}):
            raise TemporalError("invalid action fields; identity and budgets are host-owned")
        db, scope = turn.db, turn.scope
        conv = db.temporal_conversation(scope)
        if (conv["activity_version"], conv["policy_version"]) != (turn.activity_version, turn.policy_version):
            raise TemporalError("turn superseded")
        kwargs = {k: v for k, v in args.items() if k != "action"}
        if action.startswith('work_'):
            required = _FIELDS[action] - {'evidence_quote'}
            if not required <= set(kwargs):
                raise TemporalError('missing required execution fields')
            from .execution import execution_tool
            result = execution_tool(turn, action, kwargs)
        elif action == "topics":
            result = {"topics": [topic_view(t) for t in db.temporal_topics(scope, **kwargs)]}
        elif action == "topic_update":
            required = {'topic_key', 'summary', 'quoted_text', 'care_decision', 'care_reason'}
            if not required <= kwargs.keys():
                raise TemporalError('topic_update requires topic_key, summary, quoted_text, care_decision=create|skip and care_reason. For create also supply review_after_seconds>=60; no separate create call is needed.')
            topic, cue = db.temporal_topic_with_care(scope, **kwargs, event_id=turn.event_id,
                expected_activity=turn.activity_version, expected_policy=turn.policy_version,
                turn_id=turn.turn_id, handoff_id=turn.handoff_id, resume_authorized=turn.resume_authorized)
            result = {'topic': topic, 'care_decision': kwargs['care_decision']}
            if cue:
                result.update(item=item_view(cue, time.time()), followup_mode=policy_of(conv).mode,
                    handoff_state=cue['body']['handoff_state'], review_after_seconds=cue['body']['review_after_seconds'])
        elif action == "recall":
            result = {"records": [record_view(r) for r in db.temporal_recall(scope, **kwargs)]}
        elif action in ("remember", "forget"):
            kwargs.update(expected_activity=turn.activity_version, expected_policy=turn.policy_version)
            if action == "remember":
                result = {"record": db.temporal_remember(scope, event_id=turn.event_id, **kwargs)}
            else:
                result = db.temporal_forget(scope, **kwargs)
        elif action == "list":
            result = db.temporal_list_items(scope, **kwargs)
            result["items"] = [item_view(o, time.time()) for o in result["items"]]
        elif action == "create":
            if kwargs.get('purpose') == 'care' and not kwargs.get('topic_key'):
                raise TemporalError('Care cues require topic_key. Use topics, then topic_update with the current user quote before creating the care cue.')
            required = _FIELDS["create"] - {"expected_after_seconds", "process_id", "purpose", "quoted_text", "topic_key"}
            if not required <= set(kwargs):
                raise TemporalError("missing required create fields")
            if "process_id" in kwargs:
                if kwargs["owner"] != "external":
                    raise TemporalError("a process observation requires owner=external")
                if process_reference is None:
                    raise TemporalError("host process observation is unavailable")
                kwargs["external_reference"] = process_reference(turn, kwargs.pop("process_id"))
            obj = db.temporal_create_draft(scope, **kwargs, turn_id=turn.turn_id, handoff_id=turn.handoff_id,
                activity_version=turn.activity_version, policy_version=turn.policy_version, event_id=turn.event_id)
            result = {"item": item_view(obj, time.time()), "followup_mode": policy_of(conv).mode,
                      "handoff_state": obj["body"]["handoff_state"],
                      "review_after_seconds": obj["body"]["review_after_seconds"],
                      "message": "Waiting for confirmed complete handoff. Dry-run never sends."}
        else:
            if "key" not in kwargs:
                raise TemporalError("key required")
            kwargs["event_id"] = turn.event_id
            if action == "resume":
                kwargs["resume_authorized"] = turn.resume_authorized
            obj = db.temporal_mutate_item(scope, action=action, **kwargs,
                expected_activity=turn.activity_version, expected_policy=turn.policy_version)
            result = {"item": item_view(obj, time.time())}
        if action == 'remember':
            result['record'] = {k: result['record'][k] for k in ('record_id', 'status', 'kind', 'previous_id')}
        elif action == 'topic_update':
            result['topic'] = {k: result['topic'][k] for k in ('topic_key', 'status', 'updated_at')}
        if 'item' in result:
            result['item'] = {k: result['item'][k] for k in ('key', 'revision', 'status', 'review_at', 'expected_by', 'reason')}
        return json.dumps({"success": True, **result}, ensure_ascii=False, separators=(',', ':'))
    except (TemporalError, TypeError) as exc:
        return json.dumps({"success": False, "error": str(exc)}, ensure_ascii=False)

