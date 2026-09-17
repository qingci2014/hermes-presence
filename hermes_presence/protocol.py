"""Temporal protocol types, policy validation and bounded factual views.

Times in durable state are UTC epoch seconds. Nothing here sends messages or
reads process-global model credentials; the gateway supplies both capabilities.
"""
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
import json
import math
import re
from zoneinfo import ZoneInfo


class TemporalError(ValueError):
    """A rejected transition; no partial state may be committed."""


class TemporalStale(TemporalError):
    """The caller no longer owns this work."""


@dataclass(frozen=True)
class TemporalScope:
    profile_id: str
    conversation_id: str

    @property
    def sql(self):
        return self.profile_id, self.conversation_id


@dataclass(frozen=True)
class TemporalClaim:
    scope: TemporalScope
    key: str
    token: str
    revision: int
    lease_until: float


@dataclass(frozen=True)
class TemporalReviewFence:
    activity_version: int
    route_version: int
    policy_version: int
    work_token: str


@dataclass(frozen=True)
class TemporalPolicy:
    enabled: bool = False
    dry_run: bool = True
    poll_interval_seconds: int = 5
    scan_scope_limit: int = 100
    batch_size: int = 10
    max_concurrent_reviews: int = 2
    max_concurrent_reviews_per_profile: int = 1
    review_total_timeout_seconds: int = 90
    commit_margin_seconds: int = 30
    lease_seconds: int = 180
    failure_backoff_seconds: int = 300
    user_activity_backoff_seconds: int = 900
    min_review_seconds: int = 60
    max_review_seconds: int = 2592000
    default_max_review_attempts: int = 24
    max_daily_reviews: int = 60
    no_progress_backoff_max_seconds: int = 3600
    default_max_notification_attempts: int = 2
    default_max_conversation_notification_attempts: int = 10
    notification_window_seconds: int = 86400
    max_items_per_conversation: int = 100
    context_max_items: int = 20
    review_message_limit: int = 20
    context_text_max_chars: int = 8000
    event_text_max_chars: int = 2000
    notification_max_chars: int = 1200
    draft_expiry_seconds: int = 86400
    ready_expiry_seconds: int = 300
    send_timeout_seconds: int = 60
    sending_unknown_after_seconds: int = 120
    max_clock_jump_seconds: int = 60
    codex_tls_max_version: str | None = None
    care_retention_seconds: int = 604800
    topic_inactivity_seconds: int = 7776000
    history_retention_seconds: int = 2592000
    quiet_hours: dict = field(default_factory=lambda: {
        "enabled": False, "start": "22:00", "end": "08:00", "timezone": "user"})

    def __post_init__(self):
        for key, value in asdict(self).items():
            if key in {"enabled", "dry_run"}:
                if type(value) is not bool:
                    raise TemporalError(f"{key} must be boolean")
            elif key == "codex_tls_max_version":
                if value not in (None, "TLSv1.2"):
                    raise TemporalError("codex_tls_max_version must be null or TLSv1.2")
            elif key != "quiet_hours" and (type(value) is not int or value < 1):
                raise TemporalError(f"{key} must be a positive integer")
        if self.review_total_timeout_seconds + self.commit_margin_seconds >= self.lease_seconds:
            raise TemporalError("review deadline plus commit margin must be shorter than lease")
        if self.send_timeout_seconds >= self.sending_unknown_after_seconds:
            raise TemporalError("send deadline must be shorter than unknown recovery interval")
        if self.min_review_seconds > self.max_review_seconds:
            raise TemporalError("invalid review interval")
        if self.notification_window_seconds > min(self.care_retention_seconds, self.history_retention_seconds):
            raise TemporalError('notification window must fit within notice retention')
        if self.batch_size > 10 or self.context_max_items > 20:
            raise TemporalError("batch/context hard limit exceeded")
        q = self.quiet_hours
        if not isinstance(q, dict) or set(q) != {"enabled", "start", "end", "timezone"}:
            raise TemporalError("invalid quiet hours")
        if type(q["enabled"]) is not bool:
            raise TemporalError("quiet hours enabled must be boolean")
        for k in ("start", "end"):
            if not isinstance(q[k], str) or not re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d", q[k]):
                raise TemporalError("quiet hours require HH:MM")
        if q["enabled"]:
            try:
                ZoneInfo(q["timezone"])
            except (KeyError, ValueError, TypeError) as exc:
                raise TemporalError("quiet hours require a resolved IANA timezone") from exc

    @classmethod
    def from_dict(cls, value):
        if not isinstance(value, dict) or set(value) - set(cls.__dataclass_fields__):
            raise TemporalError("unknown temporal configuration")
        return cls(**value)

    def delay(self, value):
        if type(value) is not int or value < 1:
            raise TemporalError("duration must be a positive integer")
        return min(self.max_review_seconds, max(self.min_review_seconds, value))

    @property
    def mode(self):
        return "dry_run" if self.dry_run else "live"

    def quiet_until(self, now):
        q = self.quiet_hours
        if not q["enabled"]:
            return None
        local = datetime.fromtimestamp(now, ZoneInfo(q["timezone"]))
        minute = local.hour * 60 + local.minute
        start, end = [int(q[k][:2]) * 60 + int(q[k][3:]) for k in ("start", "end")]
        inside = (start <= minute < end) if start < end else (minute >= start or minute < end)
        if not inside:
            return None
        finish = local.replace(hour=end // 60, minute=end % 60, second=0, microsecond=0)
        if finish <= local:
            finish += timedelta(days=1)
        # fold=1 conservatively includes both occurrences of an ambiguous end.
        return finish.replace(fold=1).timestamp()


def text_field(value, name, limit):
    if not isinstance(value, str) or not value.strip() or len(value) > limit:
        raise TemporalError(f"{name} must contain 1..{limit} characters")
    return value.strip()


def item_key(value):
    if not isinstance(value, str) or not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}", value):
        raise TemporalError("invalid commitment key")
    return value


def timestamp(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise TemporalError("invalid UTC timestamp")
    return float(value)


def item_view(item, now):
    body = item["body"]
    armed, observed, expected = [body.get(k) for k in ("armed_at", "observed_at", "expected_by")]
    fresh = body.get("fresh_for_seconds")
    return {
        "key": item["item_key"], "revision": item["revision"], "status": item["status"],
        "purpose": body.get("purpose", "progress"), "care_source_quote": body.get("care_source_quote"),
        "topic_key": body.get("topic_key"),
        **{k: body.get(k) for k in ("owner", "summary", "awaited_event", "reason",
                                   "review_attempts", "max_review_attempts", "notification_attempts",
                                   "notification_reservations", "max_notification_attempts",
                                   "dry_run_notification_attempts")},
        "review_at": item["review_at"], "last_observation": body.get("observation"),
        "armed_at": armed, "expected_by": expected,
        "expected_after_seconds": body.get("expected_after_seconds"),
        "review_after_seconds": body.get("review_after_seconds"),
        "overdue_seconds": None if expected is None else max(0, now - expected),
        "occurred_at": body.get("occurred_at"),
        "elapsed_since_handoff_seconds": None if armed is None else max(0, now - armed),
        "expected_late": None if expected is None else now > expected,
        "observation_age_seconds": None if observed is None else max(0, now - observed),
        "evidence_stale": None if observed is None or fresh is None else now - observed > fresh,
    }


def validate_decisions(output, claims, policy):
    if isinstance(output, str):
        try:
            def unique_object(pairs):
                result = {}
                for key, value in pairs:
                    if key in result:
                        raise ValueError("duplicate JSON key")
                    result[key] = value
                return result
            output = json.loads(output, object_pairs_hook=unique_object)
        except (ValueError, TypeError) as exc:
            raise TemporalError("review must return JSON") from exc
    if not isinstance(output, dict) or set(output) != {"decisions", "notification"}:
        raise TemporalError("invalid review envelope")
    rows = output["decisions"]
    expected = {c.key: c.revision for c in claims}
    if not isinstance(rows, list) or len(rows) != len(expected):
        raise TemporalError("one decision is required per claim")
    seen, notify = set(), set()
    for row in rows:
        if not isinstance(row, dict) or set(row) != {"key", "revision", "action", "reason", "after_seconds"}:
            raise TemporalError("invalid decision fields")
        key = row["key"]
        if not isinstance(key, str) or key in seen or key not in expected:
            raise TemporalError("unknown or duplicate decision key")
        if type(row["revision"]) is not int or row["revision"] != expected[key]:
            raise TemporalError("invalid decision revision")
        if row["action"] not in ("wait", "pause", "resolve", "cancel", "notify"):
            raise TemporalError("invalid decision action")
        text_field(row["reason"], "reason", 1000)
        delay = row["after_seconds"]
        if type(delay) is not int or not policy.min_review_seconds <= delay <= policy.max_review_seconds:
            raise TemporalError("invalid decision delay")
        seen.add(key)
        if row["action"] == "notify":
            notify.add(key)
    notice = output["notification"]
    if not notify:
        if notice is not None:
            raise TemporalError("silent decisions cannot contain a notification")
    else:
        if not isinstance(notice, dict) or set(notice) != {"item_keys", "message"}:
            raise TemporalError("notify requires a complete notification")
        keys = notice["item_keys"]
        if (not isinstance(keys, list) or not all(isinstance(k, str) for k in keys)
                or len(keys) != len(set(keys)) or set(keys) != notify):
            raise TemporalError("notification keys must equal notify decisions")
        text_field(notice["message"], "message", policy.notification_max_chars)
    return output


def iso_time(now):
    return datetime.fromtimestamp(now, timezone.utc).isoformat()
