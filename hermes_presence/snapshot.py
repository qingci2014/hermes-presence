"""Bounded model views; durable evidence is left intact in the database."""
from .common import dumps
from .protocol import TemporalError, item_view


def record_view(record):
    return {k: v for k, v in record.items() if k not in ('profile_id', 'conversation_id', 'source_event_id')}


def topic_view(topic):
    return {k: v for k, v in topic.items() if k not in ('profile_id', 'conversation_id')}


def review_item(item, now):
    return {k: v for k, v in item_view(item, now).items() if v is not None and k not in (
        'armed_at', 'occurred_at', 'expected_after_seconds', 'review_after_seconds', 'notification_reservations')}


def fit_snapshot(snapshot, maximum):
    # Contact preferences and all claimed items are essential. Drop expendable
    # history first; callers shrink the batch if essentials still do not fit.
    for topic in snapshot['topics']:
        topic['body'].pop('milestones', None)
    while len(dumps(snapshot)) > maximum and len(snapshot['recent_events']) > 1:
        snapshot['recent_events'].pop(0)
    while len(dumps(snapshot)) > maximum and snapshot['topics']:
        snapshot['topics'].pop()
    if len(dumps(snapshot)) > maximum:
        raise TemporalError('essential review context exceeds configured limit')
    return snapshot
