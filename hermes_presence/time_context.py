"""Small temporal views; elapsed time changes interpretation, not historical facts."""
import json
import time

from .common import SCOPE
from .protocol import iso_time


def observation_time(observed_at, now):
    return {
        'observed_at_utc': iso_time(observed_at),
        'elapsed_seconds': round(max(0, now - observed_at), 3),
        'clock_order_uncertain': now < observed_at,
    }


def topic_time(topic, now=None):
    now = time.time() if now is None else now
    body = topic['body']
    observed = body.get('source_observed_at')
    if observed is None:
        milestones = body.get('milestones', [])
        observed = milestones[-1]['at'] if milestones else topic['updated_at']
    topic['time_context'] = {
        **observation_time(observed, now),
        'current_outcome': 'reported_closed' if topic['status'] == 'closed' else 'unconfirmed',
        'meaning': 'Tracking status is not current activity; time alone proves no outcome.',
    }
    return topic


def record_time(record, now):
    record['time_context'] = {
        **observation_time(record.get('source_observed_at', record['created_at']), now),
        'recorded_at_utc': iso_time(record['created_at']),
        'meaning': 'Age does not expire preferences or make past experiences current.',
    }
    return record


def conversation_time(db, scope, event_id, now=None):
    """Bound by the actual user event, including /new; no transcript injection."""
    now = time.time() if now is None else now
    event = db._read_one(f'SELECT seq,received_at FROM temporal_events WHERE {SCOPE} '
                         "AND event_id=? AND kind='user_accepted'", (*scope.sql, event_id))
    if event is None:
        return {}
    # Control commands must not disguise a three-day absence as a two-second gap.
    previous = db._read_one(f'SELECT received_at FROM temporal_events WHERE {SCOPE} '
        "AND kind='user_accepted' AND seq<? AND substr(ltrim(json_extract(body_json,'$.text')),1,1)!='/' "
        'ORDER BY seq DESC LIMIT 1', (*scope.sql, event['seq']))
    result = {
        'current_message_at_utc': iso_time(event['received_at']),
        'previous_user_message': observation_time(previous['received_at'], now) if previous else None,
    }
    # Only an index: no quotes, summaries or personal records are loaded after /new.
    rows = db._read_all(f'SELECT topic_key,status,updated_at,body_json FROM temporal_topics WHERE {SCOPE} '
                        'ORDER BY updated_at DESC,topic_key LIMIT 3', scope.sql)
    result['topic_index'] = []
    for row in rows:
        body = json.loads(row['body_json'])
        milestones = body.get('milestones', [])
        observed = body.get('source_observed_at', milestones[-1]['at'] if milestones else row['updated_at'])
        result['topic_index'].append({'topic_key': row['topic_key'], 'tracking_status': row['status'],
                                     **observation_time(observed, now)})
    return result
