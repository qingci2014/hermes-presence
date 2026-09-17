"""A small, source-backed thread of developments, separate from outreach attempts."""
import json
import time
from .protocol import TemporalError, item_key, text_field
from .common import SCOPE, cancel_ready, changed, conversation, dumps, item_row, policy_of, save_item


def topic_view(row):
    if row is None:
        return None
    result = dict(row)
    result['body'] = json.loads(result.pop('body_json'))
    from .time_context import topic_time
    return topic_time(result)


def retire_topic_cues(conn, scope, key, now, reason):
    rows = conn.execute(f"SELECT * FROM temporal_items WHERE {SCOPE} AND status IN ('draft','active') "
                        "AND json_extract(body_json,'$.topic_key')=?", (*scope.sql, key)).fetchall()
    if rows:
        cancel_ready(conn, scope, now, reason, policy_of(conversation(conn, scope)).user_activity_backoff_seconds)
    for row in rows:
        # cancel_ready may have released reservations on a merged notification.
        obj = item_row(conn.execute(f"SELECT * FROM temporal_items WHERE {SCOPE} AND item_key=?",
                                    (*scope.sql, row['item_key'])).fetchone())
        obj['status'], obj['review_at'] = 'cancelled', None
        obj['body']['care_retired_at'] = now
        changed(obj, reason)
        save_item(conn, obj)


class TemporalTopicsMixin:
    def temporal_topics(self, scope, *, topic_key=None, limit=10):
        if type(limit) is not int or not 1 <= limit <= 20:
            raise TemporalError('invalid topic limit')
        where, args = SCOPE, list(scope.sql)
        if topic_key is not None:
            where += ' AND topic_key=?'; args.append(item_key(topic_key))
        results = [topic_view(r) for r in self._read_all(
            f'SELECT * FROM temporal_topics WHERE {where} ORDER BY updated_at DESC,topic_key LIMIT ?', (*args, limit))]
        if topic_key is None:
            for result in results:
                result['body'].pop('milestones', None)
        return results

    def temporal_update_topic(self, scope, *, topic_key, summary, quoted_text, event_id,
                              expected_activity, expected_policy, topic_status=None, resume_authorized=False, now=None):
        now = time.time() if now is None else now
        key = item_key(topic_key)
        summary = text_field(summary, 'topic summary', 500)
        quoted_text = text_field(quoted_text, 'topic source quote', 500)
        if topic_status not in (None, 'active', 'paused', 'closed'):
            raise TemporalError('invalid topic status')
        def write(conn):
            conv = conversation(conn, scope)
            if (conv['activity_version'], conv['policy_version']) != (expected_activity, expected_policy):
                raise TemporalError('turn superseded')
            event = conn.execute(f"SELECT * FROM temporal_events WHERE {SCOPE} AND event_id=? AND kind='user_accepted'",
                                 (*scope.sql, event_id)).fetchone()
            if event is None or quoted_text not in json.loads(event['body_json']).get('text', ''):
                raise TemporalError('topic quote must occur in the host-bound user message')
            old = conn.execute(f'SELECT * FROM temporal_topics WHERE {SCOPE} AND topic_key=?', (*scope.sql, key)).fetchone()
            if old is None and conn.execute(f'SELECT count(*) FROM temporal_topics WHERE {SCOPE}', scope.sql).fetchone()[0] >= conv['max_items']:
                raise TemporalError('conversation topic limit reached')
            body = json.loads(old['body_json']) if old else {'milestones': [], 'last_care_seq': 0}
            if old and event['seq'] < body['source_seq']:
                raise TemporalError('stale topic evidence')
            status = topic_status or (old['status'] if old else 'active')
            if old and old['status'] in ('closed', 'expired') and status != old['status']:
                raise TemporalError('ended topic cannot be reopened; a new development may start a related topic')
            if old and old['status'] == 'paused' and status == 'active' and not resume_authorized:
                raise TemporalError('resuming a paused topic requires explicit user authorization')
            if old and event['seq'] == body['source_seq'] and summary == body['summary'] and status == old['status']:
                return topic_view(old)
            retire_topic_cues(conn, scope, key, now, 'topic_updated')
            milestone = dict(event_id=event_id, quote=quoted_text, summary=summary, at=event['received_at'])
            history = [m for m in body['milestones'] if m['event_id'] != event_id]
            body.update(summary=summary, source_event_id=event_id, source_seq=event['seq'],
                        source_observed_at=event['received_at'],
                        milestones=(history+[milestone])[-8:])
            ended = now if status in ('closed','expired') else None
            if old and old['ended_at'] is not None: ended = old['ended_at']
            conn.execute('INSERT INTO temporal_topics VALUES (?,?,?,?,?,?,?) ON CONFLICT(profile_id,conversation_id,topic_key) '
                         'DO UPDATE SET status=excluded.status,updated_at=excluded.updated_at,ended_at=excluded.ended_at,body_json=excluded.body_json',
                         (*scope.sql, key, status, now, ended, dumps(body)))
            return topic_view(conn.execute(f'SELECT * FROM temporal_topics WHERE {SCOPE} AND topic_key=?', (*scope.sql, key)).fetchone())
        return self._execute_write(write)
