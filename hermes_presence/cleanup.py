"""Bounded retention of derived care data; original chats and personal records stay separate."""
import json
import time
from .protocol import TemporalScope
from .common import SCOPE, cancel_ready, changed, dumps, item_row, save_item
from .topics import retire_topic_cues


class TemporalCleanupMixin:
    def temporal_housekeep(self, profile_id, policy, *, now=None, limit=100):
        now = time.time() if now is None else now
        limit = max(1, min(100, int(limit)))
        def write(conn):
            counts = dict(topics_expired=0, topics_deleted=0, cues_deleted=0, notices_deleted=0, events_compacted=0)
            topics = conn.execute("SELECT * FROM temporal_topics WHERE profile_id=? AND status IN ('active','paused') "
                "AND updated_at<=? ORDER BY updated_at LIMIT ?", (profile_id, now-policy.topic_inactivity_seconds, limit)).fetchall()
            for row in topics:
                scope = TemporalScope(profile_id, row['conversation_id'])
                retire_topic_cues(conn, scope, row['topic_key'], now, 'topic_inactive')
                conn.execute(f"UPDATE temporal_topics SET status='expired',ended_at=? WHERE {SCOPE} AND topic_key=?",
                    (row['updated_at']+policy.topic_inactivity_seconds, *scope.sql, row['topic_key']))
                counts['topics_expired'] += 1
            # Establish one retirement timestamp; no user response is required to retire an opportunity.
            rows = conn.execute("SELECT * FROM temporal_items WHERE profile_id=? AND json_extract(body_json,'$.purpose')='care' "
                "AND json_extract(body_json,'$.care_retired_at') IS NULL AND (status IN ('paused','resolved','cancelled') "
                "OR coalesce(json_extract(body_json,'$.care_expires_at'),json_extract(body_json,'$.created_at')+"
                "json_extract(body_json,'$.review_after_seconds')+?)<=?) ORDER BY json_extract(body_json,'$.created_at') LIMIT ?",
                (profile_id, policy.care_retention_seconds, now, limit)).fetchall()
            for row in rows:
                scope = TemporalScope(profile_id, row['conversation_id'])
                if row['status'] in ('active','draft'):
                    cancel_ready(conn, scope, now, 'care_window_expired', policy.user_activity_backoff_seconds)
                obj = item_row(conn.execute(f'SELECT * FROM temporal_items WHERE {SCOPE} AND item_key=?', (*scope.sql,row['item_key'])).fetchone())
                if obj['status'] in ('active','draft'):
                    obj['status'], obj['review_at'] = 'cancelled', None
                    changed(obj, 'care_window_expired')
                obj['body']['care_retired_at'] = now
                save_item(conn, obj)
            cutoff = now-policy.history_retention_seconds
            conn.execute('DELETE FROM presence_review_usage WHERE request_id IN '
                '(SELECT request_id FROM presence_review_usage WHERE profile_id=? AND started_at<? LIMIT ?)',
                (profile_id, min(cutoff, now-86400), limit))
            notices = conn.execute("SELECT notice_id FROM temporal_notifications WHERE profile_id=? "
                "AND state NOT IN ('ready','sending') AND coalesce(completed_at,created_at)<=? "
                "ORDER BY created_at LIMIT ?", (profile_id,cutoff,limit)).fetchall()
            for row in notices:
                # Never detach accounting from an unsettled item.
                if not conn.execute('SELECT 1 FROM temporal_items WHERE pending_notice_id=?', (row[0],)).fetchone():
                    conn.execute('DELETE FROM temporal_notifications WHERE notice_id=?', (row[0],))
                    counts['notices_deleted'] += 1
            care_cutoff = now-policy.care_retention_seconds
            cues = conn.execute("SELECT * FROM temporal_items WHERE profile_id=? AND status IN ('paused','resolved','cancelled') "
                "AND pending_notice_id IS NULL AND lease_token IS NULL AND json_extract(body_json,'$.purpose')='care' "
                "AND json_extract(body_json,'$.care_retired_at')<=? ORDER BY json_extract(body_json,'$.care_retired_at') LIMIT ?",
                (profile_id,care_cutoff,limit)).fetchall()
            for row in cues:
                scope = TemporalScope(profile_id,row['conversation_id'])
                linked = conn.execute(f'SELECT * FROM temporal_notifications WHERE {SCOPE} AND EXISTS '
                    '(SELECT 1 FROM json_each(item_keys_json) WHERE value=?)', (*scope.sql,row['item_key'])).fetchall()
                if any(n['state'] in ('ready','sending') or (n['completed_at'] or n['created_at']) > care_cutoff
                       or json.loads(n['item_keys_json']) != [row['item_key']] for n in linked):
                    continue  # A merged or unsettled receipt must keep all of its item references.
                for n in linked:
                    conn.execute('DELETE FROM temporal_notifications WHERE notice_id=?', (n['notice_id'],))
                    counts['notices_deleted'] += 1
                conn.execute(f'DELETE FROM temporal_items WHERE {SCOPE} AND item_key=?', (*scope.sql,row['item_key']))
                counts['cues_deleted'] += 1
            ended = conn.execute("SELECT * FROM temporal_topics WHERE profile_id=? AND status IN ('closed','expired') "
                "AND ended_at<=? ORDER BY ended_at LIMIT ?", (profile_id,cutoff,limit)).fetchall()
            for row in ended:
                scope = TemporalScope(profile_id,row['conversation_id'])
                if not conn.execute(f"SELECT 1 FROM temporal_items WHERE {SCOPE} AND json_extract(body_json,'$.topic_key')=?",
                                    (*scope.sql,row['topic_key'])).fetchone():
                    conn.execute(f'DELETE FROM temporal_topics WHERE {SCOPE} AND topic_key=?', (*scope.sql,row['topic_key']))
                    counts['topics_deleted'] += 1
            # Keep event IDs as deduplication tombstones. Removing user-event rows could make an old
            # transport retry look like new input. Preserve every source still used by a live record.
            events = conn.execute("SELECT e.event_id FROM temporal_events e WHERE e.profile_id=? AND e.received_at<=? "
                "AND coalesce(json_extract(e.body_json,'$.retention_compacted'),0)=0 "
                "AND e.seq<(SELECT max(seq) FROM temporal_events WHERE profile_id=e.profile_id AND conversation_id=e.conversation_id) "
                "AND NOT EXISTS (SELECT 1 FROM temporal_records r WHERE r.source_event_id=e.event_id) "
                "AND NOT EXISTS (SELECT 1 FROM temporal_items i WHERE i.profile_id=e.profile_id AND i.conversation_id=e.conversation_id "
                "AND (json_extract(i.body_json,'$.care_source_event_id')=e.event_id OR json_extract(i.body_json,'$.observation_event_id')=e.event_id)) "
                "AND NOT EXISTS (SELECT 1 FROM temporal_topics t,json_each(t.body_json,'$.milestones') m "
                "WHERE t.profile_id=e.profile_id AND t.conversation_id=e.conversation_id AND json_extract(m.value,'$.event_id')=e.event_id) "
                "ORDER BY e.received_at LIMIT ?", (profile_id,cutoff,limit)).fetchall()
            for row in events:
                conn.execute('UPDATE temporal_events SET body_json=? WHERE event_id=?', (dumps({'text':'','retention_compacted':True}),row[0]))
                counts['events_compacted'] += 1
            return counts
        return self._execute_write(write)
