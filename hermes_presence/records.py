"""Small, source-backed personal records; no history crawler or extra model calls."""
import json
import time
import uuid

from .protocol import TemporalError, text_field
from .common import SCOPE, conversation


class TemporalRecordsMixin:
    def temporal_remember(self, scope, *, event_id, expected_activity, expected_policy,
                          kind, content, quoted_text, epistemic_status="explicit",
                          previous_id=None, use_for_followup=False, now=None):
        now = time.time() if now is None else now
        record_id = uuid.uuid4().hex
        if kind not in ("preference", "view", "relationship"):
            raise TemporalError("invalid record kind")
        if epistemic_status not in ("explicit", "observed", "inferred"):
            raise TemporalError("invalid evidence type")
        text_field(content, "content", 500)
        text_field(quoted_text, "source quote", 500)
        if type(use_for_followup) is not bool:
            raise TemporalError("use_for_followup must be boolean")
        if use_for_followup and (kind != "preference" or epistemic_status != "explicit"):
            raise TemporalError("only explicit preferences can guide proactive follow-ups")
        def write(conn):
            conv = conversation(conn, scope)
            if (conv["activity_version"], conv["policy_version"]) != (expected_activity, expected_policy):
                raise TemporalError("turn superseded")
            event = conn.execute(f"SELECT * FROM temporal_events WHERE {SCOPE} AND event_id=? AND kind='user_accepted'",
                                 (*scope.sql, event_id)).fetchone()
            if event is None or quoted_text not in json.loads(event["body_json"])["text"]:
                raise TemporalError("quote must occur in the host-bound user message")
            old = conn.execute(f"SELECT * FROM temporal_records WHERE {SCOPE} AND source_event_id=? AND kind=? AND content=?",
                               (*scope.sql, event_id, kind, content)).fetchone()
            if old:
                return dict(old)
            if previous_id:
                previous = conn.execute(f"SELECT * FROM temporal_records WHERE {SCOPE} AND record_id=?",
                                        (*scope.sql, previous_id)).fetchone()
                if previous is None or previous["kind"] != kind or previous["status"] == "superseded":
                    raise TemporalError("previous record must be a current record of the same kind")
                if epistemic_status != "explicit":
                    raise TemporalError("an inference cannot replace an existing record")
                conn.execute("UPDATE temporal_records SET status='superseded' WHERE record_id=?", (previous_id,))
            conn.execute("INSERT INTO temporal_records VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                         (record_id, *scope.sql, kind, content, epistemic_status,
                          "active" if epistemic_status == "explicit" else "tentative",
                          event_id, quoted_text, previous_id, int(use_for_followup), now))
            return dict(conn.execute("SELECT * FROM temporal_records WHERE record_id=?", (record_id,)).fetchone())
        return self._execute_write(write)

    def temporal_recall(self, scope, *, query="", kind=None, include_history=False, limit=5, followup_only=False, record_id=None):
        if type(limit) is not int or not 1 <= limit <= 20:
            raise TemporalError("invalid record limit")
        if not isinstance(query, str) or len(query) > 200:
            raise TemporalError("invalid recall query")
        if kind is not None and kind not in ("preference", "view", "relationship"):
            raise TemporalError("invalid record kind")
        if type(include_history) is not bool:
            raise TemporalError("include_history must be boolean")
        if record_id is not None:
            text_field(record_id, "record ID", 128)
            if query or kind or followup_only:
                raise TemporalError("record ID history cannot be combined with search filters")
            rows = self._read_all(
                f"WITH RECURSIVE history(record_id,previous_id) AS (SELECT record_id,previous_id FROM temporal_records "
                f"WHERE {SCOPE} AND record_id=? UNION SELECT r.record_id,r.previous_id FROM temporal_records r "
                "JOIN history h ON r.record_id=h.previous_id WHERE r.profile_id=? AND r.conversation_id=?) "
                "SELECT r.* FROM temporal_records r JOIN history h USING(record_id) "
                "WHERE (? OR r.status='active') ORDER BY r.created_at DESC,r.record_id LIMIT ?",
                (*scope.sql, record_id, *scope.sql, include_history, limit))
            return self._record_time_views(rows)
        where, args = SCOPE, list(scope.sql)
        if not include_history:
            where += " AND status='active'"
        if kind:
            where += " AND kind=?"
            args.append(kind)
        if query:
            where += " AND (instr(lower(content),lower(?))>0 OR instr(lower(quoted_text),lower(?))>0)"
            args.extend((query, query))
        if followup_only:
            where += " AND status='active' AND kind='preference' AND epistemic_status='explicit' AND use_for_followup=1"
        rows = self._read_all(f"SELECT * FROM temporal_records WHERE {where} ORDER BY created_at DESC,record_id LIMIT ?",
                              (*args, limit))
        return self._record_time_views(rows)

    def _record_time_views(self, rows):
        from .time_context import record_time
        now = time.time()
        results = []
        for row in rows:
            record = dict(row)
            source = self._read_one('SELECT received_at FROM temporal_events WHERE event_id=?',
                                    (record['source_event_id'],))
            if source:
                record['source_observed_at'] = source['received_at']
            results.append(record_time(record, now))
        return results

    def temporal_forget(self, scope, *, record_id, expected_activity, expected_policy):
        """Delete this record and dependent revisions, without copying content into audit."""
        text_field(record_id, "record ID", 128)
        def write(conn):
            conv = conversation(conn, scope)
            if (conv["activity_version"], conv["policy_version"]) != (expected_activity, expected_policy):
                raise TemporalError("turn superseded")
            ids = [r[0] for r in conn.execute(
                f"WITH RECURSIVE dependent(id) AS (SELECT record_id FROM temporal_records WHERE {SCOPE} AND record_id=? "
                "UNION SELECT r.record_id FROM temporal_records r JOIN dependent d ON r.previous_id=d.id) "
                "SELECT id FROM dependent", (*scope.sql, record_id))]
            conn.executemany("DELETE FROM temporal_records WHERE record_id=?", [(i,) for i in ids])
            return {"deleted": len(ids)}
        return self._execute_write(write)
