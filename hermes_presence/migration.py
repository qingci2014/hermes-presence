"""Explicit offline import from the existing temporal tables, never a background job."""
import hashlib
import json
from pathlib import Path
import sqlite3
import time

from .common import TABLES, SCOPE, cancel_ready, dumps
from .protocol import TemporalError, TemporalScope


def _snapshot(source_path, source_profile):
    path = Path(source_path).resolve(strict=True)
    source = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)
    source.row_factory = sqlite3.Row
    try:
        source.execute('BEGIN')
        profiles = [r[0] for r in source.execute('SELECT DISTINCT profile_id FROM temporal_conversations')]
        if source_profile is None:
            if len(profiles) != 1:
                raise TemporalError('select one legacy profile explicitly')
            source_profile = profiles[0]
        if source_profile not in profiles:
            raise TemporalError('legacy profile not found')
        # Only the known current legacy schema is supported. Unknown columns fail later.
        rows = {table: [dict(r) for r in source.execute(f'SELECT * FROM {table} WHERE profile_id=?',
                                                       (source_profile,))] for table in TABLES}
        sessions = {r['session_id'] for r in rows['temporal_session_bindings']}
        sessions.update(r['current_session_id'] for r in rows['temporal_conversations'])
        if any(not source.execute('SELECT 1 FROM sessions WHERE id=?', (sid,)).fetchone() for sid in sessions):
            raise TemporalError('legacy host session reference is missing')
        canonical = {t: sorted(v, key=dumps) for t, v in rows.items()}
        digest = hashlib.sha256(dumps(canonical).encode('utf-8')).hexdigest()
        return rows, sessions, digest
    finally:
        source.close()


def import_legacy(store, source_path, *, source_profile=None, now=None):
    """Import once into an empty store. Source may be a read-only consistent backup.

    Same input is idempotent; a changed input cannot overwrite a previous import.
    User enable/disable choices and evidence survive, but old delivery authority
    does not. Rebinding to a verified host route is a separate adapter operation.
    """
    rows, sessions, digest = _snapshot(source_path, source_profile)
    now = time.time() if now is None else now
    def write(conn):
        previous = conn.execute("SELECT value FROM presence_meta WHERE key='legacy_import'").fetchone()
        if previous:
            report = json.loads(previous[0])
            if report['source_digest'] != digest:
                raise TemporalError('different legacy snapshot already imported; refusing overwrite')
            return {**report, 'already_imported': True}
        if any(conn.execute(f'SELECT 1 FROM {table} LIMIT 1').fetchone() for table in TABLES):
            raise TemporalError('legacy import requires an empty Presence store')
        conn.executemany('INSERT OR IGNORE INTO presence_host_sessions VALUES (?)', ((sid,) for sid in sorted(sessions)))
        for table in TABLES:
            columns = tuple(r[1] for r in conn.execute(f'PRAGMA table_info({table})'))
            for original in rows[table]:
                if set(original) != set(columns):
                    raise TemporalError(f'unsupported legacy columns in {table}')
                row = {**original, 'profile_id': store.profile_id}
                conn.execute(f"INSERT INTO {table} ({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})",
                             tuple(row[k] for k in columns))
        # Routes and process-local tokens are no longer valid after a migration.
        # Preserve evidence/history and enabled choices, including explicit off.
        conn.execute("UPDATE temporal_conversations SET status=CASE WHEN status='active' THEN 'suspended' ELSE status END,"
                     "route_json=NULL,session_key=NULL,work_token=NULL,route_version=route_version+1")
        for conv in rows['temporal_conversations']:
            scope = TemporalScope(store.profile_id, conv['conversation_id'])
            cancel_ready(conn, scope, now, 'migration_requires_verified_route')
        conn.execute("UPDATE temporal_notifications SET state='unknown',completed_at=?,"
                     "error_text='migrated_unconfirmed_send' WHERE state='sending'", (now,))
        conn.execute("UPDATE temporal_notifications SET send_token=NULL")
        for raw in conn.execute('SELECT * FROM temporal_items').fetchall():
            row = dict(raw)
            body = json.loads(row['body_json'])
            status = {'active': 'paused', 'draft': 'cancelled'}.get(row['status'], row['status'])
            if status != row['status']:
                body['reason'] = 'migration_requires_verified_route'
            body['notification_reservations'] = 0
            conn.execute('UPDATE temporal_items SET status=?,review_at=NULL,lease_until=NULL,lease_token=NULL,'
                         'pending_notice_id=NULL,revision=revision+1,body_json=? '
                         f'WHERE {SCOPE} AND item_key=?', (status, dumps(body), row['profile_id'],
                                                         row['conversation_id'], row['item_key']))
        conn.execute('UPDATE temporal_conversations SET notice_reservations=0')
        if conn.execute('PRAGMA foreign_key_check').fetchall():
            raise TemporalError('migrated database has broken references')
        if conn.execute('PRAGMA quick_check').fetchone()[0] != 'ok':
            raise TemporalError('migrated database integrity failure')
        report = {'source_digest': digest, 'profile_id': store.profile_id,
                  'counts': {t: len(v) for t, v in rows.items()}, 'migrated_at': now,
                  'delivery_requires_rebinding': True}
        conn.execute('INSERT INTO presence_meta VALUES (?,?)', ('legacy_import', dumps(report)))
        return {**report, 'already_imported': False}
    return store._execute_write(write)
