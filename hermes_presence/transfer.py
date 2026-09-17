"""Offline portable snapshots. No host credentials, transcripts or delivery rights."""
import json
from pathlib import Path
import shutil
import tempfile
import time
import zipfile

from .common import SCOPE, cancel_ready, dumps
from .lease import GatewayLease
from .protocol import TemporalError, TemporalScope
from .settings import policy_from_settings
from .store import PresenceStore


def route_identity(route):
    return [route.get(k) for k in ('platform', 'account_id', 'chat_id', 'thread_id', 'user_id', 'user_id_alt')]


def prepare_transfer(store):
    now = time.time()
    def write(conn):
        # Execution speed on a new server is not assumed to match the old server.
        conn.execute("DELETE FROM presence_meta WHERE key='execution_environment_id'")
        previous = conn.execute("SELECT value FROM presence_meta WHERE key='transfer_routes'").fetchone()
        routes = json.loads(previous[0]) if previous else {}
        for row in conn.execute('SELECT * FROM temporal_conversations').fetchall():
            route = json.loads(row['route_json'] or '{}')
            if route.get('authorized') and route.get('platform'):
                routes[row['conversation_id']] = route_identity(route)
            cancel_ready(conn, TemporalScope(row['profile_id'], row['conversation_id']), now, 'server_transfer')
        conn.execute("INSERT OR REPLACE INTO presence_meta VALUES ('transfer_routes',?)", (dumps(routes),))
        refs = [r[0] for r in conn.execute('SELECT id FROM presence_host_sessions')]
        conn.execute("INSERT OR REPLACE INTO presence_meta VALUES ('detached_sessions',?)", (dumps(refs),))
        conn.execute("UPDATE temporal_conversations SET status=CASE WHEN status='active' THEN 'suspended' ELSE status END,"
                     'route_json=NULL,session_key=NULL,work_token=NULL,notice_reservations=0,route_version=route_version+1')
        conn.execute("UPDATE temporal_notifications SET state='unknown',completed_at=?,error_text='server_transfer' "
                     "WHERE state='sending'", (now,))
        conn.execute('UPDATE temporal_notifications SET send_token=NULL')
        for row in conn.execute('SELECT * FROM temporal_items').fetchall():
            body = json.loads(row['body_json'])
            body['notification_reservations'] = 0
            if row['status'] in ('active', 'draft'):
                body['reason'] = 'server_transfer'
            status = {'active': 'paused', 'draft': 'cancelled'}.get(row['status'], row['status'])
            conn.execute('UPDATE temporal_items SET status=?,review_at=NULL,lease_token=NULL,lease_until=NULL,'
                         'pending_notice_id=NULL,revision=revision+1,body_json=? WHERE item_key=? AND ' + SCOPE,
                         (status, dumps(body), row['item_key'], row['profile_id'], row['conversation_id']))
    store._execute_write(write)


def detached_sessions(store):
    row = store._read_one("SELECT value FROM presence_meta WHERE key='detached_sessions'")
    return set(json.loads(row[0])) if row else set()


def rebind_transfer(store, session_id, session_key, route):
    """Only a fresh authorized SAME account/private-user route can reconnect records."""
    if route.get('authorized') is not True or route.get('live_capable') is not True or route.get('chat_type') not in ('dm', 'private'):
        raise TemporalError('verified private route required')
    def write(conn):
        row = conn.execute("SELECT value FROM presence_meta WHERE key='transfer_routes'").fetchone()
        if not row:
            return
        routes = json.loads(row[0])
        matches = [cid for cid, identity in routes.items() if identity == route_identity(route)]
        if not matches:
            return
        if len(matches) != 1:
            raise TemporalError('multiple transferred conversations match this route; explicit reconciliation required')
        cid = matches[0]
        bound = conn.execute('SELECT conversation_id FROM temporal_session_bindings WHERE profile_id=? AND session_id=?',
                             (store.profile_id, session_id)).fetchone()
        if bound and bound[0] != cid:
            raise TemporalError('current host session already belongs to another conversation')
        conn.execute('INSERT OR IGNORE INTO presence_host_sessions VALUES (?)', (session_id,))
        conn.execute('INSERT OR IGNORE INTO temporal_session_bindings VALUES (?,?,?,?,?)',
                     (store.profile_id, session_id, cid, 'resume', time.time()))
        conn.execute("UPDATE temporal_conversations SET current_session_id=?,session_key=?,route_json=?,status='active',"
                     'route_version=route_version+1 WHERE ' + SCOPE,
                     (session_id, session_key, dumps(route), store.profile_id, cid))
        del routes[cid]
        conn.execute("UPDATE presence_meta SET value=? WHERE key='transfer_routes'", (dumps(routes),))
        detached = detached_sessions_in_transaction(conn)
        detached.discard(session_id)
        conn.execute("UPDATE presence_meta SET value=? WHERE key='detached_sessions'", (dumps(sorted(detached)),))
    store._execute_write(write)


def detached_sessions_in_transaction(conn):
    row = conn.execute("SELECT value FROM presence_meta WHERE key='detached_sessions'").fetchone()
    return set(json.loads(row[0])) if row else set()


def export_data(data_dir, destination, settings):
    policy_from_settings(settings.get('temporal', {}))
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with GatewayLease(data_dir), PresenceStore(data_dir) as source:
        with tempfile.TemporaryDirectory(prefix='presence-export-', dir=Path(data_dir)) as temp:
            snapshot = Path(temp) / 'presence.db'
            source.backup(snapshot)
            with PresenceStore(snapshot.parent) as portable:
                prepare_transfer(portable)
                portable._execute_write(lambda c: c.execute('SELECT 1').fetchone())
            # Every store connection closes; SQLite has checkpointed its last WAL.
            with zipfile.ZipFile(destination, 'x', compression=zipfile.ZIP_DEFLATED) as archive:
                archive.write(snapshot, 'presence.db')
                archive.writestr('settings.json', dumps(settings))
                archive.writestr('manifest.json', dumps({'format': 1, 'profile_id': source.profile_id,
                    'delivery_authority': False, 'same_verified_route_required': True}))
    return destination


def import_data(archive_path, data_dir):
    data_dir = Path(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    with GatewayLease(data_dir):
        if (data_dir / 'presence.db').exists():
            raise TemporalError('import requires a new data directory; preserve existing data separately')
        with tempfile.TemporaryDirectory(prefix='presence-import-', dir=data_dir) as temp:
            with zipfile.ZipFile(archive_path) as archive:
                if set(archive.namelist()) != {'presence.db', 'settings.json', 'manifest.json'} or len(archive.namelist()) != 3:
                    raise TemporalError('invalid Presence transfer archive')
                if any(info.file_size > 512*1024*1024 for info in archive.infolist()):
                    raise TemporalError('transfer member is too large')
                manifest = json.loads(archive.read('manifest.json'))
                settings = json.loads(archive.read('settings.json'))
                policy_from_settings(settings.get('temporal', {}))
                if manifest.get('format') != 1 or manifest.get('delivery_authority') is not False:
                    raise TemporalError('unsupported transfer format')
                target = Path(temp) / 'presence.db'
                with archive.open('presence.db') as src, target.open('xb') as dst:
                    shutil.copyfileobj(src, dst)
            with PresenceStore(temp) as source:
                if source.profile_id != manifest['profile_id']:
                    raise TemporalError('transfer identity mismatch')
                if source._read_one('PRAGMA quick_check')[0] != 'ok' or source._read_all('PRAGMA foreign_key_check'):
                    raise TemporalError('transfer database integrity failure')
                prepare_transfer(source)  # Never trust a bundle's persisted send permissions.
                source.backup(data_dir / 'presence.db')
    return settings
