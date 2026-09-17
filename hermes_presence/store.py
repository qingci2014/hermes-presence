"""One profile's SQLite store, with no import of Hermes or its credentials."""
from contextlib import contextmanager, nullcontext
from pathlib import Path
import sqlite3
import threading
import uuid

from .cleanup import TemporalCleanupMixin
from .common import TABLES
from .identity import TemporalIdentityMixin
from .items import TemporalItemsMixin
from .lifecycle import TemporalLifecycleMixin
from .notices import TemporalNoticesMixin
from .protocol import TemporalError, text_field
from .records import TemporalRecordsMixin
from .reviews import TemporalReviewsMixin
from .schema import TEMPORAL_SCHEMA_SQL
from .topics import TemporalTopicsMixin
from .usage import UsageMixin, USAGE_SCHEMA

SCHEMA_VERSION = 1
APPLICATION_ID = 0x48505253
STORE_SCHEMA = '''
CREATE TABLE IF NOT EXISTS presence_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS presence_host_sessions (id TEXT PRIMARY KEY);
'''


class PresenceStore(TemporalIdentityMixin, TemporalItemsMixin, TemporalReviewsMixin,
                    TemporalNoticesMixin, TemporalLifecycleMixin, TemporalRecordsMixin,
                    TemporalTopicsMixin, TemporalCleanupMixin, UsageMixin):
    """Use ctx.state.data_dir as data_dir once the gateway integration is available.

    Host session IDs are references only. The adapter must deliver authorized
    creation/deletion events; no transcripts are copied into this registry.
    """

    def __init__(self, data_dir):
        self.data_dir = Path(data_dir).resolve()
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.data_dir / 'presence.db'
        self._lock = threading.RLock()
        self._closed = False
        with self._connection() as conn:
            app_id = conn.execute('PRAGMA application_id').fetchone()[0]
            version = conn.execute('PRAGMA user_version').fetchone()[0]
            tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if (app_id, version) != (APPLICATION_ID, SCHEMA_VERSION) and (app_id or version or tables):
                raise TemporalError('unsupported Presence database; migration required')
            if tables and not set((*TABLES, 'presence_meta', 'presence_host_sessions')) <= tables:
                raise TemporalError('incomplete Presence database')
            if conn.execute('PRAGMA journal_mode=WAL').fetchone()[0].lower() != 'wal':
                raise TemporalError('Presence requires a local filesystem with SQLite WAL support')
            conn.executescript('BEGIN IMMEDIATE;\n' + STORE_SCHEMA + TEMPORAL_SCHEMA_SQL + USAGE_SCHEMA)
            try:
                conn.execute('INSERT OR IGNORE INTO presence_meta VALUES (?,?)', ('profile_id', str(uuid.uuid4())))
                profile = conn.execute("SELECT value FROM presence_meta WHERE key='profile_id'").fetchone()[0]
                uuid.UUID(profile)
                conn.execute(f'PRAGMA application_id={APPLICATION_ID}')
                conn.execute(f'PRAGMA user_version={SCHEMA_VERSION}')
                conn.commit()
                self.profile_id = profile
            except BaseException:
                conn.rollback()
                raise

    @contextmanager
    def _connection(self):
        if self._closed:
            raise TemporalError('Presence store is closed')
        conn = sqlite3.connect(self.db_path, timeout=5, isolation_level=None)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute('PRAGMA foreign_keys=ON')
            conn.execute('PRAGMA synchronous=FULL')
            yield conn
        finally:
            conn.close()

    @contextmanager
    def _read_ctx(self):
        with self._lock, self._connection() as conn:
            conn.execute('BEGIN')
            try:
                yield conn
            finally:
                conn.rollback()

    def _read_one(self, sql, args=()):
        with self._read_ctx() as conn:
            return conn.execute(sql, args).fetchone()

    def _read_all(self, sql, args=()):
        with self._read_ctx() as conn:
            return conn.execute(sql, args).fetchall()

    def _execute_write(self, fn, *, commit_guard=None):
        # SQLite handles cross-instance lock waiting; never retry an ambiguous COMMIT.
        with self._lock, self._connection() as conn:
            conn.execute('BEGIN IMMEDIATE')
            try:
                result = fn(conn)
                with commit_guard() if commit_guard is not None else nullcontext():
                    conn.commit()
                return result
            except BaseException:
                conn.rollback()
                raise

    def host_session_created(self, session_id):
        text_field(session_id, 'host session ID', 256)
        self._execute_write(lambda c: c.execute(
            'INSERT OR IGNORE INTO presence_host_sessions VALUES (?)', (session_id,)).rowcount)

    def host_session_deleted(self, session_id):
        """A real host deletion erases related scopes; /new is NOT a deletion."""
        return self._execute_write(lambda c: c.execute(
            'DELETE FROM presence_host_sessions WHERE id=?', (session_id,)).rowcount)

    def backup(self, destination):
        """SQLite snapshot includes committed WAL data; refuses any existing target."""
        destination = Path(destination)
        destination.parent.mkdir(parents=True, exist_ok=True)
        with destination.open('xb'):
            pass
        try:
            with self._lock, self._connection() as source:
                target = sqlite3.connect(destination)
                try:
                    source.backup(target)
                    if target.execute('PRAGMA quick_check').fetchone()[0] != 'ok':
                        raise TemporalError('backup integrity check failed')
                finally:
                    target.close()
        except BaseException:
            destination.unlink(missing_ok=True)
            raise
        return destination

    def close(self):
        with self._lock:
            self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
