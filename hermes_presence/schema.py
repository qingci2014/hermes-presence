"""Additive temporal schema; owned by the standalone Presence store."""

TEMPORAL_SCHEMA_SQL = r"""
CREATE TABLE IF NOT EXISTS temporal_conversations (
    profile_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    current_session_id TEXT NOT NULL,
    session_key TEXT,
    status TEXT NOT NULL DEFAULT 'active'
        CHECK (status IN ('active','suspended','closed')),
    enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0,1)),
    activity_version INTEGER NOT NULL DEFAULT 0,
    event_seq INTEGER NOT NULL DEFAULT 0,
    route_version INTEGER NOT NULL DEFAULT 0,
    policy_version INTEGER NOT NULL DEFAULT 0,
    notice_reservations INTEGER NOT NULL DEFAULT 0 CHECK (notice_reservations >= 0),
    notice_attempts INTEGER NOT NULL DEFAULT 0 CHECK (notice_attempts >= 0),
    max_notice_attempts INTEGER NOT NULL,
    max_items INTEGER NOT NULL,
    dry_run_notice_attempts INTEGER NOT NULL DEFAULT 0,
    route_json TEXT,
    policy_json TEXT NOT NULL,
    work_token TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    PRIMARY KEY (profile_id, conversation_id),
    UNIQUE (profile_id, current_session_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_temporal_active_route
ON temporal_conversations(profile_id, session_key)
WHERE status = 'active' AND session_key IS NOT NULL;

CREATE TABLE IF NOT EXISTS temporal_session_bindings (
    profile_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    relation TEXT NOT NULL CHECK (relation IN ('origin','compression','resume')),
    created_at REAL NOT NULL,
    PRIMARY KEY (profile_id, session_id),
    FOREIGN KEY (profile_id, conversation_id) REFERENCES temporal_conversations(profile_id, conversation_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_temporal_bindings_conversation
ON temporal_session_bindings(profile_id, conversation_id);

CREATE TABLE IF NOT EXISTS temporal_items (
    profile_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    item_key TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('draft','active','paused','resolved','cancelled')),
    review_at REAL,
    lease_until REAL,
    lease_token TEXT,
    revision INTEGER NOT NULL,
    pending_notice_id TEXT,
    body_json TEXT NOT NULL,
    PRIMARY KEY (profile_id, conversation_id, item_key),
    FOREIGN KEY (profile_id, conversation_id) REFERENCES temporal_conversations(profile_id, conversation_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_temporal_due
ON temporal_items(profile_id, review_at, conversation_id)
WHERE status = 'active' AND review_at IS NOT NULL AND pending_notice_id IS NULL;
CREATE INDEX IF NOT EXISTS idx_temporal_scope
ON temporal_items(profile_id, conversation_id, status);
CREATE INDEX IF NOT EXISTS idx_temporal_drafts
ON temporal_items(profile_id, json_extract(body_json,'$.created_at')) WHERE status='draft';
CREATE INDEX IF NOT EXISTS idx_temporal_process_events
ON temporal_items(profile_id, conversation_id, item_key)
WHERE status IN ('active','paused')
AND json_extract(body_json,'$.external_reference.adapter')='process_registry'
AND json_extract(body_json,'$.external_reference.completed')=0;

CREATE TABLE IF NOT EXISTS temporal_notifications (
    notice_id TEXT PRIMARY KEY,
    profile_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('ready','sending','sent','failed','unknown','cancelled','dry_run')),
    mode TEXT NOT NULL CHECK (mode IN ('live','dry_run')),
    item_keys_json TEXT NOT NULL,
    item_revisions_json TEXT NOT NULL,
    next_review_json TEXT NOT NULL,
    activity_version INTEGER NOT NULL,
    route_version INTEGER NOT NULL,
    policy_version INTEGER NOT NULL,
    route_snapshot_json TEXT NOT NULL,
    reservation_state TEXT NOT NULL CHECK (reservation_state IN ('reserved','consumed','released','none')),
    message TEXT NOT NULL,
    created_at REAL NOT NULL,
    send_started_at REAL,
    send_token TEXT,
    completed_at REAL,
    transport_message_id TEXT,
    error_text TEXT,
    late_receipt_json TEXT,
    FOREIGN KEY (profile_id, conversation_id) REFERENCES temporal_conversations(profile_id, conversation_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_temporal_notice_state
ON temporal_notifications(profile_id, state, created_at);
CREATE UNIQUE INDEX IF NOT EXISTS idx_temporal_one_pending_notice
ON temporal_notifications(profile_id, conversation_id)
WHERE state IN ('ready','sending');

CREATE TABLE IF NOT EXISTS temporal_events (
    event_id TEXT PRIMARY KEY,
    profile_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    kind TEXT NOT NULL,
    source TEXT NOT NULL,
    source_event_id TEXT NOT NULL,
    received_at REAL NOT NULL,
    occurred_at REAL,
    body_json TEXT NOT NULL,
    UNIQUE (profile_id, source, source_event_id),
    UNIQUE (profile_id, conversation_id, seq),
    FOREIGN KEY (profile_id, conversation_id) REFERENCES temporal_conversations(profile_id, conversation_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_temporal_events_recent
ON temporal_events(profile_id, conversation_id, seq DESC);

CREATE TABLE IF NOT EXISTS temporal_records (
    record_id TEXT PRIMARY KEY,
    profile_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('preference','view','relationship')),
    content TEXT NOT NULL,
    epistemic_status TEXT NOT NULL CHECK (epistemic_status IN ('explicit','observed','inferred')),
    status TEXT NOT NULL CHECK (status IN ('active','tentative','superseded')),
    source_event_id TEXT NOT NULL REFERENCES temporal_events(event_id) ON DELETE CASCADE,
    quoted_text TEXT NOT NULL,
    previous_id TEXT,
    use_for_followup INTEGER NOT NULL DEFAULT 0 CHECK (use_for_followup IN (0,1)),
    created_at REAL NOT NULL,
    FOREIGN KEY (profile_id, conversation_id) REFERENCES temporal_conversations(profile_id, conversation_id) ON DELETE CASCADE,
    UNIQUE (profile_id, conversation_id, source_event_id, kind, content)
);
CREATE INDEX IF NOT EXISTS idx_temporal_records_scope
ON temporal_records(profile_id, conversation_id, status, kind, created_at);

CREATE TRIGGER IF NOT EXISTS temporal_session_deleted BEFORE DELETE ON presence_host_sessions
BEGIN
    DELETE FROM temporal_conversations WHERE (profile_id, conversation_id) IN
      (SELECT profile_id, conversation_id FROM temporal_session_bindings WHERE session_id = OLD.id);
END;

CREATE TABLE IF NOT EXISTS temporal_topics (
    profile_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    topic_key TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active','paused','closed','expired')),
    updated_at REAL NOT NULL,
    ended_at REAL,
    body_json TEXT NOT NULL,
    PRIMARY KEY (profile_id,conversation_id,topic_key),
    FOREIGN KEY (profile_id,conversation_id) REFERENCES temporal_conversations(profile_id,conversation_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_temporal_topics_age ON temporal_topics(profile_id,status,updated_at);
"""
