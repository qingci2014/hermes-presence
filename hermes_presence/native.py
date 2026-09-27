"""Hermes gateway bridge v1 adapter. Business data stays in PresenceStore."""
import asyncio
from dataclasses import replace
from functools import partial
import json
import logging
import time

from .common import dumps
from .protocol import TemporalError, iso_time
from .runtime import TemporalGate


class FeishuTransport:
    def __init__(self, adapter):
        self.adapter = adapter

    @property
    def account_id(self):
        return getattr(self.adapter, '_app_id', None)

    @property
    def ready(self):
        return bool(self.account_id and getattr(self.adapter, '_client', None)
                    and getattr(self.adapter, '_app_lock_identity', None) == self.account_id)

    async def temporal_send_once(self, route, message, send_token):
        a = self.adapter
        if (not self.ready or route.get('account_id') != self.account_id
                or not isinstance(message, str) or not message or len(message) > a.MAX_MESSAGE_LENGTH):
            return {'outcome': 'failed', 'error': 'invalid_route_or_message'}
        try:
            from .feishu_delivery import send_request_once
            chat, thread = route['chat_id'], route.get('thread_id')
            if thread:
                target, kind = thread, 'thread_id'
            elif chat.startswith('feishu_user_id:'):
                target, kind = chat.split(':', 1)[1], 'user_id'
            else:
                target, kind = chat, 'open_id' if chat.startswith('ou_') else 'chat_id'
            body = a._build_create_message_body(receive_id=target, msg_type='text',
                content=json.dumps({'text': message}, ensure_ascii=False), uuid_value=send_token)
            request = a._build_create_message_request(kind, body)
            return await a._run_blocking(send_request_once, a._client.im.v1.message.config, request)
        except Exception as exc:
            return {'outcome': 'unknown', 'error': type(exc).__name__}


class NativeGateway:
    plugin_id = 'hermes-presence'
    verified_delivery_contract = True

    def __init__(self, app, gateway):
        self.app, self.gateway = app, gateway
        self.gates = {}
        self.checkpoints = []
        self.restoring = True
        self._session_cursor = ''

    def gate_for(self, key):
        return self.gates.setdefault(key, TemporalGate())

    def transport(self, source):
        # Deliberately certify only the platform whose single-send path is tested.
        if getattr(source.platform, 'value', source.platform) != 'feishu':
            return None
        adapter = self.gateway._adapter_for_source(source)
        return FeishuTransport(adapter) if adapter is not None else None

    def route(self, source):
        if source.chat_type not in ('dm', 'private') or not self.gateway._is_user_authorized_for_source(source):
            raise TemporalError('an authorized private conversation is required')
        transport = self.transport(source)
        if transport is None or not transport.ready:
            raise TemporalError('this platform does not have a verified Presence delivery adapter')
        return {'platform': source.platform.value, 'account_id': transport.account_id,
                'chat_id': str(source.chat_id), 'thread_id': str(source.thread_id) if source.thread_id else None,
                'user_id': source.user_id, 'user_id_alt': source.user_id_alt, 'chat_type': source.chat_type,
                'owner_id': self.app.owner_id,
                'authorized': True, 'live_capable': True}

    def route_adapter(self, conv, *, previous_owner=False):
        from gateway.config import Platform
        from gateway.session import SessionSource
        route = json.loads(conv['route_json'] or '{}')
        if (not route.get('authorized') or not route.get('platform')
                or (not previous_owner and route.get('owner_id') != self.app.owner_id)):
            return None
        source = SessionSource(platform=Platform(route['platform']), chat_id=route['chat_id'],
            thread_id=route.get('thread_id'), profile=route.get('profile'), user_id=route.get('user_id'),
            user_id_alt=route.get('user_id_alt'), chat_type=route.get('chat_type', 'dm'))
        if not self.gateway._is_user_authorized_for_source(source):
            return None
        transport = self.transport(source)
        return transport if transport and transport.ready and transport.account_id == route.get('account_id') else None

    async def route_current(self, conv):
        return bool(conv['session_key'] and await self.gateway.async_session_store.peek_session_id(
                    conv['session_key']) == conv['current_session_id'])

    def busy(self, conv):
        g, key = self.gateway, conv['session_key']
        transport = self.route_adapter(conv)
        return (self.restoring or getattr(g, '_draining', False)
                or getattr(g, '_startup_restore_in_progress', False)
                or g._is_session_running(key) or transport is None
                or any(key in getattr(transport.adapter, name, {}) for name in ('_active_sessions', '_pending_messages'))
                or any(key in getattr(g, name, {}) for name in ('_pending_messages', '_queued_events')))

    @staticmethod
    def redact(text):
        from agent.redact import redact_sensitive_text
        return redact_sensitive_text(text)

    async def start(self):
        if getattr(self.gateway.config, 'multiplex_profiles', False):
            raise TemporalError('Presence v0.1 requires one gateway process per Hermes profile')
        # Acquire the file lease before touching persisted ownership. No second scheduler.
        await self.app.attach_gateway(self)
        try:
            after = ''
            while scopes := await asyncio.to_thread(self.app.store.temporal_scopes, self.app.store.profile_id, after=after):
                for scope in scopes:
                    conv = await asyncio.to_thread(self.app.store.temporal_conversation, scope)
                    route = json.loads(conv['route_json'] or '{}')
                    if route.get('owner_id') == self.app.owner_id:
                        continue
                    transport = self.route_adapter(conv, previous_owner=True)
                    verified = bool(transport and await self.route_current(conv))
                    restored = verified and await asyncio.to_thread(self.app.store.temporal_restore_checkpoint,
                        scope, self.app.owner_id, self.app.policy)
                    await asyncio.to_thread(self.app.store.temporal_recover, scope, inputs_verified=restored,
                        route_verified=restored, previous_owner_stopped=bool(transport))
                after = scopes[-1].conversation_id
            await self.housekeeping()
            self.restoring = False
            logging.getLogger('gateway.run').info('Presence gateway service ready: enabled=%s mode=%s api=1',
                                                  self.app.policy.enabled, self.app.policy.mode)
        except BaseException:
            self.app.unload()
            raise

    async def accept_event(self, event, source, internal):
        if internal or self.app._closing:
            return None
        key = self.gateway._session_key_for_source(source)
        old = getattr(event, '_presence_turn', None)
        if getattr(event, '_presence_accepted_key', None) == key:
            return old
        event._presence_accepted_key = key
        gate = self.gate_for(key)
        gate.input_started()
        route = self.route(source)
        entry = await self.gateway.async_session_store.get_or_create_session(source)
        # SessionStore creates the binding; native SessionDB registration normally happens
        # later in the agent path. Register it here before owning an external reference.
        wrapper = self.gateway._session_db
        db = getattr(wrapper, '_db', wrapper)
        await asyncio.to_thread(db.ensure_session, entry.session_id, source=source.platform.value)
        from .transfer import rebind_transfer
        await asyncio.to_thread(rebind_transfer, self.app.store, entry.session_id, key, route)
        turn = await self.app.accept_user_event(session_id=entry.session_id, session_key=key,
            route=route, message_id=str(event.message_id or ''), text=event.text or '')
        gate.input_persisted()
        event._presence_turn = turn
        command = (event.text or '').split(maxsplit=1)[0].lower() if event.text else ''
        if command in ('/new', '/reset'):
            event._presence_reset = await self.app.begin_reset(key)
        elif command in ('/clear', '/resume', '/branch'):
            await asyncio.to_thread(self.app.store.temporal_suspend, turn.scope)
        return turn

    async def prepare_turn(self, event, entry):
        turn = getattr(event, '_presence_turn', None)
        if not turn or turn.session_id != entry.session_id or not self.app.policy.enabled:
            return None
        conv = await asyncio.to_thread(self.app.store.temporal_conversation, turn.scope)
        if conv['activity_version'] != turn.activity_version:
            return None  # A newer busy input revoked this turn's authority.
        page = await asyncio.to_thread(self.app.store.temporal_context, turn.scope, on_demand=True)
        from .time_context import conversation_time
        page['conversation_time'] = await asyncio.to_thread(
            conversation_time, self.app.store, turn.scope, turn.event_id)
        from .timezone_context import context_timezone
        turn = replace(turn, context_json=dumps({'temporal': {'now_utc': iso_time(time.time()),
            'timezone': context_timezone(self.app.ctx.get_config('timezone', None)), 'conversation_enabled': bool(conv['enabled']),
            'mode': self.app.policy.mode, **page}}))
        event._presence_turn = turn
        return turn

    async def complete_handoff(self, event, receipt):
        turn = getattr(event, '_presence_turn', None)
        if turn is not None and not self.app.closed:
            await self.app.complete_handoff(turn, receipt)

    async def finish_reset(self, event, entry):
        reset = getattr(event, '_presence_reset', None)
        if reset and entry:
            wrapper = self.gateway._session_db
            await asyncio.to_thread(getattr(wrapper, '_db', wrapper).ensure_session,
                                   entry.session_id, source=event.source.platform.value)
            await self.app.finish_reset(reset, entry.session_id, self.route(event.source))

    async def housekeeping(self):
        # Bounded reconciliation preserves native session deletion semantics across stores.
        wrapper = self.gateway._session_db
        db = getattr(wrapper, '_db', wrapper)
        rows = await asyncio.to_thread(self.app.store._read_all,
            'SELECT id FROM presence_host_sessions WHERE id>? ORDER BY id LIMIT 100', (self._session_cursor,))
        from .transfer import detached_sessions
        detached = await asyncio.to_thread(detached_sessions, self.app.store)
        for row in rows:
            if row[0] not in detached and await asyncio.to_thread(db.get_session, row[0]) is None:
                await asyncio.to_thread(self.app.store.host_session_deleted, row[0])
        self._session_cursor = rows[-1][0] if len(rows) == 100 else ''

    async def stop(self):
        if self.app.closed:
            return
        self.app._defer_close = True
        self.app.unload()
        if self.app.task:
            done, _ = await asyncio.wait({self.app.task}, timeout=10)
            if not done:
                return
        runtime = self.app.runtime
        if runtime.transports or runtime.settlements or runtime.tasks or runtime.clock.frozen:
            return
        after = ''
        while scopes := await asyncio.to_thread(self.app.store.temporal_scopes, self.app.store.profile_id, after=after):
            for scope in scopes:
                conv = await asyncio.to_thread(self.app.store.temporal_conversation, scope)
                key = conv['session_key']
                gate = self.gate_for(key)
                transport = self.route_adapter(conv)
                if (not key or gate.pending or gate.work_token or not transport
                        or self.gateway._is_session_running(key)
                        or any(key in getattr(transport.adapter, n, {}) for n in ('_active_sessions', '_pending_messages'))
                        or any(key in getattr(self.gateway, n, {}) for n in ('_pending_messages', '_queued_events'))):
                    continue
                self.checkpoints.append((scope, conv['event_seq'], gate, gate.generation))
            after = scopes[-1].conversation_id

    async def checkpoint(self, *, clean):
        try:
            if clean and self.app.task and self.app.task.done():
                for scope, seq, gate, generation in self.checkpoints:
                    await asyncio.to_thread(self.app.store.temporal_checkpoint, scope,
                        expected_event_seq=seq, commit_guard=partial(gate.guard, generation))
        finally:
            self.checkpoints.clear()
            self.app._defer_close = False
            if self.app.task is None or self.app.task.done():
                self.app._finalize()
