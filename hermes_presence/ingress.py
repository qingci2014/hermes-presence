"""Authorized input and real delivery boundaries for the gateway adapter."""
import asyncio
import time
import uuid

from .common import dumps
from .context import PresenceTurn
from .protocol import TemporalError, iso_time


class PresenceIngress:
    async def accept_user_event(self, *, session_id, session_key, route, message_id, text):
        """Host-only API; route and session IDs never come from tool arguments.

        The gateway must call after authorization and before dispatching the turn,
        including busy-input paths. Its gate is shared with background commits.
        """
        if not self.gateway_ready:
            raise TemporalError('gateway adapter is unavailable')
        if route.get('authorized') is not True or route.get('live_capable') is not True:
            raise TemporalError('authorized verified route required')
        if route.get('chat_type') not in ('dm', 'private'):
            raise TemporalError('Presence currently requires an explicitly verified private conversation')
        if not isinstance(message_id, str) or not message_id or not isinstance(text, str):
            raise TemporalError('a real host message ID and user text are required')
        gate = self.gate_for(session_key)
        gate.input_started()
        route = {**route, 'owner_id': self.owner_id}
        # On failure keep the gate closed. A later host reconciliation is needed;
        # a model must never reopen permission after failed persistence.
        await asyncio.to_thread(self.store.host_session_created, session_id)
        scope = await asyncio.to_thread(self.store.temporal_bind_origin, self.store.profile_id,
            session_id, session_key, route, self.policy, resume=True)
        await asyncio.to_thread(self.store.temporal_apply_policy, scope, self.policy)
        namespace = dumps([route['platform'], route['account_id'], route['chat_id'], route.get('thread_id')])
        event = await asyncio.to_thread(self.store.temporal_accept_user_event,
            scope, namespace, message_id, text)
        conv = await asyncio.to_thread(self.store.temporal_conversation, scope)
        context = dumps({'temporal': {'now_utc': iso_time(time.time()),
                         'conversation_enabled': bool(conv['enabled']), 'mode': self.policy.mode}})
        turn = PresenceTurn(self.store, scope, session_id, uuid.uuid4().hex, uuid.uuid4().hex,
            event['event_id'], conv['activity_version'], conv['policy_version'], context)
        gate.input_persisted()
        return turn

    async def complete_handoff(self, turn, receipt):
        if turn.db is not self.store:
            raise TemporalError('handoff belongs to another Presence store')
        turn.execution.close()
        return await asyncio.to_thread(self.store.temporal_handoff_receipt, turn.scope,
            handoff_id=turn.handoff_id, turn_id=turn.turn_id, **receipt)

    async def begin_reset(self, session_key):
        if not self.gateway_ready:
            raise TemporalError('gateway adapter is unavailable')
        gate = self.gate_for(session_key)
        gate.input_started()
        scope = await asyncio.to_thread(self.store.temporal_scope_for_route, self.store.profile_id, session_key)
        if scope is None:
            gate.input_persisted()
            return None
        old_id = await asyncio.to_thread(self.store.temporal_begin_new, scope)
        return scope, old_id, session_key

    async def finish_reset(self, reset, new_session_id, route):
        if reset is None:
            return
        scope, old_id, key = reset
        await asyncio.to_thread(self.store.host_session_created, new_session_id)
        await asyncio.to_thread(self.store.temporal_finish_new, scope, old_id, new_session_id,
                                {**route, 'owner_id': self.owner_id})
        self.gate_for(key).input_persisted()
