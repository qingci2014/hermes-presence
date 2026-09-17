"""Plugin-owned controls and scheduler lifecycle; no global gateway discovery."""
import asyncio
import json
import threading
import uuid

from .common import dumps
from .context import current_turn
from .lease import GatewayLease
from .ingress import PresenceIngress
from .protocol import TemporalError
from .runtime import ReviewCapacity, TemporalRuntime, _REVIEW_SYSTEM
from .settings import policy_from_settings
from .store import PresenceStore
from .tool import temporal_tool


class PresenceApp(PresenceIngress):
    def __init__(self, ctx):
        self.ctx = ctx
        self.policy = policy_from_settings(ctx.get_config('temporal', {}))
        self.store = PresenceStore(ctx.state.data_dir)
        self.host = self.runtime = self.capacity = self.task = None
        self.owner_id = uuid.uuid4().hex
        self.closed = False
        self._closing = False
        self._lock = threading.RLock()
        self._loop = None
        self._lease = None
        self._gates = {}
        self.last_error = None
        self._defer_close = False

    @property
    def gateway_ready(self):
        return self.host is not None and not self.closed and not self._closing

    def status(self):
        state = 'disabled' if not self.policy.enabled else 'waiting_for_gateway_adapter'
        if self.gateway_ready and self.policy.enabled:
            state = 'running' if self.task and not self.task.done() else 'gateway_attached'
        return {'enabled': self.policy.enabled, 'mode': self.policy.mode, 'state': state,
                'gateway_ready': self.gateway_ready, 'last_error': self.last_error}

    def command(self, raw_args):
        args = (raw_args or 'status').split()
        action = args[0].lower()
        try:
            if action in ('on', 'off') and len(args) == 1:
                self.set_enabled(action == 'on')
                return 'Temporal: ' + dumps(self.status())
            if action == 'status' and len(args) == 1:
                return 'Temporal: ' + dumps(self.status())
            if action == 'usage' and len(args) == 1:
                usage = self.store.review_usage(self.policy.max_daily_reviews)
                known = usage['total_tokens']
                return (f"Temporal · 最近 24 小时\n后台评估：{usage['attempts']}/{usage['limit']} 次，剩余 {usage['remaining']} 次。\n"
                        f"已知 token：{known if known is not None else '暂无数据'}；"
                        f"用量未知：{usage['unknown_usage_requests'] or 0} 次。\n"
                        "仅统计本插件后台评估，包含失败/撤销的预留；不含普通聊天和原生记忆复盘，不等同于账单。")
            turn = self.bound_turn()
            if turn is None:
                return 'Temporal: a verified conversation is required; no personal records are exposed from this context.'
            from .controls import topic_command, record_command
            if action in ('topics', 'topic'):
                return topic_command(self.store, turn.scope, args)
            if action in ('records', 'record', 'forget'):
                return record_command(self.store, turn.scope, args)
            if action == 'show' and len(args) == 2:
                return 'Temporal: ' + dumps(self.store.temporal_get_item(turn.scope, args[1]))
            if action in ('pause', 'resume', 'cancel') and len(args) == 2:
                result = self.store.temporal_mutate_item(turn.scope, args[1], action,
                    resume_authorized=True, after_seconds=self.policy.user_activity_backoff_seconds)
                return 'Temporal: ' + dumps(result)
            if action in ('pause-all', 'resume-all') and len(args) == 1:
                result = self.store.temporal_control_all(turn.scope, action.split('-')[0],
                    after_seconds=self.policy.user_activity_backoff_seconds)
                return 'Temporal: ' + dumps(result)
            views = {'list': lambda: self.store.temporal_list_items(turn.scope, include_terminal=True)}
            if action in views and len(args) == 1:
                return 'Temporal: ' + dumps(views[action]())
            return '/temporal status|usage|on|off|list|show KEY|pause KEY|resume KEY|cancel KEY|pause-all|resume-all|topics|topic KEY|records [query]|record ID|forget ID'
        except (TemporalError, ValueError) as exc:
            return 'Temporal: ' + str(exc)

    def set_enabled(self, enabled):
        with self._lock:
            if self.closed or self._closing:
                raise TemporalError('Presence is unloading')
            self.ctx.set_config('temporal.enabled', enabled)
            from dataclasses import replace
            self.policy = replace(self.policy, enabled=enabled)
            if self.runtime:
                self.runtime.policy = self.policy
            after = ''
            while scopes := self.store.temporal_scopes(self.store.profile_id, after=after):
                for scope in scopes:
                    self.store.temporal_apply_policy(scope, self.policy)
                after = scopes[-1].conversation_id

    def bound_turn(self):
        turn = current_turn.get()
        if turn is None:
            try:
                from gateway.extension_services import current_gateway_extensions
                turn = (current_gateway_extensions.get() or {}).get('hermes-presence')
            except ImportError:
                pass
        if not self.gateway_ready or turn is None or turn.db is not self.store:
            return None
        scope = self.store.temporal_scope_for_session(self.store.profile_id, turn.session_id)
        if scope != turn.scope or self.store.temporal_conversation(scope)['current_session_id'] != turn.session_id:
            return None
        return turn

    def tool(self, args, **kwargs):
        turn = self.bound_turn()
        # Hermes registry supplies session_id outside model-generated args.
        # Delegated agents inherit ContextVars, but not the parent's session ID.
        if turn is not None and kwargs.get('session_id') != turn.session_id:
            turn = None
        return temporal_tool(args, turn=turn,
                             process_reference=getattr(self.host, 'process_reference', None))

    def evaluate(self, snapshot, timeout):
        response = self.ctx.llm.complete(
            messages=[{'role': 'system', 'content': _REVIEW_SYSTEM},
                      {'role': 'user', 'content': dumps(snapshot)}],
            max_tokens=1800, timeout=timeout, purpose='presence_review')
        return response

    def gate_for(self, key):
        gate = self.host.gate_for(key)
        self._gates[key] = gate
        return gate

    async def attach_gateway(self, host):
        """Called by the host adapter on its event loop, never by a model tool.

        Required methods: gate_for, busy, route_adapter, route_current, redact.
        Host owns authorization, live route validation and input/handoff events.
        """
        with self._lock:
            if self.closed or self._closing:
                raise TemporalError('Presence is unloading')
            if self.host is host:
                return  # Same start signal must not spawn another scheduler.
            if self.host is not None:
                raise TemporalError('another gateway already owns this Presence instance')
            required = ('gate_for', 'busy', 'route_adapter', 'route_current', 'redact')
            if any(not callable(getattr(host, name, None)) for name in required):
                raise TemporalError('gateway adapter lacks required delivery and lifecycle methods')
            if getattr(host, 'verified_delivery_contract', False) is not True:
                raise TemporalError('gateway has not verified its delivery contract')
            self._loop = asyncio.get_running_loop()
            self._lease = GatewayLease(self.store.data_dir)
            self.capacity = ReviewCapacity(self.policy.max_concurrent_reviews)
            self.runtime = TemporalRuntime(self.store, self.store.profile_id, self.policy,
                gate_for=self.gate_for, busy=lambda conv: self._closing or host.busy(conv), route_adapter=host.route_adapter,
                route_current=host.route_current, owner_id=self.owner_id, capacity=self.capacity,
                evaluator=self.evaluate, redact=host.redact)
            self.host = host
            coroutine = self._run()
            try:
                self.task = self.ctx.spawn_task(coroutine, name='presence-review-loop')
            except BaseException:
                coroutine.close()
                self.host = None
                self.capacity.close()
                self._lease.close()
                raise
            # A task cancelled before its first step never enters _run's finally.
            self.task.add_done_callback(lambda _: self._finalize())

    async def _run(self):
        try:
            while not self._closing:
                delay = self.policy.poll_interval_seconds
                try:
                    housekeeping = getattr(self.host, 'housekeeping', None)
                    if housekeeping:
                        await housekeeping()
                    await self.runtime.tick()
                    self.last_error = None
                except Exception as exc:
                    self.last_error = type(exc).__name__
                    delay = min(60, self.policy.failure_backoff_seconds)
                await asyncio.sleep(delay)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.last_error = type(exc).__name__
        finally:
            try:
                await self.runtime.close()
                # Retain ownership while an already dispatched transport can still
                # publish a late receipt. Unload prevents all NEW review/send work.
                if self.runtime.transports:
                    await asyncio.gather(*list(self.runtime.transports), return_exceptions=True)
                    await asyncio.sleep(0)
                if self.runtime.settlements:
                    await asyncio.gather(*list(self.runtime.settlements), return_exceptions=True)
            finally:
                self._finalize()

    def _finalize(self):
        if self.closed:
            return
        if self._defer_close:
            return
        if self.capacity:
            self.capacity.close()
        self.host = None
        self.store.close()
        if self._lease:
            self._lease.close()
        self.closed = True

    def unload(self):
        """Synchronous loader teardown fences new calls before cancelling its worker."""
        with self._lock:
            if self.closed or self._closing:
                return
            self._closing = True
            if self.runtime:
                from dataclasses import replace
                self.runtime.policy = replace(self.runtime.policy, enabled=False)
            if self.task and not self.task.done():
                self._loop.call_soon_threadsafe(self._fence_and_cancel)
            elif self.task is None or self.task.done():
                self.store.close()
                self.closed = True

    def _fence_and_cancel(self):
        for gate in list(self._gates.values()):
            gate.input_started()
            if self._defer_close:
                gate.input_persisted()  # Revoke workers without inventing an unpersisted user input.
        if self.task and not self.task.done() and not self.task.cancelling():
            self.task.cancel()
