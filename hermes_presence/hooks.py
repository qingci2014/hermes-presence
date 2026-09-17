"""Native extension registration; no discovery of credentials or global gateway."""


def register_hooks(ctx, app):
    def factory(gateway, api_version, **kwargs):
        if api_version != 1:
            raise RuntimeError('Unsupported Hermes gateway service API')
        from .native import NativeGateway
        return NativeGateway(app, gateway)

    def before_turn(session_id='', parent_session_id='', **kwargs):
        turn = app.bound_turn()
        if turn and not parent_session_id and turn.session_id == session_id:
            turn.execution.identify(turn.db, kwargs.get('model'), app.ctx.get_config('execution_environment_tag', ''))
            return {'context': turn.context_json}

    def after_tool(tool_name='', tool_call_id='', result=None, status='', session_id='', **kwargs):
        turn = app.bound_turn()
        if turn and turn.session_id == session_id:
            turn.execution.observe(tool_name, tool_call_id, result, status)

    def before_tool(tool_name='', tool_input=None, session_id='', **kwargs):
        turn = app.bound_turn()
        if turn and session_id == turn.session_id:
            turn.execution.tool_started(tool_name, kwargs.get('tool_call_id', ''))
        args = tool_input or kwargs.get('args') or kwargs.get('arguments') or {}
        if tool_name != 'memory' or not turn or session_id != turn.session_id:
            return
        if args.get('target') != 'user':
            return
        operations = args.get('operations')
        writes = (any(isinstance(op, dict) and op.get('action') in ('add', 'replace') for op in operations)
                  if isinstance(operations, list) else args.get('action') in ('add', 'replace'))
        if writes:
            return {'action': 'block', 'message': 'Use temporal_commitment remember/recall with the exact current user quote for personal records. No global user memory was written.'}

    if not callable(getattr(ctx, 'register_hook', None)):
        return
    ctx.register_hook('pre_llm_call', before_turn)
    ctx.register_hook('pre_tool_call', before_tool)
    ctx.register_hook('post_tool_call', after_tool)
    from hermes_cli.plugins import VALID_HOOKS
    if 'gateway_service' in VALID_HOOKS:
        ctx.register_hook('gateway_service', factory)
    else:
        app.last_error = 'gateway_bridge_v1_required'
