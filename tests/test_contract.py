"""Synthetic contract checks; no live Hermes imports, network or credentials."""
import ast
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from hermes_presence.app import PresenceApp
from hermes_presence.hooks import register_hooks
from hermes_presence.plugin import register


class Context:
    def __init__(self, root):
        self.state = types.SimpleNamespace(data_dir=Path(root))
        self.hooks = {}
        self.tools = {}
        self.commands = {}
        self.sections = {}
        self.cleanup = []

    def get_config(self, key, default=None):
        return default

    def set_config(self, key, value):
        raise AssertionError('No config changes expected')

    def on_unload(self, callback):
        self.cleanup.append(callback)

    def spawn_task(self, *args, **kwargs):
        raise AssertionError('No task should start on an unsupported host')

    def register_command(self, name, handler, *args):
        self.commands[name] = handler
        return object()

    def register_tool(self, name, **kwargs):
        self.tools[name] = kwargs
        return object()

    def register_system_prompt_section(self, key, text):
        self.sections[key] = text

    def register_hook(self, name, callback):
        self.hooks[name] = callback


def modules(hooks):
    plugin_host = types.ModuleType('hermes_cli.plugins')
    plugin_host.VALID_HOOKS = hooks
    registry = types.ModuleType('tools.registry')
    registry.no_cache_check_fn = lambda fn: fn
    return {'hermes_cli': types.ModuleType('hermes_cli'),
            'hermes_cli.plugins': plugin_host,
            'tools': types.ModuleType('tools'), 'tools.registry': registry}


class Contracts(unittest.TestCase):
    def test_unsupported_host_is_inert(self):
        with tempfile.TemporaryDirectory() as tmp, patch.dict(sys.modules, modules(set())):
            ctx = Context(tmp)
            app = register(ctx)
            try:
                self.assertFalse(app.gateway_ready)
                self.assertEqual(app.status()['state'], 'waiting_for_gateway_adapter')
                self.assertEqual(app.last_error, 'gateway_bridge_v1_required')
                self.assertFalse(ctx.tools['temporal_commitment']['check_fn']())
                self.assertIsNone(app.task)
                self.assertEqual(set(ctx.hooks), {'pre_llm_call', 'pre_tool_call', 'post_tool_call'})
                self.assertEqual(set(ctx.commands), {'temporal'})
                self.assertEqual(set(ctx.sections), {'presence.continuity', 'presence.time'})
                self.assertIn('verified conversation is required', app.command('records'))
            finally:
                app.unload()

    def test_bridge_hook_is_conditional_and_version_checked(self):
        with tempfile.TemporaryDirectory() as tmp, patch.dict(sys.modules, modules({'gateway_service'})):
            ctx = Context(tmp)
            app = PresenceApp(ctx)
            try:
                register_hooks(ctx, app)
                self.assertIn('gateway_service', ctx.hooks)
                with self.assertRaisesRegex(RuntimeError, 'Unsupported'):
                    ctx.hooks['gateway_service'](None, 2)
                self.assertFalse(app.gateway_ready)
            finally:
                app.unload()

    def test_missing_context_api_rejected(self):
        with self.assertRaisesRegex(RuntimeError, 'newer Hermes plugin APIs'):
            register(object())

    def test_all_runtime_modules_parse(self):
        root = Path(__file__).resolve().parents[1]
        for file in (root / 'hermes_presence').glob('*.py'):
            with self.subTest(file=file.name):
                ast.parse(file.read_text(encoding='utf-8'))


if __name__ == '__main__':
    unittest.main()
