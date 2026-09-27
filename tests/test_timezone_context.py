"""Isolated timezone resolution tests; no real clock/configuration changes."""
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from hermes_presence import timezone_context as tz


class TimezoneContextTests(unittest.TestCase):
    def resolve(self, configured, hermes_value):
        module = types.ModuleType('hermes_time')
        setattr(module, 'get_timezone_name', lambda: hermes_value)
        with patch.dict(sys.modules, {'hermes_time': module}):
            return tz.context_timezone(configured)

    def test_explicit_setting_wins(self):
        self.assertEqual(self.resolve('Asia/Shanghai', 'Europe/London'), 'Asia/Shanghai')

    def test_effective_hermes_timezone_wins_over_system(self):
        with patch.object(tz, '_system_timezone_name', return_value='Asia/Shanghai'):
            self.assertEqual(self.resolve(None, 'Europe/London'), 'Europe/London')

    def test_invalid_settings_fall_back_to_system(self):
        with patch.object(tz, '_system_timezone_name', return_value='Asia/Shanghai'):
            self.assertEqual(self.resolve('CST', 'CST'), 'Asia/Shanghai')

    def test_system_tz_environment(self):
        with patch.dict(tz.os.environ, {'TZ': 'Asia/Shanghai'}):
            self.assertEqual(tz._system_timezone_name(), 'Asia/Shanghai')

    def test_localtime_symlink(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / 'zoneinfo'
            target = root / 'Asia' / 'Shanghai'
            target.parent.mkdir(parents=True)
            target.write_bytes(b'synthetic')
            link = Path(tmp) / 'localtime'
            link.symlink_to(target)
            with patch.dict(tz.os.environ, {'TZ': ''}), patch.object(tz, 'TZPATH', (str(root),)), patch.object(tz, 'LOCALTIME', link):
                self.assertEqual(tz._system_timezone_name(), 'Asia/Shanghai')

    def test_unresolved_timezone_is_explicit_utc(self):
        with patch.object(tz, '_system_timezone_name', return_value=None):
            self.assertEqual(self.resolve(None, 'CST'), 'Etc/UTC')


if __name__ == '__main__':
    unittest.main()
