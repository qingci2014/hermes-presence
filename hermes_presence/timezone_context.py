"""Resolve a usable IANA name without changing host configuration or clock."""
from datetime import datetime
import logging
import os
from pathlib import Path
from zoneinfo import TZPATH, ZoneInfo, ZoneInfoNotFoundError

logger = logging.getLogger(__name__)
LOCALTIME = Path('/etc/localtime')
TIMEZONE_FILE = Path('/etc/timezone')


def _iana_name(value):
    if not isinstance(value, str) or not value.strip():
        return None
    name = value.strip()
    try:
        return ZoneInfo(name).key
    except (ValueError, ZoneInfoNotFoundError):
        return None


def _system_timezone_name():
    # TZ can be an IANA name or a colon-prefixed zoneinfo filename. Never infer
    # an IANA region from an ambiguous abbreviation such as CST or UTC offset.
    env = os.environ.get('TZ', '').lstrip(':')
    if name := _iana_name(env):
        return name
    if name := _iana_name(getattr(datetime.now().astimezone().tzinfo, 'key', None)):
        return name
    for path in (Path(env) if env.startswith('/') else LOCALTIME, LOCALTIME):
        try:
            resolved = path.resolve(strict=True)
            for root in TZPATH:
                try:
                    relative = resolved.relative_to(Path(root).resolve()).as_posix()
                except ValueError:
                    continue
                if name := _iana_name(relative):
                    return name
        except (OSError, RuntimeError):
            pass
    try:
        if name := _iana_name(TIMEZONE_FILE.read_text(encoding='utf-8').strip()):
            return name
    except (OSError, UnicodeError):
        pass
    # Optional host dependency (also handles Windows registry → IANA mapping).
    # Do not install packages or guess a region when discovery is unavailable.
    try:
        from tzlocal import get_localzone_name
        return _iana_name(get_localzone_name())
    except (ImportError, OSError, ValueError, KeyError, RuntimeError):
        return None


def context_timezone(configured=None):
    if name := _iana_name(configured):
        return name
    from hermes_time import get_timezone_name
    if name := _iana_name(get_timezone_name()):
        return name
    if name := _system_timezone_name():
        return name
    logger.warning('Presence could not identify the local IANA timezone; using explicit UTC context')
    return 'Etc/UTC'
