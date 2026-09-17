"""Defaults for a fresh plugin install; explicit user settings always win."""
from .protocol import TemporalPolicy


def policy_from_settings(settings=None):
    # Registration will pass ctx.get_config('temporal', {}) here. Existing
    # database policy is migrated separately, never replaced by these defaults.
    if settings is None:
        settings = {}
    if not isinstance(settings, dict):
        return TemporalPolicy.from_dict(settings)  # Shared validation/error type.
    return TemporalPolicy.from_dict({'enabled': True, 'dry_run': False, **settings})
