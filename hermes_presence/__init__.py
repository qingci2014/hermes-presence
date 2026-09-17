"""Portable Presence storage. Gateway plugin entrypoint is not shipped yet."""
from .store import PresenceStore

__all__ = ['PresenceStore']
