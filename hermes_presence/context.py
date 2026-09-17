"""Only the authorized host adapter may bind a tool's current user turn."""
from contextvars import ContextVar
from dataclasses import dataclass, field
from .execution import ExecutionWindow


@dataclass(frozen=True)
class PresenceTurn:
    db: object
    scope: object
    session_id: str
    turn_id: str
    handoff_id: str
    event_id: str
    activity_version: int
    policy_version: int
    context_json: str
    resume_authorized: bool = False
    toolsets: tuple = ('presence',)
    execution: object = field(default_factory=ExecutionWindow, compare=False, repr=False)


current_turn = ContextVar('presence_authorized_turn', default=None)
