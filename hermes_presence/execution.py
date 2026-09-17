"""Measured, same-turn work samples. No inferred durations or extra model calls."""
import hashlib
import json
import statistics
import sys
import threading
import time
import uuid

from .common import SCOPE, append_event, conversation
from .protocol import TemporalError, text_field


class ExecutionWindow:
    def __init__(self):
        self.lock = threading.RLock()
        self.runtime = None
        self.model = None
        self.started = self.finished = None
        self.results = []
        self.pending_tools = set()
        self.closed = False

    def identify(self, db, model, environment_tag=''):
        if not isinstance(model, str) or not model:
            return
        with self.lock:
            def write(conn):
                conn.execute('INSERT OR IGNORE INTO presence_meta VALUES (?,?)',
                             ('execution_environment_id', uuid.uuid4().hex))
                return conn.execute("SELECT value FROM presence_meta WHERE key='execution_environment_id'").fetchone()[0]
            identity = db._execute_write(write)
            runtime = hashlib.sha256(json.dumps([identity, sys.platform, model, environment_tag]).encode()).hexdigest()
            if self.runtime and runtime != self.runtime:
                self.closed = True  # Mid-run model changes cannot share a timing sample.
            self.runtime, self.model = runtime, model

    def observe(self, tool_name, tool_call_id, result, status):
        with self.lock:
            started_here = (tool_name, tool_call_id) in self.pending_tools
            self.pending_tools.discard((tool_name, tool_call_id))
            if self.closed or not self.started or self.finished or status != 'ok' or not started_here:
                return
            if tool_name in ('temporal_commitment', 'memory', 'tool_search', 'tool_describe'):
                return
            text = result if isinstance(result, str) else json.dumps(result, ensure_ascii=False)
            # Transient evidence only. Persist just the explicitly selected short quote.
            self.results.append({'tool': tool_name, 'id': tool_call_id, 'text': text[:12000]})
            self.results = self.results[-8:]

    def tool_started(self, tool_name, tool_call_id):
        with self.lock:
            if self.started and not self.finished and not self.closed and tool_call_id:
                self.pending_tools.add((tool_name, tool_call_id))

    def close(self):
        with self.lock:
            self.closed = True
            self.results.clear()
            self.pending_tools.clear()


def history(turn, task_type, *, now=None):
    text_field(task_type, 'task type', 80)
    window = turn.execution
    if not window.runtime or window.closed:
        raise TemporalError('host model/runtime identity unavailable for execution timing')
    now = time.time() if now is None else now
    rows = turn.db._read_all(f'SELECT body_json FROM temporal_events WHERE {SCOPE} '
        "AND kind='execution_finished' AND received_at>? "
        "AND json_extract(body_json,'$.runtime')=? AND json_extract(body_json,'$.task_type')=? "
        "AND json_extract(body_json,'$.outcome')='completed' ORDER BY seq DESC LIMIT 20",
        (*turn.scope.sql, now-30*86400, window.runtime, task_type))
    samples = [json.loads(row[0]) for row in rows]
    elapsed = [sample['elapsed_seconds'] for sample in samples]
    ratios = [sample['elapsed_seconds']/sample['predicted_seconds'] for sample in samples]
    return {
        'task_type': task_type, 'sample_count': len(samples), 'model': window.model,
        'measured_range_seconds': [min(elapsed), max(elapsed)] if elapsed else None,
        'median_seconds': statistics.median(elapsed) if elapsed else None,
        'median_actual_to_predicted_ratio': statistics.median(ratios) if ratios else None,
        'samples': [{k: sample[k] for k in ('criteria', 'predicted_seconds', 'elapsed_seconds', 'completion_assessment')}
                    for sample in samples[:5]],
        'limits': 'Only same-turn measured work in this local environment/model, last 30 days. '
                  'Includes tool/network waits inside the run, excludes waiting for user replies. '
                  'Compare scope and acceptance criteria before using these descriptive statistics. '
                  'Not a calibrated probability or proof an entire project completed.',
    }


def execution_tool(turn, action, args):
    window = turn.execution
    with window.lock:
        if action == 'work_history':
            return {'execution_history': history(turn, args['task_type'])}
        if window.closed or not window.runtime:
            raise TemporalError('execution window unavailable or closed')
        if action == 'work_start':
            task_type = text_field(args['task_type'], 'task type', 80)
            criteria = text_field(args['criteria'], 'acceptance criteria', 500)
            prediction = args['predicted_seconds']
            if type(prediction) is not int or not 1 <= prediction <= 604800:
                raise TemporalError('predicted_seconds must be an integer from 1 to 604800')
            if window.started:
                if (task_type, criteria, prediction) != tuple(window.started[k] for k in ('task_type', 'criteria', 'predicted_seconds')):
                    raise TemporalError('only one measured work segment per turn; original prediction is immutable')
                return {'execution': execution_receipt(window.started)}
            body = dict(task_type=task_type, criteria=criteria, predicted_seconds=prediction,
                        runtime=window.runtime, model=window.model, turn_id=turn.turn_id,
                        started_at=time.time())
            _write_event(turn, 'execution_started', body)
            window.started = {**body, 'monotonic': time.monotonic()}
            return {'execution': execution_receipt(body)}
        if not window.started:
            raise TemporalError('work_start must precede execution; retrospective guesses are not samples')
        if window.finished:
            return {'execution': execution_receipt(window.finished)}
        outcome = args['outcome']
        if outcome not in ('completed', 'failed', 'cancelled', 'waiting'):
            raise TemporalError('invalid execution outcome')
        evidence = None
        if outcome == 'completed':
            # The finish tool itself is pending; other work must have settled.
            if any(name != 'temporal_commitment' for name, _ in window.pending_tools):
                raise TemporalError('other measured tools are still running')
            quote = text_field(args.get('evidence_quote'), 'completion evidence quote', 400)
            evidence = next((dict(tool=r['tool'], tool_call_id=r['id'], quote=quote)
                             for r in reversed(window.results) if quote in r['text']), None)
            if evidence is None:
                raise TemporalError('completion requires an exact quote from a successful tool result observed after work_start')
        elapsed = time.monotonic()-window.started['monotonic']
        if elapsed < 0:
            raise TemporalError('monotonic clock invalid')
        body = {k: v for k, v in window.started.items() if k != 'monotonic'}
        body.update(outcome=outcome, elapsed_seconds=round(elapsed, 3), evidence=evidence,
                    completion_assessment='model_with_tool_evidence' if evidence else 'not_completed')
        _write_event(turn, 'execution_finished', body)
        window.finished = body
        window.results.clear()
        return {'execution': execution_receipt(body)}


def execution_receipt(body):
    return {k: v for k, v in body.items() if k not in ('runtime', 'model', 'turn_id', 'monotonic', 'criteria', 'evidence')}


def _write_event(turn, kind, body):
    def write(conn):
        conv = conversation(conn, turn.scope)
        if (conv['activity_version'], conv['policy_version']) != (turn.activity_version, turn.policy_version):
            raise TemporalError('turn superseded')
        append_event(conn, turn.scope, event_id=uuid.uuid4().hex, source='presence_execution',
                     source_event_id=f'{turn.turn_id}:{kind}', kind=kind, now=time.time(), body=body)
    turn.db._execute_write(write)
