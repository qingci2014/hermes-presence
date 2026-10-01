"""Undated continuity contracts; scripted tools only, no model or message transport."""
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from hermes_presence.context import PresenceTurn
from hermes_presence.protocol import TemporalPolicy
from hermes_presence.store import PresenceStore
from hermes_presence.tool import temporal_tool


class OngoingTopics(unittest.TestCase):
    def check_continuity(self, key, quote, development):
        with tempfile.TemporaryDirectory() as tmp, PresenceStore(tmp) as db:
            db.host_session_created('initial')
            route = dict(platform='feishu', account_id='synthetic-app', chat_id='synthetic-chat',
                         owner_id='synthetic-owner', authorized=True, live_capable=True)
            scope = db.temporal_bind_origin(db.profile_id, 'initial', 'synthetic-route', route,
                                           TemporalPolicy(enabled=True, dry_run=False), now=1000)

            def turn(text, stamp, mid):
                event = db.temporal_accept_user_event(scope, 'user', mid, text, now=stamp)
                conv = db.temporal_conversation(scope)
                return PresenceTurn(db, scope, conv['current_session_id'], mid, mid,
                                    event['event_id'], conv['activity_version'], conv['policy_version'], '{}')

            def call(current, **kwargs):
                return json.loads(temporal_tool(kwargs, turn=current))

            current = turn(quote, 1000, 'undated')
            args = dict(action='topic_update', topic_key=key, summary=quote, quoted_text=quote,
                        care_decision='skip', care_reason='Worth retaining; no useful contact window yet')
            saved = call(current, **args)
            self.assertTrue(saved['success'])
            self.assertNotIn('item', saved)
            self.assertEqual(call(current, **args), saved)
            self.assertEqual(db.temporal_list_items(scope, include_terminal=True)['total'], 0)
            self.assertEqual(db.temporal_recall(scope), [])
            self.assertEqual(db.temporal_list_due_scopes(db.profile_id, now=1000+7*86400), [])

            old_id = db.temporal_begin_new(scope, now=1100)
            db.host_session_created('fresh-session')
            db.temporal_finish_new(scope, old_id, 'fresh-session', route, now=1101)
            current = turn(development, 1200, 'new-development')
            topics = call(current, action='topics', topic_key=key)['topics']
            self.assertEqual(len(topics), 1)
            self.assertEqual(topics[0]['body']['summary'], quote)
            self.assertEqual(db.temporal_list_items(scope, include_terminal=True)['total'], 0)

            result = call(current, action='topic_update', topic_key=key, summary=development,
                          quoted_text=development, care_decision='create',
                          care_reason='New development makes later contact worthwhile', review_after_seconds=86400)
            self.assertTrue(result['success'])
            self.assertEqual(result['item']['status'], 'draft')
            self.assertEqual(len(db.temporal_topics(scope)), 1)
            topic = db.temporal_topics(scope, topic_key=key)[0]
            self.assertEqual([m['quote'] for m in topic['body']['milestones']], [quote, development])
            self.assertEqual(db.temporal_recall(scope), [])
            self.assertEqual(db.temporal_list_due_scopes(db.profile_id, now=2000000000), [])
            item = db.temporal_get_item(scope, result['item']['key'])
            stamp = item['body']['created_at']+1
            db.temporal_handoff_receipt(scope, handoff_id=current.handoff_id, turn_id=current.turn_id,
                outcome='accepted', delivery_ids=['synthetic-receipt'], acknowledged_at=stamp, now=stamp)
            self.assertEqual(db.temporal_list_due_scopes(db.profile_id, now=stamp+86400), [scope])

    def test_undecided_career_change(self):
        self.check_continuity('career', 'I am considering changing jobs but have not decided',
                              'I have an interview on Monday and feel nervous')

    def test_project_idea(self):
        self.check_continuity('project', 'I am planning a project and would like advice',
                              'My prototype is ready for friends to try on Friday')

    def test_stalled_progress(self):
        self.check_continuity('stalled', 'My project has not been progressing lately',
                              'Tomorrow I will discuss the blocker with my partner')


if __name__ == '__main__':
    unittest.main()
