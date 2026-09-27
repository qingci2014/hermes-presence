"""Synthetic 0.1.4 topic/care transaction checks; no live messages or personal data."""
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from hermes_presence.protocol import TemporalError, TemporalPolicy
from hermes_presence.store import PresenceStore


class TopicCareTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db = PresenceStore(self.tmp.name)
        self.addCleanup(self.db.close)
        self.db.host_session_created('session-test')
        route = {'platform': 'feishu', 'chat_type': 'dm', 'authorized': True, 'live_capable': True,
                 'account_id': 'synthetic-account', 'chat_id': 'synthetic-chat', 'owner_id': 'synthetic-owner'}
        self.scope = self.db.temporal_bind_origin(self.db.profile_id, 'session-test', 'synthetic-route', route,
                                                  TemporalPolicy(enabled=True, dry_run=False))
        self.quote = '下周有一件重要的事'
        event = self.db.temporal_accept_user_event(self.scope, 'feishu', 'synthetic-event', self.quote)
        self.event_id = event['event_id']
        conv = self.db.temporal_conversation(self.scope)
        self.params = dict(topic_key='personal-milestone', summary='下周有一件重要的事',
                           quoted_text=self.quote, event_id=self.event_id,
                           expected_activity=conv['activity_version'], expected_policy=conv['policy_version'],
                           turn_id='turn-test', handoff_id='handoff-test')

    def test_create_atomic_and_retry_idempotent(self):
        args = dict(self.params, care_decision='create', care_reason='到时值得问候', review_after_seconds=3600)
        topic, cue = self.db.temporal_topic_with_care(self.scope, **args)
        self.assertEqual(topic['body']['care_decision']['decision'], 'create')
        self.assertEqual(cue['status'], 'draft')
        self.assertEqual(cue['body']['purpose'], 'care')
        self.assertEqual(self.db.temporal_topic_with_care(self.scope, **args)[1]['item_key'], cue['item_key'])
        self.assertEqual(len(self.db.temporal_list_items(self.scope, include_terminal=True)['items']), 1)
        with self.assertRaises(TemporalError):
            self.db.temporal_topic_with_care(self.scope, **dict(self.params, care_decision='skip', care_reason='不联系'))
        self.assertEqual(self.db.temporal_topics(self.scope, topic_key='personal-milestone')[0]['body']['care_decision']['decision'], 'create')

    def test_skip_records_reason_without_cue(self):
        topic, cue = self.db.temporal_topic_with_care(self.scope, **dict(self.params, care_decision='skip', care_reason='无需后续联系'))
        self.assertIsNone(cue)
        self.assertEqual(topic['body']['care_decision']['reason'], '无需后续联系')
        self.assertEqual(self.db.temporal_list_items(self.scope, include_terminal=True)['total'], 0)
        self.assertEqual(len(self.db.temporal_topics(self.scope, query='重要')), 1)

    def test_failed_create_rolls_back_topic(self):
        with self.assertRaises(TemporalError):
            self.db.temporal_topic_with_care(self.scope, **dict(self.params, care_decision='create', care_reason='后续跟进', review_after_seconds=1))
        self.assertEqual(self.db.temporal_topics(self.scope, topic_key='personal-milestone'), [])


if __name__ == '__main__':
    unittest.main()
