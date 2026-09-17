"""Small human-readable inspection of the existing personal record table."""
from datetime import datetime
from .protocol import TemporalError


def topic_command(db, scope, args):
    if args[0].lower() == 'topics' and len(args) == 1:
        topics = db.temporal_topics(scope, limit=20)
    elif args[0].lower() == 'topic' and len(args) == 2:
        topics = db.temporal_topics(scope, topic_key=args[1])
    else:
        return '/temporal topics | /temporal topic KEY'
    states = {'active':'仍在发展', 'paused':'暂不主动提起', 'closed':'已结束', 'expired':'长期无新进展'}
    if not topics:
        return '没有匹配的主题。'
    return '\n\n'.join(f"[{t['topic_key']}] {states[t['status']]}\n{t['body']['summary']}" +
        ''.join(f"\n· {m['summary']}\n  依据：{m['quote']}" for m in t['body'].get('milestones', [])) for t in topics)


def record_command(db, scope, args):
    action = args[0].lower()
    if action == "records":
        records = db.temporal_recall(scope, query=" ".join(args[1:]), include_history=True, limit=20)
    elif action in ("record", "forget") and len(args) == 2:
        prefix = args[1]
        if len(prefix) < 8 or any(c not in "0123456789abcdef" for c in prefix):
            raise TemporalError("请使用记录显示的编号（至少 8 位）。")
        matches = db._read_all("SELECT record_id FROM temporal_records WHERE profile_id=? AND conversation_id=? "
                               "AND instr(record_id,?)=1 LIMIT 2", (*scope.sql, prefix))
        if len(matches) != 1:
            raise TemporalError("记录不存在或编号不唯一，请先查看 /temporal records。")
        record_id = matches[0][0]
        if action == "forget":
            conv = db.temporal_conversation(scope)
            result = db.temporal_forget(scope, record_id=record_id, expected_activity=conv["activity_version"],
                                         expected_policy=conv["policy_version"])
            return f"已删除 {result['deleted']} 条个人记录及依赖版本；原聊天记录保留。"
        records = db.temporal_recall(scope, record_id=record_id, include_history=True, limit=20)
    else:
        return "/temporal records [关键词] | record 编号 | forget 编号"
    if not records:
        return "还没有匹配的个人记录。这里显示的是项目记录，不是原生 USER 记忆。"
    from hermes_time import get_timezone
    kinds = {"preference": "人格与偏好", "view": "观点", "relationship": "互动历史"}
    states = {"active": "当前", "tentative": "待确认", "superseded": "旧版本"}
    return "\n\n".join(
        f"[{r['record_id'][:8]}] {kinds[r['kind']]} · {states[r['status']]} · "
        f"{datetime.fromtimestamp(r['created_at'], tz=get_timezone()).strftime('%m-%d %H:%M')}\n"
        f"{r['content']}\n依据：{r['quoted_text']}" for r in records)
