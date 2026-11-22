"""档口牌批次追溯的纯内存单元测试（不依赖 MySQL）。

运行：
    PYTHONPATH=backend python3 -m unittest backend.tests.test_trace -v
"""

import copy
import unittest

from src.services import trace
from src.services.domain import SPEC
from src.services.trace import (
    SIGN_INSUFFICIENT,
    SIGN_LISTED,
    SIGN_UNLISTED,
    SIGN_WITHDRAWN,
    build_trace,
    replay_batches,
)


def event(batch, stream, sequence, sign_id, content, operation='put', event_id=None):
    return {
        'batch': batch, 'stream': stream, 'sequence': sequence,
        'event_id': event_id or f'E-{sequence}', 'sign_id': sign_id,
        'operation': operation, 'content': content,
    }


BASE_PAYLOAD = {
    'checkpoints': [
        {'stream': 'NORTH', 'sequence': 12},
        {'stream': 'SOUTH', 'sequence': 4},
        {'stream': 'WEST', 'sequence': 0},
    ],
    'snapshot': [
        {'stream': 'NORTH', 'sign_id': 'N-07', 'sequence': 12, 'content': '桂花米酿｜22元'},
        {'stream': 'NORTH', 'sign_id': 'N-08', 'sequence': 11, 'content': '青团｜12元'},
        {'stream': 'SOUTH', 'sign_id': 'S-02', 'sequence': 4, 'content': '烤梨｜16元'},
    ],
    'events': [],
}


class SampleScenarioTests(unittest.TestCase):
    """BUSINESS.md 固定样例：提交 5、忽略 2、隔离 3、暂缓 2。"""

    def setUp(self):
        self.payload = copy.deepcopy(SPEC['sample'])
        self.batches, self.offsets, self.snapshot, self.journal = replay_batches(self.payload)

    def test_replay_metrics_match_spec(self):
        counts = {state: sum(row['state'] == state for row in self.journal)
                  for state in ('applied', 'duplicate', 'conflict', 'pending', 'history')}
        self.assertEqual(counts['applied'], 5)
        self.assertEqual(counts['duplicate'], 2)
        self.assertEqual(counts['conflict'], 3)
        self.assertEqual(counts['pending'], 2)
        self.assertEqual(counts['history'], 0)
        self.assertEqual(self.offsets, {'NORTH': 14, 'SOUTH': 5, 'WEST': 2})
        # 最终发布单只有两块在用牌
        final_projection = self.batches[-1]['projection']
        self.assertEqual(set(final_projection), {('NORTH', 'N-07'), ('SOUTH', 'S-02')})

    def test_listed_sign_content_and_source(self):
        result = build_trace(self.payload, 'N-07')
        self.assertEqual(result['status'], SIGN_LISTED)
        self.assertEqual(result['stream'], 'NORTH')
        # 批 1 只到货未推进（NORTH 等 13 号），批 2 才实际生效
        b1 = next(row for row in result['timeline'] if row['batch_no'] == 1)
        self.assertEqual(b1['effect'], 'carry')
        self.assertEqual(b1['content'], '桂花米酿｜22元')
        self.assertEqual(b1['source_sequence'], 12)
        b2 = next(row for row in result['timeline'] if row['batch_no'] == 2)
        self.assertEqual(b2['effect'], 'put')
        self.assertEqual(b2['content'], '桂花米酿｜18元')
        self.assertEqual(b2['source_sequence'], 14)
        self.assertEqual(b2['source_receipt'], 1)
        # 批 3 原样重投被忽略，牌面沿用
        b3 = next(row for row in result['timeline'] if row['batch_no'] == 3)
        self.assertEqual(b3['effect'], 'carry')
        self.assertEqual(result['as_of']['content'], '桂花米酿｜18元')
        self.assertEqual(result['as_of']['source_sequence'], 14)
        self.assertEqual(result['baseline'], {'content': '桂花米酿｜22元', 'source_sequence': 12})

    def test_withdrawn_sign_is_traceable(self):
        result = build_trace(self.payload, 'N-08')
        self.assertEqual(result['status'], SIGN_WITHDRAWN)
        b1 = next(row for row in result['timeline'] if row['batch_no'] == 1)
        self.assertEqual(b1['status'], SIGN_LISTED)
        self.assertEqual(b1['content'], '青团｜12元')
        b2 = next(row for row in result['timeline'] if row['batch_no'] == 2)
        self.assertEqual(b2['effect'], 'withdraw')
        self.assertEqual(b2['status'], SIGN_WITHDRAWN)
        self.assertEqual(b2['source_sequence'], 13)
        self.assertEqual(b2['content'], '')
        b3 = next(row for row in result['timeline'] if row['batch_no'] == 3)
        self.assertEqual(b3['status'], SIGN_WITHDRAWN)
        self.assertIsNone(result['as_of'])

    def test_put_then_withdraw_in_one_batch(self):
        result = build_trace(self.payload, 'W-03')
        self.assertEqual(result['status'], SIGN_WITHDRAWN)
        b1 = next(row for row in result['timeline'] if row['batch_no'] == 1)
        self.assertEqual(b1['status'], SIGN_LISTED)
        self.assertEqual(b1['source_sequence'], 1)
        b2 = next(row for row in result['timeline'] if row['batch_no'] == 2)
        self.assertEqual(b2['effect'], 'withdraw')
        self.assertEqual(b2['source_sequence'], 2)

    def test_conflicted_sign_never_listed(self):
        # N-09 的 15 号两种报价均隔离，流阻塞在 15，牌从未挂牌
        result = build_trace(self.payload, 'N-09')
        self.assertEqual(result['status'], SIGN_UNLISTED)
        self.assertTrue(all(row['status'] == SIGN_UNLISTED for row in result['timeline']))

    def test_held_sign_never_listed(self):
        # N-10 的 16 号暂缓，等 15 号矛盾解除
        result = build_trace(self.payload, 'N-10')
        self.assertEqual(result['status'], SIGN_UNLISTED)
        self.assertEqual(result['unverified_events'], [])

    def test_late_conflict_does_not_change_listed_content(self):
        result = build_trace(self.payload, 'S-02')
        self.assertEqual(result['status'], SIGN_LISTED)
        self.assertEqual(result['as_of']['content'], '烤梨｜14元')
        self.assertEqual(result['as_of']['source_sequence'], 5)
        b3 = next(row for row in result['timeline'] if row['batch_no'] == 3)
        self.assertEqual(b3['effect'], 'carry')

    def test_unknown_sign_is_unlisted(self):
        result = build_trace(self.payload, 'N-99')
        self.assertEqual(result['status'], SIGN_UNLISTED)
        self.assertIsNone(result['stream'])
        self.assertEqual(result['timeline'], [])

    def test_as_of_batch_filter(self):
        result = build_trace(self.payload, 'N-07', batch_no=1)
        self.assertEqual(len(result['timeline']), 1)
        self.assertEqual(result['as_of']['content'], '桂花米酿｜22元')
        result2 = build_trace(self.payload, 'N-07', batch_no=2)
        self.assertEqual(result2['as_of']['content'], '桂花米酿｜18元')
        with self.assertRaises(ValueError):
            build_trace(self.payload, 'N-07', batch_no=99)

    def test_stream_filter(self):
        result = build_trace(self.payload, 'N-07', stream='NORTH')
        self.assertEqual(result['stream'], 'NORTH')
        with self.assertRaises(ValueError):
            build_trace(self.payload, 'N-07', stream='MARS')


class GapFillTests(unittest.TestCase):
    """晚到缺号补齐后，从实际推进的批次才生效。"""

    def test_gap_fill_takes_effect_in_later_batch(self):
        payload = copy.deepcopy(BASE_PAYLOAD)
        payload['events'] = [
            # 批 1：6 号先到，5 号缺位，SOUTH 停在 4（等 5 号）
            event(1, 'SOUTH', 6, 'S-02', '烤梨｜13元'),
            # 批 2：补齐 5 号后连续消费 5、6
            event(2, 'SOUTH', 5, 'S-02', '烤梨｜14元'),
        ]
        result = build_trace(payload, 'S-02')
        b1 = next(row for row in result['timeline'] if row['batch_no'] == 1)
        self.assertEqual(b1['effect'], 'carry')
        self.assertEqual(b1['content'], '烤梨｜16元')  # 仍是快照文案
        b2 = next(row for row in result['timeline'] if row['batch_no'] == 2)
        # 批末只看最后一次变更 -> 6 号的 13 元
        self.assertEqual(b2['effect'], 'put')
        self.assertEqual(b2['content'], '烤梨｜13元')
        self.assertEqual(b2['source_sequence'], 6)
        self.assertEqual(b2['change_count'], 2)
        self.assertEqual(result['as_of']['source_sequence'], 6)

    def test_last_change_wins_within_batch_by_receipt_order(self):
        payload = copy.deepcopy(BASE_PAYLOAD)
        payload['events'] = [
            event(1, 'WEST', 1, 'W-01', '甲｜10元'),
            event(1, 'WEST', 2, 'W-01', '乙｜11元'),
            event(1, 'WEST', 3, 'W-01', '丙｜12元'),
        ]
        batches, _, _, _ = replay_batches(payload)
        committed = batches[0]['committed']
        self.assertEqual([row['seq'] for row in committed], [1, 2, 3])
        result = build_trace(payload, 'W-01')
        head = result['timeline'][0]
        self.assertEqual(head['change_count'], 3)
        self.assertEqual(head['content'], '丙｜12元')
        self.assertEqual(head['source_receipt'], committed[-1]['receipt'])


class HistoryUnverifiedTests(unittest.TestCase):
    """旧档缺件：可核实版本照常返回，分清未挂牌与证据不足。"""

    def test_history_event_without_envelope_is_insufficient_when_absent(self):
        payload = copy.deepcopy(BASE_PAYLOAD)
        # NORTH 起始位点 12；收到 10 号事件却没有可核对的已登记信封
        payload['events'] = [
            event(1, 'NORTH', 10, 'N-40', '旧档牌｜9元'),
            event(2, 'NORTH', 13, 'N-40', '新档牌｜10元'),
        ]
        result = build_trace(payload, 'N-40')
        # 13 号正常提交，牌在用；历史未核实行仍被保留
        self.assertEqual(result['status'], SIGN_LISTED)
        self.assertEqual(result['as_of']['content'], '新档牌｜10元')
        self.assertEqual(len(result['unverified_events']), 1)
        self.assertEqual(result['unverified_events'][0]['sequence'], 10)

    def test_absent_sign_with_history_event_is_insufficient(self):
        payload = copy.deepcopy(BASE_PAYLOAD)
        payload['events'] = [
            event(1, 'NORTH', 10, 'N-41', '只有历史信封｜9元'),
        ]
        result = build_trace(payload, 'N-41')
        self.assertEqual(result['status'], SIGN_INSUFFICIENT)
        self.assertEqual(result['status_label'], '证据不足')
        self.assertIsNone(result['as_of'])
        self.assertEqual(len(result['unverified_events']), 1)

    def test_snapshot_sign_with_history_event_remains_verifiable(self):
        payload = copy.deepcopy(BASE_PAYLOAD)
        payload['events'] = [
            event(1, 'NORTH', 10, 'N-07', '桂花米酿｜旧价9元'),
        ]
        result = build_trace(payload, 'N-07')
        # 快照版本可核实，照常返回在用；历史信封只作记录
        self.assertEqual(result['status'], SIGN_LISTED)
        self.assertEqual(result['baseline'], {'content': '桂花米酿｜22元', 'source_sequence': 12})
        self.assertEqual(len(result['unverified_events']), 1)

    def test_history_duplicate_envelope_is_ignored_not_flagged(self):
        payload = copy.deepcopy(BASE_PAYLOAD)
        payload['events'] = [
            event(1, 'NORTH', 10, 'N-42', '历史｜1元'),
            event(2, 'NORTH', 10, 'N-42', '历史｜1元'),  # 与首行完全一致
        ]
        result = build_trace(payload, 'N-42')
        self.assertEqual(result['status'], SIGN_INSUFFICIENT)
        self.assertEqual(len(result['unverified_events']), 1)


class DeleteSemanticsTests(unittest.TestCase):
    def test_delete_missing_sign_still_consumes_sequence(self):
        payload = copy.deepcopy(BASE_PAYLOAD)
        payload['events'] = [
            event(1, 'WEST', 1, 'W-09', '', operation='delete'),
            event(1, 'WEST', 2, 'W-10', '后续牌｜5元'),
        ]
        batches, offsets, _, journal = replay_batches(payload)
        self.assertEqual(offsets['WEST'], 2)
        applied = [row for row in journal if row['state'] == 'applied']
        self.assertEqual([row['seq'] for row in applied], [1, 2])
        result = build_trace(payload, 'W-09')
        self.assertEqual(result['status'], SIGN_UNLISTED)
        head = result['timeline'][0]
        self.assertEqual(head['effect'], 'withdraw')
        self.assertEqual(head['status'], SIGN_UNLISTED)


if __name__ == '__main__':
    unittest.main()
