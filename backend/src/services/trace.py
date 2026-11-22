"""按到货批次重建单块档口牌的文案与来源（只读追溯）。

本模块不连接数据库、不创建临时表、不触发 worker，只依据已存档的档案输入
做内存重放，规则与 :mod:`src.services.domain` 的 SQL 回放保持一致：

* 每批先登记全部到货事件，再从各流当前位点 +1 消费；
* 同批按接收行（提交）顺序处理，矛盾事件在登记阶段即隔离；
* 缺号补齐后，事件只在实际推进的批次生效；
* 批末对同一块牌只保留最后一次已提交变更。
"""

import re
from collections import defaultdict
from itertools import groupby
from operator import itemgetter

# 与 domain._ENVELOPE 保持一致：信封一致性的比较字段
ENVELOPE = ('seq', 'event_id', 'sign_id', 'operation', 'content')
_IDENTIFIER = re.compile(r'[A-Za-z0-9][A-Za-z0-9._-]{0,63}')


def _check_identifier(value, label):
    if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
        raise ValueError(f'{label}需为 1–64 位字母、数字、点、下划线或连字符，首位须为字母或数字')
    return value


# 批末牌面状态
SIGN_LISTED = 'listed'          # 在用
SIGN_WITHDRAWN = 'withdrawn'    # 本批撤牌（可追查撤牌来源）
SIGN_UNLISTED = 'unlisted'      # 从未挂牌
SIGN_INSUFFICIENT = 'insufficient'  # 历史区间无信封可核实


class _StreamReplay:
    """单条分区流的内存重放状态。"""

    def __init__(self, offset):
        self.offset = offset
        # seq -> 已登记信封（pending/conflict/applied/history），按接收顺序
        self.journal = []

    def register(self, event):
        """登记一个到货事件，返回处置状态；duplicate 时不入日志。"""
        overlaps = [row for row in self.journal
                    if row['seq'] == event['seq'] or row['event_id'] == event['event_id']]
        identical = [row for row in overlaps
                     if all(row[key] == event[key] for key in ENVELOPE)]
        if identical:
            event['state'] = 'duplicate'
            event['reason'] = f"与接收行 {identical[0]['receipt']} 的事件信封完全一致"
            return event
        if overlaps:
            # 新信封与已登记版本矛盾：待处理的旧版本一并隔离
            for row in overlaps:
                if row['state'] == 'pending':
                    row['state'] = 'conflict'
                    row['reason'] = (f"与接收行 {event['receipt']} 的事件序号或事件编号重叠，"
                                     '信封内容不同')
            event['state'] = 'conflict'
            event['reason'] = (f"与接收行 {overlaps[0]['receipt']} 的事件序号或事件编号重叠，"
                               '信封内容不同')
        elif event['seq'] <= self.offset:
            event['state'] = 'history'
            event['reason'] = '序号处在起始位点内，缺少原始事件信封供核对'
        else:
            event['state'] = 'pending'
            event['reason'] = ''
        self.journal.append(event)
        return event

    def consume(self):
        """按升序应用连续且无矛盾的待处理事件，返回本批提交行。"""
        committed = []
        waiting = sorted(
            (row for row in self.journal
             if row['seq'] > self.offset and row['state'] in ('pending', 'conflict')),
            key=lambda row: (row['seq'], row['receipt']),
        )
        for event in waiting:
            if event['seq'] != self.offset + 1 or event['state'] != 'pending':
                break
            event['state'] = 'applied'
            event['reason'] = '已随位点提交'
            committed.append(event)
            self.offset = event['seq']
        return committed


def _arrival(events):
    rows = []
    for receipt, row in enumerate(events, 1):
        rows.append({
            'receipt': receipt,
            'batch_no': row['batch'],
            'stream': row['stream'],
            'seq': row['sequence'],
            'event_id': row['event_id'],
            'sign_id': row['sign_id'],
            'operation': row['operation'],
            'content': row['content'],
        })
    return sorted(rows, key=itemgetter('batch_no'))


def replay_batches(payload):
    """内存重放全部批次。

    返回 ``(batches, streams, snapshot, journal)``：

    * ``batches``：按批号升序的列表，每项含批号、提交行和批末投影；
    * ``streams``：``{stream: 最终位点}``；
    * ``snapshot``：起始快照 ``{(stream, sign_id): {content, seq}}``；
    * ``journal``：全部接收行（含 duplicate），按接收行号排列。
    """
    checkpoints = {row['stream']: row['sequence'] for row in payload['checkpoints']}
    snapshot = {}
    for row in payload['snapshot']:
        snapshot[(row['stream'], row['sign_id'])] = {
            'content': row['content'], 'seq': row['sequence'],
        }

    replays = {stream: _StreamReplay(offset) for stream, offset in checkpoints.items()}
    arrivals = _arrival(payload['events'])
    journal = []
    batches = []

    # 投影在批次间累积，批末做快照
    projection = {key: dict(value) for key, value in snapshot.items()}
    for batch_no, grouped in groupby(arrivals, key=itemgetter('batch_no')):
        batch_events = list(grouped)
        committed = []
        # 第一步：登记本批全部到货事件（同批保留接收行序）
        for event in batch_events:
            result = replays[event['stream']].register(dict(event))
            journal.append(result)
        # 第二步：各流从位点 +1 连续消费；投影按应用顺序（流内序号升序）更新
        for stream in sorted({event['stream'] for event in batch_events}):
            applied = replays[stream].consume()
            committed.extend(applied)
            for event in applied:
                key = (event['stream'], event['sign_id'])
                if event['operation'] == 'delete':
                    projection.pop(key, None)
                else:
                    projection[key] = {'content': event['content'], 'seq': event['seq']}
        # 事件处置展示按接收行序
        committed.sort(key=itemgetter('receipt'))
        batches.append({
            'batch_no': batch_no,
            'committed': committed,
            'projection': {key: dict(value) for key, value in projection.items()},
        })

    journal.sort(key=itemgetter('receipt'))
    streams = {stream: replay.offset for stream, replay in replays.items()}
    return batches, streams, snapshot, journal


def _change_entries(batch, sign_id):
    """该批中针对指定牌的全部已提交变更，按流内应用顺序（序号升序）。"""
    return sorted(
        (event for event in batch['committed'] if event['sign_id'] == sign_id),
        key=itemgetter('seq'),
    )


def _history_flags(journal):
    """统计每个 (stream, sign_id) 的历史未核实信封。"""
    flagged = defaultdict(list)
    for event in journal:
        if event['state'] == 'history':
            flagged[(event['stream'], event['sign_id'])].append(event)
    return flagged


def build_trace(payload, sign_id, stream=None, batch_no=None):
    """构建单块档口牌的批次级追溯视图。

    :param payload: 档案原始输入（checkpoints/snapshot/events）
    :param sign_id: 要追溯的牌面编号，大小写敏感
    :param stream: 可选分区流；缺省时在全部流中按牌面号精确匹配
    :param batch_no: 可选到货批号；给出时只返回到该批为止的批末状态
    """
    _check_identifier(sign_id, '牌面编号')
    if stream is not None:
        _check_identifier(stream, '分区流')
    if batch_no is not None and (type(batch_no) is not int or batch_no < 1):
        raise ValueError('到货批号须为不小于 1 的整数')
    batches, final_offsets, snapshot, journal = replay_batches(payload)
    history_flags = _history_flags(journal)

    # 定位这块牌出现过的流：快照、投影、任何接收行
    touched_streams = set()
    for key in snapshot:
        if key[1] == sign_id:
            touched_streams.add(key[0])
    for event in journal:
        if event['sign_id'] == sign_id:
            touched_streams.add(event['stream'])
    if stream is not None:
        if stream not in final_offsets:
            raise ValueError(f'分区流 {stream} 未在起始位点中登记')
        matched = [stream]
    else:
        matched = sorted(touched_streams)

    if not matched:
        return {
            'sign_id': sign_id,
            'stream': None,
            'queried_batch': batch_no,
            'available_batches': [row['batch_no'] for row in batches],
            'status': SIGN_UNLISTED,
            'status_label': '未挂牌',
            'as_of': None,
            'timeline': [],
            'note': '起始快照与全部到货事件中均无该牌面编号，按未挂牌处理。',
        }

    stream = matched[0]
    key = (stream, sign_id)
    available = [row['batch_no'] for row in batches]
    if batch_no is not None and batch_no not in available:
        raise ValueError(f'到货批号 {batch_no} 不存在，可选批号：{", ".join(map(str, available)) or "无"}')

    timeline = []
    last_state = None
    ever_listed = key in snapshot
    for index, batch in enumerate(batches):
        changes = _change_entries(batch, sign_id)
        in_projection = key in batch['projection']
        if changes:
            # 批末只看这块牌最后一次变更
            last_change = changes[-1]
            if last_change['operation'] == 'delete':
                # 撤下从未挂牌的牌只消费序号，牌面状态仍按未挂牌处理
                status = SIGN_WITHDRAWN if ever_listed else SIGN_UNLISTED
                effect_label = '撤牌' if ever_listed else '撤牌（牌面未挂牌）'
                entry = {
                    'batch_no': batch['batch_no'],
                    'effect': 'withdraw',
                    'effect_label': effect_label,
                    'content': last_change['content'],
                    'source_sequence': last_change['seq'],
                    'source_event_id': last_change['event_id'],
                    'source_receipt': last_change['receipt'],
                    'change_count': len(changes),
                    'status': status,
                }
            else:
                entry = {
                    'batch_no': batch['batch_no'],
                    'effect': 'put',
                    'effect_label': '挂牌/更新',
                    'content': last_change['content'],
                    'source_sequence': last_change['seq'],
                    'source_event_id': last_change['event_id'],
                    'source_receipt': last_change['receipt'],
                    'change_count': len(changes),
                    'status': SIGN_LISTED,
                }
        elif in_projection:
            current = batch['projection'][key]
            entry = {
                'batch_no': batch['batch_no'],
                'effect': 'carry',
                'effect_label': '沿用前批',
                'content': current['content'],
                'source_sequence': current['seq'],
                'source_event_id': None,
                'source_receipt': None,
                'change_count': 0,
                'status': SIGN_LISTED,
            }
        else:
            # 本批结束时牌不在投影里：此前挂牌过即已撤牌，否则未挂牌
            entry = {
                'batch_no': batch['batch_no'],
                'effect': 'absent',
                'effect_label': '已撤牌' if ever_listed else '未挂牌',
                'content': '',
                'source_sequence': None,
                'source_event_id': None,
                'source_receipt': None,
                'change_count': 0,
                'status': SIGN_WITHDRAWN if ever_listed else SIGN_UNLISTED,
            }
        if in_projection:
            ever_listed = True
        timeline.append(entry)
        last_state = entry

    # 截止批号过滤
    if batch_no is not None:
        timeline = [row for row in timeline if row['batch_no'] <= batch_no]
        last_state = timeline[-1] if timeline else None

    # 历史未核实：该牌有序号落在起始位点内、却无信封可核对的接收行
    unverified = history_flags.get(key, [])
    if last_state is None:
        status = SIGN_UNLISTED
        status_label = '未挂牌'
        as_of = None
    elif unverified and last_state['status'] == SIGN_UNLISTED and key not in snapshot:
        status = SIGN_INSUFFICIENT
        status_label = '证据不足'
        as_of = None
    else:
        status = last_state['status']
        status_label = {SIGN_LISTED: '在用', SIGN_WITHDRAWN: '已撤牌',
                        SIGN_UNLISTED: '未挂牌'}[status]
        as_of = {
            'batch_no': last_state['batch_no'],
            'content': last_state['content'],
            'source_sequence': last_state['source_sequence'],
            'source_event_id': last_state['source_event_id'],
            'source_receipt': last_state['source_receipt'],
        } if status == SIGN_LISTED else None

    # 起始快照基线（第 0 批之前的可核实版本）
    baseline = snapshot.get(key)
    return {
        'sign_id': sign_id,
        'stream': stream,
        'matched_streams': matched,
        'queried_batch': batch_no,
        'available_batches': available,
        'status': status,
        'status_label': status_label,
        'baseline': None if baseline is None else {
            'content': baseline['content'],
            'source_sequence': baseline['seq'],
        },
        'as_of': as_of,
        'timeline': timeline,
        'unverified_events': [
            {
                'batch_no': event['batch_no'],
                'receipt': event['receipt'],
                'sequence': event['seq'],
                'event_id': event['event_id'],
                'reason': event['reason'],
            }
            for event in unverified
        ],
        'note': _note(status, sign_id, stream, unverified),
    }


def _note(status, sign_id, stream, unverified):
    if status == SIGN_UNLISTED:
        return f'{stream} 流的起始快照与各批投影中均无 {sign_id}，按未挂牌处理。'
    if status == SIGN_INSUFFICIENT:
        rows = '、'.join(f"接收行 {event['receipt']}（序号 {event['seq']}）" for event in unverified)
        return f'{sign_id} 在 {stream} 流起始位点内的历史版本缺少原始信封，可核实证据不足：{rows}。'
    if status == SIGN_WITHDRAWN:
        return f'{sign_id} 已在追溯截止批次前撤牌，撤牌批次与来源见时间线。'
    return f'{sign_id} 在 {stream} 流处于在用状态，文案与来源序号见时间线。'
