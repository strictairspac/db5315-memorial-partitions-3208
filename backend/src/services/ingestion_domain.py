"""入园凭条分段汇集的纯领域规则。

两个登记点把同一批入园凭条切成若干段分几次送达，允许乱序、重复送达和
中断后补传。本模块只负责规则判定，不触碰数据库，因此可以离线完整测试。

持久化层（services/ingestion.py）负责在事务里调用这些规则，并保证重试
入库的幂等性与封存/补传并发时的隔离。
"""
from dataclasses import dataclass, field
from datetime import date

from src.constants.ingestion import BatchState, Station

SPEC = {
    'name': '松门纪念园·入园凭条分段汇集',
    'summary': '两个登记点分段补传入园凭条，乱序可收、重试不重、同号冲突可溯源，收齐无冲突才封存并整批回放。',
    'customer': '松门纪念园·票务台账组',
    'entity_labels': ['汇集批次', '分段回执', '封存整批', '回放记录'],
    'field_labels': {
        'station': '登记点',
        'segment_count': '分段总数',
        'segment_index': '段序号（1 起）',
        'checksum': '段内容校验码',
        'slips': '凭条明细',
        'slip_id': '凭条编号',
        'entered_on': '入园日期',
    },
    'guidance': '先按登记点和分段总数建立批次，再逐段送达；段可乱序、可重复送达。同号日期冲突须指出出自哪两段，收齐且无冲突才能封存。',
    'sample': {
        'station': Station.EAST.value,
        'segment_count': 3,
        'segments': [
            {'segment_index': 1, 'slips': [
                {'slip_id': 'SM-0801-01', 'entered_on': '2026-08-01'},
                {'slip_id': 'SM-0831-01', 'entered_on': '2026-08-31'},
            ]},
            {'segment_index': 3, 'slips': [
                {'slip_id': 'SM-1001-01', 'entered_on': '2026-10-01'},
            ]},
            {'segment_index': 2, 'slips': [
                {'slip_id': 'SM-0901-01', 'entered_on': '2026-09-01'},
                {'slip_id': 'SM-0930-01', 'entered_on': '2026-09-30'},
            ]},
        ],
    },
}


class IngestionError(ValueError):
    """业务规则拒绝，HTTP 层映射为 4xx。"""


@dataclass(frozen=True)
class Slip:
    slip_id: str
    entered_on: date


@dataclass(frozen=True)
class SegmentInput:
    segment_index: int
    slips: tuple[Slip, ...]
    checksum: str


@dataclass
class SegmentReceipt:
    """一段入库后的回执。accepted=False 表示该段此前已入库（幂等重发）。"""
    segment_index: int
    accepted: bool
    slip_count: int
    checksum: str
    duplicate_ids: list[str] = field(default_factory=list)


def parse_date(value, label):
    if not isinstance(value, str):
        raise IngestionError(f'{label}须为 YYYY-MM-DD 文本')
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        raise IngestionError(f'{label}须为有效日期（YYYY-MM-DD）') from None
    if parsed.isoformat() != value:
        raise IngestionError(f'{label}须为 YYYY-MM-DD 格式')
    return parsed


def validate_station(value):
    try:
        return Station(value)
    except ValueError:
        raise IngestionError(f'登记点须为 {[s.value for s in Station]} 之一') from None


def validate_segment_count(value):
    if not isinstance(value, int) or isinstance(value, bool) or not 1 <= value <= 1000:
        raise IngestionError('分段总数须为 1 到 1000 之间的整数')
    return value


def validate_segment(segment_index, segment_count, rows):
    """校验并规范化一段送达内容。段内编号重复在入库前直接拒绝。"""
    if not isinstance(segment_index, int) or isinstance(segment_index, bool):
        raise IngestionError('段序号须为整数')
    if not 1 <= segment_index <= segment_count:
        raise IngestionError(f'段序号 {segment_index} 超出 1..{segment_count} 范围')
    if not isinstance(rows, list) or not rows:
        raise IngestionError(f'第 {segment_index} 段凭条明细须为非空数组')
    slips = []
    seen = set()
    for pos, raw in enumerate(rows, 1):
        # 路由层可能传入 Pydantic 模型，统一规范化为 dict
        row = raw.model_dump() if hasattr(raw, 'model_dump') else raw
        if not isinstance(row, dict):
            raise IngestionError(f'第 {segment_index} 段第 {pos} 行须为对象')
        slip_id = row.get('slip_id')
        if not isinstance(slip_id, str) or not slip_id or slip_id != slip_id.strip() or '\x00' in slip_id:
            raise IngestionError(f'第 {segment_index} 段第 {pos} 行凭条编号须为非空文本，且不能含空字符或首尾空白')
        if slip_id in seen:
            raise IngestionError(f'第 {segment_index} 段内凭条编号 {slip_id} 重复')
        seen.add(slip_id)
        entered_on = parse_date(row.get('entered_on'), f'凭条 {slip_id} 入园日期')
        slips.append(Slip(slip_id, entered_on))
    return SegmentInput(segment_index, tuple(slips), checksum_of(slips))


def checksum_of(slips):
    """段内容指纹：编号与入园日期的规范排列哈希，用于识别重复送达。

    与段内排列顺序无关，重发时只要编号和日期集合一致即视为同一段。
    """
    import hashlib
    canonical = '\n'.join(
        f'{s.slip_id}|{s.entered_on.isoformat()}'
        for s in sorted(slips, key=lambda item: (item.slip_id, item.entered_on.isoformat())))
    return hashlib.sha256(canonical.encode('utf-8')).hexdigest()


def missing_segments(segment_count, received_indexes):
    """恢复时只补缺段：返回尚未入库的段序号。"""
    return [index for index in range(1, segment_count + 1) if index not in set(received_indexes)]


def detect_conflicts(segments):
    """同号日期冲突检测。

    不按到达顺序挑一张，而是把同一 slip_id 在不同段中的不同入园日期全部
    列出，并指出各自出自哪一段。segments 为 (segment_index, (Slip,...))。
    返回 {slip_id: {'date': str, 'segments': [段序号...]}} 的冲突视图。
    """
    appearances = {}
    for segment_index, slips in segments:
        for slip in slips:
            appearances.setdefault(slip.slip_id, []).append((segment_index, slip.entered_on))
    conflicts = {}
    for slip_id, places in appearances.items():
        distinct_dates = sorted({day for _, day in places})
        if len(distinct_dates) > 1:
            # 同一日期可能在多段重复出现，逐日期标注来源段
            per_date = []
            for day in distinct_dates:
                source_segments = sorted({seg for seg, d in places if d == day})
                per_date.append({'date': day.isoformat(), 'segments': source_segments})
            conflicts[slip_id] = per_date
    return dict(sorted(conflicts.items()))


def ledger_view(segment_count, stored_segments, state):
    """生成账簿视图：缺哪段、哪笔冲突、何时可以回放。

    stored_segments: {segment_index: (Slip,...)}，只含已接受入库的段。
    """
    received = sorted(stored_segments)
    missing = missing_segments(segment_count, received)
    ordered = [(index, stored_segments[index]) for index in received]
    conflicts = detect_conflicts(ordered)

    # 同号同日期的跨段重复不算冲突，但要在账簿里可见，便于核对张数
    duplicated = {}
    seen_ids = {}
    for segment_index, slips in ordered:
        for slip in slips:
            seen_ids.setdefault(slip.slip_id, []).append(segment_index)
    for slip_id, places in seen_ids.items():
        if len(places) > 1 and slip_id not in conflicts:
            duplicated[slip_id] = sorted(places)

    unique_count = len(seen_ids)
    row_count = sum(len(slips) for _, slips in ordered)
    ready_to_seal = not missing and not conflicts
    if state == BatchState.SEALED:
        replayable = '已封存，可随时回放'
    elif conflicts:
        replayable = '存在同号日期冲突，处置冲突后才能封存回放'
    elif missing:
        replayable = f'仍缺 {len(missing)} 段（{missing}），收齐后才能封存'
    else:
        replayable = '已收齐且无冲突，可封存回放'

    return {
        'received_segments': received,
        'missing_segments': missing,
        'conflicts': conflicts,
        'duplicated_ids': dict(sorted(duplicated.items())),
        'row_count': row_count,
        'unique_slip_count': unique_count,
        'ready_to_seal': ready_to_seal,
        'replay_hint': replayable,
    }


def sealed_snapshot(segment_count, stored_segments):
    """封存时刻的整批不可变视图。

    封存只认此刻已入库的整批内容；封存后迟到的片段由持久化层拒绝，无法
    混入。同一编号同日期的重复行只保留一张（去重），冲突编号直接报错。
    """
    view = ledger_view(segment_count, stored_segments, BatchState.GATHERING)
    if view['missing_segments']:
        raise IngestionError(
            f"段未收齐，缺段 {view['missing_segments']}，不能封存")
    if view['conflicts']:
        raise IngestionError('存在同号日期冲突，不能封存')
    chosen = {}
    source_segments = {}
    for segment_index in sorted(stored_segments):
        for slip in stored_segments[segment_index]:
            previous = chosen.get(slip.slip_id)
            if previous is not None and previous.entered_on != slip.entered_on:
                # 理论上 detect_conflicts 已拦截，这里保留防线
                raise IngestionError(f'凭条 {slip.slip_id} 存在日期冲突')
            chosen.setdefault(slip.slip_id, slip)
            source_segments.setdefault(slip.slip_id, []).append(segment_index)
    slips = sorted(chosen.values(), key=lambda slip: (slip.entered_on, slip.slip_id))
    import hashlib
    canonical = '\n'.join(f'{s.slip_id}|{s.entered_on.isoformat()}' for s in slips)
    digest = hashlib.sha256(canonical.encode('utf-8')).hexdigest()
    return {
        'slips': [{'slip_id': s.slip_id, 'entered_on': s.entered_on.isoformat()} for s in slips],
        'source_segments': {slip_id: sorted(set(segs)) for slip_id, segs in sorted(source_segments.items())},
        'digest': digest,
        'segment_count': segment_count,
    }
