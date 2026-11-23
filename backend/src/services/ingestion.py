"""分段汇集的持久化服务。

并发与幂等口径：
- 每个批次的写入都在单条事务内完成，并对批次行加锁（PostgreSQL 行锁，
  SQLite/MySQL 退化为事务串行），封存与补传不会互相穿插。
- 段以 (batch_id, segment_index) 唯一约束做幂等：重复送达只返回首次入库
  的回执，不再写第二遍，段内凭条也不会重复入库。
- 封存把整批快照写入批次行并改状态；封存之后到达的片段一律拒收并登记到
  ingestion_rejects，回放只读取封存快照，迟到片段无法混入。
"""
import uuid

from fastapi import HTTPException
from sqlalchemy import select

from src.config.settings import engine
from src.constants.ingestion import BatchState, ReplayState
from src.models.tables import (audit, ingestion_batches, ingestion_replays,
                               ingestion_rejects, ingestion_segments,
                               ingestion_slips)
from src.services import ingestion_domain as domain
from src.utils.encoding import canonical
from src.utils.time import utc_time


def _log(conn, actor, action, reference):
    conn.execute(audit.insert().values(actor=actor, action=action,
                 reference=str(reference), created_at=utc_time()))


def _lock_batch(conn, batch_id):
    """锁定并读取批次。PostgreSQL 用 FOR UPDATE；其他方言靠事务串行。"""
    statement = select(ingestion_batches).where(ingestion_batches.c.id == batch_id)
    if engine.dialect.name == 'postgresql':
        statement = statement.with_for_update()
    row = conn.execute(statement).mappings().first()
    if row is None:
        raise HTTPException(404, '找不到该汇集批次')
    return dict(row)


def _get_batch(conn, batch_id):
    row = conn.execute(select(ingestion_batches)
                       .where(ingestion_batches.c.id == batch_id)).mappings().first()
    if row is None:
        raise HTTPException(404, '找不到该汇集批次')
    return dict(row)


def _stored_segments(conn, batch_id):
    segment_rows = conn.execute(select(ingestion_segments.c.segment_index,
                                       ingestion_segments.c.payload)
                                .where(ingestion_segments.c.batch_id == batch_id)).mappings().all()
    stored = {}
    for row in segment_rows:
        slips = tuple(domain.Slip(item['slip_id'], domain.parse_date(item['entered_on'], '入园日期'))
                      for item in row['payload'])
        stored[row['segment_index']] = slips
    return stored


def _rejects(conn, batch_id):
    rows = conn.execute(select(ingestion_rejects.c.segment_index,
                               ingestion_rejects.c.reason, ingestion_rejects.c.created_at)
                        .where(ingestion_rejects.c.batch_id == batch_id)
                        .order_by(ingestion_rejects.c.id)).mappings().all()
    return [dict(row) for row in rows]


def _ledger_payload(conn, batch, view=None):
    if view is None:
        stored = _stored_segments(conn, batch['id'])
        view = domain.ledger_view(batch['segment_count'], stored, BatchState(batch['state']))
    return {
        'batch_id': batch['id'], 'station': batch['station'],
        'segment_count': batch['segment_count'], 'state': batch['state'],
        'sealed_at': batch['sealed_at'], 'sealed_digest': batch['sealed_digest'],
        **view, 'rejected_late_segments': _rejects(conn, batch['id']),
    }


def create_batch(form, actor):
    try:
        station = domain.validate_station(form.station)
        segment_count = domain.validate_segment_count(form.segment_count)
        if len(canonical(form.model_dump())) > 256 * 1024:
            raise ValueError('批次输入超过 256 KiB')
    except (domain.IngestionError, ValueError) as exc:
        raise HTTPException(422, str(exc)) from None
    with engine.begin() as conn:
        batch_id = conn.execute(ingestion_batches.insert().values(
            slug=uuid.uuid4().hex, station=station.value, segment_count=segment_count,
            state=BatchState.GATHERING.value, author=actor, created_at=utc_time())
        ).inserted_primary_key[0]
        _log(conn, actor, 'create_ingestion_batch', batch_id)
        return _get_batch(conn, batch_id)


def upload_segment(batch_id, form, actor):
    with engine.begin() as conn:
        batch = _lock_batch(conn, batch_id)
        try:
            segment = domain.validate_segment(form.segment_index, batch['segment_count'], form.slips)
        except domain.IngestionError as exc:
            raise HTTPException(422, str(exc)) from None
        sealed = batch['state'] == BatchState.SEALED.value

        existing = None
        if not sealed:
            existing = conn.execute(select(ingestion_segments).where(
                ingestion_segments.c.batch_id == batch_id,
                ingestion_segments.c.segment_index == segment.segment_index)).mappings().first()
            if existing is not None:
                # 幂等重发：不重复入库，返回首次入库回执
                if existing['checksum'] != segment.checksum:
                    raise HTTPException(
                        409, f"第 {segment.segment_index} 段已入库但内容不一致（校验码 {existing['checksum'][:12]}…），"
                             '同一段序号不能送达不同内容')
                return {'segment_index': segment.segment_index, 'accepted': False,
                        'slip_count': len(segment.slips), 'checksum': segment.checksum,
                        'duplicate_ids': [s.slip_id for s in segment.slips],
                        'note': '该段此前已入库，本次为重复送达，未重复写入'}

        if sealed:
            # 封存后迟到的片段不得混入；先在本事务登记拒收证据，提交后再抛 409，
            # 避免异常回滚把拒收留痕一并撤销
            conn.execute(ingestion_rejects.insert().values(
                batch_id=batch_id, segment_index=segment.segment_index,
                reason='batch_sealed', payload=[s.model_dump() for s in form.slips],
                actor=actor, created_at=utc_time()))
            _log(conn, actor, 'reject_late_segment', f'{batch_id}:{segment.segment_index}')
            late_index = segment.segment_index
        else:
            conn.execute(ingestion_segments.insert().values(
                batch_id=batch_id, segment_index=segment.segment_index, checksum=segment.checksum,
                payload=[s.model_dump() for s in form.slips], accepted_at=utc_time()))
            slip_rows = [{'batch_id': batch_id, 'segment_index': segment.segment_index,
                          'slip_id': slip.slip_id, 'entered_on': slip.entered_on.isoformat()}
                         for slip in segment.slips]
            conn.execute(ingestion_slips.insert(), slip_rows)
            _log(conn, actor, 'accept_segment', f'{batch_id}:{segment.segment_index}')

            stored = _stored_segments(conn, batch_id)
            view = domain.ledger_view(batch['segment_count'], stored, BatchState(batch['state']))
            if view['conflicts']:
                # 冲突不阻断单段入库（先收下、留痕），但批次转入冲突态、禁止封存
                conn.execute(ingestion_batches.update().where(ingestion_batches.c.id == batch_id)
                             .values(state=BatchState.CONFLICT.value))
                batch['state'] = BatchState.CONFLICT.value
            elif batch['state'] == BatchState.CONFLICT.value:
                # 已在冲突态时保持，冲突只能靠人工处置，不会因后续段自动解除
                pass
            response = {'segment_index': segment.segment_index, 'accepted': True,
                        'slip_count': len(segment.slips), 'checksum': segment.checksum,
                        'duplicate_ids': [], 'ledger': _ledger_payload(conn, batch, view)}

    if sealed:
        raise HTTPException(409, f"批次已封存，第 {late_index} 段迟到，已拒收且未混入整批")
    return response


def seal_batch(batch_id, actor):
    with engine.begin() as conn:
        batch = _lock_batch(conn, batch_id)
        stored = _stored_segments(conn, batch_id)
        try:
            snapshot = domain.sealed_snapshot(batch['segment_count'], stored)
        except domain.IngestionError as exc:
            raise HTTPException(409, str(exc)) from None
        conn.execute(ingestion_batches.update().where(ingestion_batches.c.id == batch_id).values(
            state=BatchState.SEALED.value, sealed_at=utc_time(),
            sealed_digest=snapshot['digest'], snapshot=snapshot))
        _log(conn, actor, 'seal_batch', batch_id)
        return _get_batch(conn, batch_id)


def replay_batch(batch_id, actor):
    with engine.begin() as conn:
        batch = _lock_batch(conn, batch_id)
        if batch['state'] != BatchState.SEALED.value or not batch['snapshot']:
            raise HTTPException(409, '批次尚未封存，回放只认封存的整批凭条')
        existing = conn.execute(select(ingestion_replays)
                                .where(ingestion_replays.c.batch_id == batch_id)).mappings().first()
        if existing is not None:
            # 回放也是幂等的：同一封存批次重复回放返回同一份报告
            return dict(existing) | {'idempotent': True}

        snapshot = batch['snapshot']
        report = {
            'summary': f"整批回放 {len(snapshot['slips'])} 张凭条，封存校验码 {snapshot['digest'][:12]}…",
            'state': ReplayState.DONE.value,
            'digest': snapshot['digest'],
            'slips': snapshot['slips'],
            'source_segments': snapshot['source_segments'],
            'rejected_late_segments': _rejects(conn, batch_id),
        }
        replay_id = conn.execute(ingestion_replays.insert().values(
            batch_id=batch_id, sealed_digest=snapshot['digest'],
            slip_count=len(snapshot['slips']), report=report,
            owner=actor, created_at=utc_time())).inserted_primary_key[0]
        _log(conn, actor, 'replay_batch', batch_id)
        report_row = conn.execute(select(ingestion_replays)
                                  .where(ingestion_replays.c.id == replay_id)).mappings().one()
        return dict(report_row) | {'idempotent': False}


def get_ledger(batch_id):
    with engine.connect() as conn:
        batch = _get_batch(conn, batch_id)
        return _ledger_payload(conn, batch)


def list_batches():
    with engine.connect() as conn:
        rows = conn.execute(select(ingestion_batches).order_by(ingestion_batches.c.id.desc())
                            .limit(100)).mappings().all()
        return [dict(row) for row in rows]


def list_replays():
    with engine.connect() as conn:
        rows = conn.execute(select(ingestion_replays).order_by(ingestion_replays.c.id.desc())
                            .limit(100)).mappings().all()
        return [dict(row) for row in rows]
