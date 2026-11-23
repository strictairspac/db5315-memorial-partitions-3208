import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from sqlalchemy import select

from tests._db import bind_engines, engine as TEST_ENGINE
bind_engines()

from src.constants.ingestion import BatchState
from src.models.tables import (ingestion_batches, ingestion_replays,
                               ingestion_rejects, ingestion_segments,
                               ingestion_slips, schema)
from src.schemas.workflow import BatchForm, SegmentForm
from src.services import ingestion as service


class IngestionServiceTests(unittest.TestCase):
    def setUp(self):
        schema.create_all(TEST_ENGINE)
        self.engine = TEST_ENGINE
        self.addCleanup(lambda: schema.drop_all(TEST_ENGINE))

    def create(self, count=3, station='east'):
        return service.create_batch(BatchForm(station=station, segment_count=count), 'operator')

    def send(self, batch_id, index, slips):
        return service.upload_segment(batch_id,
            SegmentForm(segment_index=index, slips=slips), 'operator')

    def test_full_out_of_order_gather_then_seal_and_replay(self):
        batch = self.create(3)
        bid = batch['id']
        # 乱序：先送第 3 段，再第 1 段，最后第 2 段
        r3 = self.send(bid, 3, [{'slip_id': 'C', 'entered_on': '2026-10-01'}])
        self.assertTrue(r3['accepted'])
        ledger = service.get_ledger(bid)
        self.assertEqual(ledger['missing_segments'], [1, 2])

        r1 = self.send(bid, 1, [{'slip_id': 'A', 'entered_on': '2026-08-01'}])
        self.assertTrue(r1['accepted'])
        r2 = self.send(bid, 2, [{'slip_id': 'B', 'entered_on': '2026-09-01'}])
        self.assertTrue(r2['accepted'])

        ledger = service.get_ledger(bid)
        self.assertEqual(ledger['missing_segments'], [])
        self.assertTrue(ledger['ready_to_seal'])

        sealed = service.seal_batch(bid, 'operator')
        self.assertEqual(sealed['state'], BatchState.SEALED.value)

        replay = service.replay_batch(bid, 'operator')
        self.assertFalse(replay['idempotent'])
        self.assertEqual(replay['slip_count'], 3)
        self.assertEqual([s['slip_id'] for s in replay['report']['slips']], ['A', 'B', 'C'])

    def test_retry_is_idempotent_and_does_not_double_insert(self):
        batch = self.create(2)
        bid = batch['id']
        slips = [{'slip_id': 'A', 'entered_on': '2026-08-01'}]
        first = self.send(bid, 1, slips)
        retry = self.send(bid, 1, slips)
        self.assertTrue(first['accepted'])
        self.assertFalse(retry['accepted'], '重复送达不应再次入库')

        with self.engine.connect() as conn:
            seg_count = conn.execute(select(ingestion_segments).where(
                ingestion_segments.c.batch_id == bid)).all()
            slip_count = conn.execute(select(ingestion_slips).where(
                ingestion_slips.c.batch_id == bid)).all()
        self.assertEqual(len(seg_count), 1, '段只入库一次')
        self.assertEqual(len(slip_count), 1, '凭条不重复入库')

        # 同段序号但内容不同必须拒绝，防止覆盖首次入库内容
        from fastapi import HTTPException
        with self.assertRaises(HTTPException):
            self.send(bid, 1, [{'slip_id': 'A', 'entered_on': '2026-08-02'}])

    def test_resume_only_fills_missing_segments(self):
        batch = self.create(3)
        bid = batch['id']
        self.send(bid, 1, [{'slip_id': 'A', 'entered_on': '2026-08-01'}])
        # 模拟中断后恢复：账簿指出只缺 2、3
        ledger = service.get_ledger(bid)
        self.assertEqual(ledger['missing_segments'], [2, 3])
        self.send(bid, 2, [{'slip_id': 'B', 'entered_on': '2026-09-01'}])
        self.send(bid, 3, [{'slip_id': 'C', 'entered_on': '2026-10-01'}])
        self.assertEqual(service.get_ledger(bid)['missing_segments'], [])

    def test_same_id_date_conflict_blocks_seal_and_traces_segments(self):
        batch = self.create(2)
        bid = batch['id']
        self.send(bid, 1, [{'slip_id': 'X', 'entered_on': '2026-08-01'}])
        self.send(bid, 2, [{'slip_id': 'X', 'entered_on': '2026-08-05'}])

        ledger = service.get_ledger(bid)
        self.assertEqual(ledger['state'], BatchState.CONFLICT.value)
        self.assertEqual(ledger['conflicts']['X'], [
            {'date': '2026-08-01', 'segments': [1]},
            {'date': '2026-08-05', 'segments': [2]},
        ])
        from fastapi import HTTPException
        with self.assertRaises(HTTPException):
            service.seal_batch(bid, 'operator')
        # 冲突批次始终未封存
        with self.engine.connect() as conn:
            state = conn.execute(select(ingestion_batches.c.state)
                                 .where(ingestion_batches.c.id == bid)).scalar_one()
        self.assertEqual(state, BatchState.CONFLICT.value)

    def test_late_segment_after_seal_is_rejected_and_excluded_from_replay(self):
        batch = self.create(2)
        bid = batch['id']
        self.send(bid, 1, [{'slip_id': 'A', 'entered_on': '2026-08-01'}])
        self.send(bid, 2, [{'slip_id': 'B', 'entered_on': '2026-09-01'}])
        service.seal_batch(bid, 'operator')

        from fastapi import HTTPException
        # 封存与补传并发：迟到的第 2 段重发（新内容）必须被挡下
        with self.assertRaises(HTTPException) as ctx:
            self.send(bid, 2, [{'slip_id': 'LATE', 'entered_on': '2026-11-01'}])
        self.assertEqual(ctx.exception.status_code, 409)

        with self.engine.connect() as conn:
            rejects = conn.execute(select(ingestion_rejects).where(
                ingestion_rejects.c.batch_id == bid)).mappings().all()
            slips = conn.execute(select(ingestion_slips).where(
                ingestion_slips.c.batch_id == bid)).mappings().all()
        self.assertEqual(len(rejects), 1)
        self.assertEqual(rejects[0]['reason'], 'batch_sealed')
        self.assertNotIn('LATE', [row['slip_id'] for row in slips])

        replay = service.replay_batch(bid, 'operator')
        replayed_ids = [row['slip_id'] for row in replay['report']['slips']]
        self.assertEqual(sorted(replayed_ids), ['A', 'B'], '迟到片段不得混入回放')
        self.assertEqual(len(replay['report']['rejected_late_segments']), 1)

        # 回放幂等：再次回放返回同一行，不产生第二条回放记录
        again = service.replay_batch(bid, 'operator')
        self.assertTrue(again['idempotent'])
        with self.engine.connect() as conn:
            count = len(conn.execute(select(ingestion_replays).where(
                ingestion_replays.c.batch_id == bid)).all())
        self.assertEqual(count, 1)

    def test_replay_before_seal_refused(self):
        batch = self.create(1)
        bid = batch['id']
        self.send(bid, 1, [{'slip_id': 'A', 'entered_on': '2026-08-01'}])
        from fastapi import HTTPException
        with self.assertRaises(HTTPException):
            service.replay_batch(bid, 'operator')

    def test_cross_segment_same_id_same_date_dedupes_in_snapshot(self):
        batch = self.create(2)
        bid = batch['id']
        self.send(bid, 1, [{'slip_id': 'A', 'entered_on': '2026-08-01'}])
        self.send(bid, 2, [
            {'slip_id': 'A', 'entered_on': '2026-08-01'},
            {'slip_id': 'B', 'entered_on': '2026-09-01'},
        ])
        ledger = service.get_ledger(bid)
        self.assertEqual(ledger['conflicts'], {})
        sealed = service.seal_batch(bid, 'operator')
        self.assertEqual(len(sealed['snapshot']['slips']), 2)
        self.assertEqual(sealed['snapshot']['source_segments']['A'], [1, 2])

    def test_concurrent_seal_and_upload_keeps_late_segment_out(self):
        import threading
        from fastapi import HTTPException
        batch = self.create(2)
        bid = batch['id']
        self.send(bid, 1, [{'slip_id': 'A', 'entered_on': '2026-08-01'}])
        self.send(bid, 2, [{'slip_id': 'B', 'entered_on': '2026-09-01'}])
        barrier = threading.Barrier(2)
        outcomes = {}

        def seal():
            barrier.wait()
            try:
                service.seal_batch(bid, 'operator')
                outcomes['seal'] = 'ok'
            except HTTPException as exc:
                outcomes['seal'] = exc.status_code

        def late_upload():
            barrier.wait()
            try:
                self.send(bid, 2, [{'slip_id': 'LATE', 'entered_on': '2026-11-01'}])
                outcomes['upload'] = 'accepted'
            except HTTPException as exc:
                outcomes['upload'] = exc.status_code

        t1 = threading.Thread(target=seal)
        t2 = threading.Thread(target=late_upload)
        t1.start(); t2.start(); t1.join(); t2.join()

        # 无论谁先拿到锁：封存成功则迟到段必被 409；迟到段先到也只能被当作
        # 同序号异内容拒绝，封存快照永远不含 LATE
        sealed = service.get_ledger(bid)
        self.assertEqual(sealed['state'], BatchState.SEALED.value)
        self.assertEqual(outcomes['seal'], 'ok')
        self.assertEqual(outcomes['upload'], 409)
        replay = service.replay_batch(bid, 'operator')
        self.assertNotIn('LATE', [row['slip_id'] for row in replay['report']['slips']])


if __name__ == '__main__':
    unittest.main(verbosity=2)
