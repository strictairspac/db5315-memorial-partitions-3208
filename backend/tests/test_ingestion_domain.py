import os
import sys
import unittest
from datetime import date

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.services import ingestion_domain as d


def slip(slip_id, day):
    return d.Slip(slip_id, date.fromisoformat(day))


class DomainRuleTests(unittest.TestCase):
    def test_segments_validate_and_checksum_stable(self):
        a = d.validate_segment(1, 3, [
            {'slip_id': 'A', 'entered_on': '2026-08-01'},
            {'slip_id': 'B', 'entered_on': '2026-08-02'},
        ])
        b = d.validate_segment(1, 3, [
            {'slip_id': 'B', 'entered_on': '2026-08-02'},
            {'slip_id': 'A', 'entered_on': '2026-08-01'},
        ])
        self.assertEqual(a.checksum, b.checksum, '段内顺序不应影响校验码')
        self.assertEqual(a.segment_index, 1)

    def test_invalid_segment_index_and_bad_dates_rejected(self):
        with self.assertRaises(d.IngestionError):
            d.validate_segment(4, 3, [{'slip_id': 'A', 'entered_on': '2026-08-01'}])
        with self.assertRaises(d.IngestionError):
            d.validate_segment(1, 3, [{'slip_id': 'A', 'entered_on': '2026/08/01'}])
        with self.assertRaises(d.IngestionError):
            d.validate_segment(1, 3, [{'slip_id': ' A ', 'entered_on': '2026-08-01'}])

    def test_duplicate_id_inside_one_segment_rejected(self):
        with self.assertRaises(d.IngestionError):
            d.validate_segment(1, 2, [
                {'slip_id': 'A', 'entered_on': '2026-08-01'},
                {'slip_id': 'A', 'entered_on': '2026-08-02'},
            ])

    def test_missing_segments_unordered_arrival(self):
        # 乱序到达：先到 3、1，缺 2；恢复时只补缺段
        stored = {3: (slip('C', '2026-10-01'),), 1: (slip('A', '2026-08-01'),)}
        view = d.ledger_view(3, stored, d.BatchState.GATHERING)
        self.assertEqual(view['missing_segments'], [2])
        self.assertFalse(view['ready_to_seal'])
        self.assertIn('缺', view['replay_hint'])

    def test_complete_no_conflict_is_ready(self):
        stored = {
            1: (slip('A', '2026-08-01'),),
            2: (slip('B', '2026-09-01'),),
            3: (slip('C', '2026-10-01'),),
        }
        view = d.ledger_view(3, stored, d.BatchState.GATHERING)
        self.assertEqual(view['missing_segments'], [])
        self.assertTrue(view['ready_to_seal'])

    def test_same_id_same_date_across_segments_is_duplicate_not_conflict(self):
        stored = {
            1: (slip('A', '2026-08-01'),),
            2: (slip('A', '2026-08-01'), slip('B', '2026-09-01')),
        }
        view = d.ledger_view(2, stored, d.BatchState.GATHERING)
        self.assertEqual(view['conflicts'], {})
        self.assertEqual(view['duplicated_ids'], {'A': [1, 2]})
        self.assertEqual(view['row_count'], 3)
        self.assertEqual(view['unique_slip_count'], 2)

    def test_same_id_date_conflict_names_both_segments(self):
        # 同号日期冲突必须指出出自哪两段，不能按到达顺序挑一张
        stored = {
            1: (slip('X', '2026-08-01'),),
            2: (slip('X', '2026-08-05'),),
            3: (slip('X', '2026-08-01'),),
        }
        view = d.ledger_view(3, stored, d.BatchState.GATHERING)
        self.assertIn('X', view['conflicts'])
        conflict = view['conflicts']['X']
        self.assertEqual(conflict, [
            {'date': '2026-08-01', 'segments': [1, 3]},
            {'date': '2026-08-05', 'segments': [2]},
        ])
        self.assertFalse(view['ready_to_seal'])

    def test_seal_requires_all_segments(self):
        stored = {1: (slip('A', '2026-08-01'),)}
        with self.assertRaises(d.IngestionError):
            d.sealed_snapshot(2, stored)

    def test_seal_refuses_conflict_and_snapshot_dedupes(self):
        stored = {
            1: (slip('X', '2026-08-01'),),
            2: (slip('X', '2026-08-09'),),
        }
        with self.assertRaises(d.IngestionError):
            d.sealed_snapshot(2, stored)

        clean = {
            1: (slip('A', '2026-08-01'),),
            2: (slip('A', '2026-08-01'), slip('B', '2026-09-01')),
        }
        snap = d.sealed_snapshot(2, clean)
        self.assertEqual([row['slip_id'] for row in snap['slips']], ['A', 'B'])
        self.assertEqual(snap['source_segments']['A'], [1, 2])
        self.assertEqual(len(snap['digest']), 64)


if __name__ == '__main__':
    unittest.main(verbosity=2)
