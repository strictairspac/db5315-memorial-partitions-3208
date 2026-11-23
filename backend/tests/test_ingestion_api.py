import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from fastapi.testclient import TestClient

from tests._db import bind_engines, engine as TEST_ENGINE
bind_engines()

from src.models.tables import schema, users
from src.services.security import hash_password
import src.main as main_module
main_module.engine = TEST_ENGINE
from src.main import app
from src.constants.workflow import Role


class IngestionApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # 共享内存库，启动前确保全部表存在（其他模块可能 drop 过）
        schema.create_all(TEST_ENGINE)
        with TEST_ENGINE.begin() as conn:
            conn.execute(users.insert(), [
                {'username': role.value, 'role': role.value,
                 'password_hash': hash_password('pw-123456')}
                for role in Role])
        cls.client = TestClient(app)
        token = cls.client.post('/login', json={'username': 'operator', 'password': 'pw-123456'}).json()['token']
        cls.headers = {'Authorization': f'Bearer {token}'}

    @classmethod
    def tearDownClass(cls):
        schema.drop_all(TEST_ENGINE)

    def test_requires_auth(self):
        self.assertEqual(self.client.get('/ingestion/batches').status_code, 401)

    def test_end_to_end_with_conflict_and_late_reject(self):
        # 建档：西登记点，分 2 段
        created = self.client.post('/ingestion/batches',
                                   json={'station': 'west', 'segment_count': 2}, headers=self.headers)
        self.assertEqual(created.status_code, 201)
        bid = created.json()['id']

        # 乱序先送第 2 段
        r2 = self.client.post(f'/ingestion/batches/{bid}/segments',
                              json={'segment_index': 2, 'slips': [
                                  {'slip_id': 'SM-002', 'entered_on': '2026-09-01'}]},
                              headers=self.headers)
        self.assertEqual(r2.status_code, 202)
        self.assertEqual(r2.json()['ledger']['missing_segments'], [1])

        # 再送第 1 段
        r1 = self.client.post(f'/ingestion/batches/{bid}/segments',
                              json={'segment_index': 1, 'slips': [
                                  {'slip_id': 'SM-001', 'entered_on': '2026-08-01'}]},
                              headers=self.headers)
        self.assertEqual(r1.json()['accepted'], True)

        # 重复送达第 1 段：幂等，不重复入库
        retry = self.client.post(f'/ingestion/batches/{bid}/segments',
                                 json={'segment_index': 1, 'slips': [
                                     {'slip_id': 'SM-001', 'entered_on': '2026-08-01'}]},
                                 headers=self.headers)
        self.assertEqual(retry.json()['accepted'], False)

        # 收齐封存
        sealed = self.client.post(f'/ingestion/batches/{bid}/seal', headers=self.headers)
        self.assertEqual(sealed.status_code, 200)
        self.assertEqual(sealed.json()['state'], 'sealed')

        # 封存后迟到片段：409 且账簿留痕
        late = self.client.post(f'/ingestion/batches/{bid}/segments',
                                json={'segment_index': 1, 'slips': [
                                    {'slip_id': 'SM-LATE', 'entered_on': '2026-11-01'}]},
                                headers=self.headers)
        self.assertEqual(late.status_code, 409)
        ledger = self.client.get(f'/ingestion/batches/{bid}/ledger', headers=self.headers).json()
        self.assertEqual(len(ledger['rejected_late_segments']), 1)

        # 回放只认封存整批，迟到编号不出现；重复回放幂等
        replay = self.client.post(f'/ingestion/batches/{bid}/replay', headers=self.headers)
        self.assertEqual(replay.status_code, 200)
        ids = [row['slip_id'] for row in replay.json()['report']['slips']]
        self.assertEqual(sorted(ids), ['SM-001', 'SM-002'])
        again = self.client.post(f'/ingestion/batches/{bid}/replay', headers=self.headers)
        self.assertTrue(again.json()['idempotent'])

    def test_conflict_is_reported_with_segments_and_blocks_seal(self):
        created = self.client.post('/ingestion/batches',
                                   json={'station': 'east', 'segment_count': 2}, headers=self.headers)
        bid = created.json()['id']
        self.client.post(f'/ingestion/batches/{bid}/segments',
                         json={'segment_index': 1, 'slips': [
                             {'slip_id': 'SM-X', 'entered_on': '2026-08-01'}]},
                         headers=self.headers)
        self.client.post(f'/ingestion/batches/{bid}/segments',
                         json={'segment_index': 2, 'slips': [
                             {'slip_id': 'SM-X', 'entered_on': '2026-08-20'}]},
                         headers=self.headers)
        ledger = self.client.get(f'/ingestion/batches/{bid}/ledger', headers=self.headers).json()
        self.assertEqual(ledger['state'], 'conflict')
        self.assertEqual(ledger['conflicts']['SM-X'], [
            {'date': '2026-08-01', 'segments': [1]},
            {'date': '2026-08-20', 'segments': [2]}])
        seal = self.client.post(f'/ingestion/batches/{bid}/seal', headers=self.headers)
        self.assertEqual(seal.status_code, 409)

    def test_viewer_cannot_create_batch(self):
        token = self.client.post('/login', json={'username': 'viewer', 'password': 'pw-123456'}).json()['token']
        response = self.client.post('/ingestion/batches',
                                    json={'station': 'east', 'segment_count': 1},
                                    headers={'Authorization': f'Bearer {token}'})
        self.assertEqual(response.status_code, 403)

    def test_invalid_inputs_rejected(self):
        bad_station = self.client.post('/ingestion/batches',
                                       json={'station': 'north', 'segment_count': 2}, headers=self.headers)
        self.assertEqual(bad_station.status_code, 422)
        bad_index = self.client.post('/ingestion/batches',
                                     json={'station': 'east', 'segment_count': 2}, headers=self.headers)
        bid = bad_index.json()['id']
        out_of_range = self.client.post(f'/ingestion/batches/{bid}/segments',
                                        json={'segment_index': 5, 'slips': [
                                            {'slip_id': 'A', 'entered_on': '2026-08-01'}]},
                                        headers=self.headers)
        self.assertEqual(out_of_range.status_code, 422)


if __name__ == '__main__':
    unittest.main(verbosity=2)
