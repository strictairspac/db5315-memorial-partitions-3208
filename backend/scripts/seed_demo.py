from sqlalchemy import func, select
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.postgresql import insert as postgres_insert

from src.config.settings import IS_MYSQL, engine
from src.constants.ingestion import BatchState
from src.constants.workflow import Role
from src.models.tables import (audit, cases, evidence, ingestion_batches,
                               ingestion_segments, ingestion_slips, runs,
                               schema, users)
from src.services import ingestion_domain as domain
from src.services.domain import SPEC
from src.services.security import hash_password
from src.utils.time import utc_time


def keep_existing(conn, table, key, values):
    if IS_MYSQL:
        statement = mysql_insert(table).values(**values).on_duplicate_key_update(id=table.c.id)
    else:
        statement = postgres_insert(table).values(**values).on_conflict_do_nothing(index_elements=[key])
    conn.execute(statement)


# 东登记点三段、乱序送达（3 → 1 → 2）且已收齐的演示批次
DEMO_SEGMENTS = {
    3: [{'slip_id': 'SM-1001-01', 'entered_on': '2026-10-01'}],
    1: [{'slip_id': 'SM-0801-01', 'entered_on': '2026-08-01'},
        {'slip_id': 'SM-0831-01', 'entered_on': '2026-08-31'}],
    2: [{'slip_id': 'SM-0901-01', 'entered_on': '2026-09-01'},
        {'slip_id': 'SM-0930-01', 'entered_on': '2026-09-30'}],
}


def _seed_ingestion(conn):
    if conn.execute(select(func.count()).select_from(ingestion_batches)).scalar_one():
        return
    batch_id = conn.execute(ingestion_batches.insert().values(
        slug='demo-ingestion', station='east', segment_count=len(DEMO_SEGMENTS),
        state=BatchState.GATHERING.value, author='demo', created_at=utc_time())
    ).inserted_primary_key[0]
    for index in sorted(DEMO_SEGMENTS):
        rows = DEMO_SEGMENTS[index]
        slips = tuple(domain.Slip(r['slip_id'], domain.parse_date(r['entered_on'], '入园日期'))
                      for r in rows)
        conn.execute(ingestion_segments.insert().values(
            batch_id=batch_id, segment_index=index, checksum=domain.checksum_of(slips),
            payload=rows, accepted_at=utc_time()))
        conn.execute(ingestion_slips.insert(), [
            {'batch_id': batch_id, 'segment_index': index,
             'slip_id': r['slip_id'], 'entered_on': r['entered_on']} for r in rows])
    conn.execute(audit.insert().values(actor='demo', action='create_ingestion_batch',
                 reference=str(batch_id), created_at=utc_time()))


def seed():
    with engine.begin() as conn:
        for role in Role:
            keep_existing(conn, users, 'username', {'username': role.value, 'role': role.value,
                'password_hash': hash_password('Demo-Walkthrough-2026')})
        keep_existing(conn, cases, 'slug', {'slug': 'demo', 'title': SPEC['name'],
            'payload': SPEC['sample'], 'author': 'demo', 'created_at': utc_time()})
        _seed_ingestion(conn)
        return {table.name: conn.execute(select(func.count()).select_from(table)).scalar_one()
                for table in (users, cases, runs, evidence, ingestion_batches, audit)}


if __name__ == '__main__':
    schema.create_all(engine)
    print(seed())
