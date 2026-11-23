from sqlalchemy import (JSON, Column, ForeignKey, Integer, MetaData, String,
                        Table, Text, UniqueConstraint)

schema = MetaData()
users = Table('accounts', schema,
    Column('id', Integer, primary_key=True),
    Column('username', String(40), nullable=False, unique=True),
    Column('password_hash', String(200), nullable=False),
    Column('role', String(20), nullable=False))
cases = Table('business_cases', schema,
    Column('id', Integer, primary_key=True),
    Column('slug', String(64), nullable=False, unique=True),
    Column('title', String(120), nullable=False),
    Column('payload', JSON, nullable=False),
    Column('author', String(40), nullable=False),
    Column('created_at', String(32), nullable=False))
runs = Table('business_runs', schema,
    Column('id', Integer, primary_key=True),
    Column('case_id', ForeignKey('business_cases.id'), nullable=False, index=True),
    Column('status', String(20), nullable=False, index=True),
    Column('owner', String(40), nullable=False),
    Column('attempt_token', String(36)),
    Column('started_at', String(32)),
    Column('finished_at', String(32)),
    Column('created_at', String(32), nullable=False),
    Column('error', Text))
evidence = Table('business_evidence', schema,
    Column('id', Integer, primary_key=True),
    Column('run_id', ForeignKey('business_runs.id'), nullable=False, unique=True),
    Column('report', JSON, nullable=False),
    Column('digest', String(64), nullable=False),
    Column('reviewer', String(40)),
    Column('review_note', String(500)),
    Column('reviewed_at', String(32)))
audit = Table('audit_events', schema,
    Column('id', Integer, primary_key=True),
    Column('actor', String(40), nullable=False),
    Column('action', String(32), nullable=False),
    Column('reference', String(50), nullable=False),
    Column('created_at', String(32), nullable=False))

# 入园凭条分段汇集：一个批次对应一个登记点的一次完整交接
ingestion_batches = Table('ingestion_batches', schema,
    Column('id', Integer, primary_key=True),
    Column('slug', String(64), nullable=False, unique=True),
    Column('station', String(20), nullable=False),
    Column('segment_count', Integer, nullable=False),
    Column('state', String(20), nullable=False, index=True),
    Column('sealed_at', String(32)),
    Column('sealed_digest', String(64)),
    Column('snapshot', JSON),
    Column('author', String(40), nullable=False),
    Column('created_at', String(32), nullable=False))
ingestion_segments = Table('ingestion_segments', schema,
    Column('id', Integer, primary_key=True),
    Column('batch_id', ForeignKey('ingestion_batches.id'), nullable=False, index=True),
    Column('segment_index', Integer, nullable=False),
    Column('checksum', String(64), nullable=False),
    Column('payload', JSON, nullable=False),
    Column('accepted_at', String(32), nullable=False),
    UniqueConstraint('batch_id', 'segment_index', name='uq_ingestion_segment'))
ingestion_slips = Table('ingestion_slips', schema,
    # 每行是「某段送达的一张凭条」，同号同日期的跨段重复各自留痕，便于溯源
    Column('id', Integer, primary_key=True),
    Column('batch_id', ForeignKey('ingestion_batches.id'), nullable=False, index=True),
    Column('segment_index', Integer, nullable=False),
    Column('slip_id', String(64), nullable=False),
    Column('entered_on', String(16), nullable=False),
    UniqueConstraint('batch_id', 'segment_index', 'slip_id', name='uq_ingestion_slip'))
ingestion_replays = Table('ingestion_replays', schema,
    Column('id', Integer, primary_key=True),
    Column('batch_id', ForeignKey('ingestion_batches.id'), nullable=False, unique=True),
    Column('sealed_digest', String(64), nullable=False),
    Column('slip_count', Integer, nullable=False),
    Column('report', JSON, nullable=False),
    Column('owner', String(40), nullable=False),
    Column('created_at', String(32), nullable=False))
ingestion_rejects = Table('ingestion_rejects', schema,
    # 封存之后才到达的片段在此留痕，证明其未混入整批
    Column('id', Integer, primary_key=True),
    Column('batch_id', ForeignKey('ingestion_batches.id'), nullable=False, index=True),
    Column('segment_index', Integer, nullable=False),
    Column('reason', String(60), nullable=False),
    Column('payload', JSON, nullable=False),
    Column('actor', String(40), nullable=False),
    Column('created_at', String(32), nullable=False))
