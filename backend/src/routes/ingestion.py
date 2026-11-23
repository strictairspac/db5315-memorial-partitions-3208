from fastapi import APIRouter, Depends

from src.schemas.workflow import BatchForm, SegmentForm
from src.services import ingestion
from src.services.ingestion_domain import SPEC
from src.services.security import current_user, operator

router = APIRouter(prefix='/ingestion', tags=['ingestion'])


@router.get('/spec')
def spec(_=Depends(current_user)):
    return SPEC


@router.get('/batches')
def list_batches(_=Depends(current_user)):
    return ingestion.list_batches()


@router.post('/batches', status_code=201)
def create_batch(form: BatchForm, actor=Depends(operator)):
    return ingestion.create_batch(form, actor)


@router.get('/batches/{batch_id}/ledger')
def get_ledger(batch_id: int, _=Depends(current_user)):
    return ingestion.get_ledger(batch_id)


@router.post('/batches/{batch_id}/segments', status_code=202)
def upload_segment(batch_id: int, form: SegmentForm, actor=Depends(operator)):
    return ingestion.upload_segment(batch_id, form, actor)


@router.post('/batches/{batch_id}/seal', status_code=200)
def seal_batch(batch_id: int, actor=Depends(operator)):
    return ingestion.seal_batch(batch_id, actor)


@router.post('/batches/{batch_id}/replay', status_code=200)
def replay_batch(batch_id: int, actor=Depends(operator)):
    return ingestion.replay_batch(batch_id, actor)


@router.get('/replays')
def list_replays(_=Depends(current_user)):
    return ingestion.list_replays()
