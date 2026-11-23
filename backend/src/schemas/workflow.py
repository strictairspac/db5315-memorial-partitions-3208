from typing import Any

from pydantic import BaseModel, Field


class LoginForm(BaseModel):
    username: str = Field(min_length=1, max_length=40)
    password: str = Field(min_length=1, max_length=128)


class CaseForm(BaseModel):
    title: str = Field(min_length=1, max_length=120)
    payload: dict[str, Any]


class ReviewForm(BaseModel):
    note: str = Field(min_length=1, max_length=500)


class BatchForm(BaseModel):
    station: str = Field(min_length=1, max_length=20)
    segment_count: int = Field(ge=1, le=1000)


class SlipForm(BaseModel):
    slip_id: str = Field(min_length=1, max_length=64)
    entered_on: str = Field(min_length=8, max_length=16)


class SegmentForm(BaseModel):
    segment_index: int = Field(ge=1)
    slips: list[SlipForm] = Field(min_length=1)
