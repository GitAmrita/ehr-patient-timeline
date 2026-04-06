from datetime import date
from typing import Optional
from pydantic import BaseModel


class Patient(BaseModel):
    folder_id: str
    patient_id: str
    patient_name: Optional[str]
    age: Optional[int]
    gender: Optional[str]
    dob: Optional[date]
    encounter_count: int
    lab_result_count: int
    abnormal_lab_count: int
    note_count: int
    first_lab_date: Optional[date]
    last_lab_date: Optional[date]


class PatientList(BaseModel):
    patient_id: str
    count: int
    results: list[Patient]


class TimelineEvent(BaseModel):
    folder_id: str
    patient_id: Optional[str]
    event_date: Optional[date]
    event_type: str
    event_subtype: Optional[str]
    description: Optional[str]
    provider: Optional[str]
    outcome: Optional[str]


class FolderTimeline(BaseModel):
    folder_id: str
    event_count: int
    events: list[TimelineEvent]


class Timeline(BaseModel):
    patient_id: str
    folder_count: int
    folders: list[FolderTimeline]
