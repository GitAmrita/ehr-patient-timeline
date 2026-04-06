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
