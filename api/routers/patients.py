from fastapi import APIRouter, HTTPException
from api.models import Patient, PatientList
from api.db import get_conn

router = APIRouter(prefix="/patients", tags=["patients"])


@router.get("/{patient_id}", response_model=PatientList)
def get_patient(patient_id: str):
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM dim_patients WHERE patient_id = ? ORDER BY folder_id",
        [patient_id]
    ).fetchall()

    if not rows:
        raise HTTPException(status_code=404, detail=f"No patients found with ID '{patient_id}'")

    cols = [d[0] for d in conn.description]
    results = [Patient(**dict(zip(cols, row))) for row in rows]

    return PatientList(
        patient_id=patient_id,
        count=len(results),
        results=results,
    )
