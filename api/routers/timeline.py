from datetime import date
from typing import Optional
from fastapi import APIRouter, HTTPException, Query
from collections import defaultdict
from api.models import Timeline, FolderTimeline, TimelineEvent
from api.db import get_conn

router = APIRouter(prefix="/patients", tags=["timeline"])


@router.get("/{patient_id}/timeline", response_model=Timeline)
def get_timeline(
    patient_id: str,
    event_type: Optional[str] = Query(
        default=None,
        description="Comma-separated event types to include: encounter, lab_visit, note"
    ),
    from_date: Optional[date] = Query(default=None, description="Filter events from this date"),
    to_date: Optional[date] = Query(default=None, description="Filter events up to this date"),
):
    conn = get_conn()

    # Verify patient exists
    exists = conn.execute(
        "SELECT 1 FROM dim_patients WHERE patient_id = ?", [patient_id]
    ).fetchone()
    if not exists:
        raise HTTPException(status_code=404, detail=f"Patient '{patient_id}' not found")

    # Build query dynamically — all filters use parameterized values
    conditions = ["patient_id = ?"]
    params: list = [patient_id]

    if event_type:
        types = [t.strip() for t in event_type.split(",")]
        placeholders = ", ".join("?" * len(types))
        conditions.append(f"event_type IN ({placeholders})")
        params.extend(types)

    if from_date:
        conditions.append("event_date >= ?")
        params.append(from_date)

    if to_date:
        conditions.append("event_date <= ?")
        params.append(to_date)

    where = " AND ".join(conditions)
    rows = conn.execute(
        f"SELECT * FROM patient_timeline WHERE {where} ORDER BY event_date",
        params
    ).fetchall()

    cols = [d[0] for d in conn.description]
    events = [TimelineEvent(**dict(zip(cols, row))) for row in rows]

    grouped: dict = defaultdict(list)
    for event in events:
        grouped[event.folder_id].append(event)

    folders = [
        FolderTimeline(folder_id=fid, event_count=len(evts), events=evts)
        for fid, evts in grouped.items()
    ]

    return Timeline(patient_id=patient_id, folder_count=len(folders), folders=folders)
