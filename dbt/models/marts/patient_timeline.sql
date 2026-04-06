-- patient_timeline
-- Unified chronological timeline of all clinical events per patient.
-- One row per event. Powers the patient timeline API endpoint.
--
-- Event types:
--   encounter  — ED visits, discharges, ICU admissions
--   lab_visit  — one row per lab collection date (aggregated across tests)
--   note       — clinical notes, consults, imaging reports
--
-- UNION (not UNION ALL) deduplicates identical rows across branches.

WITH encounters AS (
    SELECT
        folder_id,
        patient_id,
        encounter_date                              AS event_date,
        'encounter'                                 AS event_type,
        encounter_type                              AS event_subtype,
        COALESCE(chief_complaint, encounter_type)   AS description,
        attending                                   AS provider,
        disposition                                 AS outcome
    FROM {{ ref('stg_encounters') }}
    WHERE encounter_date IS NOT NULL
),

lab_visits AS (
    SELECT
        folder_id,
        patient_id,
        collection_date_parsed                      AS event_date,
        'lab_visit'                                 AS event_type,
        'lab_panel'                                 AS event_subtype,
        COUNT(*)::VARCHAR || ' tests collected, '
            || COUNT(CASE WHEN is_abnormal = true THEN 1 END)::VARCHAR
            || ' abnormal'                          AS description,
        NULL                                        AS provider,
        NULL                                        AS outcome
    FROM {{ ref('stg_labs') }}
    WHERE collection_date_parsed IS NOT NULL
    GROUP BY folder_id, patient_id, collection_date_parsed
),

notes AS (
    SELECT
        folder_id,
        patient_id,
        note_date                                   AS event_date,
        'note'                                      AS event_type,
        note_type                                   AS event_subtype,
        note_summary                                AS description,
        NULL                                        AS provider,
        NULL                                        AS outcome
    FROM {{ ref('stg_notes') }}
    WHERE note_date IS NOT NULL
)

SELECT * FROM encounters
UNION
SELECT * FROM lab_visits
UNION
SELECT * FROM notes
ORDER BY folder_id, event_date
