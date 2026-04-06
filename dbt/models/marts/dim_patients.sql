-- dim_patients
-- One row per patient. Core demographic attributes enriched with
-- activity counts across all event types.
-- This is the spine that the API and timeline query against.

WITH encounter_counts AS (
    SELECT folder_id, COUNT(*) AS encounter_count
    FROM {{ ref('stg_encounters') }}
    WHERE encounter_date IS NOT NULL
    GROUP BY folder_id
),

lab_counts AS (
    SELECT folder_id,
        COUNT(*)                                    AS lab_result_count,
        COUNT(CASE WHEN is_abnormal THEN 1 END)     AS abnormal_lab_count,
        MIN(collection_date_parsed)                 AS first_lab_date,
        MAX(collection_date_parsed)                 AS last_lab_date
    FROM {{ ref('stg_labs') }}
    GROUP BY folder_id
),

note_counts AS (
    SELECT folder_id, COUNT(*) AS note_count
    FROM {{ ref('stg_notes') }}
    GROUP BY folder_id
)

SELECT
    d.folder_id,
    d.patient_id,
    d.patient_name,
    d.age,
    d.gender,
    d.dob,
    -- Activity summary
    COALESCE(e.encounter_count, 0)      AS encounter_count,
    COALESCE(l.lab_result_count, 0)     AS lab_result_count,
    COALESCE(l.abnormal_lab_count, 0)   AS abnormal_lab_count,
    COALESCE(n.note_count, 0)           AS note_count,
    l.first_lab_date,
    l.last_lab_date
FROM {{ ref('stg_demographics') }} d
LEFT JOIN encounter_counts e    ON e.folder_id = d.folder_id
LEFT JOIN lab_counts l          ON l.folder_id = d.folder_id
LEFT JOIN note_counts n         ON n.folder_id = d.folder_id
