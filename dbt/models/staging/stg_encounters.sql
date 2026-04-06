-- stg_encounters
-- One row per encounter document. Casts and cleans the raw encounter data.
-- Filters out rows where encounter_date looks like a DOB (before 2000)
-- which is a known parsing artifact from the ingestion layer.

SELECT
    folder_id,
    patient_id,
    encounter_type,
    attending,
    chief_complaint,
    disposition,
    note_text,
    -- Parse and validate encounter date — reject dates before year 2000
    -- as these are likely DOB values mis-extracted by the ingestion parser
    CASE
        WHEN TRY_STRPTIME(encounter_date, '%B %d, %Y') >= '2000-01-01'
        THEN TRY_STRPTIME(encounter_date, '%B %d, %Y')::DATE
        ELSE NULL
    END                                                         AS encounter_date
FROM {{ source('raw', 'encounters') }}
WHERE folder_id IS NOT NULL
