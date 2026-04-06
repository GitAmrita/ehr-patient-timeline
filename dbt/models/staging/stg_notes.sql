-- stg_notes
-- One row per clinical note document (consults, imaging, ECG reports).

SELECT
    folder_id,
    patient_id,
    note_type,
    note_text,
    TRY_CAST(note_date AS DATE)                                 AS note_date
FROM {{ source('raw', 'notes') }}
WHERE folder_id IS NOT NULL
