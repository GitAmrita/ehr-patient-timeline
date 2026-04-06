-- stg_notes
-- One row per clinical note document (consults, imaging, ECG reports).

SELECT
    folder_id,
    patient_id,
    note_type,
    note_text,
    TRY_STRPTIME(note_date, '%B %d, %Y')::DATE                  AS note_date
FROM {{ source('raw', 'notes') }}
WHERE folder_id IS NOT NULL
