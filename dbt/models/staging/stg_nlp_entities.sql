-- stg_nlp_entities
-- Structured medical entities extracted by Claude from clinical notes.
-- One row per entity value (e.g. one row per diagnosis, one per medication).

SELECT
    folder_id,
    patient_id,
    note_type,
    LOWER(TRIM(entity_type))                                    AS entity_type,
    TRIM(entity_value)                                          AS entity_value,
    TRY_CAST(note_date AS DATE)                                 AS note_date
FROM {{ source('raw', 'nlp_entities') }}
WHERE folder_id IS NOT NULL
  AND entity_value IS NOT NULL
  AND TRIM(entity_value) != ''
