-- stg_nlp_entities
-- Structured medical entities extracted by Claude from clinical notes.
-- One row per entity value (e.g. one row per diagnosis, one per medication).
--
-- Grounding fields:
--   is_inferred  — true if the entity was implied rather than explicitly stated
--   source_text  — exact sentence from the source note supporting the entity;
--                  NULL indicates Claude could not cite a source (hallucination signal)

SELECT
    folder_id,
    patient_id,
    note_type,
    LOWER(TRIM(entity_type))                                    AS entity_type,
    TRIM(entity_value)                                          AS entity_value,
    TRY_CAST(note_date AS DATE)                                 AS note_date,
    COALESCE(is_inferred, false)                                AS is_inferred,
    NULLIF(TRIM(source_text), '')                               AS source_text
FROM {{ source('raw', 'nlp_entities') }}
WHERE folder_id IS NOT NULL
  AND entity_value IS NOT NULL
  AND TRIM(entity_value) != ''
