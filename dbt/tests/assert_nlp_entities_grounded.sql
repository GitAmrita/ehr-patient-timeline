-- assert_nlp_entities_grounded
--
-- Grounding test: every non-inferred entity must have a source_text citation.
-- A null source_text on a non-inferred entity means Claude returned an entity
-- it could not find in the document — a hallucination signal.
--
-- This is a dbt singular test: it passes when the query returns zero rows.
-- Any rows returned identify entities that need manual review.

SELECT
    folder_id,
    note_type,
    entity_type,
    entity_value
FROM {{ ref('stg_nlp_entities') }}
WHERE is_inferred = false
  AND source_text IS NULL
