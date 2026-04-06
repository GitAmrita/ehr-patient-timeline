-- stg_notes
-- One row per clinical note document (consults, imaging, ECG reports).
--
-- note_summary: strips markdown bold field lines (e.g. **Patient:** James Anderson)
-- and remaining markdown syntax, then collapses whitespace into a clean
-- 200-char summary for display in the timeline.
--
-- Date filter: excludes dates before 2000-01-01 to prevent DOB lines in
-- note headers being parsed as the document date.

SELECT
    folder_id,
    patient_id,
    note_type,
    note_text,
    TRY_STRPTIME(note_date, '%B %d, %Y')::DATE                          AS note_date,
    LEFT(
        TRIM(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(note_text, '\*\*[^*\n]+\*\*[^*\n]*', '', 'g'),
                    '[#\*`\-=]', '', 'g'
                ),
                '\s+', ' ', 'g'
            )
        ),
        200
    )                                                                     AS note_summary
FROM {{ source('raw', 'notes') }}
WHERE folder_id IS NOT NULL
  AND TRY_STRPTIME(note_date, '%B %d, %Y')::DATE >= '2000-01-01'
