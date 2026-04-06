-- stg_demographics
-- One row per patient (folder_id), coalescing demographic fields across all
-- source files. Explicit field values are preferred over inferred ones.
-- Deduplication happens here — the raw layer has one row per source file.

SELECT
    folder_id,
    MAX(patient_id)                                             AS patient_id,
    MAX(patient_name)                                           AS patient_name,
    COALESCE(
        MAX(CASE WHEN NOT age_inferred    THEN age    END),
        MAX(age)
    )                                                           AS age,
    COALESCE(
        MAX(CASE WHEN NOT gender_inferred THEN gender END),
        MAX(gender)
    )                                                           AS gender,
    MAX(dob)                                                    AS dob
FROM {{ source('raw', 'demographics_raw') }}
WHERE patient_id IS NOT NULL
GROUP BY folder_id
