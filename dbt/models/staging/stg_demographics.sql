-- stg_demographics
-- One row per patient (folder_id), coalescing demographic fields across all
-- source files. Explicit field values are preferred over inferred ones.
-- Deduplication happens here — the raw layer has one row per source file.

WITH ranked AS (
    SELECT
        folder_id,
        patient_id,
        patient_name,
        age,
        gender,
        dob,
        age_inferred,
        gender_inferred,
        -- Rank sources: explicit patient_summary fields first, then other explicit, then inferred
        ROW_NUMBER() OVER (
            PARTITION BY folder_id
            ORDER BY
                CASE WHEN source_file LIKE '%patient_summary%' THEN 0 ELSE 1 END,
                age_inferred ASC,
                gender_inferred ASC
        ) AS rn
    FROM {{ source('raw', 'demographics_raw') }}
    WHERE patient_id IS NOT NULL
)

SELECT
    folder_id,
    -- Coalesce best available value per field across all source files
    MAX(patient_id)                                             AS patient_id,
    MAX(patient_name)                                           AS patient_name,
    MAX(CASE WHEN NOT age_inferred    THEN age    END)          AS age,
    MAX(CASE WHEN NOT gender_inferred THEN gender END)          AS gender,
    MAX(dob)                                                    AS dob,
    -- Flag data completeness
    MAX(dob) IS NOT NULL                                        AS has_dob,
    BOOL_OR(NOT age_inferred)                                   AS age_is_explicit,
    BOOL_OR(NOT gender_inferred)                                AS gender_is_explicit
FROM {{ source('raw', 'demographics_raw') }}
WHERE patient_id IS NOT NULL
GROUP BY folder_id
