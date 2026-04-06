-- stg_demographics
-- One row per patient (folder_id), coalescing demographic fields across all
-- source files. Explicit field values are preferred over inferred ones.
-- Deduplication happens here — the raw layer has one row per source file.
--
-- Normalisation:
--   age → INTEGER  (strips "years old", "Years" etc.)
--   dob → DATE     (handles MM/DD/YYYY and "Month DD, YYYY" formats)

WITH normalised AS (
    SELECT
        folder_id,
        patient_id,
        patient_name,
        gender,
        gender_inferred,
        -- Extract first numeric sequence: "76 years old" → 76, "56 Years" → 56
        TRY_CAST(REGEXP_EXTRACT(age, '\d+') AS INTEGER)             AS age,
        age_inferred,
        -- Parse dob to DATE, handling two formats:
        --   MM/DD/YYYY          e.g. "03/12/1948" or "03/12/1948 (Age: 76)"
        --   Month DD, YYYY      e.g. "March 12, 1948"
        COALESCE(
            TRY_STRPTIME(REGEXP_EXTRACT(dob, '\d{2}/\d{2}/\d{4}'), '%m/%d/%Y'),
            TRY_STRPTIME(REGEXP_EXTRACT(dob, '[A-Za-z]+ \d{1,2},? \d{4}'), '%B %d, %Y')
        )::DATE                                                      AS dob
    FROM {{ source('raw', 'demographics_raw') }}
    WHERE patient_id IS NOT NULL
)

SELECT
    folder_id,
    MAX(patient_id)                                                  AS patient_id,
    MAX(patient_name)                                                AS patient_name,
    COALESCE(
        MAX(CASE WHEN NOT age_inferred    THEN age    END),
        MAX(age)
    )                                                                AS age,
    COALESCE(
        MAX(CASE WHEN NOT gender_inferred THEN gender END),
        MAX(gender)
    )                                                                AS gender,
    MAX(dob)                                                         AS dob
FROM normalised
GROUP BY folder_id
