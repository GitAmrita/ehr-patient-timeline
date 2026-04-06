-- stg_labs
-- One row per lab test result. Casts result to numeric where possible,
-- normalises status to uppercase, and classifies each result as
-- abnormal / normal / uninformative based on agreed status taxonomy.
--
-- Status classification:
--   abnormal    : HIGH, LOW, CRITICAL, CRITICAL HIGH, CRITICAL LOW,
--                 ABNORMAL, POSITIVE, BORDERLINE, BORDERLINE HIGH
--   normal      : NORMAL, NEGATIVE, OPTIMAL, LOW NORMAL, LOW RISK,
--                 AVERAGE RISK, DESIRABLE, TARGET MET, HIGH/OPTIMAL
--   uninformative: -, --, NULL, ROOM AIR, NO GROWTH TO DATE, PRELIMINARY
--                 → converted to NULL

WITH normalised AS (
    SELECT
        folder_id,
        patient_id,
        collection_date,
        TRIM(test_name)                                             AS test_name,
        TRIM(result)                                                AS result_raw,
        TRY_CAST(REGEXP_REPLACE(TRIM(result), '[^0-9.]', '', 'g') AS DOUBLE)  AS result_numeric,                                                         
        reference_range,
        TRY_CAST(collection_date AS DATE)                           AS collection_date_parsed,
        -- Normalise status: uninformative values → NULL, everything else → UPPERCASE
        CASE
            WHEN UPPER(TRIM(status)) IN ('-', '--', 'NULL', 'ROOM AIR','NO GROWTH TO DATE', 'PRELIMINARY')
            THEN NULL
            ELSE UPPER(TRIM(status))
        END                                                         AS status
    FROM {{ source('raw', 'labs') }}
    WHERE folder_id IS NOT NULL
      AND test_name IS NOT NULL
      AND result IS NOT NULL
)

SELECT
    *,
    CASE
        WHEN status IN ('HIGH', 'LOW', 'CRITICAL', 'CRITICAL HIGH', 'CRITICAL LOW',
                        'ABNORMAL', 'POSITIVE', 'BORDERLINE', 'BORDERLINE HIGH')
        THEN TRUE
        WHEN status IN ('NORMAL', 'NEGATIVE', 'OPTIMAL', 'LOW NORMAL', 'LOW RISK',
                        'AVERAGE RISK', 'DESIRABLE', 'TARGET MET', 'HIGH/OPTIMAL')
        THEN FALSE
        ELSE NULL  -- status is NULL (uninformative) — cannot classify
    END AS is_abnormal
FROM normalised
