-- stg_labs
-- One row per lab test result. Casts result to numeric where possible
-- and normalises the status field to uppercase.

SELECT
    folder_id,
    patient_id,
    collection_date,
    TRIM(test_name)                                             AS test_name,
    TRIM(result)                                                AS result_raw,
    -- Attempt numeric cast — NULL if result contains units or text
    TRY_CAST(REGEXP_REPLACE(TRIM(result), '[^0-9.]', '', 'g') AS DOUBLE)
                                                                AS result_numeric,
    reference_range,
    UPPER(TRIM(status))                                         AS status,
    -- Derived flag: is this result abnormal?
    UPPER(TRIM(status)) IN ('HIGH', 'LOW', 'CRITICAL', 'ABNORMAL')
                                                                AS is_abnormal,
    -- Parse collection date
    TRY_CAST(collection_date AS DATE)                           AS collection_date_parsed
FROM {{ source('raw', 'labs') }}
WHERE folder_id IS NOT NULL
  AND test_name IS NOT NULL
  AND result IS NOT NULL
