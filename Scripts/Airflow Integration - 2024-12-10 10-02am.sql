SELECT * FROM EDW_DEV.ETL.INGESTION_METADATA_CONFIG;

SELECT * FROM EDW_DEV.ETL.CLARITY_DAILY_CONFIG;

ALTER TABLE EDW_DEV.ETL.INGESTION_METADATA_CONFIG ADD COLUMN CUSTOM_PARAMS VARIANT;

SELECT * FROM EDW_DEV.ETL.INGESTION_METADATA_CONFIG;


-- new query to filter metadata config table based on filterby parameter that should be passed by ADF
WITH INPUT_JSON AS (
    -- Input JSON for filtering
    SELECT PARSE_JSON('{ "clientId": "*" }') AS FILTER_CRITERIA
),
FILTER_KEYS AS (
    -- Flatten the input JSON into key-value pairs
    SELECT 
        f.KEY AS FILTER_KEY,
        f.VALUE AS FILTER_VALUE
    FROM INPUT_JSON,
         LATERAL FLATTEN(INPUT_JSON.FILTER_CRITERIA) f
),
SCALAR_PARAMS AS (
    -- Extract scalar key-value pairs from CUSTOM_PARAMS
    SELECT 
        t.METADATA_CONFIG_KEY AS ID,
        p.KEY AS PARAM_KEY,
        p.VALUE AS PARAM_VALUE
    FROM EDW_DEV.ETL.INGESTION_METADATA_CONFIG t,
         LATERAL FLATTEN(t.CUSTOM_PARAMS) p
    WHERE NOT IS_ARRAY(p.VALUE)
),
ARRAY_PARAMS AS (
    -- Flatten the CUSTOM_PARAMS column into key-value pairs
    SELECT 
        t.METADATA_CONFIG_KEY AS ID,
        p.KEY AS PARAM_KEY,
        f.VALUE AS PARAM_VALUE
    FROM EDW_DEV.ETL.INGESTION_METADATA_CONFIG t,
         LATERAL FLATTEN(t.CUSTOM_PARAMS) p,
         LATERAL FLATTEN(p.VALUE) f
    WHERE IS_ARRAY(p.VALUE)
),
EXPLODED_PARAMS AS (
    -- Combine scalar and array results into a unified set
    SELECT * FROM SCALAR_PARAMS
    UNION ALL
    SELECT * FROM ARRAY_PARAMS
),
MATCHED_ROWS AS (
    -- Join FILTER_KEYS and EXPLODED_PARAMS to find matches
    SELECT DISTINCT e.ID
    FROM EXPLODED_PARAMS e
    JOIN FILTER_KEYS fk
      ON (fk.FILTER_KEY = '*' OR e.PARAM_KEY = fk.FILTER_KEY)
         -- Handle wildcard (`*`) match
         AND (fk.FILTER_VALUE = '*' 
              OR (
                  -- Handle exact match for scalar values
                  IS_CHAR(fk.FILTER_VALUE) AND fk.FILTER_VALUE = e.PARAM_VALUE
              )
              OR (
                  -- Handle array match (filter value contains the parameter value)
                  IS_ARRAY(fk.FILTER_VALUE) AND ARRAY_CONTAINS(e.PARAM_VALUE, fk.FILTER_VALUE)
              )
         )
)

SELECT *
FROM EDW_DEV.ETL.INGESTION_METADATA_CONFIG t
WHERE 
    ENABLED = 1 AND
    t.METADATA_CONFIG_KEY IN (SELECT ID FROM MATCHED_ROWS);


-- validation
SELECT * FROM STAGE_DEV.FIS_DB_DBO.ORDERS;
SELECT * FROM STAGE_DEV.FIS_DB_DBO.PRODUCTS;

SELECT * FROM DATALAKE_DEV.FIS_DB_DBO.ORDERS;
SELECT * FROM DATALAKE_DEV.FIS_DB_DBO.PRODUCTS;
