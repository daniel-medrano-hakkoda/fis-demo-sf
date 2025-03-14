UPDATE EDW_DEV.ETL.INGESTION_METADATA_CONFIG SET ENABLED = '1' WHERE SOURCE_TABLE_NAME = 'Orders';
UPDATE EDW_DEV.ETL.INGESTION_METADATA_CONFIG SET ENABLED = '0' WHERE SOURCE_TABLE_NAME != 'Orders';

USE ROLE EDW_ROLE;

SELECT * FROM STAGE_DEV.FIS_DB_DBO.ORDERS;

SELECT * FROM DATALAKE_DEV.FIS_DB_DBO.ORDERS;


-- test clustering on iceberg tables



-- cleanup
USE ROLE SYSADMIN;

DROP TABLE DATALAKE_DEV.FIS_DB_DBO.ORDERS;
DROP TABLE STAGE_DEV.FIS_DB_DBO.ORDERS;

DROP PIPE STAGE_DEV.FIS_DB_DBO.PIPE_ORDERS;

DROP STREAM STAGE_DEV.FIS_DB_DBO.STREAM_INSERT_ORDERS;


ALTER TASK STAGE_DEV.FIS_DB_DBO.TASK_TRUNCATE_ORDERS SUSPEND;


DROP TASK STAGE_DEV.FIS_DB_DBO.TASK_TRUNCATE_ORDERS;
DROP TASK STAGE_DEV.FIS_DB_DBO.TASK_INSERT_ORDERS;
DROP TASK STAGE_DEV.FIS_DB_DBO.TASK_UPDATE_LLD_ORDERS;

SELECT * FROM EDW_DEV.ETL.INGESTION_METADATA_CONFIG;

UPDATE EDW_DEV.ETL.INGESTION_METADATA_CONFIG SET ENABLED = '1' WHERE SOURCE_TABLE_NAME = 'Orders';
UPDATE EDW_DEV.ETL.INGESTION_METADATA_CONFIG SET ENABLED = '0' WHERE SOURCE_TABLE_NAME != 'Orders';


UPDATE EDW_DEV.ETL.INGESTION_METADATA_CONFIG SET ENABLED = '0' WHERE SOURCE_TABLE_NAME = 'Orders';
UPDATE EDW_DEV.ETL.INGESTION_METADATA_CONFIG SET ENABLED = '1' WHERE SOURCE_TABLE_NAME != 'Orders';


SELECT * FROM EDW_DEV.ETL.INGESTION_METADATA_CONFIG;


UPDATE EDW_DEV.ETL.INGESTION_METADATA_CONFIG SET ENABLED = '1', DELTA_VALUE = '-1', CHANGE_TRACKING_TYPE = 'SQL Server', CHANGE_TRACKING = '1' WHERE SOURCE_TABLE_NAME = 'Orders';

