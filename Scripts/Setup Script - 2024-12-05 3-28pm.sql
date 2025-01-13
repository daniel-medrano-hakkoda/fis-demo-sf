/* 1. Begin by using the SYSADMIN role to create the role and user required to load data from Azure to Snowflake. */
USE ROLE SYSADMIN;

/* 2. Set variables for Environment and overall project */
SET environment = 'DEV'; -- change this to create TEST and PROD objects.
SET environment_ = (SELECT CASE WHEN $environment = 'PROD' THEN '' ELSE '_' || $environment END);

/* 3. Set the names for the metadata management environment. */
SET project_name = 'EDW'; -- Name of the project, and also the name of the metadata database.  Change this if you'd like the metadata database named differently. Note: If this changes, the Azure Data Factory pipeline reference must be changed as well.
SET metadata_schema_name = 'ETL'; -- Name of the metadata database schema.  Change this if you'd like the metadata schema named differently. Note: If this changes, the Azure Data Factory pipeline reference must be changed as well.
SET metadata_config = 'INGESTION_METADATA_CONFIG'; -- Name of the metadata configuration table.  Change this if you'd like the metadata config table named differently.  Note: If this changes, the Azure Data Factory pipeline reference must be changed as well.

/* 4. Create variables used for creating all objects. */
SET role_name = $project_name || '_ROLE'; -- Role for the service account
SET user_name = 'AZUREADMIN'; -- User for the service account.  Store this value in Azure KeyVault.
SET user_password = 'ViMo2304'; -- Password for the service account.  Store this value in Azure KeyVault.
SET warehouse_name = $project_name || '_WH'; -- Name of the warehouse that is used to load data
SET metadata_database_name = $project_name || $environment_; -- Name of the Metadata database used for loading data
SET metadata_config_name = $metadata_database_name || '.' || $metadata_schema_name || '.' || $metadata_config; -- Full string of the Metadata configuration table
SET trigger_log = $metadata_database_name || '.' || $metadata_schema_name || '.TRIGGER_LOG'; -- Full string of the metadata trigger log
SET stage_database_name = 'STAGE' || $environment_; -- Name of the Staging Database.  Change this if you'd like the Staging database named differently.
SET datalake_database_static_name = 'DATALAKE'; -- Name of the Target Database used for creating the storage integration.  Change this if you'd like the Target database named differently.
SET datalake_database_name = 'DATALAKE' || $environment_; -- Name of the Target Database.  Change this if you'd like the Target database named differently.
SET clarity_dbo_schema_name = 'CLARITY_DBO'; -- Change/add the other data sources and schemas you'd like to load.
SET clarity_epic_schema_name = 'CLARITY_EPIC';
SET caboodle_dbo_schema_name = 'EPIC_WAREHOUSE_DBO';
SET caboodle_epic_schema_name = 'EPIC_WAREHOUSE_EPIC';
SET azure_tenant_id = '1acc27f8-8478-4411-a70d-8a7eca63c288'; -- Change to your Tenant ID.
SET storage_account_name = 'fisdemostorageaccount'; -- Change to your storage account name.
SET storage_container = 'datalake-iceberg-demo';
SET storage_container_url = 'azure://' || $storage_account_name || '.blob.core.windows.net/' || $storage_container;


-- iceberg
SET iceberg_storage_account_name = 'eastus2devicebergdemosa'; -- Change to your storage account name.
SET iceberg_storage_container = 'iceberg-demo';
SET iceberg_storage_container_url = 'azure://' || $storage_account_name || '.blob.core.windows.net/' || $storage_container;



--SET dev_storage_url = 'azure://' || $storage_account_name || '.blob.core.windows.net/' || LOWER($datalake_database_static_name) || '-dev'; -- Hard coded since there is only 1 integration needed and it needs all 3 storage containers in the ALLOWED_LOCATIONS parameter.
-- SET test_storage_url = 'azure://' || $storage_account_name || '.blob.core.windows.net/' || LOWER($datalake_database_static_name) || '-test';
-- SET prod_storage_url = 'azure://' || $storage_account_name || '.blob.core.windows.net/' || LOWER($datalake_database_static_name);
SET storage_allowed_locations = '(' || $storage_container_url || ')';
SET storage_queue_name = 'eastus-dev-icebergdemo-queue'; -- Change to your storage queue name.
SET azure_storage_queue_primary_uri = 'https://fisdemostorageaccount.queue.core.windows.net/eastus-dev-icebergdemo-queue';

SET notification_integration_name = 'SNOWPIPE_AZURE_EVENT';  -- Name of the Notification Integration. Change this if you'd like the Notification Integration named differently.
SET storage_integration_name = 'AZURE_STORAGE'; -- Name of the Storage Integration. Change this if you'd like the Storage Integration named differently.
SET external_stage_name = 'UTIL_DB.PUBLIC.AZURE_STAGE' || $environment_; -- Name of the External Stage. Change this if you'd like the External Stage named differently.
/* 5. Below are the scripts to create the warehouse, role, user, objects and permissions required for the role and pipelines to load data into Snowflake. */
CREATE WAREHOUSE IF NOT EXISTS identifier($warehouse_name)
WAREHOUSE_SIZE = xsmall
WAREHOUSE_TYPE = standard
-- MAX_CLUSTER_COUNT = 10  -- Uncomment
-- MIN_CLUSTER_COUNT = 1
AUTO_SUSPEND = 10
AUTO_RESUME = true
INITIALLY_SUSPENDED = true
COMMENT = 'Dedicated Warehouse for ELT executions & operations to ingest data. Recommended WAREHOUSE_SIZE is Extra Small with a high MAX_CLUSTER_COUNT for running pipelines/triggers concurrently.';
use role ACCOUNTADMIN;

CREATE OR REPLACE ROLE identifier($role_name)
COMMENT = 'Role used by the ELT process.';
GRANT ROLE identifier($role_name) to role SYSADMIN;


CREATE USER IF NOT EXISTS identifier($user_name)
PASSWORD = $user_password
DEFAULT_ROLE = $role_name
DEFAULT_WAREHOUSE = $warehouse_name
COMMENT = 'User for executing ELT processes.';

GRANT ROLE identifier($role_name) TO USER identifier($user_name);

GRANT MODIFY ON WAREHOUSE identifier($warehouse_name) TO ROLE identifier($role_name);
GRANT OPERATE ON WAREHOUSE identifier($warehouse_name) TO ROLE identifier($role_name);
GRANT USAGE ON WAREHOUSE identifier($warehouse_name) TO ROLE identifier($role_name);
/* 6. Create the Metadata database, schema and objects.*/
use role SYSADMIN;

CREATE DATABASE IF NOT EXISTS identifier($metadata_database_name)
COMMENT = 'Snowflake Database to host Sources loaded from Azure Data Factory to ingest data into Snowflake using a variety of incremental loading techniques. The database also contains a schema for all metadata to operate and host logging of pipeline/trigger executions.';

USE DATABASE identifier($metadata_database_name);
CREATE SCHEMA IF NOT EXISTS identifier($metadata_schema_name)
   COMMENT = 'Schema for metadata and logging of pipeline/trigger executions.';

USE SCHEMA identifier($metadata_schema_name);
CREATE OR REPLACE TABLE  INGESTION_METADATA_CONFIG
-- identifier($metadata_config_name)
(
METADATA_CONFIG_KEY STRING
,LOGICAL_NAME STRING
,SOURCE_TYPE STRING
,DATABASE_NAME STRING
,SCHEMA_NAME STRING
,SOURCE_TABLE_NAME STRING
,DELTA_COLUMN STRING
,DELTA_VALUE STRING
,CHANGE_TRACKING STRING
,CHANGE_TRACKING_TYPE STRING
,ENABLED STRING
,PRIORITY_FLAG STRING
,PAYLOAD STRING
,BLOB_LAST_LOAD_DT STRING
,STAGE_LAST_LOAD_DT	STRING
,TARGET_LAST_LOAD_DT STRING
,ROW_COUNT STRING
)
COMMENT = 'Snowflake table for storing all metadata needed to populate parameters in the Azure Data Factory pipeline and dynamically load data from source to Snowflake for a given table or file.';

create or replace view EDW_DEV.ETL.CLARITY_DAILY_CONFIG(
	METADATA_CONFIG_KEY,
	LOGICAL_NAME,
	SOURCE_TYPE,
	DATABASE_NAME,
	SCHEMA_NAME,
	SOURCE_TABLE_NAME,
	DELTA_COLUMN,
	DELTA_VALUE,
	CHANGE_TRACKING,
	CHANGE_TRACKING_TYPE,
	ENABLED,
	PRIORITY_FLAG,
	PAYLOAD,
	BLOB_LAST_LOAD_DT,
	STAGE_LAST_LOAD_DT,
	TARGET_LAST_LOAD_DT,
	ROW_COUNT,
    CUSTOM_PARAMS
) as
SELECT * FROM EDW_DEV.ETL.INGESTION_METADATA_CONFIG
WHERE 1=1
AND LOGICAL_NAME = 'Clarity';


CREATE TABLE IF NOT EXISTS identifier($trigger_log)
(
	DATA_FACTORY_NAME VARCHAR(16777216),
	PIPELINE_NAME VARCHAR(16777216),
	RUN_ID VARCHAR(16777216),
	TRIGGER_TYPE VARCHAR(16777216),
	TRIGGER_ID VARCHAR(16777216),
	TRIGGER_NAME VARCHAR(16777216),
	TRIGGER_START_TIME VARCHAR(16777216),
	TRIGGER_END_TIME VARCHAR(16777216),
	EXECUTION_STATUS VARCHAR(16777216)
)
COMMENT = 'Snowflake table for storing all trigger logs from the Azure Data Factory pipeline for logging, monitoring and troubleshooting.';

CREATE OR REPLACE PROCEDURE INSERT_INTO_METADATA_CONFIG
(
LOGICAL_NAME VARCHAR(16777216), 
SOURCE_TYPE VARCHAR(16777216), 
DATABASE_NAME VARCHAR(16777216), 
SCHEMA_NAME VARCHAR(16777216), 
SOURCE_TABLE_NAME VARCHAR(16777216), 
DELTA_COLUMN VARCHAR(16777216), 
DELTA_VALUE VARCHAR(16777216), 
CHANGE_TRACKING VARCHAR(16777216), 
CHANGE_TRACKING_TYPE VARCHAR(16777216),
ENABLED VARCHAR(16777216), 
PRIORITY_FLAG VARCHAR(16777216), 
PAYLOAD VARCHAR(16777216), 
BLOB_LAST_LOAD_DT VARCHAR(16777216), 
STAGE_LAST_LOAD_DT VARCHAR(16777216), 
TARGET_LAST_LOAD_DT VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var sql_command = `INSERT INTO INGESTION_METADATA_CONFIG (METADATA_CONFIG_KEY,LOGICAL_NAME,SOURCE_TYPE,DATABASE_NAME,SCHEMA_NAME,SOURCE_TABLE_NAME,DELTA_COLUMN,DELTA_VALUE,CHANGE_TRACKING,CHANGE_TRACKING_TYPE,ENABLED,PRIORITY_FLAG,PAYLOAD, BLOB_LAST_LOAD_DT, STAGE_LAST_LOAD_DT, TARGET_LAST_LOAD_DT)
SELECT SHA2(CONCAT(
 COLUMN1
,COLUMN2
,COLUMN3
,COLUMN4
,COLUMN5
), 256) METADATA_CONFIG_KEY
,COLUMN1 LOGICAL_NAME
,COLUMN2 SOURCE_TYPE
,COLUMN3 DATABASE_NAME
,COLUMN4 SCHEMA_NAME
,COLUMN5 SOURCE_TABLE_NAME
,COLUMN6 DELTA_COLUMN
,COLUMN7 DELTA_VALUE
,COLUMN8 CHANGE_TRACKING
,COLUMN9 CHANGE_TRACKING_TYPE
,COLUMN10 ENABLED
,COLUMN11 PRIORITY_FLAG
,COLUMN12 PAYLOAD
,COLUMN13 BLOB_LAST_LOAD_DT
,COLUMN14 STAGE_LAST_LOAD_DT
,COLUMN15 TARGET_LAST_LOAD_DT
FROM VALUES(
  ''${LOGICAL_NAME}''
  ,''${SOURCE_TYPE}''
  ,''${DATABASE_NAME}''
  ,''${SCHEMA_NAME}''
  ,''${SOURCE_TABLE_NAME}''
  ,''${DELTA_COLUMN}''
  ,''${DELTA_VALUE}''
  ,${CHANGE_TRACKING}
  ,''${CHANGE_TRACKING_TYPE}''
  ,${ENABLED}
  ,${PRIORITY_FLAG}
  ,''${PAYLOAD}''
  ,''${BLOB_LAST_LOAD_DT}''
  ,''${STAGE_LAST_LOAD_DT}''
  ,''${TARGET_LAST_LOAD_DT}''
) S
WHERE NOT EXISTS (
  SELECT 1
  FROM INGESTION_METADATA_CONFIG T
  WHERE T.METADATA_CONFIG_KEY = SHA2(CONCAT(
 COLUMN1
,COLUMN2
,COLUMN3
,COLUMN4
,COLUMN5
), 256)
)
;` 
    try {
        snowflake.execute (
            {sqlText: sql_command}
            );
        return "Succeeded.";   // Return a success/error indicator.
        }
    catch (err)  {
        return "Failed: " + err;   // Return a success/error indicator.
        }
    ';

GRANT USAGE ON DATABASE identifier($metadata_database_name) TO ROLE identifier($role_name);
GRANT USAGE ON SCHEMA identifier($metadata_schema_name) TO ROLE identifier($role_name);
GRANT ALL ON ALL TABLES IN SCHEMA identifier($metadata_schema_name) TO ROLE identifier($role_name);
GRANT ALL ON ALL VIEWS IN SCHEMA identifier($metadata_schema_name) TO ROLE identifier($role_name);
USE ROLE sysadmin;
GRANT USAGE ON PROCEDURE INSERT_INTO_METADATA_CONFIG (STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING) TO ROLE identifier($role_name);

SHOW PROCEDURES;
/* 7. Create the Stage database.*/

CREATE DATABASE IF NOT EXISTS identifier($stage_database_name)
COMMENT = 'Snowflake database for staging data coming from ADLS.  The data stored in the stage database can be either transient or persistent.';
GRANT USAGE ON DATABASE identifier($stage_database_name) TO ROLE identifier($role_name);
/* 8. Create the Target database.*/
CREATE DATABASE IF NOT EXISTS identifier($datalake_database_name)
COMMENT = 'Snowflake database that consumes data from staging and is the target for replicated data from the source.';
GRANT USAGE ON DATABASE identifier($datalake_database_name) TO ROLE identifier($role_name);
/* 9. Customize this portion of the script for whatever stage and target schemas you want to load data into.*/
USE DATABASE identifier($stage_database_name);
CREATE SCHEMA IF NOT EXISTS identifier($clarity_dbo_schema_name);
CREATE SCHEMA IF NOT EXISTS identifier($clarity_epic_schema_name);
CREATE SCHEMA IF NOT EXISTS identifier($caboodle_dbo_schema_name);
CREATE SCHEMA IF NOT EXISTS identifier($caboodle_epic_schema_name);
GRANT USAGE ON SCHEMA identifier($clarity_dbo_schema_name) TO ROLE identifier($role_name);
GRANT USAGE ON SCHEMA identifier($clarity_epic_schema_name) TO ROLE identifier($role_name);
GRANT USAGE ON SCHEMA identifier($caboodle_dbo_schema_name) TO ROLE identifier($role_name);
GRANT USAGE ON SCHEMA identifier($caboodle_epic_schema_name) TO ROLE identifier($role_name);

USE DATABASE identifier($datalake_database_name);
CREATE SCHEMA IF NOT EXISTS identifier($clarity_dbo_schema_name);
CREATE SCHEMA IF NOT EXISTS identifier($clarity_epic_schema_name);
CREATE SCHEMA IF NOT EXISTS identifier($caboodle_dbo_schema_name);
CREATE SCHEMA IF NOT EXISTS identifier($caboodle_epic_schema_name);
GRANT USAGE ON SCHEMA identifier($clarity_dbo_schema_name) TO ROLE identifier($role_name);
GRANT USAGE ON SCHEMA identifier($clarity_epic_schema_name) TO ROLE identifier($role_name);
GRANT USAGE ON SCHEMA identifier($caboodle_dbo_schema_name) TO ROLE identifier($role_name);
GRANT USAGE ON SCHEMA identifier($caboodle_epic_schema_name) TO ROLE identifier($role_name);

/* 10. Grant USAGE on the UTIL_DB database to access File Formats and External Stages.*/

CREATE DATABASE UTIL_DB;

GRANT USAGE ON DATABASE UTIL_DB TO ROLE identifier($role_name);
CREATE FILE FORMAT UTIL_DB.PUBLIC.PARQUET
TYPE = 'PARQUET'
COMPRESSION = 'AUTO';
GRANT ALL PRIVILEGES ON ALL FILE FORMATS IN DATABASE UTIL_DB TO ROLE identifier($role_name);



/* 11. Create Notification Integration - This allows us to view the Azure Storage Queue to trigger Snowpipe executions.*/
USE ROLE accountadmin;


CREATE OR REPLACE NOTIFICATION INTEGRATION identifier($notification_integration_name)
ENABLED = TRUE
TYPE = QUEUE
NOTIFICATION_PROVIDER = AZURE_STORAGE_QUEUE
AZURE_STORAGE_QUEUE_PRIMARY_URI = $azure_storage_queue_primary_uri
AZURE_TENANT_ID = $azure_tenant_id;

DESCRIBE INTEGRATION identifier($notification_integration_name);

/* Once this is done, use DESCRIBE INTEGRATION {Notification Integration Name} to get the AZURE_CONSENT_URL value.  Paste this in a new tab and authenticate the Snowflake Service Principal.  Once authenticated and it appears in IAM for the storage queue, grant it the 'Storage Queue Data Contributor' role. If you can't find the Service Principal name, it can be found by running DESCRIBE INTEGRATION {Notification Integration Name} and finding the value for AZURE_MULTI_TENANT_APP_NAME.*/

GRANT USAGE ON INTEGRATION identifier($notification_integration_name) TO ROLE identifier($role_name);
/* 12. Create Storage Integration - This allows us to access Azure Storage and stage data for loading.*/

CREATE STORAGE INTEGRATION IF NOT EXISTS identifier($storage_integration_name)
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = AZURE
ENABLED = TRUE
AZURE_TENANT_ID = $azure_tenant_id
STORAGE_ALLOWED_LOCATIONS = ('*');

DESCRIBE INTEGRATION identifier($storage_integration_name);
/* Once this is done, use DESCRIBE INTEGRATION {Storage Integration Name} to get the AZURE_CONSENT_URL value.  Paste this in a new tab and authenticate the Snowflake Service Principal.  Once authenticated and it appears in IAM for the storage account, grant it the 'Storage Blob Data Reader' and 'Storage Blob Data Contributor' roles. If you can't find the Service Principal name, it can be found by running DESCRIBE INTEGRATION {Storage Integration Name} and finding the value for AZURE_MULTI_TENANT_APP_NAME.*/
GRANT USAGE ON INTEGRATION identifier($storage_integration_name) TO ROLE identifier($role_name);
/* 13. Grant permissions and create the external stage.*/
use role sysadmin;
GRANT CREATE STAGE ON SCHEMA UTIL_DB.PUBLIC TO ROLE identifier($role_name);

CREATE OR REPLACE STAGE identifier($external_stage_name)
STORAGE_INTEGRATION = $storage_integration_name
URL = $storage_container_url;

GRANT ALL PRIVILEGES ON STAGE identifier($external_stage_name) TO ROLE identifier($role_name);

/* 14. Grant the role the EXECUTE TASKS and EXECUTE MANAGED TASKS privilege.This allows for tasks to be created and executed by the role/user.  
The GRANT MANAGED TASK command needs to be executed using the ACCOUNTADMIN role.  If the person running this script is not ACCOUNTADMIN, only EXECUTE TASK privilege can be granted and serverless tasks can't be used. 

If serverless tasks will not be used, the Azure Data Factory pipeline code for creating tasks must be changed as well.*/

USE ROLE ACCOUNTADMIN; -- Stay as SYSADMIN if user is not ACCOUNTADMIN;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE identifier($role_name);
GRANT EXECUTE MANAGED TASK ON ACCOUNT TO ROLE identifier($role_name); -- Must be executed by ACCOUNTADMIN.

/* Revert back to SYSADMIN */

USE ROLE SYSADMIN; 
COMMIT;
