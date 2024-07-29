USE warehouse demo_wh1;

create database person_db1;
use database person_db1;

--c
CREATE OR REPLACE FILE FORMAT json_format
TYPE = 'JSON';


CREATE OR REPLACE STAGE my_S3_stage
  URL='s3://asg3-bucket/json-folder/'
  CREDENTIALS=(AWS_KEY_ID='***************************' 
                AWS_SECRET_KEY='**********************************')
  FILE_FORMAT = (FORMAT_NAME = 'json_format');

  list @my_S3_stage;

--d
CREATE OR REPLACE TABLE PERSON_NESTED (
    raw_data VARIANT
);


--e
CREATE OR REPLACE PIPE asg3_pipe
AUTO_INGEST = TRUE
AS
COPY INTO PERSON_NESTED
FROM @my_S3_stage
FILE_FORMAT = (FORMAT_NAME = 'json_format')
ON_ERROR = 'CONTINUE';

--f
show pipes;

--g
alter pipe asg3_pipe refresh;

--b. Validation if snowpipe ran successfully

--1
select SYSTEM$PIPE_STATUS('asg3_pipe');

--2
select *
from table(information_schema.copy_history(TABLE_NAME=>'PERSON_NESTED', 
START_TIME=> DATEADD(hours, -1, CURRENT_TIMESTAMP())));

--3
select * from PERSON_NESTED;

--Change Data Capture using Streams, Tasks and Merge.
--1.

CREATE OR REPLACE STREAM PERSON_NESTED_STREAM ON TABLE PERSON_NESTED;

--2
CREATE OR REPLACE TABLE PERSON_MASTER (
    PERSON_ID NUMBER,
    NAME VARCHAR,
    AGE NUMBER,
    LOCATION VARCHAR,
    ZIP VARCHAR
);

CREATE OR REPLACE PROCEDURE PERSON_MASTER_PROCEDURE()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    // Unnest and merge data into PERSON_MASTER
    var sql_unset_person_master = `
        MERGE INTO PERSON_MASTER t
        USING (
            SELECT
                item:ID::NUMBER AS PERSON_ID,
                item:Name::VARCHAR AS NAME,
                item:age::NUMBER AS AGE,
                item:location::VARCHAR AS LOCATION,
                item:zip::VARCHAR AS ZIP
            FROM PERSON_NESTED,
            LATERAL FLATTEN(input => raw_data:_1) AS f
        ) s
        ON t.PERSON_ID = s.PERSON_ID
        WHEN MATCHED THEN UPDATE SET
            NAME = s.NAME,
            AGE = s.AGE,
            LOCATION = s.LOCATION,
            ZIP = s.ZIP
        WHEN NOT MATCHED THEN INSERT (
            PERSON_ID,
            NAME,
            AGE,
            LOCATION,
            ZIP
        ) VALUES (
            s.PERSON_ID,
            s.NAME,
            s.AGE,
            s.LOCATION,
            s.ZIP
        );
    `;

    var sql_insert_location = `
        INSERT INTO PERSON_LOCATION (NAME, LOCATION)
        SELECT NAME, LOCATION
        FROM PERSON_MASTER
        WHERE LOCATION IS NOT NULL
    `;

    var sql_insert_age = `
        INSERT INTO PERSON_AGE (NAME, AGE)
        SELECT NAME, AGE
        FROM PERSON_MASTER
        WHERE AGE IS NOT NULL
    `;

    var stmt1 = snowflake.createStatement({sqlText: sql_unset_person_master});
    stmt1.execute();

    var stmt2 = snowflake.createStatement({sqlText: sql_insert_location});
    stmt2.execute();
    
    var stmt3 = snowflake.createStatement({sqlText: sql_insert_age});
    stmt3.execute();

    return "Data successfully loaded into PERSON_MASTER, PERSON_LOCATION, and PERSON_AGE tables.";
$$;



--3 Create a task
CREATE OR REPLACE TASK person_master_task 
WAREHOUSE='demo_wh1' 
SCHEDULE='1 minute'
WHEN SYSTEM$STREAM_HAS_DATA('PERSON_NESTED_STREAM') AS
CALL PERSON_MASTER_PROCEDURE();

SHOW TASKS LIKE 'person_master_task';

ALTER TASK person_master_task RESUME;



--4. Test PIPELINE

--a. ensure all the tables are empty
TRUNCATE TABLE PERSON_NESTED;

TRUNCATE TABLE PERSON_MASTER;

TRUNCATE TABLE PERSON_AGE;

TRUNCATE TABLE PERSON_LOCATION;

--b. upload file into s3

--c. Select data from PERSON_NESTED: Snowpipe would have loaded data to PERSON_NESTED table based on s3 sqs event notification.

SELECT * FROM PERSON_NESTED;

--d. check copy_history
SELECT *
FROM table(information_schema.copy_history(TABLE_NAME=>'PERSON_NESTED', 
START_TIME=> DATEADD(hours, -1, CURRENT_TIMESTAMP())));


--e.
SELECT * FROM PERSON_NESTED_STREAM;

--f
SELECT * FROM PERSON_MASTER;

--g.
SELECT * FROM PERSON_AGE;
SELECT * FROM PERSON_LOCATION;




--5. ELT IN SNOWFLAKE USING STORED PROCEDURE

--a.
CREATE OR REPLACE TABLE PERSON_AGE (
    NAME VARCHAR,
    AGE NUMBER
);

CREATE OR REPLACE TABLE PERSON_LOCATION (
    NAME VARCHAR,
    LOCATION VARCHAR
);

--b. Stored procedure call
CREATE OR REPLACE PROCEDURE PERSON_MASTER_PROCEDURE()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    // Unnest and merge data into PERSON_MASTER
    var sql_unset_person_master = `
        MERGE INTO PERSON_MASTER t
        USING (
            SELECT
                value:ID::NUMBER AS PERSON_ID,
                value:Name::VARCHAR AS NAME,
                value:age::NUMBER AS AGE,
                value:location::VARCHAR AS LOCATION,
                value:zip::VARCHAR AS ZIP
            FROM PERSON_NESTED,
            LATERAL FLATTEN(input => raw_data:_1) AS f
        ) s
        ON t.PERSON_ID = s.PERSON_ID
        WHEN MATCHED THEN UPDATE SET
            NAME = s.NAME,
            AGE = s.AGE,
            LOCATION = s.LOCATION,
            ZIP = s.ZIP
        WHEN NOT MATCHED THEN INSERT (
            PERSON_ID,
            NAME,
            AGE,
            LOCATION,
            ZIP
        ) VALUES (
            s.PERSON_ID,
            s.NAME,
            s.AGE,
            s.LOCATION,
            s.ZIP
        );
    `;

    var sql_insert_location = `
        INSERT INTO PERSON_LOCATION (NAME, LOCATION)
        SELECT NAME, LOCATION
        FROM PERSON_MASTER
        WHERE LOCATION IS NOT NULL
    `;

    var sql_insert_age = `
        INSERT INTO PERSON_AGE (NAME, AGE)
        SELECT NAME, AGE
        FROM PERSON_MASTER
        WHERE AGE IS NOT NULL
    `;

    var stmt1 = snowflake.createStatement({sqlText: sql_unset_person_master});
    stmt1.execute();

    var stmt2 = snowflake.createStatement({sqlText: sql_insert_location});
    stmt2.execute();
    
    var stmt3 = snowflake.createStatement({sqlText: sql_insert_age});
    stmt3.execute();

    return "Data successfully loaded into PERSON_MASTER, PERSON_LOCATION, and PERSON_AGE tables.";
$$;



CALL PERSON_MASTER_PROCEDURE();

