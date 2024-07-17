--SUSAN P - 1009075

1. How will you use to change the warehouse for workload processing to a warehouse named ‘COMPUTE_WH_XL’?


2. "Consider a table vehicle_inventory that stores vehicle information of all vehicles in your dealership. The table has only one VARIANT column called vehicle_data which stores information in JSON format. The data is given below:
{
“date_of_arrival”: “2021-04-28”,
“supplier_name”: “Hillside Honda”,
“contact_person”: {
“name”: “Derek Larssen”,
“phone”: “8423459854”
},
“vehicle”: [
{
“make”: “Honda”,
“model”: “Civic”,
“variant”: “GLX”,
“year”: “2020”
}
]
}
What is the command to retrieve supplier_name?"


3. From a terminal window, how to start SnowSQL from the command prompt ? And write the steps to load the data from local folder into a Snowflake table using three types of internal stages.


4. "Create an X-Small warehouse named xf_tuts_wh using the CREATE WAREHOUSE command with below options 
a) Size with x-small
b) which can be automatically suspended after 10 mins
c) setup how to automatically resume the warehouse
d) Warehouse should be suspended once after created"


5. "A CSV file ‘customer.csv’ consists of 1 or more records, with 1 or more fields in each record, and sometimes a header record. Records and fields in each file are separated by delimiters. How will
Load the file into snowflake table ?"

6. Write the commands to disable < auto-suspend > option for a virtual warehouse

7. What is the command to concat the column named 'EMPLOYEE' between two % signs ? 

8. "You have stored the below JSON in a table named car_sales as a variant column

{
  ""customer"": [
    {
      ""address"": ""San Francisco, CA"",
      ""name"": ""Joyce Ridgely"",
      ""phone"": ""16504378889""
    }
  ],
  ""date"": ""2017-04-28"",
  ""dealership"": ""Valley View Auto Sales"",
  ""salesperson"": {
    ""id"": ""55"",
    ""name"": ""Frank Beasley""
  },
  ""vehicle"": [
    {
      ""extras"": [
        ""ext warranty"",
        ""paint protection""
      ],
      ""make"": ""Honda"",
      ""model"": ""Civic"",
      ""price"": ""20275"",
      ""year"": ""2017""
    }
  ]
}
How will you query the table to get the dealership data?"

9. A medium size warehouse runs in Auto-scale mode for 3 hours with a resize from Medium (4 servers per cluster) to Large (8 servers per cluster). Warehouse is resized from Medium to Large at 1:30 hours, Cluster 1 runs continuously, Cluster 2 runs continuously for the 2nd and 3rd hours, Cluster 3 runs for 15 minutes in the 3rd hour. How many total credits will be consumed


10. What is the command to check status of snowpipe?

11. What are the different methods of getting/accessing/querying data from Time travel , Assume the table name is 'CUSTOMER' and please write the command for each method.

12. If comma is defined as column delimiter in file "employee.csv" and if we get extra comma in the data how to handle this scenario?

13. What is the command to read data directly from S3 bucket/External/Internal Stage

14. Lets assume we have table with name 'products' which contains duplicate rows. How will delete the duplicate rows ?

15. How is data unloaded out of Snowflake?



--1
USE WAREHOUSE COMPUTE_WH_XL;



--2
SELECT vehicle_data:supplier_name AS supplier_name
FROM vehicle_inventory;



--3
step 1: open command prompt, type SNOWSQL -A <ACCOUNT_IDENTIFIER>
                                    USERID : 
                                    PASSWORD :
                                    ENTER
step 2: 3 types of internal stages to load data from local to snowflake table
    1)user 2)named 3)table
    USER STAGE:     will be created automatically
    TABLE STAGE:     same name as table named and referenced using @%table_name;
    NAMED STAGE:    create stage stage_name; 
    
step 3: 
            use warehouse warehouse_name
            create table structure
    USER STAGE:
            PUT file:://file_location\file_name.file_format @~my_stage;
    TABLE STAGE:
            PUT file:://file_location\file_name.fileformat @%my_stage;
    NAMED stage:
            PUT file:://file_location\file_name.fileformat @my_stage;
           
STEP 4: LISTING THE FILES IN STAGES
    USER STAGE: LIST @~;
    TABLE STAGE: LIST@%my_stage;
    NAMED STAGE: LIST @my_stage;

STEP 5: COPYING FROM LOCAL TO TARGET TABLE
    USER STAGE:
        copy into table_name
        from @~
        file_format = (type = csv field optionally_enclosed_by='"')
        pattern = ' '
        on_error = 'skip_file';
    TABLE STAGE:
        copy into table_name
        from @%table_name
        file_format = (type = csv field optionally_enclosed_by='"')
        pattern = ' '
        on_error = 'skip_file';
    NAMED STAGE:
        copy into table_name
        from @table_name
        file_format = (type = csv field optionally_enclosed_by='"')
        pattern = ' '
        on_error = 'skip_file'; 


        
--4
CREATE WAREHOUSE xf_tuts_wh
WITH WAREHOUSE_SIZE = 'x-small'
AUTO_SUSPEND = 600
AUTO_RESUME = TRUE
INITIALLY_SUSPENDED = TRUE;

--5
CREATE OR REPLACE FILE FORMAT CUSTOMER.CSV
TYPE = CSV
FIELD_DELIMITER = ','
SKIP_HEADER=1;

USING "COPY INTO" COMMAND WE LOAD THE FILE INTO SNOWFLAKE TABLE.


--6
ALTER WAREHOUSE xf_tuts_wh SET AUTO_SUSPEND = NULL;

--7
SELECT CONCAT('%', EMPLOYEE, '%') AS EMPLOYEE_with_signs
FROM your_table_name;


--8
SELECT data:dealership AS dealership
FROM car_sales;


--9


--10
SELECT SYSTEM$PIPE_STATUS;


--11

STEP 1: undrop a table immediately after drop command
    UNDROP TABLE CUSTOMER;

STEP 2: querying data at specific timestamp
    SELECT * FROM CUSTOMER AT (TIMESTAMP => '2023-07-15 12:00:00');

STEP 3: using query id
    SELECT * FROM CUSTOMER BEFORE (STATEMENT => 'XXXXXXXXXX');


--12
CREATE OR REPLACE FILE FORMAT my_csv_format
TYPE = 'CSV'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
SKIP_HEADER = 1
FIELD_DELIMITER = ',';


--13
--READING FROM S3 BUCKET:
1. create external stage
    CREATE OR REPLACE STAGE MY_STAGE
    URL = "";
2. copy data from s3 bucket
    COPY INTO MY_TABLE
    FROM @MY_STAGE
    FILE_FORMAT = (TYPE='CSV', FIELD_OPTIONALLY_ENCLOSED_BY = '"');
--READ FROM EXTERNAL STAGE
    COPY INTO my_table
    FROM @my_external_stage
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' );
--READ FROM INTERNAL STAGE
    COPY INTO my_table
    FROM @my_external_stage
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' );


--14



--15: unloading the data
create external stage and use get command to unload the data.
