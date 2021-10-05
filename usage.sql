-- Sample usage with Snowflake's TPCH sample data. Run on X-Small warehouse:

create or replace temporary stage INFER_DELIMITED;

copy into @INFER_DELIMITED from (select * from "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."LINEITEM" limit 100000) header = true;

-- Note: You may need to change the database and schema for the SKIP_HEADER file format depending on where you ran the install SQL.
call UTIL_DB.PUBLIC.INFER_DELIMITED_SCHEMA('@INFER_DELIMITED', 'UTIL_DB.PUBLIC.SKIP_HEADER', true, 'LINEITEM');
