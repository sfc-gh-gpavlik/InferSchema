-- Sample usage with Snowflake's TPCH sample data. Run on X-Small warehouse:

create or replace stage INFER_DELIMITED;

-- Get some good rows:
copy into @INFER_DELIMITED from (

  select
  	 L_ORDERKEY::string
	,L_PARTKEY::string
	,L_SUPPKEY::string
	,L_LINENUMBER::string
	,L_QUANTITY::string
	,L_EXTENDEDPRICE::string
	,L_DISCOUNT::string
	,L_TAX::string
	,L_RETURNFLAG::string
	,L_LINESTATUS::string
	,L_SHIPDATE::string
	,L_COMMITDATE::string
	,L_RECEIPTDATE::string
	,L_SHIPINSTRUCT::string
	,L_SHIPMODE::string
	,L_COMMENT::string
    
  from "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."LINEITEM"
union all
  select 'NOT_A_NUMBER', '30674', '5681', '1', '6.00', '9628.02', '0.01', '0.09', 'R', 'F', '1992-06-25', '1992-07-04', '1992-07-02', 'Shipping instructions', 'MAIL', 'Comments' -- Bad integer
union all
  select '12345', '30674', '5681', '1', '6.00', '9628.02', '0.01', '0.09', 'R', 'F', '1992-06-45', '1992-07-04', '1992-07-02', 'Shipping instructions', 'MAIL', 'Comments' -- Bad date

) header = true;


-- Note: You may need to change the database and schema for the SKIP_HEADER file format depending on where you ran the install SQL.
call UTIL_DB.PUBLIC.INFER_DELIMITED_SCHEMA('@INFER_DELIMITED', 'UTIL_DB.PUBLIC.SKIP_HEADER', true, 'LINEITEM');
