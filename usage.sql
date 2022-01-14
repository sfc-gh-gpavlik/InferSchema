-- Sample usage with Snowflake's TPCH sample data. Run on X-Small warehouse:

create or replace stage INFER_DELIMITED;

-- Get some good and bad rows for testing:
copy into @INFER_DELIMITED from (

  select
  	 L_ORDERKEY::string      as L_ORDERKEY
	,L_PARTKEY::string       as L_PARTKEY
	,L_SUPPKEY::string       as L_SUPPKEY
	,L_LINENUMBER::string    as L_LINENUMBER
	,L_QUANTITY::string      as L_QUANTITY
	,L_EXTENDEDPRICE::string as L_EXTENDEDPRICE
	,L_DISCOUNT::string      as L_DISCOUNT
	,L_TAX::string           as L_TAX
	,L_RETURNFLAG::string    as L_RETURNFLAG
	,L_LINESTATUS::string    as L_LINESTATUS
	,L_SHIPDATE::string      as L_SHIPDATE
	,L_COMMITDATE::string    as L_COMMITDATE
	,L_RECEIPTDATE::string   as L_RECEIPTDATE
	,L_SHIPINSTRUCT::string  as L_SHIPINSTRUCT
	,L_SHIPMODE::string      as L_SHIPMODE
	,L_COMMENT::string       as L_COMMENT
    
  from "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."LINEITEM"
union all
  select 'NOT_A_NUMBER', '30674', '5681', '1', '6.00', '9628.02', '0.01', '0.09', 'R', 'F', '1992-06-25', '1992-07-04', '1992-07-02', 'Shipping instructions', 'MAIL', 'Comments' -- Bad integer
union all
  select '12345', '30674', '5681', '1', '6.00', '9628.02', '0.01', '0.09', 'R', 'F', '1992-06-45', '1992-07-04', '1992-07-02', 'Shipping instructions', 'MAIL', 'Comments' -- Bad date

) header = true;


-- Note: You may need to change the database and schema for the SKIP_HEADER file format depending on where you ran the install SQL.
call UTIL_DB.PUBLIC.INFER_DELIMITED_SCHEMA('@INFER_DELIMITED', 'UTIL_DB.PUBLIC.SKIP_HEADER', true, 'LINEITEM');
