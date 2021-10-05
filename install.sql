/********************************************************************************************************
*                                                                                                       *
*                                  Snowflake Infer Delimited Schema                                     *
*                                                                                                       *
*  Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.                                     *
*                                                                                                       *
*  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in  *
*  compliance with the License. You may obtain a copy of the License at                                 *
*                                                                                                       *
*                             http://www.apache.org/licenses/LICENSE-2.0                                *
*                                                                                                       *
*  Unless required by applicable law or agreed to in writing, software distributed under the License    *
*  is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or  *
*  implied. See the License for the specific language governing permissions and limitations under the   *
*  License.                                                                                             *
*                                                                                                       *
*  Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.                                     *
*                                                                                                       *
********************************************************************************************************/

use database UTIL_DB;
use schema PUBLIC;

create or replace function TRY_MULTI_TIMESTAMP(STR string)
returns timestamp
language SQL
as
$$
    case
        when STR RLIKE '[A-Za-z]{3} \\d{2} \\d{4} \\d{2}:\\d{2}:\\d{2} UTC' then try_to_timestamp(left(STR, 20), 'MON DD YYYY HH24:MI:SS')
        when STR RLIKE '\\d{1,4}-\\d{1,2}-\\d{2} \\d{1,2}:\\d{2}:\\d{2} [A|P][M]' then try_to_timestamp(STR, 'YYYY-MM-DD HH12:MI:SS AM')
        when STR RLIKE '\\d{1,2}/\\d{1,2}/\\d{4}' then try_to_timestamp(STR, 'mm/dd/yyyy')
        when STR RLIKE '\\d{1,2}\\/\\d{1,2}\\/\\d{4} \\d{1,2}:\\d{2}:\\d{2} [A-Za-z]{2}' then try_to_timestamp(STR, 'MM/DD/YYYY HH12:MI:SS AM')
        when STR RLIKE '\\d{1,2}\\/\\d{1,2}\\/\\d{4} \\d{1,2}:\\d{2}' then try_to_timestamp(STR, 'MM/DD/YYYY HH24:MI')
        when STR RLIKE '[A-Za-z]{3}, \\d{1,2} [A-Za-z]{3} \\d{4} \\d{1,2}:\\d{1,2}:\\d{1,2} [A-Za-z]{3}' then try_to_timestamp(left(STR, len(STR) - 4) || ' ' || '00:00', 'DY, DD MON YYYY HH:MI:SS TZH:TZM')   -- Fri, 17 Apr 2020 17:55:45 GMT  (from Snowflake "LIST" command)
        when STR RLIKE '\\d{1,2}/\\d{1,2}/\\d{2} \\d{1,2}:\\d{2} [A|P][M]' then try_to_timestamp(STR, 'MM/DD/YY HH12:MI AM')
        when STR RLIKE '[A-Za-z]{3} [A-Za-z]{3} \\d{2} \\d{4} \\d{1,2}:\\d{2}:\\d{2} GMT.*' then try_to_timestamp(left(replace(substr('Sat Oct 02 2021 17:53:40 GMT+0000 (Coordinated Universal Time)', 5), 'GMT', ''), 26), 'MON DD YYYY HH:MI:SS TZHTZM')  -- Javascript
        else try_to_timestamp(STR)                                                                                                                                                                          -- Last chance try for unknown timestamp format
    end
$$;

create or replace function TRY_EXACT_DATE(STR string)
returns date
language sql
as
$$
    iff(
        try_multi_timestamp(STR) is not null and try_multi_timestamp(STR) = try_multi_timestamp(STR)::date,
        try_multi_timestamp(STR)::date,
        null
    )
$$;

create or replace function TRY_EXACT_INTEGER(STR string)
returns int
language sql
as
$$
    iff(
        try_to_double(STR) is not null and try_to_double(STR) = try_to_double(STR)::int,
        try_to_double(STR)::int,
        null
    )
$$;

create or replace file format READ_LINES
type = 'csv'
compression = 'auto'
field_delimiter = 'none'
record_delimiter = '\n' 
skip_header = 0 
field_optionally_enclosed_by = 'none'
trim_space = false
escape = 'none'
escape_unenclosed_field = '\134'
;

create or replace file format SKIP_HEADER
type = 'csv'
compression = 'auto'
field_delimiter = ','
record_delimiter = '\n' 
skip_header = 0 
field_optionally_enclosed_by = 'none'
trim_space = false
error_on_column_count_mismatch = true
escape = 'none'
escape_unenclosed_field = '\134'
date_format = 'auto'
timestamp_format = 'auto'
null_if = ('\\N')
;

create or replace procedure UTIL_DB.PUBLIC.INFER_DELIMITED_SCHEMA(STAGE_PATH string, FILE_FORMAT string, FIRST_ROW_IS_HEADER boolean, NEW_TABLE_NAME string)
returns string
language javascript
execute as caller       -- This is for Disney (find the Jira) and Jerome Caron
as
$$

/****************************************************************************************************
*  Preferences Section                                                                              *
****************************************************************************************************/

MAX_ROW_SAMPLES          = 100000;        // Sets the maximum number of rows the inference will test.
PROJECT_NAMESPACE        = "UTIL_DB.PUBLIC"
USE_TRY_MULTI_TIMESTAMP  = true;
NUMBERED_COLUMN_PREFIX   = "COLUMN_";

/****************************************************************************************************
*  Do not modify below this section                                                                 *
****************************************************************************************************/

/****************************************************************************************************
*  DataType Classes                                                                                 *
****************************************************************************************************/

class Query{
    constructor(statement){
        this.statement = statement;
    }
}

class DataType {
    constructor(column, ordinalPosition, sourceQuery) {
        this.sourceQuery = sourceQuery
        this.column = column;
        this.ordinalPosition = ordinalPosition;
        this.insert = '@~COLUMN~@';
        this.totalCount = 0;
        this.notNullCount = 0;
        this.typeCount = 0;
        this.blankCount = 0;
        this.minTypeOf  = 0.95;
        this.minNotNull = 1.00;
    }
    setSQL(sqlTemplate){
        this.sql = sqlTemplate;
        this.sql = this.sql.replace(/@~COLUMN~@/g, this.column);
    }
    getCounts(){
        var rs;
        rs = GetResultSet(this.sql);
        rs.next();
        this.totalCount   = rs.getColumnValue("TOTAL_COUNT");
        this.notNullCount = rs.getColumnValue("NON_NULL_COUNT");
        this.typeCount    = rs.getColumnValue("TO_TYPE_COUNT");
        this.blankCount   = rs.getColumnValue("BLANK");
    }
    isCorrectType(){
        return (this.typeCount / (this.notNullCount - this.blankCount) >= this.minTypeOf);
    }
    isNotNull(){
        return (this.notNullCount / this.totalCount >= this.minNotNull);
    }
}

class DateType extends DataType{
    constructor(column, ordinalPosition, sourceQuery){
        super(column, ordinalPosition, sourceQuery)
        this.syntax = "date";
        this.insert = `${PROJECT_NAMESPACE}.try_exact_date(trim(@~COLUMN~@))`;
        this.sourceQuery = sourceQuery;
        this.setSQL(GetCheckTypeSQL(this.insert, this.sourceQuery));
        this.getCounts();
    }
}

class TimestampType extends DataType{
    constructor(column, ordinalPosition, sourceQuery){
        super(column, ordinalPosition, sourceQuery)
        this.syntax = "timestamp";
        this.insert = `${PROJECT_NAMESPACE}.try_multi_timestamp(trim(@~COLUMN~@))`;
        this.sourceQuery = sourceQuery;
        this.setSQL(GetCheckTypeSQL(this.insert, this.sourceQuery));
        this.getCounts();
    }
}

class IntegerType extends DataType{
    constructor(column, ordinalPosition, sourceQuery){
        super(column, ordinalPosition, sourceQuery)
        this.syntax = "number(38,0)";
        this.insert = `${PROJECT_NAMESPACE}.try_exact_integer(trim(@~COLUMN~@))`;
        this.setSQL(GetCheckTypeSQL(this.insert, this.sourceQuery));
        this.getCounts();
    }
}

class DoubleType extends DataType{
    constructor(column, ordinalPosition, sourceQuery){
        super(column, ordinalPosition, sourceQuery)
        this.syntax = "double";
        this.insert = 'try_to_double(trim(@~COLUMN~@))';
        this.setSQL(GetCheckTypeSQL(this.insert, this.sourceQuery));
        this.getCounts();
    }
}

class BooleanType extends DataType{
    constructor(column, ordinalPosition, sourceQuery){
        super(column, ordinalPosition, sourceQuery)
        this.syntax = "boolean";
        this.insert = 'try_to_boolean(trim(@~COLUMN~@))';
        this.setSQL(GetCheckTypeSQL(this.insert, this.sourceQuery));
        this.getCounts();
    }
}

 // Catch all is STRING data type
class StringType extends DataType{
    constructor(column, ordinalPosition, sourceQuery){
        super(column, ordinalPosition, sourceQuery)
        this.syntax = "string";
        this.totalCount   = 1;
        this.notNullCount = 0;
        this.typeCount    = 1;
        this.minTypeOf    = 0;
        this.minNotNull   = 1;
    }
}

/****************************************************************************************************
*  Main function                                                                                    *
****************************************************************************************************/

let headerSQL = `select $1 as HEADER from ${STAGE_PATH} (file_format => '${PROJECT_NAMESPACE}.READ_LINES') limit 1;`;
let headerRow = ExecuteSingleValueQuery('HEADER', headerSQL);

let header;

if (FIRST_ROW_IS_HEADER) {
    header = headerRow.split(',');
} else {
    header = [];
    let cols = headerRow.split(',');
    for (let colPos = 0; colPos < cols.length; colPos++ ) {
        header.push("$" + colPos+1);
    }
}

let sql = "select\n";
for (let i = 0; i < header.length; i++) {
    sql += (i > 0 ? ",$" : "$") + `${i+1} as ${header[i]}\n`;
}
sql += `from ${STAGE_PATH} ( file_format => 'SKIP_HEADER') limit ${MAX_ROW_SAMPLES}`;

let qMain = GetQuery(sql);

let column;
let typeOf;
let ins = '';

var newTableDDL = '';
var insertDML   = '';

for (let c = 0; c < header.length; c++) {
    if(c > 0){
        newTableDDL += ",\n";
        insertDML   += ",\n";
    }
    if (FIRST_ROW_IS_HEADER) {
        column = header[c];
    } else {
        column = "$" + c+1;
    }

    typeOf = InferDataType(header[c], c + 1, qMain.statement.getQueryId());
    newTableDDL += GetColumnDdlName(typeOf, FIRST_ROW_IS_HEADER, NUMBERED_COLUMN_PREFIX) + ' ' + typeOf.syntax;
    ins = typeOf.insert;
    insertDML   += ins.replace(/@~COLUMN~@/g, "$" + typeOf.ordinalPosition);
}

return GetOpeningComments()                +
       GetDDLPrefixSQL(NEW_TABLE_NAME)     +
       newTableDDL                         +
       GetDDLSuffixSQL()                   +
       GetDividerSQL()                     +
       GetInsertPrefixSQL(NEW_TABLE_NAME)  +
       insertDML                           +
       GetInsertSuffixSQL(STAGE_PATH)      ;

/****************************************************************************************************
*  Helper functions                                                                                 *
****************************************************************************************************/

function InferDataType(column, ordinalPosition, sourceQuery){

    var typeOf;

    typeOf = new IntegerType(column, ordinalPosition, sourceQuery);
    if (typeOf.isCorrectType()) return typeOf;

    typeOf = new DoubleType(column, ordinalPosition, sourceQuery);
    if (typeOf.isCorrectType()) return typeOf;

    typeOf = new BooleanType(column, ordinalPosition, sourceQuery);        // May want to do a distinct and look for two values
    if (typeOf.isCorrectType()) return typeOf;

    typeOf = new DateType(column, ordinalPosition, sourceQuery);
    if (typeOf.isCorrectType()) return typeOf;

    typeOf = new TimestampType(column, ordinalPosition, sourceQuery);
    if (typeOf.isCorrectType()) return typeOf;

    typeOf = new StringType(column, ordinalPosition, sourceQuery);
    if (typeOf.isCorrectType()) return typeOf;

    return null;
}

function GetQuery(sql){
    cmd = {sqlText: sql};
    var query = new Query(snowflake.createStatement(cmd));
    query.resultSet = query.statement.execute();
    return query;
}

/****************************************************************************************************
*  SQL Template Functions                                                                           *
****************************************************************************************************/

function GetColumnDdlName(typeOf, firstRowIsHeader, numberedColumnPrefix) {
    if (firstRowIsHeader) {
        return '"' + typeOf.column + '"';
    } else {
        return numberedColumnPrefix + typeOf.ordinalPosition;
    }
}

function GetCheckTypeSQL(insert, sourceQuery){

var sql = 
`
select      count(1)                              as TOTAL_COUNT,
            count("@~COLUMN~@")                   as NON_NULL_COUNT,
            count(${insert})                      as TO_TYPE_COUNT,
            sum(iff(trim("@~COLUMN~@")='', 1, 0)) as BLANK
from        (select * from table(result_scan('${sourceQuery}')))
`;

return sql;
}

function GetTableColumnsSQL(dbName, schemaName, tableName){

var sql = 
`
select  COLUMN_NAME 
from    ${dbName}.INFORMATION_SCHEMA.COLUMNS
where   TABLE_CATALOG = '${dbName}' and
        TABLE_SCHEMA  = '${schemaName}' and
        TABLE_NAME    = '${tableName}'
order by ORDINAL_POSITION;
`;
  
return sql;
}

function GetOpeningComments(){
return `
/**************************************************************************************************************
*   Copy, paste, review and run to create a typed table and insert into the new table from stage.             *
**************************************************************************************************************/
`;
}

function GetDDLPrefixSQL(table) {

var sql =
`
create or replace table ${table}
(
`;

    return sql;
}

function GetDDLSuffixSQL(){
    return "\n);";
}

function GetDividerSQL(){
return `\n
/**************************************************************************************************************
*   The SQL statement below this attempts to copy all rows from the stage to the typed table.                 *
**************************************************************************************************************/
`;
}

function GetInsertPrefixSQL(table) {
var sql =
`\ninsert into ${table} select\n`;
return sql;
}

function GetInsertSuffixSQL(stagePath){
var sql =
`\nfrom ${stagePath} ;`;
return sql;
}

/****************************************************************************************************
*  SQL functions                                                                                    *
****************************************************************************************************/

function GetResultSet(sql) {
    cmd = {sqlText: sql};
    stmt = snowflake.createStatement(cmd);
    var rs;
    rs = stmt.execute();
    return rs;
}

function ExecuteNonQuery(queryString) {
    var out = '';
    cmd1 = {sqlText: queryString};
    stmt = snowflake.createStatement(cmd1);
    var rs;
    rs = stmt.execute();
}

function ExecuteSingleValueQuery(columnName, queryString) {
    var out;
    cmd1 = {sqlText: queryString};
    stmt = snowflake.createStatement(cmd1);
    var rs;
    try{
        rs = stmt.execute();
        rs.next();
        return rs.getColumnValue(columnName);
    }
    catch(err) {
        if (err.message.substring(0, 18) == "ResultSet is empty"){
            throw "ERROR: No rows returned in query.";
        } else {
            throw "ERROR: " + err.message.replace(/\n/g, " ");
        } 
    }
    return out;
}

function ExecuteFirstValueQuery(queryString) {
    var out;
    cmd1 = {sqlText: queryString};
    stmt = snowflake.createStatement(cmd1);
    var rs;
    try{
        rs = stmt.execute();
        rs.next();
        return rs.getColumnValue(1);
    }
    catch(err) {
        if (err.message.substring(0, 18) == "ResultSet is empty"){
            throw "ERROR: No rows returned in query.";
        } else {
            throw "ERROR: " + err.message.replace(/\n/g, " ");
        } 
    }
    return out;
}

function getQuery(sql){
    var cmd = {sqlText: sql};
    var query = new Query(snowflake.createStatement(cmd));
    try {
        query.resultSet = query.statement.execute();
    } catch (err) {
        throw "ERROR: " + err.message.replace(/\n/g, " ");
    }
    return query;
}

$$;
  
-- Sample usage:
-- call UTIL_DB.PUBLIC.INFER_DELIMITED_SCHEMA('@CSV_INFER_SCHEMA.PUBLIC.TPCH_CSV', '"CSV_INFER_SCHEMA"."PUBLIC".SKIP_HEADER', true, 'TPCH_INFERRED');

