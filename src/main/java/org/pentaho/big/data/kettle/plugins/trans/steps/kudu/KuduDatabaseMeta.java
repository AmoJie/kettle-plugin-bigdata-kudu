package org.pentaho.big.data.kettle.plugins.trans.steps.kudu;

import java.util.List;

import org.apache.commons.compress.utils.Lists;
import org.pentaho.di.core.database.*;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.plugins.DatabaseMetaPlugin;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.Utils;

/**
 * DatabaseMeta数据库插件- 如果有kudu数据库的插件的时候可以使用该类，如Impala
 */
@DatabaseMetaPlugin(type = "KUDU", typeDescription = "kudu")
public class KuduDatabaseMeta extends BaseDatabaseMeta implements DatabaseInterface {

    private static final String STRICT_BIGNUMBER_INTERPRETATION = "STRICT_NUMBER_38_INTERPRETATION";

    @Override
    public int[] getAccessTypeList() {
        return new int[]{DatabaseMeta.TYPE_ACCESS_NATIVE};
    }

    @Override
    public int getDefaultDatabasePort() {
        return 7051;
    }

    /**
     * 当前数据库是否支持自增类型的字段
     */
    @Override
    public boolean supportsAutoInc() {
        return false;
    }

    /**
     * 获取限制读取条数的数据，追加再select语句后实现限制返回的结果数
     *
     * @see org.pentaho.di.core.database.DatabaseInterface#getLimitClause(int)
     */
    @Override
    public String getLimitClause(int nrRows) {
        return "";
    }

    /**
     * 返回获取表所有字段信息的语句
     *
     * @param tableName
     * @return The SQL to launch.
     */
    @Override
    public String getSQLQueryFields(String tableName) {
        return "";
    }

    @Override
    public String getSQLTableExists(String tablename) {
        return getSQLQueryFields(tablename);
    }

    @Override
    public String getSQLColumnExists(String columnname, String tablename) {
        return getSQLQueryColumnFields(columnname, tablename);
    }

    public String getSQLQueryColumnFields(String columnname, String tableName) {
        return "";
    }

    @Override
    public boolean needsToLockAllTables() {
        return false;
    }

    @Override
    public String getDriverClass() {
        return "kudu-client-1.8.0.jar";
    }

    @Override
    public String getURL(String hostname, String port, String databaseName) throws KettleDatabaseException {
        String url = "%s:%s";
        String _hostname = hostname;
        String _port = port;
        if (Utils.isEmpty(hostname)) {
            _hostname = "localhost";
        }
        if (Utils.isEmpty(port) || port.equals("-1")) {
            _port = String.valueOf(getDefaultDatabasePort());
        }
        String[] hosts = hostname.split(",");
        String[] ports = port.split(",");
        List<String> masters = Lists.newArrayList();
        if(ports.length == 1){
            for (String host : hosts) {
                masters.add(String.format(url, host,_port));
            }
        }else if(ports.length == hosts.length){
            for(int i = 0; i < ports.length; i++){
                masters.add(String.format(url, hosts[i],ports[i]));
            }
        }else{
            throw new KettleDatabaseException("主机地址和端口号数量不匹配");
        }
        return String.join(",",masters);
    }

    /**
     * Oracle doesn't support options in the URL, we need to put these in a
     * Properties object at connection time...
     */
    @Override
    public boolean supportsOptionsInURL() {
        return false;
    }

    /**
     * @return true if the database supports sequences
     */
    @Override
    public boolean supportsSequences() {
        return true;
    }

    /**
     * Check if a sequence exists.
     *
     * @param sequenceName The sequence to check
     * @return The SQL to get the name of the sequence back from the databases data
     * dictionary
     */
    @Override
    public String getSQLSequenceExists(String sequenceName) {
        return "";
    }

    /**
     * Get the current value of a database sequence
     *
     * @param sequenceName The sequence to check
     * @return The current value of a database sequence
     */
    @Override
    public String getSQLCurrentSequenceValue(String sequenceName) {
        return "";
    }

    /**
     * Get the SQL to get the next value of a sequence. (Oracle only)
     *
     * @param sequenceName The sequence name
     * @return the SQL to get the next value of a sequence. (Oracle only)
     */
    @Override
    public String getSQLNextSequenceValue(String sequenceName) {
        return "";
    }

    @Override
    public boolean supportsSequenceNoMaxValueOption() {
        return true;
    }

    /**
     * @return true if we need to supply the schema-name to getTables in order to
     * get a correct list of items.
     */
    @Override
    public boolean useSchemaNameForTableList() {
        return true;
    }

    /**
     * @return true if the database supports synonyms
     */
    @Override
    public boolean supportsSynonyms() {
        return true;
    }

    /**
     * Generates the SQL statement to add a column to the specified table
     *
     * @param tablename   The table to add
     * @param v           The column defined as a value
     * @param tk          the name of the technical key field
     * @param use_autoinc whether or not this field uses auto increment
     * @param pk          the name of the primary key field
     * @param semicolon   whether or not to add a semi-colon behind the statement.
     * @return the SQL statement to add a column to the specified table
     */
    @Override
    public String getAddColumnStatement(String tablename, ValueMetaInterface v, String tk, boolean use_autoinc,
                                        String pk, boolean semicolon) {
        return "";
    }

    /**
     * Generates the SQL statement to drop a column from the specified table
     *
     * @param tablename   The table to add
     * @param v           The column defined as a value
     * @param tk          the name of the technical key field
     * @param use_autoinc whether or not this field uses auto increment
     * @param pk          the name of the primary key field
     * @param semicolon   whether or not to add a semi-colon behind the statement.
     * @return the SQL statement to drop a column from the specified table
     */
    @Override
    public String getDropColumnStatement(String tablename, ValueMetaInterface v, String tk, boolean use_autoinc,
                                         String pk, boolean semicolon) {
        return "";
    }

    /**
     * Generates the SQL statement to modify a column in the specified table
     *
     * @param tablename   The table to add
     * @param v           The column defined as a value
     * @param tk          the name of the technical key field
     * @param use_autoinc whether or not this field uses auto increment
     * @param pk          the name of the primary key field
     * @param semicolon   whether or not to add a semi-colon behind the statement.
     * @return the SQL statement to modify a column in the specified table
     */
    @Override
    public String getModifyColumnStatement(String tablename, ValueMetaInterface v, String tk, boolean use_autoinc,
                                           String pk, boolean semicolon) {
        return "";
    }

    @Override
    public String getFieldDefinition(ValueMetaInterface v, String tk, String pk, boolean use_autoinc,
                                     boolean add_fieldname, boolean add_cr) {
        return "";
    }

    /*
     * (non-Javadoc)
     *
     * @see com.ibridge.kettle.core.database.DatabaseInterface#getReservedWords()
     */
    @Override
    public String[] getReservedWords() {
        return new String[]{"ALIAS", "AND", "AS", "AT", "BEGIN", "BETWEEN", "BIGINT", "BIT", "BY", "BOOLEAN", "BOTH",
                "CALL", "CASE", "CAST", "CHAR", "CHARACTER", "COMMIT", "CONSTANT", "CURSOR", "COALESCE", "CONTINUE",
                "CONVERT", "CURRENT_DATE", "CURRENT_TIMESTAMP", "CURRENT_USER", "DATE", "DEC", "DECIMAL", "DECLARE",
                "DEFAULT", "DECODE", "DELETE", "ELSE", "ELSIF", "END", "EXCEPTION", "EXECUTE", "EXIT", "EXTRACT",
                "FALSE", "FETCH", "FLOAT", "FOR", "FROM", "FUNCTION", "GOTO", "IF", "IN", "INT", "INTO", "IS",
                "INTEGER", "IMMEDIATE", "INDEX", "INOUT", "INSERT", "LEADING", "LIKE", "LIMIT", "LOCALTIME",
                "LOCALTIMESTAMP", "LOOP", "NCHAR", "NEXT", "NOCOPY", "NOT", "NULLIF", "NULL", "NUMBER", "NUMERIC",
                "OPTION", "OF", "OR", "OUT", "OVERLAY", "PERFORM", "POSITION", "PRAGMA", "PROCEDURE", "QUERY", "RAISE",
                "RECORD", "RENAME", "RETURN", "REVERSE", "ROLLBACK", "REAL", "SELECT", "SAVEPOINT", "SETOF", "SMALLINT",
                "SUBSTRING", "SQL", "SYSDATE", "SESSION_USER", "THEN", "TO", "TYPE", "TABLE", "TIME", "TIMESTAMP",
                "TINYINT", "TRAILING", "TREAT", "TRIM", "TRUE", "TYPE", "UID", "UPDATE", "USER", "USING", "VARCHAR",
                "VARCHAR2", "VALUES", "WITH", "WHEN", "WHILE", "LEVEL"};
    }

    /**
     * @return The SQL on this database to get a list of stored procedures.
     */
    @Override
    public String getSQLListOfProcedures() {
        /*
         * return
         * "SELECT DISTINCT DECODE(package_name, NULL, '', package_name||'.') || object_name "
         * + "FROM user_arguments " + "ORDER BY 1";
         */
        return "";
    }

    @Override
    public String getSQLLockTables(String[] tableNames) {
       return "";
    }

    @Override
    public String getSQLUnlockTables(String[] tableNames) {
        return null;
    }

    /**
     * @return extra help text on the supported options on the selected database
     * platform.
     */
    @Override
    public String getExtraOptionsHelpText() {
        return "https://kudu.apache.org/";
    }

    @Override
    public String[] getUsedLibraries() {
        return new String[]{"kudu-client-1.8.0.jar"};
    }

    /**
     * Verifies on the specified database connection if an index exists on the
     * fields with the specified name.
     *
     * @param database   a connected database
     * @param schemaName
     * @param tableName
     * @param idx_fields
     * @return true if the index exists, false if it doesn't.
     * @throws KettleDatabaseException
     */
    @Override
    public boolean checkIndexExists(Database database, String schemaName, String tableName, String[] idx_fields)
            throws KettleDatabaseException {

        return false;
    }

    @Override
    public boolean requiresCreateTablePrimaryKeyAppend() {
        return true;
    }

    /**
     * Most databases allow you to retrieve result metadata by preparing a SELECT
     * statement.
     *
     * @return true if the database supports retrieval of query metadata from a
     * prepared statement. False if the query needs to be executed first.
     */
    @Override
    public boolean supportsPreparedStatementMetadataRetrieval() {
        return false;
    }

    /**
     * @return The maximum number of columns in a database, <=0 means: no known
     * limit
     */
    @Override
    public int getMaxColumnsInIndex() {
        return 32;
    }

    /**
     * @return The SQL on this database to get a list of sequences.
     */
    @Override
    public String getSQLListOfSequences() {
        return "";
    }

    /**
     * @param string
     * @return A string that is properly quoted for use in an Oracle SQL statement
     * (insert, update, delete, etc)
     */
    @Override
    public String quoteSQLString(String string) {
       return "";
    }

    /**
     * Returns a false as Oracle does not allow for the releasing of savepoints.
     */
    @Override
    public boolean releaseSavepoint() {
        return false;
    }

    @Override
    public boolean supportsErrorHandlingOnBatchUpdates() {
        return false;
    }

    /**
     * @return true if Kettle can create a repository on this type of database.
     */
    @Override
    public boolean supportsRepository() {
        return true;
    }

    @Override
    public int getMaxVARCHARLength() {
        return 2000;
    }

    /**
     * Oracle does not support a construct like 'drop table if exists', which is
     * apparently legal syntax in many other RDBMSs. So we need to implement the
     * same behavior and avoid throwing 'table does not exist' exception.
     *
     * @param tableName Name of the table to drop
     * @return 'drop table if exists'-like statement for Oracle
     */
    @Override
    public String getDropTableIfExistsStatement(String tableName) {
        return "";
    }

    @Override
    public SqlScriptParser createSqlScriptParser() {
        return new SqlScriptParser(false);
    }

    /**
     * @return true if using strict number(38) interpretation
     */
    public boolean strictBigNumberInterpretation() {
        return "Y".equalsIgnoreCase(getAttributes().getProperty(STRICT_BIGNUMBER_INTERPRETATION, "N"));
    }

    /**
     * @param strictBigNumberInterpretation true if use strict number(38) interpretation
     */
    public void setStrictBigNumberInterpretation(boolean strictBigNumberInterpretation) {
        getAttributes().setProperty(STRICT_BIGNUMBER_INTERPRETATION, strictBigNumberInterpretation ? "Y" : "N");
    }
}