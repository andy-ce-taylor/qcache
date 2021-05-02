<?php
/** @noinspection PhpComposerExtensionStubsInspection
 * @noinspection SqlNoDataSourceInspection
 * @noinspection SqlDialectInspection
 */

namespace acet\qcache\connector;

use acet\qcache\Constants;
use acet\qcache\exception as QcEx;
use acet\qcache\SqlResultSet;
use DateTime;

class DbConnectorMSSQL extends DbConnector implements DbConnectorIfc
{
    const SERVER_NAME = 'Microsoft Server';
    const CACHED_UPDATES_TABLE = true;

    /**
     * DbConnectorMSSQL constructor.
     *
     * @param array    $qcache_config
     * @param string[] $db_connection_data
     * @throws QcEx\ConnectionException
     */
    function __construct($qcache_config, $db_connection_data)
    {
        static $_connection = [];

        $key = implode(':', $db_connection_data);

        if (array_key_exists($key, $_connection)) {
            if (!$_connection[$key]) {
                throw new QcEx\ConnectionException(self::SERVER_NAME);
            }

            $this->conn = $_connection[$key];
        } else {
            $_connection[$key] = $this->conn = sqlsrv_connect(
                $db_connection_data['host'],
                [   'Database' => $db_connection_data['name'],
                    'UID' => $db_connection_data['user'],
                    'PWD' => $db_connection_data['pass']
                ]
            );

            if (!$this->conn) {
                $_connection_ok[$key] = false;
                throw new QcEx\ConnectionException(self::SERVER_NAME);
            }

            ini_set('mssql.charset', 'utf-8');
        }

        parent::__construct($qcache_config, $db_connection_data, self::SERVER_NAME, self::CACHED_UPDATES_TABLE);
    }

    /**
     * @param string $str
     * @return string
     */
    public function escapeString($str)
    {
        if (is_numeric($str)) {
            return $str;
        }

        return '0x' . unpack('H*hex', $str)['hex'];
    }

    /**
     * @param mixed $data
     * @return mixed
     */
    public function escapeBinData($data)
    {
        return $this->escapeString($data);
    }

    /**
     * Returns the difference (in seconds) between database timestamps and the current system time.
     *
     * @return int
     */
    public function getDbTimeOffset()
    {
        static $database_time_offset_l1c;

        if (!$database_time_offset_l1c) {
            $database_time_offset_l1c = time() - strtotime($this->getCurrentTimestamp());
        }

        return $database_time_offset_l1c;
    }

    /**
     * Returns the database engine specific SQL command which will be used to produce an
     * array of records with specific fields (name/value pairs).
     * If $selector or $selector_values is empty, all records are returned.
     *   Analogous to "SELECT field1, field2 FROM source"
     * Otherwise, $selector and $selector_values are used to filter records.
     *   Analogous to "SELECT field1, field2 FROM source WHERE selector IN selector_values"
     *
     * @param string           $table
     * @param string[]|string  $fields - For all fields, use '*'
     * @param string           $selector
     * @param string[]         $selector_values
     * @param int              $limit     - 0 = no limit
     *
     * @return string
     */
    public function prepareSimpleSQL($table, $fields, $selector, $selector_values, $limit=0)
    {
        $fields_csv = $fields == '*' ? '*' : '[' . implode('],[', $fields) . ']';

        $where = '';

        if (!empty($selector) && !empty($selector_values)) {

            $selector = strpos($selector, ',') !== false
                ? "CONCAT(" . str_replace(',', ', " ", ', $selector) . ')'
                : $selector;

            foreach ($selector_values as $val) {
                $where .= "{$selector} = '{$val}' OR ";
//                $where .= "{$selector} = " . $this->escapeString($val) . " OR ";
            }

            // get rid of final 'OR'
            $where = 'WHERE ' . substr($where, 0, -4);
        }

        $limit = $limit > 0 ? "TOP($limit)" : '';

        return "SELECT $limit $fields_csv FROM $table $where";
    }

    /**
     * Process a table read request, such as SELECT, and return the response.
     * @param string  $sql
     * @param bool    $return_resultset
     * @return SqlResultSet|array
     */
    public function read($sql, $return_resultset=true)
    {
        $data = [];

        if (($result = sqlsrv_query($this->conn, $sql)) === false) {
            $errors = sqlsrv_errors();
            throw new QcEx\TableReadException('', $sql, self::SERVER_NAME, reset($errors)['message']);
        }

        while ($row = sqlsrv_fetch_array($result, SQLSRV_FETCH_ASSOC)) {
            $data[] = $row;
        }

        $this->freeResultset($result);

        if (!$return_resultset) {
            return $data;
        }

        return new SqlResultSet($data);
    }

    /**
     * Process a SELECT for a single column and return as a numerically indexed array.
     * @param string $sql
     * @return array
     * @throws QcEx\TableReadException
     */
    public function readCol($sql)
    {
        $data = [];

        if (($result = sqlsrv_query($this->conn, $sql)) === false) {
            $errors = sqlsrv_errors();
            throw new QcEx\TableReadException('', $sql, self::SERVER_NAME, reset($errors)['message']);
        }

        while ($row = sqlsrv_fetch_array($result, SQLSRV_FETCH_NUMERIC)) {
            $data[] = $row[0];
        }

        $this->freeResultset($result);

        return $data;
    }

    /**
     * Process a table write request, such as INSERT or UPDATE.
     * @param string $sql
     * @return bool
     * @throws QcEx\TableWriteException
     */
    public function write($sql)
    {
        if (sqlsrv_query($this->conn, $sql) === false) {
            $errors = sqlsrv_errors();
            throw new QcEx\TableWriteException('', $sql, self::SERVER_NAME, reset($errors)['message']);
        }

        return true;
    }

    /**
     * Process multiple queries.
     * @param string $sql
     * @return bool
     * @throws QcEx\TableWriteException
     */
    public function multi_write($sql)
    {
        return $this->write($sql);
    }

    /**
     * Convert a native resultset into a SqlResultSet.
     *
     * @param resource $native_resultset
     * @return SqlResultSet
     */
    public function toSqlResultSet($native_resultset)
    {
        $data = [];

        while ($row = sqlsrv_fetch_array($native_resultset, SQLSRV_FETCH_ASSOC)) {
            $data[] = $row;
        }

        $this->freeResultset($native_resultset);

        return new SqlResultSet($data);
    }

    /**
     * @param resource $resultset
     * @return bool
     */
    public function freeResultset($resultset)
    {
        return (bool)sqlsrv_free_stmt($resultset);
    }

    /**
     * Returns the change times for the given tables.
     *
     * Dev Notes: Table changes are recorded in sys.dm_db_index_usage_stats, but unlike the
     * equivalent MySQL method, this table is reset whenever MSSQL restarts. For this
     * reason, a file is used to cache the latest table change times.
     *
     * @param mixed $db_connection_cache
     * @param string[]|null $tables
     * @return int[]|false
     * @throws QcEx\TableReadException
     */
    public function getTableTimes($db_connection_cache, $tables=null)
    {
        $table_update_times = $this->readTableUpdateTimesTable($db_connection_cache);

        $specific_tables_clause = $tables ? "AND OBJECT_ID IN (OBJECT_ID('".implode("'),OBJECT_ID('", $tables)."'))" : '';

        $db_name = $this->getDbName();

        $sql =
            "SELECT OBJECT_NAME(OBJECT_ID) AS TableName, last_user_update
             FROM sys.dm_db_index_usage_stats
             WHERE database_id = DB_ID('$db_name') $specific_tables_clause";

        if (($stmt = sqlsrv_query($this->conn, $sql)) === false) {
            $errors = sqlsrv_errors();
            throw new QcEx\TableReadException('sys.dm_db_index_usage_stats', $sql, self::SERVER_NAME, reset($errors)['message']);
        }

        $current_timestamp = (int)(new DateTime($this->getCurrentTimestamp()))->format('U');

        while ($row = sqlsrv_fetch_array($stmt, SQLSRV_FETCH_ASSOC)) {

            $table_name = $row['TableName'];

            if ($update_time = $row['last_user_update']) {
                // typical mssql timestamp value: 2019-02-05 12:34:56.789
                $timestamp = (int)(new DateTime(date_format($update_time, 'Y-m-d H:i:s')))->format('U'); // use the updated time
            } else {
                // sys.dm_db_index_usage_stats hasn't been updated since SQL Server was started

                // check whether update_time has previously been cached

                if (array_key_exists($table_name, $table_update_times)) { // get the cached update_time {
                    $timestamp = $table_update_times[$table_name];

                } else {
                    // set update_time to the current time and store it in the table_update_times cache
                    $timestamp = $current_timestamp;
                }
            }

            $table_update_times[$table_name] = $timestamp;
        }

        $db_connection_cache->writeTableUpdateTimesTable($table_update_times);

        $this->freeResultset($stmt);

        return $table_update_times;
    }

    /**
     * Returns TRUE if it is possible to read table change times.
     * Throws an exception if there are permission problems.
     *
     * @return true
     * @throws QcEx\TableReadException
     */
    public function verifyGetTableTimes()
    {
        $db_name = $this->getDbName();

        $sql =
            "SELECT OBJECT_NAME(OBJECT_ID) AS TableName, last_user_update
             FROM sys.dm_db_index_usage_stats
             WHERE database_id = DB_ID('$db_name')";

        if (($stmt = sqlsrv_query($this->conn, $sql)) === false) {
            $errors = sqlsrv_errors();
            throw new QcEx\TableReadException('sys.dm_db_index_usage_stats', $sql, self::SERVER_NAME, reset($errors)['message']);
        }

        return true;
    }

    /**
     * @return string
     * @throws QcEx\TableReadException
     */
    private function getSQLServerStartTime()
    {
        $sql = "SELECT sqlserver_start_time FROM sys.dm_os_sys_info";

        if (($result = sqlsrv_query($this->conn, $sql)) === false) {
            $errors = sqlsrv_errors();
            throw new QcEx\TableReadException('sys.dm_os_sys_info', $sql, self::SERVER_NAME, reset($errors)['message']);
        }

        $this->freeResultset($result);

        return date_format(sqlsrv_fetch_array($result, SQLSRV_FETCH_ASSOC)['sqlserver_start_time'], 'Y-m-d H:i:s');
    }

    /**
     * @return string
     * @throws QcEx\TableReadException
     */
    private function getCurrentTimestamp()
    {
        $sql = "SELECT CURRENT_TIMESTAMP AS [CURRENT_TIMESTAMP]";

        if (($result = sqlsrv_query($this->conn, $sql)) === false) {
            $errors = sqlsrv_errors();
            throw new QcEx\TableReadException('CURRENT_TIMESTAMP', $sql, self::SERVER_NAME, reset($errors)['message']);
        }

        $this->freeResultset($result);

        return date_format(sqlsrv_fetch_array($result, SQLSRV_FETCH_ASSOC)['CURRENT_TIMESTAMP'], 'Y-m-d H:i:s');
    }

    /**
     * Delete all rows from the given table.
     *
     * @param string $table
     * @return bool
     * @throws QcEx\TableWriteException
     */
    public function truncateTable($table)
    {
        $sql = "TRUNCATE TABLE $table";
        try {

            return $this->write($sql);

        } catch (QcEx\TableWriteException $ex) {
            $errors = sqlsrv_errors();
            throw new QcEx\TableWriteException($table, $sql, self::SERVER_NAME, reset($errors)['message']);
        }
    }

    /**
     * Return TRUE if the given table exists, otherwise FALSE.
     *
     * @param string $schema
     * @param string $table
     * @return bool
     * @throws QcEx\TableReadException
     */
    public function tableExists($schema, $table)
    {
        $sql =
            "SELECT * FROM information_schema.tables 
             WHERE TABLE_SCHEMA='$schema'
             AND TABLE_NAME='$table'";

        try {

            return (bool)$this->read($sql, false);

        } catch (QcEx\TableReadException $ex) {
            $errors = sqlsrv_errors();
            throw new QcEx\TableReadException('information_schema.tables', $sql, self::SERVER_NAME, reset($errors)['message']);
        }
    }

    /**
     * Returns SQL to create the cache information table.
     * @param string  $table_name
     * @return string
     */
    public function getCreateTableSQL_cache_info($table_name)
    {
        $table_name = 'dbo.' . $table_name;

        return "IF OBJECT_ID('$table_name', 'U') IS NOT NULL BEGIN
                    DROP TABLE $table_name;
                END;
                CREATE TABLE $table_name (
                    hash            CHAR(32)        NOT NULL  PRIMARY KEY,
                    access_time     INT             DEFAULT NULL,
                    av_microsecs    FLOAT           DEFAULT NULL,
                    impressions     INT             DEFAULT NULL,
                    description     VARCHAR(500)    DEFAULT NULL,
                    tables_csv      VARCHAR(1000)   DEFAULT NULL
                );";
    }

    /**
     * Returns SQL to create the logs table.
     * @param string  $table_name
     * @return string
     */
    public function getCreateTableSQL_logs($table_name)
    {
        $table_name = 'dbo.' . $table_name;
        return "IF OBJECT_ID('$table_name', 'U') IS NOT NULL BEGIN
                    DROP TABLE $table_name;
                END;
                CREATE TABLE $table_name (
                    id              INT             IDENTITY(1,1) PRIMARY KEY,
                    time            INT             DEFAULT NULL,
                    microtime       FLOAT           DEFAULT NULL,
                    status          CHAR(8)         DEFAULT NULL,
                    hash            CHAR(32)        DEFAULT NULL
                );";
    }

    /**
     * Returns SQL to create the update times table.
     * @param string  $table_name
     * @return string
     */
    public function getCreateTableSQL_table_update_times($table_name)
    {
        $table_name = 'dbo.' . $table_name;
        return "IF OBJECT_ID('$table_name', 'U') IS NOT NULL BEGIN
                    DROP TABLE $table_name;
                END;
                CREATE TABLE $table_name (
                    name            VARCHAR(80)     NOT NULL  PRIMARY KEY,
                    update_time     INT             DEFAULT NULL
                );";
    }

    /**
     * Returns the primary keys for the given table.
     * @param string $table
     * @return string[]
     * @throws QcEx\TableReadException
     */
    public function getPrimary($table)
    {
        $sql = "SELECT KU.table_name as TABLENAME, column_name as PRIMARYKEYCOLUMN
                FROM information_schema.table_constraints AS TC 
                INNER JOIN information_schema.key_column_usage AS KU
                    ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY' 
                    AND TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME 
                    AND KU.table_name='$table'
                ORDER BY KU.TABLE_NAME, KU.ORDINAL_POSITION";

        $data = [];

        if (($result = sqlsrv_query($this->conn, $sql)) === false) {
            $errors = sqlsrv_errors();
            throw new QcEx\TableReadException('information_schema', $sql, self::SERVER_NAME, reset($errors)['message']);
        }

        while ($row = sqlsrv_fetch_array($result, SQLSRV_FETCH_ASSOC)) {
            $data[] = $row['column_name'];
        }

        $this->freeResultset($result);

        return $data;
    }


    /**
     * Returns the names of all external tables.
     * @return string[]
     * @throws QcEx\TableReadException
     */
    public function getTableNames()
    {
        static $table_names = [];

        $db_name = $this->getDbName();

        if (!isset($table_names[$db_name])) {
            $sql = "SELECT table_name FROM information_schema.tables WHERE TABLE_CATALOG LIKE '$db_name'";
           
            try {

                $table_names[$db_name] = $this->readCol($sql);

            } catch (QcEx\TableReadException $ex) {
                $errors = sqlsrv_errors();
                throw new QcEx\TableReadException('information_schema', $sql, self::SERVER_NAME, reset($errors)['message']);
            }
        }

        return $table_names[$db_name];
    }

    /**
     * Returns the names of all columns in the given external table.
     * @return string[]
     * @throws QcEx\TableReadException
     */
    public function getColumnNames($table)
    {
        return $this->readCol(
            "SELECT COLUMN_NAME, * 
             FROM information_schema.columns
             WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='$table'"
        );
    }
}