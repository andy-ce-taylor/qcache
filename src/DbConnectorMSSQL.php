<?php
/** @noinspection PhpComposerExtensionStubsInspection
 * @noinspection SqlNoDataSourceInspection
 * @noinspection SqlDialectInspection
 */

namespace acet\qcache;

use DateTime;

class DbConnectorMSSQL extends DbConnector implements DbConnectorInterface
{
    const CACHED_UPDATES_TABLE = true;

    /**
     * DbConnectorMSSQL constructor.
     *
     * @param string  $host
     * @param string  $user
     * @param string  $pass
     * @param string  $database_name
     * @param string  $module_id
     * @throws QCacheConnectionException
     */
    function __construct($host, $user, $pass, $database_name, $module_id='')
    {
        $this->conn = sqlsrv_connect(
            $host,
            [
                'Database'  => $database_name,
                'UID'       => $user,
                'PWD'       => $pass
            ]
        );

        if (!$this->conn)
            throw new QCacheConnectionException("MSSQL connection error");

        parent::__construct($database_name, self::CACHED_UPDATES_TABLE, $module_id);
    }

    /**
     * @param string $str
     * @return string
     */
    public function escapeString($str)
    {
        if (is_numeric($str))
            return $str;

        $unpacked = unpack('H*hex', $str);

        return '0x' . $unpacked['hex'];
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

        if (!$database_time_offset_l1c)
            $database_time_offset_l1c = time() - strtotime($this->getCurrentTimestamp());

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

            $selector = "[{$selector}]";

            if (strpos($selector, ',') !== false)
                $selector = "CONCAT(" . str_replace(',', ', " ", ', $selector) . ')';

            foreach ($selector_values as $val)
                $where .= "{$selector} = '{$val}' OR ";

            // get rid of final 'OR'
            $where = 'WHERE ' . substr($where, 0, -4);
        }

        $limit = $limit > 0 ? "TOP($limit)" : '';

        return "SELECT $limit $fields_csv FROM $table $where";
    }

    /**
     * Process a table read request, such as SELECT, and return the response.
     * @param string $sql
     * @return array
     */
    public function read($sql)
    {
        $data = [];

        if ($result = sqlsrv_query($this->conn, $sql))
            while ($row = sqlsrv_fetch_array($result, SQLSRV_FETCH_ASSOC))
                $data[] = $row;

        return $data;
    }

    /**
     * Process a SELECT for a single columns and return as a numerically indexed array.
     * @param string $sql
     * @return array
     */
    public function readCol($sql)
    {
        $data = [];

        if ($result = sqlsrv_query($this->conn, $sql))
            while ($row = sqlsrv_fetch_array($result, SQLSRV_FETCH_NUMERIC))
                $data[] = $row[0];

        return $data;
    }

    /**
     * Process a table write request, such as INSERT or UPDATE.
     * @param string $sql
     * @return bool
     */
    public function write($sql)
    {
        return (bool)sqlsrv_query($this->conn, $sql);
    }

    /**
     * Process multiple queries.
     * @param string $sql
     * @return bool
     */
    public function multi_query($sql)
    {
        return (bool)sqlsrv_query($this->conn, $sql);
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
     * @param mixed          $cache_db
     * @param string[]|null  $tables
     * @return int[]|false
     */
    public function getTableTimes($cache_db, $tables=null)
    {
        $table_update_times = $cache_db->readTableUpdateTimesTable();

        $specific_tables_clause = $tables ? "AND OBJECT_ID IN (OBJECT_ID('".implode("'),OBJECT_ID('", $tables)."'))" : '';

        $db_name = $this->getDbName();

        $sql_query =
            "SELECT OBJECT_NAME(OBJECT_ID) AS TableName, last_user_update
             FROM sys.dm_db_index_usage_stats
             WHERE database_id = DB_ID('$db_name') $specific_tables_clause";

        if (($stmt1 = sqlsrv_query($this->conn, $sql_query)) == false)
            return false;

        $current_timestamp = (int)(new DateTime($this->getCurrentTimestamp()))->format('U');

        while ($row = sqlsrv_fetch_array($stmt1, SQLSRV_FETCH_ASSOC)) {

            $table_name = $row['TableName'];

            if ($update_time = $row['last_user_update']) // typical mssql timestamp value: 2019-02-05 12:34:56.789
                $timestamp = (int)(new DateTime(date_format($update_time, 'Y-m-d H:i:s')))->format('U'); // use the updated time

            else { // sys.dm_db_index_usage_stats hasn't been updated since SQL Server was started

                // check whether update_time has previously been cached

                if (array_key_exists($table_name, $table_update_times)) // get the cached update_time
                    $timestamp = $table_update_times[$table_name];

                else // set update_time to the current time and store it in the table_update_times cache
                    $timestamp = $current_timestamp;
            }

            $table_update_times[$table_name] = $timestamp;
        }

        $cache_db->writeTableUpdateTimesTable($table_update_times);

        $this->freeResultset($stmt1);

        return $table_update_times;
    }

    /**
     * Returns
     *
     * @return string
     */
    private function getSQLServerStartTime()
    {
        $sql = "SELECT sqlserver_start_time FROM sys.dm_os_sys_info";

        $result = sqlsrv_query($this->conn, $sql);
        return date_format(sqlsrv_fetch_array($result, SQLSRV_FETCH_ASSOC)['sqlserver_start_time'], 'Y-m-d H:i:s');
    }

    /**
     * @return string
     */
    private function getCurrentTimestamp()
    {
        $sql = "SELECT CURRENT_TIMESTAMP AS [CURRENT_TIMESTAMP]";

        $result = sqlsrv_query($this->conn, $sql);
        return date_format(sqlsrv_fetch_array($result, SQLSRV_FETCH_ASSOC)['CURRENT_TIMESTAMP'], 'Y-m-d H:i:s');
    }

    /**
     * Delete all rows from the given table.
     *
     * @param string $table
     * @return bool
     */
    public function truncateTable($table)
    {
        return (bool)$this->write("TRUNCATE TABLE $table");
    }

    /**
     * Return TRUE if the given table exists, otherwise FALSE.
     *
     * @param string $schema
     * @param string $table
     * @return bool
     */
    public function tableExists($schema, $table)
    {
        $sql = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='$schema' AND TABLE_NAME='$table'))";

        return (bool)$this->read($sql);
    }

    /**
     * Returns SQL to create the cache table.
     * @param string  $table_name
     * @return string
     */
    public function getCreateTableSQL_cache($table_name)
    {
        $table_name = 'dbo.' . $table_name;
        $max_resultset_size = Constants::MAX_DB_RESULTSET_SIZE;

        return "IF OBJECT_ID('$table_name', 'U') IS NOT NULL BEGIN
                    DROP TABLE $table_name;
                END;
                CREATE TABLE $table_name (
                    hash            CHAR(32)            NOT NULL  PRIMARY KEY,
                    access_time     INT             DEFAULT NULL,
                    script          VARCHAR(4000)   DEFAULT NULL,
                    av_nanosecs     FLOAT           DEFAULT NULL,
                    impressions     INT             DEFAULT NULL,
                    description     VARCHAR(200)    DEFAULT NULL,
                    tables_csv      VARCHAR(1000)   DEFAULT NULL,
                    resultset       VARCHAR($max_resultset_size)
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
                    context         CHAR(4)         DEFAULT NULL,
                    nanosecs        FLOAT           DEFAULT NULL,
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
                    name            VARCHAR(80)         NOT NULL  PRIMARY KEY,
                    update_time     INT             DEFAULT NULL
                );";
    }

    /**
     * Returns the primary keys for the given table.
     * @return string[]
     */
    public function getPrimary($table)
    {
        $sql = "SELECT KU.table_name as TABLENAME, column_name as PRIMARYKEYCOLUMN
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC 
                INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU
                    ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY' 
                    AND TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME 
                    AND KU.table_name='$table'
                ORDER BY KU.TABLE_NAME, KU.ORDINAL_POSITION";

        $data = [];

        if ($result = sqlsrv_query($this->conn, $sql))
            while ($row = sqlsrv_fetch_array($result, SQLSRV_FETCH_ASSOC))
                $data[] = $row['column_name'];

        return $data;
    }


    /**
     * Returns the names of all external tables.
     * @return string[]
     */
    public function getTableNames()
    {
        static $table_names = [];

        $db_name = $this->getDbName();

        if (!isset($table_names[$db_name])) {
            $sql = "SELECT table_name FROM information_schema.tables WHERE TABLE_CATALOG LIKE '$db_name'";
            $table_names[$db_name] = $this->readCol($sql);
        }

        return $table_names[$db_name];
    }

    /**
     * Returns the names of all columns in the given external table.
     * @return string[]
     */
    public function getColumnNames($table)
    {
        return $this->readCol(
            "SELECT COLUMN_NAME, * 
             FROM INFORMATION_SCHEMA.COLUMNS
             WHERE TABLE_NAME = '$table' AND TABLE_SCHEMA='dbo'"
        );
    }
}