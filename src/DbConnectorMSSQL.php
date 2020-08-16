<?php
/** @noinspection PhpComposerExtensionStubsInspection
 * @noinspection SqlNoDataSourceInspection
 * @noinspection SqlDialectInspection
 */

namespace acet\qcache;

use DateTime;
use Exception;

class DbConnectorMSSQL extends DbChangeDetection implements DbConnectorInterface
{
    const MSSQL_TABLES_INFO_FILE_NAME = 'mssql_tables_info.json';

    /** @var resource */
    private $conn;

    /** @var string */
    private $database_name;

    /** @var string */
    private $table_qc_table_times;

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

        $this->database_name = $database_name;

        if ($module_id)
            $module_id .= '_';

        $this->table_qc_table_times = 'qc_' . $module_id . 'table_times';
    }

    /**
     * @param string $str
     * @return string
     */
    public function sql_escape_string($str)
    {
        $unpacked = unpack('H*hex', $str);
        return '0x' . $unpacked['hex'];
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
            $where = ' WHERE ' . substr($where, 0, -4);
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
     * Returns the change times for the given tables.
     *
     * FYI: Table changes are recorded in sys.dm_db_index_usage_stats, but unlike the
     * equivalent MySQL method, this table gets reset whenever SQL is restarted. For
     * this reason, a file is used to maintain the latest table change times.
     *
     * @param string[]|null  $tables
     * @return int[]|false
     */
    public function getTableTimes($tables=null)
    {
        $specific_tables = $tables ? "AND OBJECT_ID IN (OBJECT_ID('".implode("'),OBJECT_ID('", $tables)."'))" : '';

        // typical timestamp value: 2019-02-05 12:07:08.345
        $sql_query = "
            SELECT OBJECT_NAME(OBJECT_ID) AS TableName, last_user_update
            FROM sys.dm_db_index_usage_stats
            WHERE database_id = DB_ID('$this->database_name') $specific_tables";

        if (($stmt1 = sqlsrv_query($this->conn, $sql_query)) == false)
            return false;

        $data = [];

        $current_timestamp = $this->getCurrentTimestamp();

        while ($row = sqlsrv_fetch_array($stmt1, SQLSRV_FETCH_ASSOC)) {

            $table_name = $row['TableName'];

            if ($update_time = $row['last_user_update'])
                $timestamp = date_format($update_time, 'Y-m-d H:i:s'); // use the updated time

            else { // sys.dm_db_index_usage_stats hasn't been updated since SQL Server was started

                // check whether update_time has been cached
                $sql = "SELECT update_time FROM $this->table_qc_table_times WHERE name='$table_name'";
                if (($stmt2 = sqlsrv_query($this->conn, $sql)) && ($row = sqlsrv_fetch_array($stmt2, SQLSRV_FETCH_ASSOC))) {
                    // get the cached update_time
                    $timestamp = $row['update_time'];
                    sqlsrv_free_stmt($stmt2);
                }
                else {
                    // set update_time to the current time and store it in the table_times cache
                    $timestamp = $current_timestamp;
                    $sql = "INSERT INTO $this->table_qc_table_times (name, update_time) VALUES('$table_name', $timestamp)";
                }
                $this->write($sql);
            }

            try {

                $data[$table_name] = (int)(new DateTime($timestamp))->format('U');

            } catch (Exception $ex) {
                sqlsrv_free_stmt($stmt1);
                return false; // wrong time format
            }
        }

        sqlsrv_free_stmt($stmt1);

        return $data;
    }

    /**
     * Returns
     *
     * @return string
     */
    public function getSQLServerStartTime()
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
        return "IF OBJECT_ID('dbo.$table_name', 'U') IS NOT NULL BEGIN
                    DROP TABLE dbo.$table_name;
                END;
                CREATE TABLE dbo.$table_name (
                    hash          NCHAR(32)          NOT NULL  PRIMARY KEY,
                    access_time   INT            DEFAULT NULL,
                    script        NVARCHAR(800)  DEFAULT NULL,
                    av_nanosecs   FLOAT          DEFAULT NULL,
                    impressions   INT            DEFAULT NULL,
                    description   NVARCHAR(200)  DEFAULT NULL,
                    tables_csv    NVARCHAR(200)  DEFAULT NULL,
                    data          TEXT
                );";
    }

    /**
     * Returns SQL to create the logs table.
     * @param string  $table_name
     * @return string
     */
    public function getCreateTableSQL_logs($table_name)
    {
        return "IF OBJECT_ID('dbo.$table_name', 'U') IS NOT NULL BEGIN
                    DROP TABLE dbo.$table_name;
                END;
                CREATE TABLE dbo.$table_name (
                    id            INT            IDENTITY(1,1) PRIMARY KEY,
                    time          INT            DEFAULT NULL,
                    context       NCHAR(3)       DEFAULT NULL,
                    nanosecs      FLOAT          DEFAULT NULL,
                    hash          NCHAR(32)      DEFAULT NULL
                );";
    }

    /**
     * Returns SQL to create the update times table.
     * @param string  $table_name
     * @return string
     */
    public function getCreateTableSQL_table_update_times($table_name)
    {
        return "IF OBJECT_ID('dbo.$table_name', 'U') IS NOT NULL BEGIN
                    DROP TABLE dbo.$table_name;
                END;
                CREATE TABLE dbo.$table_name (
                    name          NVARCHAR(200)      NOT NULL  PRIMARY KEY,
                    update_time   INT            DEFAULT NULL
                );";
    }
}