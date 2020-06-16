<?php
/** @noinspection PhpComposerExtensionStubsInspection */
/** @noinspection SqlDialectInspection */

namespace acet\qcache;

use acet\ResLock\ResLock;
use DateTime;
use Exception;

class DbConnectorMSSQL extends DbChangeDetection implements DbConnectorInterface
{
    /** @var resource */
    private $conn;

    /** @var string */
    private $database_name;

    /** @var string */
    private $table_times_file;

    /** @var ResLock */
    private $reslock;
    /**
     * DbConnectorMSSQL constructor.
     *
     * @param string  $host
     * @param string  $user
     * @param string  $pass
     * @param string  $database_name
     * @param string  $table_times_file;
     * @param ResLock $reslock
     * @throws QCacheConnectionException
     */
    function __construct($host, $user, $pass, $database_name, $table_times_file, $reslock)
    {
        $this->conn = sqlsrv_connect(
            $host,
            [
                "Database"  => $database_name,
                "UID"       => $user,
                "PWD"       => $pass
            ]
        );

        if (!$this->conn) {
            throw new QCacheConnectionException("MSSQL connection error");
        }

        $this->database_name = $database_name;
        $this->table_times_file = $table_times_file;
        $this->reslock = $reslock;
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
        if ($fields == '*') {
            $fields_csv = '*';
        }
        else {
            $fields_csv = '[' . implode('],[', $fields) . ']';
        }

        $where = '';
        if (!empty($selector) && !empty($selector_values)) {

            $selector = "[{$selector}]";

            if (strpos($selector, ',') !== false) {
                $selector = "CONCAT(" . str_replace(',', ', " ", ', $selector) . ')';
            }

            foreach ($selector_values as $val) {
                $where .= "{$selector} = '{$val}' OR ";
            }

            // get rid of final 'OR'
            $where = ' WHERE ' . substr($where, 0, -4);
        }

        $limit = $limit > 0 ? "TOP($limit)" : '';

        return "SELECT $limit $fields_csv FROM $table $where";
    }

    /**
     * @param string $sql
     * @return array
     */
    public function processQuery($sql)
    {
        $data = [];

        if ($result = sqlsrv_query($this->conn, $sql)) {

            while ($row = sqlsrv_fetch_array($result, SQLSRV_FETCH_ASSOC)) {
                $data[] = $row;
            }
        }

        return $data;
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
        $specific_tables = '';

        if ($tables) {
            $specific_tables = "AND OBJECT_ID IN (OBJECT_ID('".implode("'),OBJECT_ID('", $tables)."'))";
        }

        // typical timestamp value: 2019-02-05 12:07:08.345
        $sql_query = "
            SELECT OBJECT_NAME(OBJECT_ID) AS TableName, last_user_update
            FROM sys.dm_db_index_usage_stats
            WHERE database_id = DB_ID('$this->database_name') $specific_tables";

        if (($res = sqlsrv_query($this->conn, $sql_query)) == false) {
            return false;
        }

        $data = [];

        $current_timestamp = $this->getCurrentTimestamp();

        $cached_table_times = JsonEncodedFileIO::readJsonEncodedArray($this->table_times_file);
        $cache_save_needed = false;

        while($row = sqlsrv_fetch_array($res, SQLSRV_FETCH_ASSOC)) {
            $table_name = $row['TableName'];

            if (array_key_exists($table_name, $cached_table_times)) {
                $cached_timestamp = $cached_table_times[$table_name];
            }
            else {
                $cached_timestamp = $cached_table_times[$table_name] = null;
            }

            if (!is_null($update_time = $row['last_user_update'])) {
                // use the updated time
                $timestamp = date_format($update_time, 'Y-m-d H:i:s');
            }
            else {
                // table hasn't been updated since SQL Server was started

                if ($cached_timestamp) {
                    // use the cached value
                    $timestamp = $cached_timestamp;
                }
                else {
                    // no cached value - use the current time
                    $timestamp = $current_timestamp;
                }
            }

            if ($timestamp != $cached_timestamp) {
                $cached_table_times[$table_name] = $timestamp;
                $cache_save_needed = true;
            }

            try {
                $data[$table_name] = (int)(new DateTime($timestamp))->format('U');
            }
            catch (Exception $ex) {
                sqlsrv_free_stmt($res);
                return false; // wrong time format
            }
        }

        sqlsrv_free_stmt($res);

        if ($cache_save_needed) {
            // sort by key (table names)
            ksort($cached_table_times);

            // lock & save
            $rl_key = $this->reslock->lock($this->table_times_file);
            {
                JsonEncodedFileIO::writeJsonEncodedArray($this->table_times_file, $cached_table_times);
            }
            $this->reslock->unlock($rl_key);
        }

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
}