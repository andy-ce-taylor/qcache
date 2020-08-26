<?php
/**
 * @noinspection PhpComposerExtensionStubsInspection
 * @noinspection SqlNoDataSourceInspection
 * @noinspection SqlDialectInspection
 */

namespace acet\qcache;

use DateTime;
use Exception;
use mysqli;
use SQLite3;
use SQLite3Result;

class DbConnectorSQLite extends DbConnector implements DbConnectorInterface
{
    const CACHED_UPDATES_TABLE = false;

    /**
     * DbConnectorMySQL constructor.
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
        try {
            $this->conn = new SQLite3($database_name, SQLITE3_OPEN_READWRITE | SQLITE3_OPEN_CREATE, $pass);
        } catch (Exception $ex) {
            throw new QCacheConnectionException("SQLite connection error: ".$ex->getMessage());
        }

        parent::__construct($database_name, self::CACHED_UPDATES_TABLE, $module_id);
    }

    /**
     * @param string $str
     * @return string
     */
    public function escapeString($str)
    {
        return "'" . $this->conn->escapeString($str) . "'";
    }

    /**
     * @param mixed $data
     * @return mixed
     */
    public function escapeBinData($data)
    {
        $unpacked = unpack('H*hex', $data);

        return "X'{$unpacked['hex']}'";
    }

    /**
     * Returns the difference (seconds) between database timestamps and the current system time.
     *
     * @return int
     */
    public function getDbTimeOffset()
    {
        static $database_time_offset_l1c;

        if (!$database_time_offset_l1c) {

            $sql = "SELECT datetime('now', 'localtime')";

            $result = $this->conn->query($sql);
            $db_timestamp = $result->fetchArray(SQLITE3_ASSOC);

            $database_time_offset_l1c = time() - strtotime($db_timestamp);
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
        $fields_csv = $fields == '*' ? '*' : '`' . implode('`,`', $fields) . '`';

        $where = '';

        if (!empty($selector) && !empty($selector_values)) {

            $selector = "`{$selector}`";

            if (strpos($selector, ',') !== false)
                $selector = "CONCAT(" . str_replace(',', ', " ", ', $selector) . ')';

            foreach ($selector_values as $val)
                $where .= "{$selector} = '{$val}' OR ";

            // get rid of final 'OR'
            $where = 'WHERE ' . substr($where, 0, -4);
        }

        $limit = $limit > 0 ? "LIMIT $limit" : '';

        return "SELECT $fields_csv FROM $table $where $limit";
    }

    /**
     * Process a table read request, such as SELECT, and return the response.
     * @param string $sql
     * @return array
     */
    public function read($sql)
    {
        $data = [];

        if ($result = $this->conn->query($sql)) {
            while ($row = $result->fetchArray(SQLITE3_ASSOC))
                $data[] = $row;

            $this->freeResultset($result);
        }

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

        if ($result = $this->conn->query($sql)) {
            while ($row = $result->fetchArray(SQLITE3_NUM))
                $data[] = $row[0];

            $this->freeResultset($result);
        }

        return $data;
    }

    /**
     * Process a table write request, such as INSERT or UPDATE.
     * @param string $sql
     * @return bool
     */
    public function write($sql)
    {
        return (bool)$this->conn->query($sql);
    }

    /**
     * Process multiple queries.
     * @param string $sql
     * @return bool
     */
    public function multi_query($sql)
    {
        return (bool)$this->conn->exec($sql);
    }

    /**
     * @param sqlite3result $resultset
     * @return bool
     */
    public function freeResultset($resultset)
    {
        return (bool)$resultset->finalize();
    }

    /**
     * Returns the change times for the given tables.
     *
     * @param mixed          $cache_db
     * @param string[]|null  $tables
     * @return int[]|false
     */
    public function getTableTimes($cache_db, $tables=null)
    {
        $specific_tables_clause = $tables ? "AND name IN ('" . implode("','", $tables) . "')" : '';

        $sql_query =
            "SELECT name, sql FROM sqlite_master
             WHERE type='table' $specific_tables_clause
             ORDER BY name;";

        if (!($result = $this->conn->query($sql_query)))
            return false; // permissions problem?

        while ($row = $result->fetchArray(SQLITE3_ASSOC)) {
            // typical sqlite timestamp value: 2020-05-24 12:34:56
            $update_time = (int)(new DateTime($row['UPDATE_TIME']))->format('U');

            $table_update_times[$row['TABLE_NAME']] = $update_time;
        }

        $this->freeResultset($result);

        return $table_update_times;
    }

    /**
     * Delete all rows from the given table.
     *
     * @param string $table
     * @return bool
     */
    public function truncateTable($table)
    {
        return (bool)$this->write("DELETE FROM $table");
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
        $sql = "SELECT name FROM sqlite_master WHERE type='table' AND name='$table'";
        return (bool)$this->read($sql);
    }

    /**
     * Returns SQL to create the cache table.
     * @param string  $table_name
     * @return string
     */
    public function getCreateTableSQL_cache($table_name)
    {
        $max_resultset_size = Constants::MAX_DB_RESULTSET_SIZE;

        return "DROP TABLE IF EXISTS $table_name;
                CREATE TABLE $table_name (
                    hash            CHAR(32)            NOT NULL PRIMARY KEY,
                    access_time     INT(11)         DEFAULT NULL,
                    script          VARCHAR(4000)   DEFAULT NULL,
                    av_nanosecs     REAL            DEFAULT NULL,
                    impressions     INT(11)         DEFAULT NULL,
                    description     VARCHAR(200)    DEFAULT NULL,
                    tables_csv      VARCHAR(1000)   DEFAULT NULL,
                    resultset       BLOB($max_resultset_size)
                );";
    }

    /**
     * Returns SQL to create the logs table.
     * @param string  $table_name
     * @return string
     */
    public function getCreateTableSQL_logs($table_name)
    {
        return "DROP TABLE IF EXISTS $table_name;
                CREATE TABLE $table_name (
                    id              INT(11)             NOT NULL PRIMARY KEY,
                    time            INT(11)         DEFAULT NULL,
                    context         CHAR(4)         DEFAULT NULL,
                    nanosecs        REAL            DEFAULT NULL,
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
        return "DROP TABLE IF EXISTS $table_name;
                CREATE TABLE $table_name (
                    name            VARCHAR(80)         NOT NULL PRIMARY KEY,
                    update_time     INT(11)         DEFAULT NULL
                );";
    }

    /**
     * Returns the primary keys for the given table.
     * @return string[]
     */
    public function getPrimary($table)
    {
        $sql = "SHOW KEYS FROM $table WHERE Key_name = 'PRIMARY'";

        $data = [];

        if ($result = $this->conn->query($sql))
            while ($row = $result->fetch_assoc())
                $data[] = $row['Column_name'];

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
            $sql = "SELECT table_name FROM information_schema.tables WHERE TABLE_SCHEMA LIKE '$db_name'";
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
        $db_name = $this->getDbName();

        return $this->readCol(
            "SELECT `COLUMN_NAME` 
             FROM `INFORMATION_SCHEMA`.`COLUMNS` 
             WHERE `TABLE_SCHEMA`='$db_name'
             AND `TABLE_NAME`='$table'"
        );
    }
}