<?php
/**
 * @noinspection PhpComposerExtensionStubsInspection
 * @noinspection SqlNoDataSourceInspection
 * @noinspection SqlDialectInspection
 */

namespace acet\qcache\connector;

use acet\qcache\Constants;
use acet\qcache\exception as QcEx;
use acet\qcache\SqlResultSet;
use DateTime;
use mysqli;
use mysqli_driver;
use mysqli_result;
use mysqli_sql_exception;

class DbConnectorMySQL extends DbConnector implements DbConnectorIfc
{
    const SERVER_NAME = 'MySQL';
    const CACHED_UPDATES_TABLE = false;
    const DFLT_CHAR_SET = 'latin1';

    /**
     * DbConnectorMySQL constructor.
     *
     * @param array    $qcache_config
     * @param string[] $db_connection_data  - includes 'character_set' (such as 'utf8')
     * @throws QcEx\ConnectionException
     */
    function __construct($qcache_config, $db_connection_data)
    {
        $this->isEnabled();
        static $_connection = [];

        $key = implode(':', $db_connection_data);

        if (array_key_exists($key, $_connection)) {
            if (!$_connection[$key]) {
                throw new QcEx\ConnectionException(self::SERVER_NAME);
            }

            $this->conn = $_connection[$key];
        } else {
            $driver = new mysqli_driver();
            $driver->report_mode = MYSQLI_REPORT_STRICT;

            try {

                $this->conn = @new mysqli(
                    $db_connection_data['host'],
                    $db_connection_data['user'],
                    $db_connection_data['pass'],
                    $db_connection_data['name']
                );

                if (array_key_exists('character_set', $db_connection_data)) {
                    $this->conn->set_charset($db_connection_data['character_set']);
                }

                $_connection[$key] = $this->conn;

            } catch (mysqli_sql_exception $ex) {
                $_connection[$key] = false;
                throw new QcEx\ConnectionException(self::SERVER_NAME);
            }
        }

        parent::__construct($qcache_config, $db_connection_data, self::SERVER_NAME, self::CACHED_UPDATES_TABLE);
    }

    /**
     * @param string $str
     * @return string
     */
    public function escapeString($str)
    {
        return "'" . $this->conn->escape_string($str) . "'";
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
     * Returns the difference (seconds) between database timestamps and the current system time.
     *
     * @return int
     * @throws QcEx\TableReadException
     */
    public function getDbTimeOffset()
    {
        static $database_time_offset_l1c;

        if (!$database_time_offset_l1c) {

            $sql = 'SELECT NOW()';

            if (($result = $this->conn->query($sql)) === false) {
                throw new QcEx\TableReadException('NOW', $sql, self::SERVER_NAME, $this->conn->error);
            }

            $db_timestamp = $result->fetch_row()[0];

            $this->freeResultset($result);

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

            if (strpos($selector, ',') !== false) {
                $selector = "CONCAT(" . str_replace(',', ', " ", ', $selector) . ')';
            }

            foreach ($selector_values as $val) {
                $where .= "{$selector} = " . $this->escapeString($val) . " OR ";
            }

            // get rid of final 'OR'
            $where = 'WHERE ' . substr($where, 0, -4);
        }

        $limit = $limit > 0 ? "LIMIT $limit" : '';

        return "SELECT $fields_csv FROM $table $where $limit";
    }

    /**
     * Process a table read request, such as SELECT, and return the response.
     * @param string  $sql
     * @param bool    $return_resultset
     * @return SqlResultSet|array
     * @throws QcEx\TableReadException
     */
    public function read($sql, $return_resultset=true)
    {
        $data = [];

        if (($result = $this->conn->query($sql)) === false) {
            throw new QcEx\TableReadException('', $sql, self::SERVER_NAME, $this->conn->error);
        }

        while ($row = $result->fetch_assoc()) {
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

        if (($result = $this->conn->query($sql)) === false) {
            throw new QcEx\TableReadException('', $sql, self::SERVER_NAME, $this->conn->error);
        }

        while ($row = $result->fetch_array(MYSQLI_NUM)) {
            $data[] = $row[0];
        }

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
        if ($this->conn->query($sql) === false) {
            throw new QcEx\TableWriteException('', $sql, self::SERVER_NAME, $this->conn->error);
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
        $this->conn->multi_query($sql);

        // flush results
        while ($this->conn->next_result());
    }

    /**
     * Convert a native resultset into a SqlResultSet.
     *
     * @param mysqli_result $native_resultset
     * @return SqlResultSet
     */
    public function toSqlResultSet($native_resultset)
    {
        $data = [];

        while ($row = $native_resultset->fetch_assoc()) {
            $data[] = $row;
        }

        $this->freeResultset($native_resultset);

        return new SqlResultSet($data);
    }

    /**
     * @param mysqli_result $resultset
     * @return bool
     * @throws mysqli_sql_exception
     */
    public function freeResultset($resultset)
    {
        if ($resultset->num_rows) {
            $resultset->free_result();
        }

        return true;
    }

    /**
     * Returns the change times for the given tables.
     *
     * @param mixed $db_connection_cache
     * @param string[]|null $tables
     * @return int[]|false
     * @throws QcEx\TableReadException
     */
    public function getTableTimes($db_connection_cache, $tables=null)
    {
        $specific_tables_clause = $tables ? "AND TABLE_NAME IN ('" . implode("','", $tables) . "')" : '';

        $db_name = $this->getDbName();

        $sql =
            "SELECT SQL_NO_CACHE TABLE_NAME, UPDATE_TIME
             FROM information_schema.tables
             WHERE TABLE_SCHEMA = '$db_name' $specific_tables_clause";

        if (($result = @$this->conn->query($sql)) === false) {
            throw new QcEx\TableReadException('information_schema.tables', $sql, self::SERVER_NAME, $this->conn->error);
        }

        $table_update_times = [];

        while ($row = $result->fetch_assoc()) {
            // typical mysql timestamp value: 2020-05-24 12:34:56
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
     * @throws QcEx\TableWriteException
     */
    public function truncateTable($table)
    {
        return $this->write("TRUNCATE TABLE $table");
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
        return (bool)$this->read("SHOW TABLES LIKE '$table'");
    }

    /**
     * Returns SQL to create the cache information table.
     * @param string  $table_name
     * @return string
     */
    public function getCreateTableSQL_cache_info($table_name)
    {
        return "DROP TABLE IF EXISTS $table_name;
                CREATE TABLE $table_name (
                    hash            CHAR(32)        NOT NULL PRIMARY KEY DEFAULT ' ',
                    access_time     INT(11)         DEFAULT NULL,
                    av_microsecs    FLOAT           DEFAULT NULL,
                    impressions     INT(11)         DEFAULT NULL,
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
        return "DROP TABLE IF EXISTS $table_name;
                CREATE TABLE $table_name (
                    id              INT(11)         NOT NULL PRIMARY KEY AUTO_INCREMENT,
                    time            INT(11)         DEFAULT NULL,
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
        return "DROP TABLE IF EXISTS $table_name;
                CREATE TABLE $table_name (
                    name            VARCHAR(80)     NOT NULL PRIMARY KEY DEFAULT ' ',
                    update_time     INT(11)         DEFAULT NULL
                );";
    }

    /**
     * Returns the primary keys for the given table.
     * @return string[]
     * @throws QcEx\TableReadException
     */
    public function getPrimary($table)
    {
        $sql = "SHOW KEYS FROM $table WHERE Key_name = 'PRIMARY'";

        $data = [];

        if (($result = @$this->conn->query($sql)) === false) {
            throw new QcEx\TableReadException($table, $sql, self::SERVER_NAME, $this->conn->error);
        }

        while ($row = $result->fetch_assoc()) {
            $data[] = $row['Column_name'];
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
            $sql = "SELECT table_name FROM information_schema.tables WHERE TABLE_SCHEMA LIKE '$db_name'";
            $table_names[$db_name] = $this->readCol($sql);
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
        $db_name = $this->getDbName();

        return $this->readCol(
            "SELECT `COLUMN_NAME` 
             FROM `INFORMATION_SCHEMA`.`COLUMNS` 
             WHERE `TABLE_SCHEMA`='$db_name'
             AND `TABLE_NAME`='$table'"
        );
    }
}