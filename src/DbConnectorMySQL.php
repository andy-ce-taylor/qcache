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

class DbConnectorMySQL extends DbChangeDetection implements DbConnectorInterface
{
    /** @var mysqli */
    protected $conn;

    /** @var string */
    protected $database_name;

    /**
     * DbConnectorMySQL constructor.
     *
     * @param string  $host
     * @param string  $user
     * @param string  $pass
     * @param string  $database_name
     * @throws QCacheConnectionException
     */
    function __construct($host, $user, $pass, $database_name)
    {
        $this->conn = new mysqli($host, $user, $pass, $database_name);

        if ($this->conn->connect_errno)
            throw new QCacheConnectionException("MySQL connection error: " . $this->conn->connect_errno);

        $this->database_name = $database_name;
    }

    /**
     * @param string $str
     * @return string
     */
    public function sql_escape_string($str)
    {
        return $this->conn->escape_string($str);
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

            $sql = 'SELECT NOW()';

            $result = $this->conn->query($sql);
            $db_timestamp = $result->fetch_row()[0];

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
     * @param string $sql
     * @return array
     */
    public function processQuery($sql)
    {
        $data = [];

        if ($result = $this->conn->query($sql))
            while ($row = $result->fetch_assoc())
                $data[] = $row;

        return $data;
    }

    /**
     * @param string $sql
     * @return bool
     */
    public function processUpdate($sql)
    {
        return (bool)$this->conn->query($sql);
    }

    /**
     * Returns the change times for the given tables.
     *
     * @param string[]|null  $tables
     * @return int[]|false
     */
    public function getTableTimes($tables=null)
    {
        $specific_tables = '';

        if ($tables)
            $specific_tables = "AND TABLE_NAME IN ('" . implode("','", $tables) . "')";

        // typical timestamp value: 2020-05-24 12:01:23
        $sql_query = "
            SELECT SQL_NO_CACHE TABLE_NAME, UPDATE_TIME
            FROM information_schema.tables
            WHERE TABLE_SCHEMA = '$this->database_name' $specific_tables";

        if (!($res = $this->conn->query($sql_query)))
            return false; // permissions problem?

        $data = [];

        while ($row = $res->fetch_assoc())
            try {
                $data[$row['TABLE_NAME']] = (int)(new DateTime($row['UPDATE_TIME']))->format('U');
            } catch (Exception $ex) {
                $res->free_result();
                return false; // wrong time format
            }

        $res->free_result();

        return $data;
    }
}