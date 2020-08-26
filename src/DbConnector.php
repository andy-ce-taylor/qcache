<?php
/**
 * @noinspection PhpComposerExtensionStubsInspection
 * @noinspection SqlNoDataSourceInspection
 * @noinspection SqlDialectInspection
 */

namespace acet\qcache;

use DateTime;
use mysqli;
use SQLite3;

class DbConnector extends DbChangeDetection
{
    /** @var mysqli|SQLite3|resource */
    protected $conn;

    /** @var string */
    private $db_name;

    /** @var bool */
    private $db_uses_cached_updates_table;

    /** @var string */
    private $updates_table = '';

    /**
     * DbConnector constructor.
     *
     * @param string  $database_name
     * @param bool    $cached_updates_table
     * @param string  $module_id
     */
    function __construct($database_name, $cached_updates_table, $module_id='')
    {
        $this->db_name = $database_name;

        if ($module_id)
            $module_id .= '_';

        $this->db_uses_cached_updates_table = $cached_updates_table;

        if ($cached_updates_table)
        $this->updates_table = 'qc_' . $module_id . 'table_update_times';
    }

    // Getters

    public    function getConnection()                  { return $this->conn; }
    public    function dbUsesCachedUpdatesTable()       { return $this->db_uses_cached_updates_table; }
    protected function getDbName()                      { return $this->db_name; }
    protected function getTableUpdateTimesTableName()   { return $this->updates_table; }


    // Universal scripts

    /**
     * Read the table update times table.
     * @return array
     */
    public function readTableUpdateTimesTable()
    {
        $table_update_times_table = $this->getTableUpdateTimesTableName();

        $table_update_times = [];

        if ($rows = $this->read("SELECT * FROM $table_update_times_table"))
            foreach ($rows as $row)
                $table_update_times[$row['name']] = $row['update_time'];

        return $table_update_times;
    }

    /**
     * Write the table update times table.
     * @param array $data
     */
    public function writeTableUpdateTimesTable($data)
    {
        $updates_table = $this->getTableUpdateTimesTableName();

        $this->truncateTable($updates_table);

        $sql = "INSERT INTO $updates_table (name, update_time) VALUES ";

        foreach ($data as $table => $update_time) {
            $sql .= "('$table',$update_time),";
        }

        $table_update_times = $this->conn->query(rtrim($sql, ","));

        return $table_update_times;
    }
}