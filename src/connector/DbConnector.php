<?php
/**
 * @noinspection PhpComposerExtensionStubsInspection
 * @noinspection SqlNoDataSourceInspection
 * @noinspection SqlDialectInspection
 */

namespace acet\qcache\connector;

use DateTime;
use mysqli;
use SQLite3;

class DbConnector extends DbChangeDetection
{
    /** @var mysqli|SQLite3|resource */
    protected $conn;

    /** @var array */
    protected $qcache_config;

    /** @var string */
    private $db_name;

    /** @var bool */
    private $db_uses_cached_updates_table;

    /** @var string */
    private $updates_table = '';

    /**
     * DbConnector constructor.
     *
     * @param array    $qcache_config
     * @param string[] $db_connection_data
     * @param bool     $cached_updates_table
     */
    function __construct($qcache_config, $db_connection_data, $cached_updates_table)
    {
        $this->qcache_config = $qcache_config;
        $this->db_name = $db_connection_data['name'];

        $this->db_uses_cached_updates_table = $cached_updates_table;

        if ($cached_updates_table)
            $this->updates_table = self::getSignature($db_connection_data) . '_table_update_times';
    }

    // Getters

    public    function getConnection()                  { return $this->conn; }
    public    function dbUsesCachedUpdatesTable()       { return $this->db_uses_cached_updates_table; }
    protected function getDbName()                      { return $this->db_name; }
    protected function getTableUpdateTimesTableName()   { return $this->updates_table; }

    /**
     * Returns a string that uniquely identifies a database connection.
     *
     * @param string[] $db_connection
     * @return string
     */
    public static function getSignature($db_connection)
    {
        $type = strtolower($db_connection['type']);
        $prefix = $type == 'mssql' ? 'dbo.' : '';
        $sig = "{$prefix}{$type}_{$db_connection['host']}_{$db_connection['name']}";

        return $prefix . $sig;
    }

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