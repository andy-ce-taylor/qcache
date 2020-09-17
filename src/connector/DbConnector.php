<?php
/**
 * @noinspection PhpComposerExtensionStubsInspection
 * @noinspection SqlNoDataSourceInspection
 * @noinspection SqlDialectInspection
 */

namespace acet\qcache\connector;

use acet\qcache\SqlResultSet;
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

    /** @var bool */
    private $enabled = true;

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
            $this->updates_table = self::getConnectorSignature($db_connection_data) . '_table_update_times';
    }

    // Getters

    public    function getConnection()                  { return $this->conn; }
    public    function dbUsesCachedUpdatesTable()       { return $this->db_uses_cached_updates_table; }
    public    function isEnabled()                      { return $this->enabled; }
    protected function getDbName()                      { return $this->db_name; }
    protected function getTableUpdateTimesTableName()   { return $this->updates_table; }

    /**
     * Returns a string that uniquely identifies a database connection.
     *
     * @param string[] $db_connection
     * @return string
     */
    public static function getConnectorSignature($db_connection)
    {
        $sig = "{$db_connection['type']}_{$db_connection['host']}_{$db_connection['name']}";

        return preg_replace("/[^a-zA-Z0-9_]+/", "", strtolower($sig));
    }

    /**
     * @param bool $enable
     */
    public function enable($enable)
    {
        // ToDo: store disabled connectors in nv location so that it can be remembered
        $this->enabled = $enable;
    }

    /**
     * Read the table update times table.
     * @param DbConnector $db_connection_cache
     * @return array
     */
    public function readTableUpdateTimesTable($db_connection_cache)
    {
        $table_update_times_table = $this->getTableUpdateTimesTableName();

        $table_update_times = [];

        /** @var SqlResultSet[] $rows */
        if ($rows = $db_connection_cache->read("SELECT * FROM $table_update_times_table"))
            while ($row = $rows->fetch_assoc())
                $table_update_times[$row['name']] = $row['update_time'];

        return $table_update_times;
    }

    /**
     * Write the table update times table.
     * @param array $data
     * @return mixed
     */
    public function writeTableUpdateTimesTable($data)
    {
        $updates_table = $this->getTableUpdateTimesTableName();

        $this->truncateTable($updates_table);

        $sql = "INSERT INTO $updates_table (name, update_time) VALUES ";

        foreach ($data as $table => $update_time) {
            $sql .= "('$table',$update_time),";
        }

        return $this->conn->query(rtrim($sql, ","));
    }
}