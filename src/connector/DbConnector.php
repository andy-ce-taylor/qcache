<?php
/**
 * @noinspection PhpComposerExtensionStubsInspection
 * @noinspection SqlNoDataSourceInspection
 * @noinspection SqlDialectInspection
 */

namespace acet\qcache\connector;

use acet\qcache\exception\TableQueryException;
use acet\qcache\QCache;
use acet\qcache\SqlResultSet;
use mysqli;
use SQLite3;

class DbConnector extends DbChangeDetectionAbs
{
    /** @var string */
    protected $server_name;

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
     * @param string   $server_name
     * @param bool     $cached_updates_table
     */
    function __construct($qcache_config, $db_connection_data, $server_name, $cached_updates_table=false)
    {
        $this->qcache_config = $qcache_config;
        $this->db_name = $db_connection_data['name'];
        $this->server_name = $server_name;

        $this->db_uses_cached_updates_table = $cached_updates_table;

        if ($cached_updates_table) {
            $this->updates_table = self::getConnectorSignature($db_connection_data) . '_table_update_times';
        }
    }

    // Getters

    public    function getConnection()                  { return $this->conn; }
    public    function getServerName()                  { return $this->server_name; }
    public    function dbUsesCachedUpdatesTable()       { return $this->db_uses_cached_updates_table; }
    public    function isEnabled()                      { return $this->enabled; }
    protected function getDbName()                      { return $this->db_name; }
    protected function getTableUpdateTimesTableName()   { return $this->updates_table; }

    /**
     * Read-in and return an entire table.
     *
     * @param string $table
     * @return array
     */
    public function getAll($table)
    {
        return $this->read("SELECT * FROM $table", false);
    }

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
     * Perform a standard query on the DB connection.
     * @param string $sql
     * @return bool|\mysqli_result|\SQLite3Result
     * @throws TableQueryException
     */
    public function query($sql)
    {
        if (($result = @$this->conn->query($sql)) === false) {
            throw new TableQueryException($result, $sql, $this->server_name, $this->conn->error);
        }

        return $result;
    }

    /**
     * Read the table update times table.
     * @param mixed $db_connection_cache
     * @return array
     */
    public function readTableUpdateTimesTable($db_connection_cache)
    {
        $table_update_times_table = $this->getTableUpdateTimesTableName();

        $table_update_times = [];

        /** @var SqlResultSet $result */
        if ($result = $db_connection_cache->read("SELECT * FROM $table_update_times_table")) {
            while ($row = $result->fetch_assoc()) {
                $table_update_times[$row['name']] = $row['update_time'];
            }
        }

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