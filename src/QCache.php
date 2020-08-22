<?php

namespace acet\qcache;

use Exception;

class QCache extends QCacheUtils
{
    const LOG_TO_DB = false;

    /** @var bool */
    private $qcache_enabled;

    /** @var string */
    private $qcache_folder;

    /** @var string */
    private $ext_conn_sig;

    /** @var string */
    private $table_qc_cache;

    /** @var string */
    private $table_qc_logs;

    /** @var mixed */
    private $loc_db_connection;

    /** @var mixed */
    private $ext_db_connection;

    /**
     * @param string[]  $loc_conn_data
     * @param string[]  $ext_conn_data
     * @param string    $qcache_folder
     * @param bool      $qcache_enabled
     * @param string    $module_id
     * @throws QCacheException
     */
    function __construct($loc_conn_data, $ext_conn_data=null, $qcache_folder='', $qcache_enabled=true, $module_id='') {

        if (!$qcache_folder)
            throw new QCacheException('The QCache folder MUST be specified');

        $this->qcache_folder = $qcache_folder;
        $this->qcache_enabled = $qcache_enabled;

        if (!$ext_conn_data)
            $ext_conn_data = $loc_conn_data;

        $this->ext_conn_sig = "{$ext_conn_data['type']}:{$ext_conn_data['host']}:{$ext_conn_data['name']}";

        $this->loc_db_connection = self::getConnection($loc_conn_data, $module_id);
        $this->ext_db_connection = self::getConnection($ext_conn_data, $module_id);

        if ($module_id)
            $module_id .= '_';

        $schema_prefix = strtolower($loc_conn_data['type']) == 'mssql' ? 'dbo.' : '';

        $this->table_qc_cache = "{$schema_prefix}qc_{$module_id}cache";
        $this->table_qc_logs  = "{$schema_prefix}qc_{$module_id}logs";
    }

    /**
     * Performs a SQL query or gets a cached result set.
     * Returns SqlResultSet if successful or FALSE if the query cannot be performed.
     * If FALSE is returned, the caller is expected to query the database directly.
     *
     * @param string  $sql
     * @param mixed   $tables       - array of table names, or a tables csv string, or null (qcache will find them))
     * @param string $description
     *
     * @return SqlResultSet|false
     */
    public function query($sql, $tables = null, $description = '')
    {
        if (!$this->qcache_enabled)
            return false;

        $start_nanosecs = hrtime(true);
        $time_now = time();

        $hash = hash('md5', $this->ext_conn_sig . ($sql = trim($sql)));

        $columns = 'access_time, script, av_nanosecs, impressions, description, tables_csv, resultset';
        $sql_get_cache = "SELECT $columns FROM $this->table_qc_cache WHERE hash='$hash'";

        $cache_file = $this->qcache_folder.DIRECTORY_SEPARATOR."#$hash.dat";

        if ($data = $this->ext_db_connection->read($sql_get_cache)) { // from database
            $data[0]['resultset'] = unserialize($data[0]['resultset']);
            $cached_data = array_values($data[0]);
            $from_db = true;
        }
        else {
            if ($cached_data = serializedDataFileIO::read($cache_file)) { // from file
                $from_db = false;
            }
        }

        if ($cached_data) {
            // SQL statement has been seen before

            [$access_time, $script, $av_nanosecs, $impressions, $description, $tables_csv, $resultset] = $cached_data;

            // check whether cache is stale (tables have changed since last access time)
            if ($this->ext_db_connection->findTableChanges($access_time, explode(',', $tables_csv), $this->loc_db_connection)) {

                // perform a fresh query and update cache
                $start_nanosecs = hrtime(true); // restart nanosecond timer
                $resultset = new SqlResultSet($this->ext_db_connection->read($sql));
                $elapsed_nanosecs = hrtime(true) - $start_nanosecs;

                $av_nanosecs = (float)($elapsed_nanosecs + $av_nanosecs * $impressions++) / $impressions;

                $resultset_esc = $this->loc_db_connection->escapeBinData(serialize($resultset));

                // decide whether to cache to db or file
                $context = strlen($resultset_esc) <= Constants::MAX_DB_RESULTSET_SIZE ? 'db' : 'qc';

                if ($context == 'db') { // save to db - faster, better for small result sets

                    $this->loc_db_connection->write(
                        "UPDATE $this->table_qc_cache ".
                        "SET access_time=$access_time,".
                        "av_nanosecs=$av_nanosecs,".
                        "impressions=$impressions,".
                        "resultset=$resultset_esc ".
                        "WHERE hash='$hash'"
                    );

                    // if the same cache file exists, delete it
                    if (!$from_db && file_exists($cache_file)) {
                        unlink($cache_file);
                    }
                }

                else { // save to file - slower, but better for large result sets
                    serializedDataFileIO::write(
                        $cache_file,
                        [$access_time, $script, $av_nanosecs, $impressions, $description, $tables_csv, $resultset]
                    );

                    // if the same db record exists, delete it
                    if ($from_db) {
                        $this->loc_db_connection->write("DELETE FROM $this->table_qc_cache WHERE hash='$hash'");
                    }
                }

                $this->logTransactionStats($time_now, $context, $elapsed_nanosecs, $hash);

                return $resultset;
            }

            // Cache is fresh - return a quick result from cache
            $elapsed_nanosecs = hrtime(true) - $start_nanosecs;

            $this->logTransactionStats($time_now, 'qc', $elapsed_nanosecs, $hash);

            return $resultset;
        }

        // previously unseen SQL statement

        if (is_null($tables)) // try to find table names within the statement
            if (($tables = QCacheUtils::getTables($sql)) == false)
                return false; // no table names found

        $tables_csv = is_array($tables) ? implode(',', $tables) : $tables;

        $start_nanosecs = hrtime(true); // restart nanosecond timer
        $resultset = new SqlResultSet($this->ext_db_connection->read($sql));
        $elapsed_nanosecs = hrtime(true) - $start_nanosecs;

        $resultset_esc = $this->loc_db_connection->escapeBinData(serialize($resultset));

        // decide whether to cache to db or file
        $context = strlen($resultset_esc) <= Constants::MAX_DB_RESULTSET_SIZE ? 'db' : 'qc';

        if ($context == 'db') { // save to db - faster, but better for small result sets
            $description_esc = $this->loc_db_connection->escapeBinData($description);
            $script_esc = $this->loc_db_connection->escapeBinData($sql);

            $this->loc_db_connection->write(
                "INSERT INTO $this->table_qc_cache (hash, access_time, script, av_nanosecs, impressions, description, tables_csv, resultset) ".
                "VALUES ('$hash', $time_now, $script_esc, $elapsed_nanosecs, 1, $description_esc, '$tables_csv', $resultset_esc)"
            );
        }

        else // save to file - slower, but better for large result sets
            serializedDataFileIO::write($cache_file, [$time_now, $sql, $elapsed_nanosecs, 1, $description, $tables_csv, $resultset]);

        $this->logTransactionStats($time_now, $context, $elapsed_nanosecs, $hash);

        return $resultset;
    }

    /**
     * Returns a suitable connector for the given connection details (MySQL or MsSQL, currently).
     *
     * @param string[]  $conn_data
     * @param string    $module_id
     * @return DbConnectorMySQL|DbConnectorMSSQL
     * @throws QCacheConnectionException
     */
    public static function getConnection($conn_data, $module_id='')
    {
        switch ($conn_data['type']) {

            case 'mysql':
                return new DbConnectorMySQL($conn_data['host'], $conn_data['user'], $conn_data['pass'], $conn_data['name'], $module_id);

            case 'mssql':
                return new DbConnectorMSSQL($conn_data['host'], $conn_data['user'], $conn_data['pass'], $conn_data['name'], $module_id);
        }

        throw new QCacheConnectionException("Unsupported database type - \"{$conn_data['type']}\"");
    }

    /**
     * Returns the external database connection.
     *
     * @return DbConnectorMySQL|DbConnectorMSSQL
     */
    public function getExtDbConnection()
    {
        return $this->ext_db_connection;
    }

    /**
     * Returns the names of all external tables.
     *
     * @return string[]
     */
    public function getExtDbTableNames()
    {
        return $this->ext_db_connection->getTableNames();
    }

    /**
     * Returns the names of all columns in the given external table.
     *
     * @return string[]
     */
    public function getExtDbColumnNames($table)
    {
        return $this->ext_db_connection->getColumnNames($table);
    }

    /**
     * Returns the PRIMARY KEY for the given external table.
     *
     * @param $table
     * @return string|string[]
     */
    public function getExtDbPrimary($table)
    {
        return $this->ext_db_connection->getPrimary($table);
    }

    /**
     * Logs transaction statistics to the local database.
     *
     * @param $time
     * @param $context
     * @param $nanosecs
     * @param $hash
     */
    private function logTransactionStats($time, $context, $nanosecs, $hash)
    {
        if (self::LOG_TO_DB)
            $this->loc_db_connection->write(
                "INSERT INTO $this->table_qc_logs (time, context, nanosecs, hash) ".
                "VALUES ($time, $context, $nanosecs, '$hash')"
            );
    }
}