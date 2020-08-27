<?php

namespace acet\qcache;

use acet\qcache\exception as QcEx;

function aa(string $str, int $level) { return $str; }

class QCache extends QCacheUtils
{
    const RESULTSET_INDEX = 6;  // index of the resultset within the array stored in cache files

    /** @var bool */
    private $qcache_enabled;

    /** @var string */
    private $qcache_folder;

    /** @var string */
    private $target_db_sig;

    /** @var string */
    private $table_qc_cache;

    /** @var string */
    private $table_qc_logs;

    /** @var mixed */
    private $cache_db_connection;

    /** @var mixed */
    private $target_db_connection;

    /**
     * @param string    $qcache_folder
     * @param string[]  $cache_db_connection_data
     * @param string[]  $target_db_connection_data
     * @param bool      $qcache_enabled
     * @param string    $module_id
     * @throws QcEx\QCacheException
     */
    function __construct($qcache_folder, $cache_db_connection_data, $target_db_connection_data=null, $qcache_enabled=true, $module_id='')
    {
        if (!function_exists('gzdeflate'))
            throw new QcEx\QCacheException("Please install 'ext-zlib'");

        $this->qcache_folder = $qcache_folder;
        $this->qcache_enabled = $qcache_enabled;

        if (!$target_db_connection_data)
            $target_db_connection_data = $cache_db_connection_data;

        $this->target_db_sig = "{$target_db_connection_data['type']}:{$target_db_connection_data['host']}:{$target_db_connection_data['name']}";

        $this->cache_db_connection = self::getConnection($cache_db_connection_data, $module_id);
        $this->target_db_connection = self::getConnection($target_db_connection_data, $module_id);

        if ($module_id)
            $module_id .= '_';

        $schema_prefix = strtolower($cache_db_connection_data['type']) == 'mssql' ? 'dbo.' : '';

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

        $hash = hash('md5', $this->target_db_sig . ($sql = trim($sql)));

        $columns = 'access_time, script, av_nanosecs, impressions, description, tables_csv, resultset';
        $sql_get_cache = "SELECT $columns FROM $this->table_qc_cache WHERE hash='$hash'";

        $cache_file = $this->qcache_folder.DIRECTORY_SEPARATOR."#$hash.dat";

        if ($data = $this->cache_db_connection->read($sql_get_cache)) { // from database
            $data[0]['resultset'] = unserialize(gzinflate($data[0]['resultset']));
            $cached_data = array_values($data[0]);
            $from_db = true;
        }
        elseif ($cached_data = serializedDataFileIO::read($cache_file)) { // from file
            $cached_data[self::RESULTSET_INDEX] = unserialize(gzinflate($cached_data[self::RESULTSET_INDEX]));
            $from_db = false;
        }

        if ($cached_data) {
            // SQL statement has been seen before

            [$access_time, $script, $av_nanosecs, $impressions, $description, $tables_csv, $resultset] = $cached_data;

            // check whether cache is stale (tables have changed since last access time)
            if ($this->target_db_connection->findTableChanges($access_time, explode(',', $tables_csv), $this->cache_db_connection)) {

                // perform a fresh query and update cache
                $start_nanosecs = hrtime(true); // restart nanosecond timer
                $resultset = new SqlResultSet($this->target_db_connection->read($sql));
                $elapsed_nanosecs = hrtime(true) - $start_nanosecs;

                $av_nanosecs = (float)($elapsed_nanosecs + $av_nanosecs * $impressions++) / $impressions;

                $resultset_gz = gzdeflate(serialize($resultset), Constants::GZ_COMPRESSION_LEVEL);
                $resultset_gz_esc = $this->cache_db_connection->escapeBinData($resultset_gz);

                // decide whether to cache to db or file
                $context = strlen($resultset_gz_esc) <= Constants::MAX_DB_RESULTSET_SIZE ? 'db' : 'qc';

                if ($context == 'db') { // save to db - faster, better for small result sets

                    $this->cache_db_connection->write(
                        "UPDATE $this->table_qc_cache ".
                        "SET access_time=$access_time,".
                        "av_nanosecs=$av_nanosecs,".
                        "impressions=$impressions,".
                        "resultset=$resultset_gz_esc ".
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
                        [$access_time, $script, $av_nanosecs, $impressions, $description, $tables_csv, $resultset_gz]
                    );

                    // if the same db record exists, delete it
                    if ($from_db) {
                        $this->cache_db_connection->write("DELETE FROM $this->table_qc_cache WHERE hash='$hash'");
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
        $resultset = new SqlResultSet($this->target_db_connection->read($sql));
        $elapsed_nanosecs = hrtime(true) - $start_nanosecs;

        $resultset_gz = gzdeflate(serialize($resultset), Constants::GZ_COMPRESSION_LEVEL);
        $resultset_gz_esc = $this->cache_db_connection->escapeBinData($resultset_gz);

        // decide whether to cache to db or file
        $context = strlen($resultset_gz_esc) <= Constants::MAX_DB_RESULTSET_SIZE ? 'db' : 'qc';

        if ($context == 'db') { // save to db - faster, but better for small result sets
            $description_esc = $this->cache_db_connection->escapeString($description);
            $script_esc = $this->cache_db_connection->escapeString($sql);

            $this->cache_db_connection->write(
                "INSERT INTO $this->table_qc_cache (hash, access_time, script, av_nanosecs, impressions, description, tables_csv, resultset) ".
                "VALUES ('$hash', $time_now, $script_esc, $elapsed_nanosecs, 1, $description_esc, '$tables_csv', $resultset_gz_esc)"
            );
        }

        else // save to file - slower, but better for large result sets
            serializedDataFileIO::write(
                $cache_file,
                [$time_now, $sql, $elapsed_nanosecs, 1, $description, $tables_csv, $resultset_gz]
            );

        $this->logTransactionStats($time_now, $context, $elapsed_nanosecs, $hash);

        return $resultset;
    }

    /**
     * Returns a suitable connector for the given connection details (MySQL, MsSQL and SQLite are currently supported).
     *
     * @param string[]  $db_connection_data
     * @param string    $module_id
     * @return DbConnectorInterface
     * @throws QcEx\ConnectionException
     */
    public static function getConnection($db_connection_data, $module_id='')
    {
        if (class_exists($class = '\acet\qcache\DbConnector' . $db_connection_data['type'])) {
            return new $class(
                $db_connection_data['host'],
                $db_connection_data['user'],
                $db_connection_data['pass'],
                $db_connection_data['name'],
                $module_id
            );
        }

        throw new QcEx\ConnectionException("Unsupported database type - \"{$db_connection_data['type']}\"");
    }

    /**
     * Returns the external database connection.
     *
     * @return DbConnectorMySQL|DbConnectorMSSQL
     */
    public function getTargetDbConnection()
    {
        return $this->target_db_connection;
    }

    /**
     * Returns the names of all external tables.
     *
     * @return string[]
     */
    public function getTargetDbTableNames()
    {
        return $this->target_db_connection->getTableNames();
    }

    /**
     * Returns the names of all columns in the given external table.
     *
     * @return string[]
     */
    public function getTargetDbColumnNames($table)
    {
        return $this->target_db_connection->getColumnNames($table);
    }

    /**
     * Returns the PRIMARY KEY for the given external table.
     *
     * @param $table
     * @return string|string[]
     */
    public function getTargetDbPrimary($table)
    {
        return $this->target_db_connection->getPrimary($table);
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
        if (Constants::LOG_TO_DB)
            $this->cache_db_connection->write(
                "INSERT INTO $this->table_qc_logs (time, context, nanosecs, hash) ".
                "VALUES ($time, $context, $nanosecs, '$hash')"
            );
    }
}
