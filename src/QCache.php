<?php

namespace acet\qcache;

use acet\qcache\connector as Conn;
use acet\qcache\exception as QcEx;

function aa(string $str, int $level) { return $str; }

class QCache extends QCacheUtils
{
    const RESULTSET_INDEX = 6;  // index of the resultset within the array stored in cache files

    /** @var array */
    private $qcache_config;

    /** @var string */
    private $table_qc_cache;

    /** @var string */
    private $table_qc_logs;

    /** @var mixed */
    private $db_connection_cache;

    /** @var mixed */
    private $db_connection_target;

    /** @var string */
    private $target_connection_sig;

    /**
     * @param array     $qcache_config
     * @param string[]  $db_connection_cache_data
     * @param string[]  $db_connection_target_data
     * @throws QcEx\QCacheException
     */
    function __construct($qcache_config, $db_connection_cache_data, $db_connection_target_data=[])
    {
        if (!function_exists('gzdeflate'))
            throw new QcEx\QCacheException("Please install 'ext-zlib'");

        $this->qcache_config = $qcache_config;

        if (!$db_connection_target_data)
            $db_connection_target_data = $db_connection_cache_data;

        $this->db_connection_cache = self::getConnection($qcache_config, $db_connection_cache_data);
        $this->db_connection_target = self::getConnection($qcache_config, $db_connection_target_data);

        $this->target_connection_sig = Conn\DbConnector::getSignature($db_connection_target_data);

        $this->table_qc_cache = $this->target_connection_sig . '_cache';
        $this->table_qc_logs  = $this->target_connection_sig . '_logs';
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
        if (!$this->qcache_config['enabled'])
            return false;

        $start_nanosecs = hrtime(true);
        $time_now = time();

        $hash = hash('md5', $sql = trim($sql));

        $columns = 'access_time, script, av_nanosecs, impressions, description, tables_csv, resultset';
        $sql_get_cache = "SELECT $columns FROM $this->table_qc_cache WHERE hash='$hash'";

        $cache_file = $this->qcache_config['qcache_folder'] . DIRECTORY_SEPARATOR . "$this->target_connection_sig#$hash.dat";

        if ($data = $this->db_connection_cache->read($sql_get_cache, false)) { // from database
            $data[0]['resultset'] = unserialize(gzinflate($data[0]['resultset']));
            $cached_data = array_values($data[0]);
            $from_db = true;
        }
        elseif ($cached_data = SerializedDataFileIO::read($cache_file)) { // from file
            $cached_data[self::RESULTSET_INDEX] = unserialize(gzinflate($cached_data[self::RESULTSET_INDEX]));
            $from_db = false;
        }

        if ($cached_data) {
            // SQL statement has been seen before

            [$access_time, $script, $av_nanosecs, $impressions, $description, $tables_csv, $resultset] = $cached_data;

            // check whether cache is stale (tables have changed since last access time)
            if ($this->db_connection_target->findTableChanges($access_time, explode(',', $tables_csv), $this->db_connection_cache)) {

                // perform a fresh query and update cache
                $start_nanosecs = hrtime(true); // restart nanosecond timer
                $resultset = $this->db_connection_target->read($sql, true);
                $elapsed_nanosecs = hrtime(true) - $start_nanosecs;

                $av_nanosecs = (float)($elapsed_nanosecs + $av_nanosecs * $impressions++) / $impressions;

                $resultset_gz = gzdeflate(serialize($resultset), $this->qcache_config['gz_compression_level']);
                $resultset_gz_esc = $this->db_connection_cache->escapeBinData($resultset_gz);

                // decide whether to cache to db or file
                $context = strlen($resultset_gz_esc) <= $this->qcache_config['max_db_resultset_size'] ? 'db' : 'qc';

                if ($context == 'db') { // save to db - faster, better for small result sets

                    $this->db_connection_cache->write(
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
                    SerializedDataFileIO::write(
                        $cache_file,
                        [$access_time, $script, $av_nanosecs, $impressions, $description, $tables_csv, $resultset_gz]
                    );

                    // if the same db record exists, delete it
                    if ($from_db) {
                        $this->db_connection_cache->write("DELETE FROM $this->table_qc_cache WHERE hash='$hash'");
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
        $resultset = $this->db_connection_target->read($sql, true);
        $elapsed_nanosecs = hrtime(true) - $start_nanosecs;

        $resultset_gz = gzdeflate(serialize($resultset), $this->qcache_config['gz_compression_level']);
        $resultset_gz_esc = $this->db_connection_cache->escapeBinData($resultset_gz);

        // decide whether to cache to db or file
        $context = strlen($resultset_gz_esc) <= $this->qcache_config['max_db_resultset_size'] ? 'db' : 'qc';

        if ($context == 'db') { // save to db - faster, but better for small result sets
            $description_esc = $this->db_connection_cache->escapeString($description);
            $script_esc = $this->db_connection_cache->escapeString($sql);

            $this->db_connection_cache->write(
                "INSERT INTO $this->table_qc_cache (hash, access_time, script, av_nanosecs, impressions, description, tables_csv, resultset) ".
                "VALUES ('$hash', $time_now, $script_esc, $elapsed_nanosecs, 1, $description_esc, '$tables_csv', $resultset_gz_esc)"
            );
        }

        else // save to file - slower, but better for large result sets
            SerializedDataFileIO::write(
                $cache_file,
                [$time_now, $sql, $elapsed_nanosecs, 1, $description, $tables_csv, $resultset_gz]
            );

        $this->logTransactionStats($time_now, $context, $elapsed_nanosecs, $hash);

        return $resultset;
    }

    /**
     * Returns a suitable connector for the given connection details (MySQL, MsSQL and SQLite are currently supported).
     *
     * @param array     $qcache_config
     * @param string[]  $db_connection_data
     * @return Conn\DbConnectorInterface
     * @throws QcEx\ConnectionException
     */
    public static function getConnection($qcache_config, $db_connection_data)
    {
        if (class_exists($class = '\acet\qcache\connector\DbConnector' . $db_connection_data['type'])) {
            return new $class($qcache_config, $db_connection_data);
        }

        throw new QcEx\ConnectionException("Unsupported database type - \"{$db_connection_data['type']}\"");
    }

    /**
     * Returns the external database connection.
     *
     * @return Conn\DbConnectorMySQL|Conn\DbConnectorMSSQL
     */
    public function getTargetDbConnection()
    {
        return $this->db_connection_target;
    }

    /**
     * Returns the names of all external tables.
     *
     * @return string[]
     */
    public function getTargetDbTableNames()
    {
        return $this->db_connection_target->getTableNames();
    }

    /**
     * Returns the names of all columns in the given external table.
     *
     * @return string[]
     */
    public function getTargetDbColumnNames($table)
    {
        return $this->db_connection_target->getColumnNames($table);
    }

    /**
     * Returns the PRIMARY KEY for the given external table.
     *
     * @param $table
     * @return string|string[]
     */
    public function getTargetDbPrimary($table)
    {
        return $this->db_connection_target->getPrimary($table);
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
        if ($this->qcache_config['log_to_cache_db'])
            $this->db_connection_cache->write(
                "INSERT INTO $this->table_qc_logs (time, context, nanosecs, hash) ".
                "VALUES ($time, $context, $nanosecs, '$hash')"
            );
    }
}
