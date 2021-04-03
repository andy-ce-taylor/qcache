<?php
/** @noinspection SqlNoDataSourceInspection */

namespace acet\qcache;

use acet\qcache\connector as Conn;
use acet\qcache\exception as QcEx;

class Qcache extends QcacheUtils
{
    // cache info record columns (if cache info storage is set to db, additional column 'hash' becomes the first column)
    const CACHE_INFO_COLUMNS = 'access_time, av_microtime, impressions, description, tables_csv';

    // log record columns
    const LOG_COLUMNS = 'time, microtime, status, hash';

    /** @var array */
    private $qcache_config;

    /** @var string */
    private $table_qc_cache_info;

    /** @var bool */
    private $cache_info_db;

    /** @var bool */
    private $log_to_db;

    /** @var string */
    private $log_file;

    /** @var string */
    private $table_qc_logs;

    /** @var mixed */
    private $db_connection_cache;

    /** @var mixed */
    private $db_connection_target;

    /** @var string */
    private $target_connection_sig;

    /** @var int */
    private $query_status = Constants::QUERY_STATUS_NULL;

    /**
     * @param string    $target_connection_sig
     * @param array     $qcache_config
     * @param string[]  $db_cache_conn_data
     * @param string[]  $db_target_conn_data
     * @throws QcEx\QcacheException
     */
    function __construct($target_connection_sig, $qcache_config, $db_cache_conn_data, $db_target_conn_data=[])
    {
        if (!function_exists('gzdeflate')) {
            throw new QcEx\QcacheException("Please install 'ext-zlib'");
        }

        if (!$qcache_config['qcache_folder'] || !is_dir($qcache_config['qcache_folder'])) {
            throw new QcEx\QcacheException("Option 'qcache_folder' must specify a valid directory");
        }

        // supply default values for missing config settings
        $this->qcache_config = array_merge([
            'enabled'                 => true,
            'gz_compression_level'    => Constants::DFLT_GZ_COMPRESSION_LEVEL,
            'cache_info_storage_type' => Constants::DFLT_CACHE_INFO_STORAGE_TYPE,
            'log_storage_type'        => Constants::DFLT_LOG_STORAGE_TYPE,
            'max_qcache_records'      => Constants::DFLT_MAX_QCACHE_TRANSACTIONS
        ], $qcache_config);

        $this->cache_info_db = $this->qcache_config['cache_info_storage_type'] == Constants::STORAGE_TYPE_DB;
        $this->log_to_db = $this->qcache_config['log_storage_type'] == Constants::STORAGE_TYPE_DB;

        // if no db connector is specified, use the same one as the cache
        if (!$db_target_conn_data) {
            $db_target_conn_data = $db_cache_conn_data;
        }

        $this->db_connection_cache = self::getConnection($qcache_config, $db_cache_conn_data);
        $this->db_connection_target = self::getConnection($qcache_config, $db_target_conn_data);

        $this->target_connection_sig = $target_connection_sig;
        $this->table_qc_cache_info = $target_connection_sig . '_cache_info';
        $this->table_qc_logs = $target_connection_sig . '_logs';

        $this->log_file = $this->qcache_config['qcache_folder'] . "/$target_connection_sig.log";
    }

    /**
     * Performs a SQL query or gets a cached result set.
     * Returns SqlResultSet if the statement is appropriate and the query successful.
     * Returns the standard response from the query otherwise.
     *
     * @param string  $stmt
     * @param mixed   $tables       - array of table names, or a tables csv string, or null (qcache will find them))
     * @param string $description
     *
     * @return SqlResultSet|bool
     * @throws QcEx\QcacheException
     */
    public function query($stmt, $tables = null, $description = '')
    {
        if (($resultset = $this->performQuery($stmt, $tables, $description)) === false) {
            // Qcache is disabled or the statement can't be processed by Qcache (not a SELECT)
            $result = $this->db_connection_target->query($stmt);
            if (is_bool($result)) {
                return $result;
            }
            $resultset = $this->db_connection_target->toSqlResultSet($result);
        }

        return $resultset;
    }

    /**
     * Add a string to the exclude list to prevent performQuery from caching matching statements.
     *
     * ToDo: Any existing cached data that matches the exclusion (case-insensitive) will be removed.
     *
     * Matching is fast and unsophisticated - if a new query starts with the excluded text, it is rejected.
     *
     * For example, if an exclude statement is "SELECT name FROM contacts WHERE company =" then a subsequent call
     * to performQuery("SELECT name FROM contacts WHERE company = 'BG Advanced Software'") will return FALSE.
     *
     * @param string $stmt
     */
    public function excludeQuery($stmt)
    {
        $slen = strlen($stmt);
        $stmt = strtolower($stmt);
        $hash = hash('md5', $stmt);

         // store in filesystem
        $exclusion_file = $this->qcache_config['qcache_folder'] .
            '/' . $this->target_connection_sig .
            '.' . Constants::EXCLUDE_STMT_FILE_EXT;

        if ($excluded = FileIO::read($exclusion_file, false, true)) {
            // ignore duplicates
            foreach ($excluded as $exc) {
                if ($exc[0] == $hash) {
                    return;
                }
            }
        } else {
            $excluded = [];
        }

        $excluded[] = [$hash, $slen, $stmt];

        FileIO::write(
            $exclusion_file,
            $excluded,
            0, true
        );
    }

    /**
     * Performs a SQL query or gets a cached result set.
     * Returns SqlResultSet if the statement is appropriate and the query successful.
     * Returns FALSE otherwise.
     *
     * @param string  $stmt
     * @param mixed   $tables       - array of table names, or a tables csv string, or null (qcache will find them))
     * @param string $description
     *
     * @return SqlResultSet|false
     * @throws QcEx\QcacheException
     */
    private function performQuery($stmt, $tables, $description)
    {
        if (!$this->qcache_config['enabled']) {
            $this->query_status = Constants::QUERY_STATUS_DISABLED;
            return false;
        }

        // Clean the statement and determine whether it looks like a SELECT
        if (!($stmt = QcacheUtils::getCleanedSelectStmt($stmt))) {
            return false;
        }

        $start_microtime = microtime(true);
        $time_now = time();
        $hash = hash('md5', $stmt);

        $gz_compression_level = $this->qcache_config['gz_compression_level'];

        $filename = $this->qcache_config['qcache_folder'] . "/$this->target_connection_sig#$hash";
        $cache_info_file = $filename . '.' . Constants::CACHE_INFO_FILE_EXT;

        $this->query_status = Constants::QUERY_STATUS_NULL;

        if ($this->cache_info_db) {
            if ($cache_info = $this->db_connection_cache->read(
                'SELECT ' . self::CACHE_INFO_COLUMNS . ' ' .
                "FROM $this->table_qc_cache_info " .
                "WHERE hash='$hash'", false
            )) {
                $this->query_status = Constants::QUERY_STATUS_CACHE_HIT;
                $cache_info = array_values($cache_info[0]);
            }
        } else { // cache info stored in filesystem
            if ($cache_info = FileIO::read($cache_info_file, false, true)) {
                $this->query_status = Constants::QUERY_STATUS_CACHE_HIT;
            }
        }

        $cache_file = $filename . '.' . Constants::CACHE_FILE_EXT;

        // Determine whether this SQL statement has been seen before (a resultset will have been cached)

        if ($this->query_status == Constants::QUERY_STATUS_CACHE_HIT) {

            // This SQL statement has been seen before
            [$access_time, $av_microtime, $impressions, $description, $tables_csv] = $cache_info;

            // determine whether cache is stale (tables have changed since last access time)
            $cache_is_stale = $this->db_connection_target->detectTableChanges($access_time, explode(',', $tables_csv), $this->db_connection_cache);

            if ($cache_is_stale) {
                $access_time = $time_now;

                // perform a fresh query and update cache
                $start_microtime = microtime(true);
                $resultset = $this->db_connection_target->read($stmt, true);
                $elapsed_microtime = microtime(true) - $start_microtime;

                $av_microtime = (float)($elapsed_microtime + $av_microtime * $impressions++) / $impressions;

                if ($this->cache_info_db) {

                    $this->db_connection_cache->write(
                        "UPDATE $this->table_qc_cache_info ".
                        "SET access_time=$access_time,".
                            "av_microtime=$av_microtime,".
                            "impressions=$impressions ".
                        "WHERE hash='$hash'"
                    );
                } else { // cache info stored in filesystem
                    FileIO::write(
                        $cache_info_file,
                        [$access_time, $av_microtime, $impressions, $description, $tables_csv],
                        0, true
                    );
                }

                // compress resultset and write it to the cache file
                FileIO::write($cache_file, $resultset, $gz_compression_level, true);

                $this->logTransactionStats($time_now, $elapsed_microtime, 'stale', $hash);

                $this->query_status = Constants::QUERY_STATUS_CACHE_STALE;

                return $resultset;
            }

            // Cache is fresh - return a quick result from cache

            $this->logTransactionStats($time_now, microtime(true) - $start_microtime, 'hit', $hash);
            $this->query_status = Constants::QUERY_STATUS_CACHE_HIT;

            return FileIO::read($cache_file, true, true);
        }

        // Previously unseen SQL statement

        if (is_null($tables)) {
            // find table names within the statement
            if (($tables = QcacheUtils::findTableNames($stmt, $hash)) == false) {
                // no table names found
                $this->query_status = Constants::QUERY_STATUS_ERROR;
                return false;
            }
        }

        // Check whether the statement has been excluded
        $exclusion_file = $this->qcache_config['qcache_folder'] .
            '/' . $this->target_connection_sig .
            '.' . Constants::EXCLUDE_STMT_FILE_EXT;

        $exclusions = FileIO::read($exclusion_file, false, true);

        if ($exclusions) {
            $stmt_lc = strtolower($stmt);
            foreach ($exclusions as [$ex_hash, $ex_slen]) {
                if (strlen($stmt_lc) >= $ex_slen) {
                    if (hash('md5', substr($stmt_lc, 0, $ex_slen)) == $ex_hash) {
                        // statement is excluded
                        $this->query_status = Constants::QUERY_STATUS_EXCLUDED;
                        return false;
                    }
                }
            }
        }

        // get the resultset - time it
        $start_microtime = microtime(true); // restart microsecond timer
        $resultset = $this->db_connection_target->read($stmt, true);
        $elapsed_microtime = microtime(true) - $start_microtime;

        // compress resultset and write it to the cache file
        FileIO::write($cache_file, $resultset, $gz_compression_level, true);

        // write the SQL statement to file
        $stmt_file = $filename . '.' . Constants::STMT_FILE_EXT;
        FileIO::write($stmt_file, $stmt, 0, false);


        $tables_csv = is_array($tables) ? implode(',', $tables) : $tables;

        if ($this->cache_info_db) {
            $description_esc = $this->db_connection_cache->escapeString($description);

            $this->db_connection_cache->write(
                "INSERT INTO $this->table_qc_cache_info (hash, " . self::CACHE_INFO_COLUMNS . ') '.
                "VALUES ('$hash', $time_now, $elapsed_microtime, 1, $description_esc, '$tables_csv')"
            );

        } else { // cache info stored in filesystem
            FileIO::write(
                $cache_info_file,
                [$time_now, $elapsed_microtime, 1, $description, $tables_csv],
                0, true
            );
        }

        $this->logTransactionStats($time_now, $elapsed_microtime, 'miss', $hash);
        $this->query_status = Constants::QUERY_STATUS_CACHE_MISS;

        return $resultset;
    }

    /**
     * Returns the status of the last query.
     *
     * @return int  - one of Constants::QUERY_STATUS_nnn
     */
    public function getQueryStatus()
    {
        return $this->query_status;
    }

    /**
     * Returns a suitable connector for the given connection details (MySQL, MsSQL and SQLite are currently supported).
     *
     * @param array     $qcache_config
     * @param string[]  $db_connection_data
     * @return Conn\DbConnectorIfc
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
     * Logs transaction statistics.
     *
     * @param $time
     * @param $microtime
     * @param $status
     * @param $hash
     */
    private function logTransactionStats($time, $microtime, $status, $hash)
    {
        if ($this->log_to_db) {
            $this->db_connection_cache->write(
                "INSERT INTO $this->table_qc_logs (" . self::LOG_COLUMNS . ') '.
                "VALUES ($time, $microtime, '$status', '$hash')"
            );
        } else { // log to file
            FileIO::append(
                $this->log_file,
                [$time, $microtime, $status, $hash],
                0, true
            );
        }
    }
}
