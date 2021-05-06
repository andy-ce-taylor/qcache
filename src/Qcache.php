<?php
/** @noinspection SqlNoDataSourceInspection */

namespace acet\qcache;

use acet\qcache\connector as Conn;
use acet\qcache\exception as QcEx;

class Qcache extends CacheInfo
{
    // log record columns
    const LOG_COLUMNS = 'time, microtime, status, hash';

    private array $qcache_config;
    private string $qcache_folder;
    private string $table_qc_cache_info;
    private bool $cache_info_to_db;
    private bool $log_to_db;
    private string $log_file;
    private string $table_qc_logs;
    private string $exclusions_file;
    private string $target_connection_sig;
    private int $query_status = Constants::QUERY_STATUS_NULL;

    /** @var mixed */
    private $db_connection_target;

    /** @var mixed */
    private $db_connection_cache;

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

        if (!($this->qcache_folder = $qcache_config['qcache_folder']) || !is_dir($this->qcache_folder)) {
            throw new QcEx\QcacheException("Option 'qcache_folder' must specify a valid directory");
        }

        // supply default values for missing config settings
        $this->qcache_config = array_merge([
            'enabled'                 => true,
            'auto_maintain'           => true,
            'gz_compression_level'    => Constants::DFLT_GZ_COMPRESSION_LEVEL,
            'cache_info_storage_type' => Constants::DFLT_CACHE_INFO_STORAGE_TYPE,
            'log_storage_type'        => Constants::DFLT_LOG_STORAGE_TYPE,
            'max_qcache_records'      => Constants::DFLT_MAX_QCACHE_TRANSACTIONS
        ], $qcache_config);

        $this->cache_info_to_db = $this->qcache_config['cache_info_storage_type'] == Constants::STORAGE_TYPE_DB;
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

        $this->exclusions_file = $this->qcache_folder .
            '/' . $target_connection_sig .
            '.' . Constants::EXCLUDE_STMT_FILE_EXT;

        $this->log_file = $this->qcache_folder . "/$target_connection_sig.log";

        parent::__construct(
            $this->cache_info_to_db,
            $this->table_qc_cache_info,
            $this->db_connection_cache,
            $this->target_connection_sig,
            $this->qcache_folder
        );
    }

    /** Getters */
    public function getCacheInfoToDb()              :bool     { return $this->cache_info_to_db; }
    public function getLoggingToDb()                :bool     { return $this->log_to_db; }
    public function getQcacheFolder()               :string   { return $this->qcache_folder; }
    public function getQcacheConfig()               :array    { return $this->qcache_config; }
    public function getTargetConnectionSignature()  :string   { return $this->target_connection_sig; }
    public function getDbConnectCache()                       { return $this->db_connection_cache; }
    public function getExclusionsFileName()         :string   { return $this->exclusions_file; }
    public function getTableQcCacheInfo()           :string   { return $this->table_qc_cache_info; }
    public function getQueryStatus()                :int      { return $this->query_status; }
    public function getTargetDbConnection()                   { return $this->db_connection_target; }

    /**
     * Performs a SQL query or gets a cached result set.
     * Returns SqlResultSet if the statement is appropriate and the query successful.
     * Returns the standard response from the query otherwise.
     *
     * @param string  $stmt
     * @param string[]|string  $tables  - array of table names, tables csv string, or empty (qcache will find them)
     * @param string  $description
     *
     * @return SqlResultSet|false
     */
    public function query(string $stmt, $tables = null, string $description = '')
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
     * Performs a SQL query or gets a cached result set.
     * Returns SqlResultSet if the statement is appropriate and the query successful.
     * Returns FALSE otherwise.
     *
     * @param string  $stmt
     * @param string[]|string  $tables  - array of table names, tables csv string, or empty (qcache will find them)
     * @param string $description
     *
     * @return SqlResultSet|false
     */
    private function performQuery(string $stmt, $tables, string $description)
    {
        if (!$this->qcache_config['enabled']) {
            $this->query_status = Constants::QUERY_STATUS_DISABLED;
            return false;
        }

        // Clean the statement and determine whether it's a SELECT
        if (!($stmt = QcacheUtils::getCleanedSelectStmt($stmt))) {
            return false;
        }

        $start_microtime = microtime(true);
        $time_now = time();
        $hash = hash('md5', $stmt);

        $gz_compression_level = $this->qcache_config['gz_compression_level'];

        $filename = $this->qcache_folder . '/' . $this->target_connection_sig . '#' . $hash;
        $cache_info_file = $filename . '.' . Constants::CACHE_INFO_FILE_EXT;

        $this->query_status = Constants::QUERY_STATUS_NULL;

        if ($cache_info = $this->getCacheInfoRecord($hash, $cache_info_file)) {
            $this->query_status = Constants::QUERY_STATUS_CACHE_HIT;
        }

        $cache_file = $filename . '.' . Constants::CACHE_FILE_EXT;

        // Determine whether this SQL statement has been seen before (a resultset will have been cached)

        if ($this->query_status == Constants::QUERY_STATUS_CACHE_HIT) {
            // This SELECT statement has been seen before

            [$access_time, $av_microsecs, $impressions, , $description, $tables_csv] = $cache_info;

            // determine whether cache is stale (tables have changed since last access time)
            $cache_is_stale = $this->db_connection_target->detectTableChanges($access_time, explode(',', $tables_csv), $this->db_connection_cache);

            if ($cache_is_stale) {
                $access_time = $time_now;

                // perform a fresh query (time it)
                $start_microtime = microtime(true);
                $resultset = $this->db_connection_target->read($stmt, true);
                $elapsed_microsecs = microtime(true) - $start_microtime;

                // compute average microsecs and importance
                $av_microsecs = (float)($elapsed_microsecs + $av_microsecs * $impressions++) / $impressions;
                $importance = self::computeCachePerformance($access_time, $av_microsecs, $impressions);

                $this->storeCacheInfoRecord(
                    'update',
                    $hash,
                    [$access_time, $av_microsecs, $impressions, $importance, $description],
                    $tables_csv,
                    $cache_info_file
                );

                // compress resultset and write it to the cache file
                FileIO::write($cache_file, $resultset, $gz_compression_level, true);

                $this->logTransactionStats($time_now, $elapsed_microsecs, 'stale', $hash);

                $this->query_status = Constants::QUERY_STATUS_CACHE_STALE;

                return $resultset;
            }

            // Cache is fresh
            $importance = self::computeCachePerformance($access_time, $av_microsecs, ++$impressions);

            $this->storeCacheInfoRecord(
                'update',
                $hash,
                [$access_time, $av_microsecs, $impressions, $importance, $description],
                $tables_csv,
                $cache_info_file
            );

            $this->logTransactionStats($time_now, microtime(true) - $start_microtime, 'hit', $hash);
            $this->query_status = Constants::QUERY_STATUS_CACHE_HIT;

            // read and decompress resultset from the cache file
            return FileIO::read($cache_file, true, true);
        }

        // Previously unseen SELECT statement

        if (!$tables) {
            // find table names within the statement
            if (($tables = QcacheUtils::findTableNames($stmt, $hash)) == false) {
                // no table names found
                $this->query_status = Constants::QUERY_STATUS_ERROR;
                return false;
            }
        }

        // Check whether the statement has been excluded
        if ($exclusions = FileIO::read($this->exclusions_file, false, true)) {
            foreach ($exclusions as [$excl_hash, $mode, $slen]) {
                if ($mode == Constants::EXCLUDE_QUERY_STARTING) {
                    if (strlen($stmt) >= $slen && hash('md5', substr($stmt, 0, $slen)) == $excl_hash) {
                        // the statement starts with an excluded string
                        $this->query_status = Constants::QUERY_STATUS_EXCLUDED;
                        return false;
                    }
                } elseif ($hash == $excl_hash) {
                    // the entire statement is excluded
                    $this->query_status = Constants::QUERY_STATUS_EXCLUDED;
                    return false;
                }
            }
        }

        // Every now and then, remove excessive cache assets
        if ($this->qcache_config['auto_maintain'] && !mt_rand(0, Constants::CLEAR_EXCESS_RND)) {
            (new QcacheMaint)->maintenance($this);
        }

        // Get the resultset - time it
        $start_microtime = microtime(true); // restart microsecond timer
        $resultset = $this->db_connection_target->read($stmt, true);
        $elapsed_microsecs = microtime(true) - $start_microtime;

        // Compress resultset and write it to the cache file
        FileIO::write($cache_file, $resultset, $gz_compression_level, true);

        // Write the SQL statement to file
        $stmt_file = $filename . '.' . Constants::STMT_FILE_EXT;
        FileIO::write($stmt_file, $stmt, 0, false);

        $tables_csv = is_array($tables) ? implode(',', $tables) : $tables;
        $importance = self::computeCachePerformance($time_now, $elapsed_microsecs, 1);

        $this->storeCacheInfoRecord(
            'insert',
            $hash,
            [$time_now, $elapsed_microsecs, 1, $importance, $description],
            $tables_csv,
            $cache_info_file
        );

        $this->logTransactionStats($time_now, $elapsed_microsecs, 'miss', $hash);
        $this->query_status = Constants::QUERY_STATUS_CACHE_MISS;

        return $resultset;
    }

    /**
     * Add a string to the exclude list to prevent performQuery from caching matching statements.
     *
     * When $mode = EXCLUDE_QUERY_WHOLE, identical new queries will be rejected.
     * When $mode = EXCLUDE_QUERY_STARTING, new queries that start with the excluded text are rejected.
     * For example, if an exclude statement is "SELECT name FROM contacts WHERE company =" then a subsequent call
     * to performQuery("SELECT name FROM contacts WHERE company = 'BG Advanced Software'") will return FALSE.
     *
     * @param string $stmt
     * @param int $mode
     */
    public function excludeQuery(string $stmt, int $mode = Constants::EXCLUDE_QUERY_WHOLE) :void
    {
        $stmt = trim($stmt);
        if (($slen = strlen($stmt)) == 0) {
            return;
        }

        $hash = hash('md5', $stmt);

        if ($excluded = FileIO::read($this->exclusions_file, false, true)) {
            // delete pre-existing record
            foreach ($excluded as $ix => $excl) {
                if ($excl[0] == $hash) {
                    unset($excluded[$ix]);
                    break;
                }
            }
        } else {
            $excluded = [];
        }

        $excluded[] = [$hash, $mode, $slen];

        FileIO::write(
            $this->exclusions_file,
            $excluded,
            0, true
        );
    }

    /**
     * Returns a suitable connector for the given connection details (MySQL, MsSQL and SQLite are currently supported).
     *
     * @param array     $qcache_config
     * @param string[]  $db_connection_data
     * @return Conn\DbConnectorIfc
     * @throws QcEx\ConnectionException
     */
    public static function getConnection(array $qcache_config, array $db_connection_data)
    {
        if (class_exists($class = '\acet\qcache\connector\DbConnector' . $db_connection_data['type'])) {
            return new $class($qcache_config, $db_connection_data);
        }

        throw new QcEx\ConnectionException("Unsupported database type - \"{$db_connection_data['type']}\"");
    }

    /**
     * Returns the names of all external tables.
     *
     * @return string[]
     */
    public function getTargetDbTableNames() :array
    {
        return $this->db_connection_target->getTableNames();
    }

    /**
     * Returns the names of all columns in the given external table.
     *
     * @param string $table
     * @return string[]
     */
    public function getTargetDbColumnNames(string $table) :array
    {
        return $this->db_connection_target->getColumnNames($table);
    }

    /**
     * Returns the PRIMARY KEY for the given external table.
     *
     * @param $table
     * @return string|string[]
     */
    public function getTargetDbPrimary(string $table)
    {
        return $this->db_connection_target->getPrimary($table);
    }

    /**
     * Returns the importance of a cache record, based on access time, query performance, and popularity.
     *
     * The importance of a cache is determined by checking how recently and how often the information is requested,
     * and how time-consuming the db operation is when compared to reading from cache.
     *
     *    a = access_time  - higher = more recent
     *    t = av_microsecs  - higher = more time costly
     *    i = impressions  - higher = more popular
     *
     * Each of the determining values are individually weighted to find the overall importance.
     *
     *    importance = (a * af) * (t * tf) * (i * if)
     *
     * @param int $access_time
     * @param float $av_microsecs
     * @param int $impressions
     * @return float
     */
    protected static function computeCachePerformance(int $access_time, float $av_microsecs, int $impressions) :float
    {
        return ($access_time  * Constants::AT_FACTOR) *
               ($av_microsecs * Constants::CA_FACTOR) *
               ($impressions  * Constants::IM_FACTOR);
    }

    /**
     * Logs transaction statistics.
     *
     * @param int $time
     * @param float $microtime
     * @param string $status
     * @param string $hash
     */
    private function logTransactionStats(int $time, float $microtime, string $status, string $hash) :void
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
