<?php
namespace acet\qcache;

use acet\reslock\ResLock;
use Exception;

class QCache extends QCacheUtils
{
    /**
     * @var string $qcache_info_file
     *
     *   QCache info is stored using a file name which is specific to the database type.
     *
     *   where 'index' is a hash of the db data, tables accessed and SQL string, such as '7b5afc25'.
     *
     *   ['index'] [
     *       'sql'                  - sql
     *       'tables'               - array of tables affected by this query
     *       'description'          - (optional)
     *       'db stats' => [        - stats for database responses
     *           'create time'          - unix timestamp created
     *           'access time'          - unix timestamp last accessed
     *           'impressions'          - number of times used
     *           'millisec av'          - millisecond execution time cumulative average (float)
     *       ]
     *       'cache stats' => [     - stats for cached responses
     *           'create time'          - unix timestamp created
     *           'access time'          - unix timestamp last accessed
     *           'impressions'          - number of times used
     *       ],
     *       'importance'           - see self::computeCachePerformance()
     *   ]
     *
     *   Individual cache file names are formed from their index hash (e.g. "#7b5afc25.dat").
     */
    private $qcache_info_file;

    /** @var string */
    private $qcache_stats_file;

    /** @var ResLock */
    private $reslock;

    /** @var bool */
    private $qcache_enabled;

    /** @var mixed */
    private $db_connection;

    /** string */
    private $qcache_folder;

    /** string */
    private $reslocks_folder;

    /** string */
    private $qcache_log_file;

    /** @var int */
    private $max_qcache_files_approx;

    /**
     * @param string  $db_type
     * @param string  $db_host
     * @param string  $db_user
     * @param string  $db_pass
     * @param string  $db_name
     * @param string  $qcache_folder
     * @param int     $max_qcache_files_approx
     * @param bool    $qcache_enabled
     * @throws QCacheConnectionException
     */
    function __construct($db_type, $db_host, $db_user, $db_pass, $db_name, $qcache_folder, $max_qcache_files_approx=1000, $qcache_enabled=true) {
        if (empty($db_type) || empty($db_host) || empty($db_user) || empty($db_pass) || empty($db_name) || empty($qcache_folder)) {
            throw new QCacheConnectionException("Missing database connection details");
        }

        $this->qcache_enabled = $qcache_enabled;
        $this->qcache_folder = str_replace(["\\", '/'], DIRECTORY_SEPARATOR, rtrim($qcache_folder, "\\ ./"));
        $this->qcache_info_file = $this->qcache_folder . DIRECTORY_SEPARATOR . Constants::QCACHE_INFO_FILE_NAME;
        $this->qcache_stats_file = $this->qcache_folder . DIRECTORY_SEPARATOR . Constants::QCACHE_STATS_FILE_NAME;
        $this->qcache_log_file = $this->qcache_folder . DIRECTORY_SEPARATOR . Constants::QCACHE_LOG_FILE_NAME;
        $this->reslocks_folder = $qcache_folder.DIRECTORY_SEPARATOR . 'reslocks';
        $this->max_qcache_files_approx = $max_qcache_files_approx;
        $this->reslock = new ResLock($this->reslocks_folder);
        $this->db_connection = $this->setConnection($db_type, $db_host, $db_user, $db_pass, $db_name);
        mt_srand($_SERVER['REQUEST_TIME']);
    }

    /**
     * @return DbConnectorMySQL|DbConnectorMSSQL
     */
    public function getDbConnection()
    {
        return $this->db_connection;
    }

    /**
     * Returns TRUE if Qcache is able to process the given SQL statement and is likely to significantly
     * improve performance, otherwise FALSE.
     *
     * @param string $sql
     * @return bool
     */
    public function cacheable($sql)
    {
        static $cacheable_l1c = [];

        $qc_key = hash("crc32b", $sql);

        if (array_key_exists($qc_key, $cacheable_l1c)) {
            return $cacheable_l1c[$qc_key];
        }

        $qstr_tmp_lc = strtolower(trim($sql));

        return $cacheable_l1c[$qc_key] = (                  // QCache can handle this if the statement...
            substr($qstr_tmp_lc, 0, 7) == 'select '     &&  // is a SELECT
            strpos($qstr_tmp_lc, ' from ', 7)           &&  // and has a FROM
            strpos($qstr_tmp_lc, ' join ', 7)           &&  // and has a JOIN
            ! strpos($qstr_tmp_lc, 'count(', 7)         &&  // but doesn't have a count (unsupported)
            ! strpos($qstr_tmp_lc, ' select ', 7)           // and doesn't have an embedded SELECT (unsupported)
        );
    }

    /**
     * @param string  $sql
     * @param mixed   $tables    array of table names, or a tables csv string, or null
     * @param string  $src_file
     * @param int     $src_line
     * @param string  $description
     *
     * @return SqlResultSet
     * @throws Exception
     */
    public function query($sql, $tables = null, $src_file = '', $src_line = 0, $description = '')
    {
        $sql = trim($sql);

        if (is_null($tables)) {
            if (($tables = self::getTables($sql)) == false) {
                throw new Exception("Bad SELECT statement");
            }
        }
        if (is_array($tables)) {
            $csv_tables = implode(',', $tables);
        }
        else { // string containing a comma-separated list of table names
            $csv_tables = $tables;
            $tables = explode(',', $csv_tables);
        }

        $qc_info = JsonEncodedFileIO::read($this->qcache_info_file) ?? [];

        $qc_key = hash("crc32b", $sql . $src_file . $src_line . $description);

        $cached_result_available = array_key_exists($qc_key, $qc_info);
        $use_cached_result = $this->qcache_enabled && $cached_result_available;
        $refresh_cache = true;

        $qc_data = $cached_result_available ? $qc_info[$qc_key] : [];

        if ($use_cached_result) {
            $last_access_time = $qc_data['db stats']['access time'];
            $changed_tables = $this->db_connection->getChangedTables($last_access_time, $tables);

            if (empty($changed_tables)) {
                // no tables have changed
                $refresh_cache = false;
            }
        }

        // get execution time (milliseconds) when regenerating cache
        $sql_millisecs = $refresh_cache ? $this->cachingProcessQuery($qc_key, $sql) : 0;

        $this->updateStats(
            $qc_data,
            $refresh_cache,
            $sql,
            $csv_tables,
            $sql_millisecs,
            $src_file,
            $src_line,
            $description
        );

        $start_nanosecs = hrtime(true);
        {
            // form a result set from the cache file
            $resultset = new SqlResultSet(unserialize(file_get_contents(self::getHashFileName($this->qcache_folder, $qc_key))));
        }
        $cache_millisecs = (hrtime(true) - $start_nanosecs) / 1000000;

        self::computeCachePerformance($qc_data);

        $rl_key = $this->reslock->lock($this->qcache_info_file);
        {
            $qc_info[$qc_key] = $qc_data;

            if (!mt_rand(0, Constants::CLEAR_EXCESS_RND)) {
                // every now and then, sort the caches and remove the less important ones
                self::sortCachesByImportance($qc_info);
                self::removeExcessCacheFiles($qc_info, $this->qcache_folder, $this->max_qcache_files_approx);
            }

            JsonEncodedFileIO::write($this->qcache_info_file, $qc_info);
        }
        $this->reslock->unlock($rl_key);

        $this->updateLogsAndStats($refresh_cache, $qc_data, $cache_millisecs, $sql_millisecs, $sql);

        return $resultset;
    }

    /**
     * @param bool  $refresh_cache
     * @param array $qc_data
     * @param $cache_millisecs
     * @param $sql_millisecs
     * @param $sql
     */
    private function updateLogsAndStats($refresh_cache, $qc_data, $cache_millisecs, $sql_millisecs, $sql)
    {
        $rl_key = $this->reslock->lock($this->qcache_log_file);
        {
            $time_now = time();

            if ($refresh_cache) { // db hit
                $rec = "$time_now,db,0,$sql_millisecs,$sql\n";
            }
            else {
                $millisec_av = $qc_data['db stats']['millisec av'];
                $rec = "$time_now,qc,$cache_millisecs,$millisec_av,$sql\n";
            }

            $logs = [];
            if (file_exists($this->qcache_log_file)) {
                $logs = explode("\n", file_get_contents($this->qcache_log_file));
                array_pop($logs);
            }
            $logs[] = $rec;

            if (count($logs) >= Constants::MAX_LOG_RECORDS) {
                $logs = array_slice($logs, -Constants::MAX_LOG_RECORDS);
            }

            file_put_contents($this->qcache_log_file, implode("\n", $logs));


            if ($qcache_stats = JsonEncodedFileIO::read($this->qcache_stats_file)) {
                $ms_diff = $millisec_av - $cache_millisecs;
                $qcache_stats['total_saved_ms'] += $ms_diff;
                if ($ms_diff > $qcache_stats['slowest_case']['ms']) {
                    $qcache_stats['slowest_case']['ms']   = $ms_diff;
                    $qcache_stats['slowest_case']['sql']  = $sql;
                    $qcache_stats['slowest_case']['time'] = $time_now;
                }
            }
            else {
                // build the qcache stats file
                $first_log_time = $time_now;
                $total_saved_ms = 0.0;
                $sc_ms = 0;
                $sc_sql = '';
                $sc_time = 0;

                if ($logs = explode("\n", file_get_contents($this->qcache_log_file))) {
                    foreach ($logs as $log) {
                        [$time, $mode, $cache_millisecs, $millisec_av, $sql] = explode(',', $log);
                        if ($mode == 'qc') {
                            $ms_diff = $millisec_av - $cache_millisecs;
                            $total_saved_ms += $ms_diff;
                            if ($ms_diff > $sc_ms) {
                                $sc_ms = $ms_diff;
                                $sc_sql = $sql;
                                $sc_time = $time;
                            }
                        }
                    }
                }

                $qcache_stats = [
                    'first_log_time' => $first_log_time,
                    'total_saved_ms' => $total_saved_ms,
                    'slowest_case' => [
                        'ms'   => $sc_ms,
                        'sql'  => $sc_sql,
                        'time' => $sc_time
                    ]
                ];
            }

            JsonEncodedFileIO::write($this->qcache_stats_file, $qcache_stats);
        }
        $this->reslock->unlock($rl_key);
    }

    /**
     * Refreshes any caches that need updating.
     *
     * NOT YET IMPLEMENTED
     *
     * @param int  $max_runtime_millisecs_approx    - abort if accumulated run-time reaches $max_runtime milliseconds
     */
    public function refreshCaches($max_runtime_millisecs_approx)
    {
########################################################################################################################
        return;
########################################################################################################################
        $rl_key = $this->reslock->lock($this->qcache_info_file);
        {
            $qc_info = JsonEncodedFileIO::read($this->qcache_info_file) ?? [];

            $max_runtime_microsecs = (float)$max_runtime_millisecs_approx / 1000;
            $start_time = microtime(true);

            self::removeExcessCacheFiles($qc_info, $this->qcache_folder, $this->max_qcache_files_approx);

            if (!empty($qc_info)) {
                do {
                    foreach ($qc_info as $qc_key => $info) {
                        $last_access_time = $qc_info[$qc_key]['db stats']['access time'];
                        $csv_tables = $qc_info[$qc_key]['tables'];
                        $changed_tables = $this->db_connection->getChangedTables(
                            $last_access_time,
                            explode(',', $csv_tables)
                        );

                        if (!empty($changed_tables)) {
                            $sql = $qc_info[$qc_key]['sql'];

                            // get cache regeneration execution time (milliseconds)
                            $millisecs = $this->cachingProcessQuery($qc_key, $sql);

                            $this->updateStats($qc_info[$qc_key], true, $sql, $csv_tables, $millisecs);
                        }

                        if (microtime(true) - $start_time >= $max_runtime_microsecs) {
                            break;
                        }
                    }
                } while (true);
            }

            JsonEncodedFileIO::write($this->qcache_info_file, $qc_info);
        }
        $this->reslock->unlock($rl_key);
    }

    /**
     * @param array   &$info
     * @param bool     $query_processed - whether the sql query was processed
     * @param string   $sql
     * @param string   $csv_tables
     * @param float    $exe_millisecs
     * @param string   $src_file
     * @param int      $src_line
     * @param string   $description
     */
    private function updateStats(
       &$info,
        $query_processed,
        $sql,
        $csv_tables,
        $exe_millisecs,
        $src_file=null,
        $src_line=null,
        $description=null
    ) {
        $cache_mode = $query_processed ? 'db stats' : 'cache stats';

        $time_now = time();

        if (!$info) { // cache doesn't exist

            $src_location = '';

            if ($src_file) {
                $src_location = "File: $src_file";
                if ($src_line) {
                    $src_location .= " ($src_line)";
                }
            }

            // create a new cache info record
            $info = [
                'sql' => $sql,                  // sql
                'tables' => $csv_tables            // csv of tables affected by this query
            ];

            if ($src_location) {
                $info['src location'] = $src_location;
            }

            if ($description) {
                $info['description'] = $description;
            }

            // create the default info record (cached)
            $info['db stats'] = [
                'create time' => 0,                     // unix timestamp created
                'access time' => 0,                     // unix timestamp last accessed
                'impressions' => 0                      // number of times used
            ];

            $info['cache stats'] = $info['db stats'];

            $info['db stats']['millisec av'] = $exe_millisecs;  // execution time cumulative average

            // whichever stats set is being used, update it's usage stats
            $usage_stats = &$info[$cache_mode];

            $usage_stats['create time'] = $time_now;
            $usage_stats['access time'] = $time_now;
            $usage_stats['impressions'] = 1;

            return;
        }

        // refresh cache info for the current cache mode
        $usage_stats = &$info[$cache_mode];

        if (!$usage_stats['create time']) {
            $usage_stats['create time'] = $time_now;
        }

        $usage_stats['access time'] = $time_now;

        if ($cache_mode == 'cache stats') {
            $usage_stats['impressions']++;

            return;
        }

        $num_impressions = $usage_stats['impressions'];
        $av_millisecs = $usage_stats['millisec av'];

        // recalculate the execution time cumulative average
        $millisecs = (float)($exe_millisecs + $av_millisecs * $num_impressions++) / $num_impressions;

        $usage_stats['impressions'] = $num_impressions;
        $usage_stats['millisec av'] = $millisecs;
    }

    /**
     * Process the given query and caches the result.
     * Returns the elapsed millisecond time;
     *
     * @param string  $qc_key
     * @param string  $sql
     * @return float
     */
    private function cachingProcessQuery($qc_key, $sql)
    {
        $start_nanosecs = hrtime(true);
        {
            $data = $this->db_connection->processQuery($sql);
        }
        $elapsed_millisecs = (hrtime(true) - $start_nanosecs) / 1000000;

        $file = self::getHashFileName($this->qcache_folder, $qc_key);

        $rl_key = $this->reslock->lock($file);
        {
            file_put_contents($file, serialize($data));
        }
        $this->reslock->unlock($rl_key);

        return $elapsed_millisecs;
    }

    /**
     * @param string  $db_type
     * @param string  $db_host
     * @param string  $db_user
     * @param string  $db_pass
     * @param string  $db_name
     * @return DbConnectorMySQL|DbConnectorMSSQL
     * @throws QCacheConnectionException
     */
    private function setConnection($db_type, $db_host, $db_user, $db_pass, $db_name)
    {
        switch ($db_type) {
            case 'mysql':
                return new DbConnectorMySQL($db_host, $db_user, $db_pass, $db_name);

            case 'mssql':
                $file = $this->qcache_folder.DIRECTORY_SEPARATOR.Constants::MSSQL_TABLES_INFO_FILE_NAME;

                return new DbConnectorMSSQL($db_host, $db_user, $db_pass, $db_name, $file, $this->reslock);
        }

        // whoops!
        return null;
    }
}