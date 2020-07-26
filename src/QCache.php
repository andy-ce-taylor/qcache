<?php
namespace acet\qcache;

use acet\reslock\ResLock;
use Exception;

class QCache
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
     *       'importance'           - see QCache::computeCachePerformance()
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
     * @param string           $sql
     * @param string|string[]  $tables    array of table names or a tables csv string
     * @param string           $src_file
     * @param int              $src_line
     * @param string           $description
     *
     * @return SqlResultSet
     * @throws Exception
     */
    public function query($sql, $tables, $src_file = '', $src_line = 0, $description = '')
    {
        $sql = trim($sql);

        if (is_array($tables)) {
            $csv_tables = implode(',', $tables);
        }
        else { // string containing a comma-separated list of table names
            $csv_tables = $tables;
            $tables = explode(',', $csv_tables);
        }

        $qc_info = JsonEncodedFileIO::readJsonEncodedArray($this->qcache_info_file);

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

        $this->computeCachePerformance($qc_data);

        $rl_key = $this->reslock->lock($this->qcache_info_file);
        {
            $qc_info = JsonEncodedFileIO::readJsonEncodedArray($this->qcache_info_file);
            $qc_info[$qc_key] = $qc_data;

            if (!mt_rand(0, Constants::CLEAR_EXCESS_RND)) {
                // every now and then, sort the caches and remove the less important ones
                self::sortCachesByImportance($qc_info);
                self::removeExcessCacheFiles($qc_info, $this->qcache_folder, $this->max_qcache_files_approx);
            }

            JsonEncodedFileIO::writeJsonEncodedArray($this->qcache_info_file, $qc_info);
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

            file_put_contents($this->qcache_log_file, $rec, FILE_APPEND);

            $ms_diff = $millisec_av - $cache_millisecs;

            if ($qcache_stats = JsonEncodedFileIO::readJsonEncodedArray($this->qcache_stats_file)) {
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

            JsonEncodedFileIO::writeJsonEncodedArray($this->qcache_stats_file, $qcache_stats);
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
            $qc_info = JsonEncodedFileIO::readJsonEncodedArray($this->qcache_info_file);

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

            JsonEncodedFileIO::writeJsonEncodedArray($this->qcache_info_file, $qc_info);
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
     * Calculates the importance of the cache file, based on access time, popularity and query performance.
     *
     * The importance of a cache is determined by checking how recently and how often the information is requested,
     * and how time consuming the db operation is.
     *
     *    a = access_time  - higher = more recent
     *    i = impressions  - higher = more popular
     *    t = millisec_av  - higher = more time costly
     *
     * Each of the determining values are individually weighted to find the overall importance.
     *
     *    importance = (a * af) * (i * if) * (t * tf)
     *
     * @param array  & $info
     */
    private function computeCachePerformance(&$info)
    {
        $a = max($info['cache stats']['access time'], $info['db stats']['access time']);
        $i = $info['cache stats']['impressions'] + $info['db stats']['impressions'];
        $t = $info['db stats']['millisec av'];

        $info['importance'] = ($a * Constants::AT_FACTOR) * ($i * Constants::IM_FACTOR) * ($t * Constants::CA_FACTOR);
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

    /**
     * @param string  $qc_key
     * @return string
     */
    private static function getHashFileName($qcache_folder, $qc_key)
    {
        return $qcache_folder . DIRECTORY_SEPARATOR . '#' . $qc_key . '.dat';
    }

    /**
     * Sorts $qc_info into descending importance order (most important will be first).
     *
     * Primarily used when deciding which caches to remove during regular housekeeping.
     *
     * @param array  & $qc_info
     */
    private static function sortCachesByImportance(&$qc_info)
    {
        uasort(
            $qc_info,
            function ($a, $b) {
                $diff = $b['importance'] - $a['importance'];

                return $diff < 0 ? -1 : ($diff > 0 ? 1 : 0);
            }
        );
    }

    /**
     * Removes excessive entries in $qc_info together with their associated cache files.
     *
     * @param array  & $qc_info
     * @param string   $qcache_folder
     * @param int      $max_qcache_files_approx
     */
    private static function removeExcessCacheFiles(&$qc_info, $qcache_folder, $max_qcache_files_approx)
    {
        if (($num_files_to_remove = count($qc_info) - $max_qcache_files_approx) > 0) {

            $obsolete_elems = array_slice($qc_info, $max_qcache_files_approx, null, true);

            foreach ($obsolete_elems as $qc_key => $qinfo) {
                unlink(self::getHashFileName($qcache_folder, $qc_key));
            }

            $qc_info = array_slice($qc_info, 0, $max_qcache_files_approx, true);
        }
    }

    /**
     * Removes cache files.
     *
     * @param string  $qcache_folder
     */
    public static function clearCacheFiles($qcache_folder)
    {
        $qcache_folder      = str_replace(["\\", '/'], DIRECTORY_SEPARATOR, rtrim($qcache_folder, "\\ ./"));
        $qcache_info_file   = $qcache_folder.DIRECTORY_SEPARATOR.Constants::QCACHE_INFO_FILE_NAME;
        $qcache_log_file = $qcache_folder.DIRECTORY_SEPARATOR.Constants::QCACHE_LOG_FILE_NAME;
        $reslocks_folder    = $qcache_folder.DIRECTORY_SEPARATOR.'reslocks';

        $reslock = new ResLock($reslocks_folder);

        $rl_key = $reslock->lock($qcache_info_file);
        {
            $qc_info = JsonEncodedFileIO::readJsonEncodedArray($qcache_info_file);

            foreach (array_keys($qc_info) as $qc_key) {
                $cache_file = self::getHashFileName($qcache_folder, $qc_key);
                if (file_exists($cache_file)) {
                    unlink($cache_file);
                }
            }

            if (file_exists($qcache_info_file)) {
                unlink($qcache_info_file);
            }
        }
        $reslock->unlock($rl_key);

        $rl_key = $reslock->lock($qcache_log_file);
        {
            if (file_exists($qcache_log_file)) {
                unlink($qcache_log_file);
            }
        }
        $reslock->unlock($rl_key);

        if (file_exists($reslocks_folder)) {
            self::rmdir_plus($reslocks_folder, false);
        }
    }

    /**
     * Returns an array containing the name and full path of the Qcache info file.
     *
     * @param string  $qcache_folder
     * @param int     $max_qcache_files_approx
     * @return string[]
     */
    public static function getQCacheInfoFile($qcache_folder, $max_qcache_files_approx=1000)
    {
        $qcache_folder    = str_replace(["\\", '/'], DIRECTORY_SEPARATOR, rtrim($qcache_folder, "\\ ./"));
        $qcache_info_file = $qcache_folder.DIRECTORY_SEPARATOR . Constants::QCACHE_INFO_FILE_NAME;
        $reslocks_folder  = $qcache_folder.DIRECTORY_SEPARATOR . 'reslocks';

        $reslock = new ResLock($reslocks_folder);

        $rl_key = $reslock->lock($qcache_info_file);
        {
            $qc_info = JsonEncodedFileIO::readJsonEncodedArray($qcache_info_file);

            self::sortCachesByImportance($qc_info);
            self::removeExcessCacheFiles($qc_info, $qcache_folder, $max_qcache_files_approx);

            JsonEncodedFileIO::writeJsonEncodedArray($qcache_info_file, $qc_info);
        }
        $reslock->unlock($rl_key);

        return [Constants::QCACHE_INFO_FILE_NAME, $qcache_info_file];
    }

    /**
     * Recursively delete sub-folders and their contents.
     * Also delete the given top folder if $delete_topdir is set.
     *
     * @param string $dir
     * @param bool $delete_topdir
     */
    private static function rmdir_plus($dir, $delete_topdir=true)
    {
        if (!file_exists($dir)) {
            return;
        }

        foreach (array_diff(scandir($dir), ['.', '..']) as $file) {
            is_dir($path = $dir.DIRECTORY_SEPARATOR.$file) ? self::rmdir_plus($path) : unlink($path);
        }

        if ($delete_topdir) {
            rmdir($dir);
        }
    }
}