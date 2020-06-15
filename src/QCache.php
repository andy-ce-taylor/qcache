<?php
namespace acet\qcache;

use acet\reslock\ResLock;
use Exception;

class QCache
{
    const QCACHE_INFO_FILE_NAME = 'qcache_info.json';
    const MSSQL_TABLES_INFO_FILE_NAME = 'mssql_tables_info.json';

    // Weightings are used when computing cache importance
    const AT_FACTOR = 1.0;          // access times (may need recency amplification (sines) rather than straight-line factors)
    const IM_FACTOR = 0.5;          // Impressions
    const CA_FACTOR = 3.5;          // Cumulative average (time cost)

    /**
     * @var string $qcache_info_file
     *
     *   QCache info is stored using a file name which is specific to the database type.
     *
     *   where 'index' is a hash of the db data, tables accessed and SQL string, such as 87224317.
     *
     *   ['index'] [
     *       'sql'                  - sql
     *       'tables'               - array of tables affected by this query
     *       'description'          - (optional)
     *       'db stats' => [        - stats for database responses
     *           'create time'          - unix timestamp created
     *           'access time'          - unix timestamp last accessed
     *           'impressions'          - number of times used
     *           'microsec av'          - execution time cumulative average (float)
     *       ]
     *       'cache stats' => [     - stats for cached responses
     *           'create time'          - unix timestamp created
     *           'access time'          - unix timestamp last accessed
     *           'impressions'          - number of times used
     *       ]
     *   ]
     *
     *   The actual cache file name is formed from the connector type and index hash (such as "clarodb_87224317.dat").
     */
    private $qcache_info_file;

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

    /** @var int */
    private $max_qcache_files;

    /**
     * @param string $db_type
     * @param string $db_host
     * @param string $db_user
     * @param string $db_pass
     * @param string $db_name
     * @param string $qcache_folder
     * @param int $max_qcache_files
     * @param bool $qcache_enabled
     * @throws QCacheConnectionException
     */
    function __construct($db_type, $db_host, $db_user, $db_pass, $db_name, $qcache_folder, $max_qcache_files = 1000, $qcache_enabled = true) {
        if (empty($db_type) || empty($db_host) || empty($db_user) || empty($db_pass) || empty($db_name) || empty($qcache_folder)) {
            throw new QCacheConnectionException("Missing database connection details");
        }

        $this->qcache_enabled = $qcache_enabled;
        $this->qcache_folder = str_replace(["\\", '/'], DIRECTORY_SEPARATOR, rtrim($qcache_folder, "\\ ./"));
        $this->qcache_info_file = $this->qcache_folder.DIRECTORY_SEPARATOR.self::QCACHE_INFO_FILE_NAME;
        $this->reslocks_folder = $qcache_folder.DIRECTORY_SEPARATOR.'reslocks';
        $this->max_qcache_files = $max_qcache_files;
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
     * @param string $sql
     * @param string[] $tables
     * @param string $src_file
     * @param int $src_line
     * @param string $description
     *
     * @return SqlResultSet
     * @throws Exception
     */
    public function query($sql, $tables, $src_file = '', $src_line = 0, $description = '')
    {
        $sql = trim($sql);

        $csv_tables = implode(',', $tables);

        $qc_key = hash("crc32b", $sql.$src_file.$src_line.$description);

        $qc_info = SerializedFileIO::readSerializedArray($this->qcache_info_file);

        $cached_result_available = $this->qcache_enabled && array_key_exists($qc_key, $qc_info);
        $refresh_cache = true;

        if ($cached_result_available) {
            $last_access_time = $qc_info[$qc_key]['db stats']['access time'];

            $changed_tables = $this->db_connection->getChangedTables($last_access_time, $tables);

            if (empty($changed_tables)) {
                // no tables have changed
                $refresh_cache = false;
            }
        }
        else {
            $qc_info[$qc_key] = [];
        }

        // get hi-res execution time when regenerating cache
        $microsecs = $refresh_cache ? $this->cachingProcessQuery($qc_key, $sql) : 0;

        $this->updateStats(
            $qc_info[$qc_key],
            $cached_result_available,
            $refresh_cache,
            $sql,
            $csv_tables,
            $microsecs,
            $src_file,
            $src_line,
            $description
        );

        // get data from the cache file
        $data = unserialize(file_get_contents(self::getHashFileName($this->qcache_folder, $qc_key)));

        $this->computeCachePerformance($qc_info[$qc_key]);

        $rl_key = $this->reslock->lock($this->qcache_info_file);
        {
            if (mt_rand(0, 10) == 0) {
                $this->sortCachesByImportance($qc_info);
                $this->removeExcessCacheFiles($qc_info);
            }

            SerializedFileIO::writeSerializedArray($this->qcache_info_file, $qc_info);
        }
        $this->reslock->unlock($rl_key);

        return new SqlResultSet($data);
    }

    /**
     * @param array  & $info
     * @param bool $cache_exists
     * @param bool $query_processed - whether the sql query was processed
     * @param string $sql
     * @param string $csv_tables
     * @param float $exe_microsecs
     * @param string $src_file
     * @param int $src_line
     * @param string $description
     */
    private function updateStats(
        &$info,
        $cache_exists,
        $query_processed,
        $sql,
        $csv_tables,
        $exe_microsecs,
        $src_file,
        $src_line,
        $description
    ) {
        $cache_mode = $query_processed ? 'db stats' : 'cache stats';

        $time_now = time();

        if (!$cache_exists) {

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

            $info['db stats']['microsec av'] = $exe_microsecs;  // execution time cumulative average

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
        $av_microsecs = $usage_stats['microsec av'];

        // recalculate the execution time cumulative average
        $microsecs = (float)($exe_microsecs + $av_microsecs * $num_impressions++) / $num_impressions;

        $usage_stats['impressions'] = $num_impressions;
        $usage_stats['microsec av'] = $microsecs;
    }

    /**
     * Calculates the importance of the cache file, based on access time, popularity and query performance.
     *
     * The importance of a cache is determined by checking how recently and how often the information is requested,
     * and how time consuming the db operation is.
     *
     *    a = access_time  - higher = more recent
     *    i = impressions  - higher = more popular
     *    t = microsec_av  - higher = more time costly
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
        $t = $info['db stats']['microsec av'];

        $info['importance'] = ($a * self::AT_FACTOR) * ($i * self::IM_FACTOR) * ($t * self::CA_FACTOR);
    }

    /**
     * Sorts $qc_info into descending importance order (most important will be first).
     *
     * Primarily used when deciding which caches to remove during regular housekeeping.
     *
     * @param array  & $qc_info
     */
    private function sortCachesByImportance(&$qc_info)
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
     * Removes cache files.
     * @param string  $qcache_folder
     */
    public static function clearCacheFiles($qcache_folder)
    {
        $qcache_folder    = str_replace(["\\", '/'], DIRECTORY_SEPARATOR, rtrim($qcache_folder, "\\ ./"));
        $qcache_info_file = $qcache_folder.DIRECTORY_SEPARATOR.self::QCACHE_INFO_FILE_NAME;
        $reslocks_folder  = $qcache_folder.DIRECTORY_SEPARATOR.'reslocks';

        $reslock = new ResLock($reslocks_folder);

        $rl_key = $reslock->lock($qcache_info_file);
        {
            $qc_info = SerializedFileIO::readSerializedArray($qcache_info_file);

            foreach (array_keys($qc_info) as $qc_key) {
                $cache_file = self::getHashFileName($qcache_folder, $qc_key);
                unlink($cache_file);
            }

            if (file_exists($qcache_info_file)) {
                unlink($qcache_info_file);
            }
        }
        $reslock->unlock($rl_key);
    }

    /**
     * Removes excessive entries in $qc_info together with their associated cache files.
     *
     * @param array  & $qc_info
     */
    private function removeExcessCacheFiles(&$qc_info)
    {
        if (($num_files_to_remove = count($qc_info) - $this->max_qcache_files) > 0) {

            $obsolete_elems = array_slice($qc_info, $this->max_qcache_files, null, true);

            foreach ($obsolete_elems as $qc_key => $qinfo) {
                unlink(self::getHashFileName($this->qcache_folder, $qc_key));
            }

            $qc_info = array_slice($qc_info, 0, $this->max_qcache_files, true);
        }
    }

    /**
     * Process the given query and caches the result.
     * Returns the elapsed microsecond time;
     *
     * @param string $qc_key
     * @param string $sql
     * @return float
     */
    private function cachingProcessQuery($qc_key, $sql)
    {
        $start_ns = hrtime(true);
        {
            $data = $this->db_connection->processQuery($sql);
        }
        $elapsed_microsecs = (hrtime(true) - $start_ns) / 1000000;

        $file = self::getHashFileName($this->qcache_folder, $qc_key);

        $rl_key = $this->reslock->lock($file);
        {
            file_put_contents($file, serialize($data));
        }
        $this->reslock->unlock($rl_key);

        return $elapsed_microsecs;
    }

    /**
     * @param string $db_type
     * @param string $db_host
     * @param string $db_user
     * @param string $db_pass
     * @param string $db_name
     * @return DbConnectorMySQL|DbConnectorMSSQL
     * @throws QCacheConnectionException
     */
    private function setConnection($db_type, $db_host, $db_user, $db_pass, $db_name)
    {
        switch ($db_type) {
            case 'mysql':
                return new DbConnectorMySQL($db_host, $db_user, $db_pass, $db_name);

            case 'mssql':
                $file = $this->qcache_folder.DIRECTORY_SEPARATOR.self::MSSQL_TABLES_INFO_FILE_NAME;

                return new DbConnectorMSSQL($db_host, $db_user, $db_pass, $db_name, $file, $this->reslock);
        }

        // whoops!
        return null;
    }

    /**
     * @param string $qc_key
     * @return string
     */
    private static function getHashFileName($qcache_folder, $qc_key)
    {
        return $qcache_folder . DIRECTORY_SEPARATOR . '#' . $qc_key . '.dat';
    }

    public function refreshCaches()
    {
        // ToDo: This can refresh the top (say) 10% of caches

    }
}