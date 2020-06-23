<?php
namespace acet\qcache;

class QCacheMonitor
{
    /**
     * QCacheMonitor
     *
     * @param string $qcache_folder         - path to the qcache folder (where cache files are stored)
     * @param int    $monitor_refresh_secs  - number of seconds between refreshes
     * @param int    $max_log_recs          - maximum number of log records to show
     */
    function __construct($qcache_folder, $monitor_refresh_secs=1, $max_log_recs=20)
    {
        $uri = str_replace('\\', '/', substr(__DIR__, strlen($_SERVER['DOCUMENT_ROOT'])));
        $qcache_folder = str_replace('\\', '/', $qcache_folder);
        $query_str = 'qcpath='.urlencode($qcache_folder) . "&rsecs=$monitor_refresh_secs" . "&maxlogs=$max_log_recs";
        header("Location: $uri/monitor/index.php?$query_str");
    }
}
