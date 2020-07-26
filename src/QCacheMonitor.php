<?php
namespace acet\qcache;

class QCacheMonitor
{
    const CONTROL_OPTIONS_MAX_LOGS = [
        10, 20, 30, 40, 50, 100, 200, 500, 1000
    ];

    /**
     * QCacheMonitor
     *
     * @param string      $qcache_folder         - path to the qcache folder (where cache files are stored)
     * @param int         $monitor_refresh_secs  - number of seconds between refreshes
     * @param int|string  $max_log_recs          - maximum number of log records to show, or 'all'
     */
    function __construct($qcache_folder, $monitor_refresh_secs=1, $max_log_recs=20)
    {
        $uri = str_replace('\\', '/', substr(__DIR__, strlen($_SERVER['DOCUMENT_ROOT'])));
        $qcache_folder = str_replace('\\', '/', $qcache_folder);

        $max_log_options = self::CONTROL_OPTIONS_MAX_LOGS;

        $max_log_recs = (int)$max_log_recs;

        if ($max_log_recs != 'all') {
            if (!in_array($max_log_recs, $max_log_options)) {
                $max_log_options[] = $max_log_recs;
            }
            sort($max_log_options);
        }

        $max_log_options[] = 'All';

        $query_str =
            'optsmaxlogs=' . implode(',', $max_log_options) .
            '&qcpath='     . urlencode($qcache_folder) .
            '&rsecs='      . $monitor_refresh_secs .
            '&maxlogs='    . $max_log_recs;

        header("Location: $uri/monitor/view.php?$query_str");
    }
}
