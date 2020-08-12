<?php
namespace acet\qcache;

class QCacheMonitor
{
    const CONTROL_OPTIONS_MAX_LOGS = [
        1, 10, 20, 30, 40, 50, 100, 200, 500
    ];

    /**
     * QCacheMonitor
     *
     * @param mixed   $conn                  - database connection
     * @param int     $monitor_refresh_secs  - number of seconds between auto refreshes
     * @param int     $max_log_recs          - maximum number of log records to show
     */
    function __construct($conn, $monitor_refresh_secs=4, $max_log_recs=20)
    {
        $uri = str_replace('\\', '/', substr(__DIR__, strlen($_SERVER['DOCUMENT_ROOT'])));

        $max_log_options = self::CONTROL_OPTIONS_MAX_LOGS;

        $max_log_recs = (int)$max_log_recs;

        if ($max_log_recs < 1)
            $max_log_recs = 1;

        if (!in_array($max_log_recs, $max_log_options))
            $max_log_options[] = $max_log_recs;

        sort($max_log_options);

        $query_str =
            'conn='         . $conn .
            '&optsmaxlogs=' . implode(',', $max_log_options) .
            '&rsecs='       . $monitor_refresh_secs .
            '&maxlogs='     . $max_log_recs;

return; // ToDo: $conn is no good. I'll deal with this later

        header("Location: $uri/monitor/view.php?$query_str");
    }
}
