<?php

session_write_close();

require_once __DIR__ . "/../../../reslock/src/ResLock.php";

use acet\reslock\ResLock;

define ('QCACHE_INFO_FILE_NAME', 'qcache_info.json');
define ('QCACHE_MISSES_FILE_NAME', 'qcache_misses.log');

$qcache_folder = $_GET['qcpath'];
$reslocks_folder = $qcache_folder.DIRECTORY_SEPARATOR . 'reslocks';
$qcache_misses_file = $qcache_folder . DIRECTORY_SEPARATOR . QCACHE_MISSES_FILE_NAME;
$qcache_info_file = $qcache_folder . DIRECTORY_SEPARATOR . QCACHE_INFO_FILE_NAME;

$monitor_refresh_secs = (int)$_GET['rsecs'];
$max_log_recs = (int)$_GET['maxlogs'];

$datasrc = [];

if (file_exists($qcache_misses_file)) {

    $reslock = new ResLock($reslocks_folder);

    if (isset($_GET['clear_logs'])) {
        $rl_key = $reslock->lock($qcache_misses_file);
        {
            unlink($qcache_misses_file);
        }
        $reslock->unlock($rl_key);
        $parsed = parse_url($_SERVER['REQUEST_URI']);
        $query = $parsed['query'];
        parse_str($query, $params);
        unset($params['clear_logs']);
        $url = $parsed['path'] . '?' . http_build_query($params);
        header("Location: $url");
    }

    // load in the data (oldest>>>latest) and split into rows
    $ar = explode("\n", file_get_contents($qcache_misses_file));
    // remove the last (empty) row
    array_pop($ar);

    // get the latest n rows
    $latest = array_slice($ar, -$max_log_recs);

    if (count($ar) >= $max_log_recs * 2) {

        // store the remaining (max n, newest) rows
        $rl_key = $reslock->lock($qcache_misses_file);
        {
            file_put_contents($qcache_misses_file, implode("\n", $latest)."\n");
        }
        $reslock->unlock($rl_key);
    }

    // reorder: latest>>>oldest
    $ar = array_reverse($latest);

    foreach ($ar as $row) {
        $parts = explode(',', $row);
        $datasrc[] = [
            'hit'       => $parts[0] == 'd' ? 'db' : 'cache',
            'timestamp' => $parts[1],
            'millisecs' => (float)$parts[2],
            'sql'       => implode(',', array_slice($parts, 3))
        ];
    }

    $clear_logs_link = $_SERVER['REQUEST_URI'] . "&clear_logs";
}

require_once __DIR__ . '/view.php';
