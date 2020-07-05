<?php

session_write_close();

require_once __DIR__ . "/../../../reslock/src/ResLock.php";

use acet\reslock\ResLock;

define ('QCACHE_INFO_FILE_NAME', 'qcache_info.json');
define ('QCACHE_MISSES_FILE_NAME', 'qcache_misses.log');

$prev_file_mtime = $_GET['fmtime'];
$qcache_folder = $_GET['qcpath'];
$reslocks_folder = $qcache_folder.DIRECTORY_SEPARATOR . 'reslocks';
$qcache_misses_file = $qcache_folder . DIRECTORY_SEPARATOR . QCACHE_MISSES_FILE_NAME;
$qcache_info_file = $qcache_folder . DIRECTORY_SEPARATOR . QCACHE_INFO_FILE_NAME;

$max_log_recs = (int)$_GET['maxlogs'];

$file_mtime = $prev_file_mtime;
$content = 'waiting...';

if (file_exists($qcache_misses_file)) {

    $reslock = new ResLock($reslocks_folder);

    if (isset($_GET['clearlogs']) && $_GET['clearlogs'] == 1) {
        $rl_key = $reslock->lock($qcache_misses_file);
        {
            unlink($qcache_misses_file);
        }
        $reslock->unlock($rl_key);
        die(json_encode([-1, '']));
    }

    $file_mtime = filemtime($qcache_misses_file);

    if ($file_mtime != $prev_file_mtime) {

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

        $datasrc = [];

        foreach ($ar as $row) {
            $parts = explode(',', $row);
            $datasrc[] = [
                'access' => $parts[0] == 'd' ? 'db' : 'cached',
                'timestamp' => $parts[1],
                'c_millisecs' => (float)$parts[2],
                'd_millisecs' => (float)$parts[3],
                'sql' => implode(',', array_slice($parts, 4))
            ];
        }

        $content =
            '<div class="t-table">' .
                '<div class="t-row t-head">' .
                    '<div class="t-col c1">type</div>' .
                    '<div class="t-col c2">time</div>' .
                    '<div class="t-col c3">cached ms</div>' .
                    '<div class="t-col c3">db ms</div>' .
                    '<div class="t-col c4">sql</div>' .
                '</div>';

        foreach ($datasrc as $ix => $data) {
            $row_css = $ix & 1 ? 'odd-row' : 'even-row';
            $row_css .= $data['access'] == 'db' ? ' db-hit' : ' cache-hit';

            $content .=     "<div class=\"t-row $row_css\">";
            $content .=     "<div class=\"t-col c1\">".$data['access'].'</div>';
            $content .=     "<div class=\"t-col c2\">".$data['timestamp'].'</div>';
            $content .=     "<div class=\"t-col c3\">".number_format($data['c_millisecs'], 3).'</div>';
            $content .=     "<div class=\"t-col c3\">".number_format($data['d_millisecs'], 3).'</div>';
            $content .=     "<div class=\"t-col c4 sql\">".$data['sql'].'</div>';
            $content .= '</div>';
        }

        $file_mtime = filemtime($qcache_misses_file);

        $content .= "<a id=\"clear_log_btn\" class=\"button\" onclick='clearLog()'>Restart recording</a>";
    }
}
else {
    $file_mtime = -1;
}

echo json_encode([$file_mtime, $content]);