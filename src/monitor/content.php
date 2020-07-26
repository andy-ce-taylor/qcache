<?php

use acet\qcache\Constants;
use acet\qcache\JsonEncodedFileIO;

require __DIR__ . '\..\Constants.php';
require __DIR__ . '\..\JsonEncodedFileIO.php';

session_write_close();

$prev_file_mtime = $_GET['fmtime'];
$qcache_folder = $_GET['qcpath'];
$qcache_log_file = $qcache_folder . DIRECTORY_SEPARATOR . Constants::QCACHE_LOG_FILE_NAME;
$qcache_info_file = $qcache_folder . DIRECTORY_SEPARATOR . Constants::QCACHE_INFO_FILE_NAME;

$prev_max_log_recs = (int)$_GET['prevmaxlogs'];
$max_log_recs = (int)$_GET['maxlogs'];
$max_log_recs_changed = $max_log_recs != $prev_max_log_recs;

$content = 'waiting...';
$num_logs = -1;
$first_log_time = $total_saved_secs = $slowest_case_secs = 'n/a';

if (file_exists($qcache_log_file)) {

    $file_mtime = filemtime($qcache_log_file);

    if ($file_mtime != $prev_file_mtime || $max_log_recs_changed) {
        // load in the data (oldest>>>latest) and split into rows
        $ar = explode("\n", file_get_contents($qcache_log_file));
        // discard the last (empty) row
        array_pop($ar);

        $num_logs = count($ar);

        $qcache_stats_file = $qcache_folder . DIRECTORY_SEPARATOR . Constants::QCACHE_STATS_FILE_NAME;
        $first_log_time = $total_saved_ms = $slowest_case_ms = $slowest_case_sql = $slowest_case_time = 'n/a';
        if (file_exists($qcache_stats_file)) {
            $stats = JsonEncodedFileIO::readJsonEncodedArray($qcache_stats_file);
            $first_log_time = date('Y/m/d H:i s', $stats['first_log_time']);
            $total_saved_secs = number_format($stats['total_saved_ms'] / 1000, 5);
            $slowest_case_secs = number_format($stats['slowest_case']['ms'] / 1000, 5);
            $slowest_case_sql = $stats['slowest_case']['sql'];
            $slowest_case_time = $stats['slowest_case']['time'];
        }

        // get the latest n rows and reorder them: latest>>>oldest
        $latest = $max_log_recs ? array_slice($ar, -$max_log_recs) : $ar;
        $ar = array_reverse($latest);

        $datasrc = [];

        foreach ($ar as $row) {
            $parts = explode(',', $row);
            $datasrc[] = [
                'timestamp' => $parts[0],
                'access' => $parts[1] == 'db' ? 'db' : 'cached',
                'c_millisecs' => (float)$parts[2],
                'd_millisecs' => (float)$parts[3],
                'sql' => implode(',', array_slice($parts, 4))
            ];
        }

        $content =
            '<div class="t-table">' .
                '<div class="t-row t-head">' .
                    '<div class="t-col c1">Date & time</div>' .
                    '<div class="t-col c2">Type</div>' .
                    '<div class="t-col c3">Cache (ms)</div>' .
                    '<div class="t-col c3">DB (ms)</div>' .
                    '<div class="t-col c4">Query</div>' .
                '</div>';

        foreach ($datasrc as $ix => $data) {
            $row_css = $ix & 1 ? 'odd-row' : 'even-row';
            $row_css .= $data['access'] == 'db' ? ' db-hit' : ' cache-hit';

            $content .=     "<div class=\"t-row $row_css\">";
            $content .=     "<div class=\"t-col c1\">".date('Y/m/d H:i s', $data['timestamp']).'</div>';
            $content .=     "<div class=\"t-col c2\">".$data['access'].'</div>';
            if ($data['c_millisecs'] == 0.0) {
                $content .= "<div class=\"t-col c3\">-</div>";
            }
            else {
                $content .= "<div class=\"t-col c3\">".number_format($data['c_millisecs'], 3).'</div>';
            }
            $content .=     "<div class=\"t-col c3\">".number_format($data['d_millisecs'], 3).'</div>';
            $content .=     "<div class=\"t-col c4 sql\">".$data['sql'].'</div>';
            $content .= '</div>';
        }

        $file_mtime = filemtime($qcache_log_file);

        if ($max_log_recs_changed) {
            ++$file_mtime;
        }
    }
}
else {
    $file_mtime = -1;
}

echo json_encode([$file_mtime, $content, $num_logs, $first_log_time, $total_saved_secs, $slowest_case_secs]);
