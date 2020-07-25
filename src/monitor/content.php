<?php

session_write_close();

require_once __DIR__ . "/../../../reslock/src/ResLock.php";

define ('QCACHE_INFO_FILE_NAME', 'qcache_info.json');
define ('QCACHE_LOG_FILE_NAME', 'qcache.log');

$prev_file_mtime = $_GET['fmtime'];
$qcache_folder = $_GET['qcpath'];
$qcache_log_file = $qcache_folder . DIRECTORY_SEPARATOR . QCACHE_LOG_FILE_NAME;
$qcache_info_file = $qcache_folder . DIRECTORY_SEPARATOR . QCACHE_INFO_FILE_NAME;

$prev_max_log_recs = (int)$_GET['prevmaxlogs'];
$max_log_recs = (int)$_GET['maxlogs'];
$max_log_recs_changed = $max_log_recs != $prev_max_log_recs;

$content = 'waiting...';

if (file_exists($qcache_log_file)) {

    $file_mtime = filemtime($qcache_log_file);

    if ($file_mtime != $prev_file_mtime || $max_log_recs_changed) {
        // load in the data (oldest>>>latest) and split into rows
        $ar = explode("\n", file_get_contents($qcache_log_file));
        // discard the last (empty) row
        array_pop($ar);

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
                    '<div class="t-col c1">date & time</div>' .
                    '<div class="t-col c2">type</div>' .
                    '<div class="t-col c3">cache ms</div>' .
                    '<div class="t-col c3">db ms</div>' .
                    '<div class="t-col c4">sql</div>' .
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

echo json_encode([$file_mtime, $content]);