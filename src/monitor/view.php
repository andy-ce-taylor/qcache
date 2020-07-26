<?php
namespace acet\qcache\monitor;

session_write_close();
require_once __DIR__ . '/../../../../autoload.php';

use acet\qcache\Constants;
use acet\qcache\JsonEncodedFileIO;

?>
<!DOCTYPE html>
<html lang="en">
<head>
    <title>MONITOR</title>
    <meta charset="UTF-8">
    <meta http-equiv="Content-Language" content="en-GB" />
    <meta http-equiv="Content-Type" content="application/xhtml+xml;charset=utf-8" />
    <script src="https://code.jquery.com/jquery-3.5.1.min.js"></script>
    <style type="text/css">
        body {
            width: 100%;
            font-family: "Courier New", Courier, monospace;
        }
        .t-table {
            display: table;
            width: 100%;
            font-size: smaller;
            border-bottom: 1px solid #ddd;
        }
        .t-head {
            color: ivory;
            background-color: maroon;
            font-size: larger;
            font-weight: bold;
        }
        .t-row {
            display: table-row;
            width: auto;
            height: 20px;
            clear: both;
        }
        .odd-row  { background-color: #f5edf0; }
        .even-row { background-color: inherit; }
        .t-col {
            float: left;
            display: table-column;
            padding: 4px 10px;
            overflow-x: hidden;
            white-space: nowrap;
        }
        .c1 { width: 152px; }
        .c2 { width: 50px; }
        .c3 { width: 85px; text-align: right; }
        .c4 { min-width: 300px; width: 55%; padding-left: 30px; }
        .sql {
            font-family: "Courier New", Courier, monospace;
            color: darkblue;
        }
        .db-hit, .cache-hit {
            color: cadetblue;
        }
        .db-hit {
            font-weight: bold;
        }
        #control {
            font-family: "Courier New", Courier, monospace;
            height: 40px;
            margin-top: 20px;
        }
        #stats1, #stats2 {
            color: #0f74a8;
        }
        #stats_first_log_time, #stats_time_saved, #stats_slowest_secs {
            font-weight: bold;
        }
        #stats_time_saved {
            font-size: larger;
            color: #0c3d5d;
        }
        #stats2 {
            padding: 12px 0;
        }
        #stats {
            float: left;
            padding-bottom: 8px;
        }
        #rows_selector {
            margin-top: -3px;
            float: right;
            padding-right: 20px;
            font-size: larger;
        }
        #rows_available {
            font-size: smaller;
            font-style: italic;
            color: #254e6f;
        }
        #num_rows_selector {
            font-size: medium;
            color: #1a4186;
        }
        #content {
            clear: both;
            float: left;
        }
    </style>
</head>
<?php
$opts_max_logs        = $_GET['optsmaxlogs'];
$qcache_folder        = $_GET['qcpath'];
$monitor_refresh_secs = $_GET['rsecs'];
$max_log_recs         = $_GET['maxlogs'];

$opts_mlogs = '';
foreach (explode(',', $opts_max_logs) as $opt) {
    $val = strtolower($opt) == 'all' ? 0 : $opt;
    $sel = $val == $max_log_recs ? ' selected' : '';
    $opts_mlogs .= "<option value=\"$val\"$sel>$opt</option>";
}

$logs = [];
$qcache_log_file = $qcache_folder . DIRECTORY_SEPARATOR . Constants::QCACHE_LOG_FILE_NAME;
if (file_exists($qcache_log_file)) {
    $logs = explode("\n", file_get_contents($qcache_log_file));
}
$num_logs = count($logs);

$qcache_stats_file = $qcache_folder . DIRECTORY_SEPARATOR . Constants::QCACHE_STATS_FILE_NAME;
$first_log_time = $total_saved_time = $slowest_case_secs = $slowest_case_sql = $slowest_case_time = 'n/a';
if (file_exists($qcache_stats_file)) {
    $stats = JsonEncodedFileIO::readJsonEncodedArray($qcache_stats_file);
    $first_log_time = date('Y/m/d H:i s', $stats['first_log_time']);
    $total_saved_time = secondsToWords($stats['total_saved_ms']);
    $slowest_case_secs = number_format($stats['slowest_case']['ms'] / 1000, 5);
    $slowest_case_sql = $stats['slowest_case']['sql'];
    $slowest_case_time = $stats['slowest_case']['time'];
}
?>
<body>
<div id="stats">
    <div id="cache_stats"></div>
    <div id="stats1">
        Total time saved since <span id="stats_first_log_time"><?php echo $first_log_time;?></span> is <span id="stats_time_saved"><?php echo $total_saved_time;?></span>
    </div>
    <div id="stats2">
        Slowest query was <span id="stats_slowest_secs"><?php echo $slowest_case_secs;?></span> seconds
    </div>
</div>
<div id="control">
    <div id="rows_selector">Show <select id="num_rows_selector"><?php echo $opts_mlogs;?></select> rows
        <span id="rows_available">(<span id="num_rows_available"><?php echo $num_logs;?></span> available)</span>
    </div>
</div>
<div id="content">Loading...</div>
<script>
    var qcache_folder = '<?php echo $qcache_folder;?>';
    var monitor_refresh_secs = <?php echo $monitor_refresh_secs;?>;
    var max_log_recs = <?php echo $max_log_recs;?>;
</script>
<script src="view.js"></script>
</body>
</html>


<?php
function secondsToWords($ms)
{
    $str = '';

    $secs = intval($ms / 1000);

    if ($days = intval($secs / (3600 * 24))) {
        $str .= "$days day".($days > 1 ? 's' : '').', ';
    }
    if ($hours = ($secs / 3600) % 24) {
        $str .= "$hours hour".($hours > 1 ? 's' : '').', ';
    }
    if ($mins = ($secs / 60) % 60) {
        $str .= "$mins minute".($mins > 1 ? 's' : '').', ';
    }

    if ($str) {
        $str = rtrim($str, ', ').' and ';
    }

    $secs = $secs % 60;
    $ms %= 1000;
    if ($secs || $ms) {
        $str .= ((float)($secs) + $ms / 1000) . ' seconds';
    }

    return $str;
}