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
            font-family: Arial, Verdana, Tahoma, serif;
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
        .c1 { width: 130px; }
        .c2 { width: 50px; }
        .c3 { width: 65px; text-align: right; }
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
            height: 40px;
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
?>
<body>
<div id="control">
    Show
    <select id="num_rows_selector"><?php echo $opts_mlogs;?></select>
    rows
</div>
<div id="content"></div>
<script>
    var qcache_folder = '<?php echo $qcache_folder;?>';
    var monitor_refresh_secs = <?php echo $monitor_refresh_secs;?>;
    var max_log_recs = <?php echo $max_log_recs;?>;
</script>
<script src="view.js"></script>
</body>
</html>
