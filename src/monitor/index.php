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
        .odd-row  { background-color: #f5edf0
        }
        .even-row { background-color: inherit }
        .t-col {
            float: left;
            display: table-column;
            padding: 4px 10px;
            overflow-x: hidden;
            white-space: nowrap;
        }
        .c1 { width: 42px; }
        .c2 { width: 52px; }
        .c3 { width: 68px; text-align: right; }
        .c4 { min-width: 300px; width: 55%; }
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
        .button {
            background-color: ivory;
            color: maroon;
            font-weight: bold;
            border-radius: 4px;
            border: 1px solid teal;
            display: inline-block;
            cursor: pointer;
            font-family: Arial, serif;
            font-size: 17px;
            margin-top: 30px;
            padding: 6px 12px;
            text-decoration: none;
        }
    </style>
</head>
<body>
<div id="content"></div>
<script>
    var monitor_refresh_secs = <?php echo $_GET['rsecs'];?>;
    var qcache_folder = '<?php echo $_GET['qcpath'];?>';
    var max_log_recs = <?php echo $_GET['maxlogs'];?>;
</script>
<script src="view.js"></script>
</body>
</html>
