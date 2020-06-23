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
            color: maroon;
            background-color: #d9cdd1;
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
        .c1 { width: 40px; }
        .c2 { width: 52px; }
        .c3 { width: 60px; text-align: right; }
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
    <meta http-equiv="Refresh" content="<?php echo $monitor_refresh_secs;?> " />
</head>
<body>
<?php {
    global $datasrc;

    if ($datasrc) {
        global $clear_logs_link;

        echo <<< EOT
        <div class="t-table">
            <div class="t-row t-head">
                <div class="t-col c1">type</div>
                <div class="t-col c2">time</div>
                <div class="t-col c3">ms</div>
                <div class="t-col c4">sql</div>
            </div>
EOT;

        foreach ($datasrc as $ix => $data) {
            $row_css = $ix & 1 ? 'odd-row' : 'even-row';
            if ($data['hit'] == 'db') {
                $row_css .= ' db-hit';
            } else {
                $data['millisecs'] = '&nbsp;';
                $row_css .= ' cache-hit';
            }

            echo "<div class=\"t-row $row_css\">";
            echo "<div class=\"t-col c1\">".$data['hit'].'</div>';
            echo "<div class=\"t-col c2\">".$data['timestamp'].'</div>';
            echo "<div class=\"t-col c3\">".$data['millisecs'].'</div>';
            echo "<div class=\"t-col c4 sql\">".$data['sql'].'</div>';
            echo '</div>';
        }
        echo "<a class=\"button\" href=\"$clear_logs_link\">Restart recording</a>";
    } else {
        echo 'waiting...';
    }
} ?>
</body>
</html>
