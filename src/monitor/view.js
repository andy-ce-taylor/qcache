/**
 * Vars set in view.php
 *
 *  qcache_folder           : Log storage path
 *  monitor_refresh_secs    : Seconds between update requests
 *  max_log_recs            : Initial number of log records to show
 */

var refresh_ms = monitor_refresh_secs * 1000;
var file_mtime = 0;
var wait_anim = 0;
var prev_max_log_recs = max_log_recs;
var $num_rows_selector = $('#num_rows_selector');
var max_log_recs_changed = false;

$num_rows_selector.on('change', function () {
    max_log_recs = $num_rows_selector.val();
    max_log_recs_changed = true;
});

var waiting_for_response = false;

var timerId = setInterval(function request() {
    var p_mlogs;

    if (waiting_for_response) {
        return;
    }

    waiting_for_response = true;

    if (max_log_recs_changed) {
        p_mlogs = prev_max_log_recs;
        max_log_recs_changed = false;
        prev_max_log_recs = max_log_recs;
    }
    else {
        p_mlogs = max_log_recs;
    }

    var args = '?qcpath=' + qcache_folder + '&prevmaxlogs=' + p_mlogs + '&maxlogs=' + max_log_recs + '&fmtime=' + file_mtime;

    $.ajax({
        method: 'post',
        url: 'content.php'+args,
        success: function (json) {
            var data = $.parseJSON(json);

            new_file_mtime = data[0];
            if (new_file_mtime === -1) {
                $('#content').html('waiting.' + str_repeat('.', ++wait_anim % 3));
            }
            else if (new_file_mtime !== file_mtime) {
                content = '';
                if (new_file_mtime) {
                    content = data[1];
                }
                $('#content').html(content);
                file_mtime = new_file_mtime;
            }

            if (data[2] !== -1) {
                $('#num_rows_available').html(data[2]);
                $('#stats_first_log_time').html(data[3]);
                $('#stats_secs').html(data[4]);
                $('#stats_slowest_secs').html(data[5]);
            }

            waiting_for_response = false;
        },
        error: function () {
            clearInterval(timerId);
        }
    });
}, refresh_ms);

function str_repeat(input, multiplier) {
    var string = "";
    while (multiplier) {
        string += input;
        multiplier--;
    }
    return string;
}
