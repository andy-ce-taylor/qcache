var refresh_ms = monitor_refresh_secs * 1000;
var clear_log = false;
var file_mtime = 0;
var wait_anim = 0;

var timerId = setInterval(function request() {
    var args = '?qcpath=' + qcache_folder + '&maxlogs=' + max_log_recs + '&fmtime=' + file_mtime;
    if (clear_log) {
        args += '&clearlogs=1'
        clear_log = false;
    }
    $.ajax({
        method: 'post',
        url: 'view.php'+args,
        success: function (json) {
            var data = $.parseJSON(json);
            new_file_mtime = data[0];
            if (new_file_mtime === -1) {
                $('#content').html('waiting.' + str_repeat('.', ++wait_anim % 3));
            }
            else if (new_file_mtime !== file_mtime) {
                $content = '';
                if (new_file_mtime) {
                    content = data[1];
                }
                $('#content').html(content);
                file_mtime = new_file_mtime;
            }
        },
        error: function () {
            clearInterval(timerId);
        }
    });
}, refresh_ms);

function clearLog() {
    clear_log = true;
    wait_anim = -1;
}

function str_repeat(input, multiplier) {
    var string = "";
    while (multiplier) {
        string += input;
        multiplier--;
    }
    return string;
}
