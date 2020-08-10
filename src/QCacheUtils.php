<?php
namespace acet\qcache;

use acet\reslock\ResLock;

class QCacheUtils
{
    /**
     * @param string  $qc_key
     * @return string
     */
    protected static function getCacheFileName($qcache_folder, $qc_key)
    {
        return $qcache_folder . DIRECTORY_SEPARATOR . '#' . $qc_key . '.dat';
    }

    /**
     * Returns the names of all tables involved in the given SQL statement.
     *
     * @param string $qstr
     * @return string[]|bool
     */
    protected static function getTables($qstr)
    {
        // remove escape sequences
        $qstr_tmp = str_replace(["\t", "\r", "\n"], ' ', trim($qstr));
        $qstr_tmp_lc = strtolower($qstr_tmp);

        $from = strpos($qstr_tmp_lc, ' from ') + 6;

        // expect table names between FROM and... [first JOIN | LIMIT | WHERE] (whichever is first)
        $tables_str = '';

        // find JOINed tables
        $first_join_pos = false;
        $join_p = $from;
        while ($join_p = strpos($qstr_tmp_lc, ' join ', $join_p)) {
            if (!$first_join_pos) {
                $tables_str = trim(substr($qstr_tmp, $from, $join_p - $from)) . ',';
                $first_join_pos = $join_p;
            }
            $join_p += 6;
            $on_p = strpos($qstr_tmp_lc, ' on ', $join_p);
            $tables_str .= substr($qstr_tmp, $join_p, $on_p - $join_p) . ',';
        }

        // if no luck with JOINs, find WHERE/LIMIT
        if (!$first_join_pos) {
            if (($where_p = strpos($qstr_tmp_lc, ' where ', $from)) === false) {
                $where_p = PHP_INT_MAX;
            }
            if (($limit_p = strpos($qstr_tmp_lc, ' limit ', $from)) === false) {
                $limit_p = PHP_INT_MAX;
            }
            if ($first_p = min($where_p, $limit_p)) {
                $tables_str = trim(substr($qstr_tmp, $from, $first_p - $from));
            }
        }
        else {
            $tables_str = trim($tables_str, ', ');
        }

        // split $tables_str into individual tables
        $tables_str = trim(str_replace(["'","`",'"'], '', $tables_str));
        $tables = [];
        foreach (explode(',', $tables_str) as $str) {
            $str = trim($str);
            if ($p = strpos($str, ' ')) {
                $str = substr($str, 0, $p);
            }
            $tables[] = $str;
        }

        if ($tables) {
            return $tables;
        }

        // Oops! - SELECT with no tables found
        return false;
    }

    /**
     * Calculates the importance of the cache file, based on access time, popularity and query performance.
     *
     * The importance of a cache is determined by checking how recently and how often the information is requested,
     * and how time consuming the db operation is.
     *
     *    a = access_time  - higher = more recent
     *    i = impressions  - higher = more popular
     *    t = millisec_av  - higher = more time costly
     *
     * Each of the determining values are individually weighted to find the overall importance.
     *
     *    importance = (a * af) * (i * if) * (t * tf)
     *
     * @param array  & $info
     */
    protected static function computeCachePerformance(&$info)
    {
        $a = max($info['cache stats']['access time'], $info['db stats']['access time']);
        $i = $info['cache stats']['impressions'] + $info['db stats']['impressions'];
        $t = $info['db stats']['millisec av'];

        $info['importance'] = ($a * Constants::AT_FACTOR) * ($i * Constants::IM_FACTOR) * ($t * Constants::CA_FACTOR);
    }

    /**
     * Removes cache files.
     *
     * @param string  $qcache_folder
     */
    public static function clearCacheFiles($qcache_folder)
    {
        $qcache_folder      = str_replace(["\\", '/'], DIRECTORY_SEPARATOR, rtrim($qcache_folder, "\\ ./"));
        $qcache_info_file   = $qcache_folder.DIRECTORY_SEPARATOR.Constants::QCACHE_INFO_FILE_NAME;
        $qcache_log_file    = $qcache_folder.DIRECTORY_SEPARATOR.Constants::QCACHE_LOG_FILE_NAME;
        $reslocks_folder    = $qcache_folder.DIRECTORY_SEPARATOR.'reslocks';

        $reslock = new ResLock($reslocks_folder);

        $rl_key = $reslock->lock($qcache_info_file);
        {
            $qc_info = JsonEncodedFileIO::read($qcache_info_file) ?? [];

            foreach (array_keys($qc_info) as $qc_key) {
                $cache_file = QCacheUtils::getCacheFileName($qcache_folder, $qc_key);
                if (file_exists($cache_file)) {
                    unlink($cache_file);
                }
            }

            if (file_exists($qcache_info_file)) {
                unlink($qcache_info_file);
            }
        }
        $reslock->unlock($rl_key);

        $rl_key = $reslock->lock($qcache_log_file);
        {
            if (file_exists($qcache_log_file)) {
                unlink($qcache_log_file);
            }
        }
        $reslock->unlock($rl_key);

        if (file_exists($reslocks_folder)) {
            self::rmdir_plus($reslocks_folder, false);
        }
    }


    /**
     * Sorts $qc_info into descending importance order (most important will be first).
     *
     * Primarily used when deciding which caches to remove during regular housekeeping.
     *
     * @param array  & $qc_info
     */
    protected static function sortCachesByImportance(&$qc_info)
    {
        uasort(
            $qc_info,
            function ($a, $b) {
                $diff = $b['importance'] - $a['importance'];

                return $diff < 0 ? -1 : ($diff > 0 ? 1 : 0);
            }
        );
    }

    /**
     * Removes excessive entries in $qc_info together with their associated cache files.
     *
     * @param array  & $qc_info
     * @param string   $qcache_folder
     * @param int      $max_qcache_files_approx
     */
    protected static function removeExcessCacheFiles(&$qc_info, $qcache_folder, $max_qcache_files_approx)
    {
        if (($num_files_to_remove = count($qc_info) - $max_qcache_files_approx) > 0) {

            $obsolete_elems = array_slice($qc_info, $max_qcache_files_approx, null, true);

            foreach ($obsolete_elems as $qc_key => $qinfo) {
                unlink(QCacheUtils::getCacheFileName($qcache_folder, $qc_key));
            }

            $qc_info = array_slice($qc_info, 0, $max_qcache_files_approx, true);
        }
    }

    /**
     * Returns an array containing the name and full path of the Qcache info file.
     *
     * @param string  $qcache_folder
     * @param int     $max_qcache_files_approx
     * @return string[]
     */
    protected static function getQCacheInfoFile($qcache_folder, $max_qcache_files_approx=1000)
    {
        $qcache_folder    = str_replace(["\\", '/'], DIRECTORY_SEPARATOR, rtrim($qcache_folder, "\\ ./"));
        $qcache_info_file = $qcache_folder.DIRECTORY_SEPARATOR . Constants::QCACHE_INFO_FILE_NAME;
        $reslocks_folder  = $qcache_folder.DIRECTORY_SEPARATOR . 'reslocks';

        $reslock = new ResLock($reslocks_folder);

        $rl_key = $reslock->lock($qcache_info_file);
        {
            $qc_info = JsonEncodedFileIO::read($qcache_info_file) ?? [];

            self::sortCachesByImportance($qc_info);
            self::removeExcessCacheFiles($qc_info, $qcache_folder, $max_qcache_files_approx);

            JsonEncodedFileIO::write($qcache_info_file, $qc_info);
        }
        $reslock->unlock($rl_key);

        return [Constants::QCACHE_INFO_FILE_NAME, $qcache_info_file];
    }

    /**
     * Recursively delete sub-folders and their contents.
     * Also delete the given top folder if $delete_topdir is set.
     *
     * @param string $dir
     * @param bool $delete_topdir
     */
    protected static function rmdir_plus($dir, $delete_topdir=true)
    {
        if (!file_exists($dir)) {
            return;
        }

        foreach (array_diff(scandir($dir), ['.', '..']) as $file) {
            is_dir($path = $dir.DIRECTORY_SEPARATOR.$file) ? self::rmdir_plus($path) : unlink($path);
        }

        if ($delete_topdir) {
            rmdir($dir);
        }
    }

    /**
     * @param string  $qc_key
     * @return string
     */
    protected static function getHashFileName($qcache_folder, $qc_key)
    {
        return $qcache_folder . DIRECTORY_SEPARATOR . '#' . $qc_key . '.dat';
    }
}