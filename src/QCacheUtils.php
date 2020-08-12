<?php
namespace acet\qcache;

use acet\reslock\ResLock;

class QCacheUtils
{
    public static function getDescription($file, $func, $line)
    {
        $class = basename($file, '.php');
        return "$file[$line]::$class::$func";
    }

    /**
     * Returns TRUE if Qcache can process the given SQL statement and is likely to significantly
     * improve performance, otherwise FALSE.
     *
     * @param string $sql
     * @return bool
     */
    public function cacheable($sql)
    {
        static $cacheable_l1c = [];

        $hash = hash('md5', $sql);

        // is the answer already known?
        if (array_key_exists($hash, $cacheable_l1c))
            return $cacheable_l1c[$hash];

        $qstr_tmp_lc = strtolower(trim($sql));

        return $cacheable_l1c[$hash] = (            // QCache can handle this if the statement...
            substr($qstr_tmp_lc, 0, 7) == 'select ' &&  // is a SELECT
            strpos($qstr_tmp_lc, ' from ', 7)       &&  // and has a FROM
            strpos($qstr_tmp_lc, ' join ', 7)       &&  // and has a JOIN
            ! strpos($qstr_tmp_lc, 'count(', 7)     &&  // and doesn't have a count (unsupported)
            ! strpos($qstr_tmp_lc, ' select ', 7)       // and doesn't have an embedded SELECT (unsupported)
        );
    }

    /**
     * Returns the names of all tables involved in the given SQL statement.
     *
     * @param string $qstr
     * @return string[]|bool
     */
    public static function getTables($qstr)
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
            if (($where_p = strpos($qstr_tmp_lc, ' where ', $from)) === false)
                $where_p = PHP_INT_MAX;

            if (($limit_p = strpos($qstr_tmp_lc, ' limit ', $from)) === false)
                $limit_p = PHP_INT_MAX;

            if ($first_p = min($where_p, $limit_p))
                $tables_str = trim(substr($qstr_tmp, $from, $first_p - $from));
        }
        else $tables_str = trim($tables_str, ', ');

        // split $tables_str into individual tables
        $tables_str = trim(str_replace(["'","`",'"'], '', $tables_str));
        $tables = [];
        foreach (explode(',', $tables_str) as $str) {
            if ($p = strpos($str = trim($str), ' '))
                $str = substr($str, 0, $p);

            $tables[] = $str;
        }

        if ($tables)
            return $tables;

        // Oops! - SELECT with no tables found
        return false;
    }

    /**
     * Removes cache files.
     *
     * @param string  $qcache_folder
     */
    public static function clearCache($qcache_folder)
    {
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
        if (!file_exists($dir))
            return;

        foreach (array_diff(scandir($dir), ['.', '..']) as $file)
            is_dir($path = $dir.DIRECTORY_SEPARATOR.$file) ? self::rmdir_plus($path) : unlink($path);

        if ($delete_topdir)
            rmdir($dir);
    }
}