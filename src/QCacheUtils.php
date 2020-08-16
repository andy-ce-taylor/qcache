<?php
/**
 * @noinspection SqlNoDataSourceInspection
 * @noinspection SqlDialectInspection
 */

namespace acet\qcache;

class QCacheUtils
{
    const QCACHE_VERSION_FILE = 'local_version.txt';

    /**
     * Returns a description suitable for passing to QCache::query.
     *
     * @param string $file  - use __FILE__
     * @param string $func  - use __FUNCTION__
     * @param string $line  - use __LINE__
     * @return string       - e.g. "my_file.php[123]::MyClass::myMethod"
     */
    public static function getDescription($file, $func, $line)
    {
        $class = basename($file, '.php');
        return "{$file}[$line]::$class::$func";
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
     * Removes cache records.
     *
     * @param string  $db_type
     * @param string  $db_host
     * @param string  $db_user
     * @param string  $db_pass
     * @param string  $db_name
     * @param string  $module_id
     */
    public function clearCache($db_type, $db_host, $db_user, $db_pass, $db_name, $module_id='')
    {
        $conn = QCache::getConnection($db_type, $db_host, $db_user, $db_pass, $db_name, $module_id);

        if ($module_id)
            $module_id .= '_';

        $table_qc_cache = 'qc_' . $module_id . 'cache';
//      $table_qc_logs  = 'qc_' . $module_id . 'logs';

        $conn->write("TRUNCATE TABLE $table_qc_cache");
//      $conn->write("TRUNCATE TABLE $table_qc_logs");
    }

    /**
     * Rebuild Qcache database tables if the Qcache version (according to the CHANGELOG) has
     * been updated or the tables are missing.
     *
     * @param string  $db_type
     * @param string  $db_host
     * @param string  $db_user
     * @param string  $db_pass
     * @param string  $db_name
     * @param string  $module_id
     */
    public function verifyQCacheTables($db_type, $db_host, $db_user, $db_pass, $db_name, $module_id='')
    {
        $v_change = self::qcacheVersionChange();

        $conn = QCache::getConnection($db_type, $db_host, $db_user, $db_pass, $db_name, $module_id);

        if ($module_id)
            $module_id .= '_';

        $prefix = "qc_{$module_id}";

        $sql = '';

        $table_name = $prefix . "cache";
        if ($v_change || !$conn->tableExists($db_name, $table_name))
            $sql .= $conn->getCreateTableSQL_cache($table_name);

        $table_name = $prefix . "logs";
        if ($v_change || !$conn->tableExists($db_name, $table_name))
            $sql .= $conn->getCreateTableSQL_logs($table_name);

        $table_name = $prefix . "table_update_times";
        if ($v_change || !$conn->tableExists($db_name, $table_name))
            $sql .= $conn->getCreateTableSQL_table_update_times($table_name);

        if ($sql)
            $conn->multi_query($sql);
    }

    /**
     * Returns TRUE if CHANGELOG reports a new version.
     *
     * @return bool
     */
    private static function qcacheVersionChange()
    {
        $clog = file_get_contents(__DIR__ . '/../CHANGELOG.md');
        $pos = strpos($clog, '###');
        $clog_version = trim(substr($clog, $pos + 4, strpos($clog, "\n", $pos) - $pos - 4));

        $qcache_version = '';

        if (file_exists($version_file = __DIR__ . '/../' . self::QCACHE_VERSION_FILE))
            $qcache_version = trim(file_get_contents($version_file));

        if ($qcache_version != $clog_version) {
            file_put_contents($version_file, $clog_version);
            return true;
        }

        return false;
    }
}