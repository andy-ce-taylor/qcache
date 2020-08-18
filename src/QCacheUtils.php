<?php
/**
 * @noinspection SqlNoDataSourceInspection
 * @noinspection SqlDialectInspection
 */

namespace acet\qcache;

class QCacheUtils
{
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
     * @param array   $conn_data
     * @param string  $qcache_folder
     * @param string  $module_id
     */
    public function clearCache($conn_data, $qcache_folder, $module_id='')
    {
        $conn = QCache::getConnection($conn_data, $module_id);

        if ($module_id)
            $module_id .= '_';

        $table_qc_cache = 'qc_' . $module_id . 'cache';
        $table_qc_logs  = 'qc_' . $module_id . 'logs';

        $conn->write("TRUNCATE TABLE $table_qc_cache");
        $conn->write("TRUNCATE TABLE $table_qc_logs");

        // delete cache files
        self::rmdir_plus($qcache_folder, false);
    }

    /**
     * Rebuild Qcache database tables if the Qcache version (according to the CHANGELOG) has
     * been updated or the tables are missing.
     *
     * @param array   $conn_data
     * @param string  $qcache_folder
     * @param string  $module_id
     */
    public function verifyQCacheTables($conn_data, $qcache_folder='', $module_id='')
    {
        $schema_changed = self::detectSchemaChange();

        if ($schema_changed) {
            // delete cache files
            if (!$qcache_folder)
                $qcache_folder = sys_get_temp_dir();

            self::rmdir_plus($qcache_folder, false);
        }

        $conn = QCache::getConnection($conn_data, $module_id);

        $db_name = $conn_data['name'];

        if ($module_id)
            $module_id .= '_';

        $prefix = 'qc_' . $module_id;

        $sql = '';

        $table_name = $prefix . "cache";
        if ($schema_changed || !$conn->tableExists($db_name, $table_name))
            $sql .= $conn->getCreateTableSQL_cache($table_name);

        $table_name = $prefix . "logs";
        if ($schema_changed || !$conn->tableExists($db_name, $table_name))
            $sql .= $conn->getCreateTableSQL_logs($table_name);

        $table_name = $prefix . "table_update_times";
        if ($schema_changed || !$conn->tableExists($db_name, $table_name))
            $sql .= $conn->getCreateTableSQL_table_update_times($table_name);

        if ($sql)
            $conn->multi_query($sql);
    }

    /**
     * Returns TRUE if table schemas have changed.
     *
     * @return bool
     */
    private static function detectSchemaChange()
    {
        $d = __DIR__ . DIRECTORY_SEPARATOR;
        $schema_crc =
            crc32(file_get_contents("{$d}DbConnectorMySQL.php")) ^
            crc32(file_get_contents("{$d}DbConnectorMSSQL.php")) ^
            crc32(file_get_contents("{$d}Constants.php"));

        $reported_schema_crc = 0;
        if (file_exists($checksum_file = __DIR__ . '/../' . Constants::SCHEMA_CHECKSUM_FILE))
            $reported_schema_crc = (int)file_get_contents($checksum_file);

        if ($reported_schema_crc != $schema_crc) {
            file_put_contents($checksum_file, $schema_crc);
            return true;
        }

        return false;
    }

    /**
     * Recursively delete sub-folders and their contents.
     * Also delete the given top folder if $delete_topdir is set.
     *
     * @param string $dir
     * @param bool $delete_topdir
     */
    private static function rmdir_plus($dir, $delete_topdir=true)
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
}