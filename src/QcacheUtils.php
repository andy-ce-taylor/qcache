<?php
/**
 * @noinspection SqlNoDataSourceInspection
 * @noinspection SqlDialectInspection
 */

namespace acet\qcache;

class QcacheUtils
{
    const CONFIG_SIG_FILE = 'config_signature.txt';
    const FOLDER_SIG_FILE = 'folder_signature.txt';

    /**
     * Returns a description suitable for passing to Qcache::query.
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
     * Determines whether method Qcache::query() would be successful in parsing table names from the given SQL
     * statement or whether table names should be supplied as an argument.
     *
     * Returns TRUE if Qcache is capable of parsing table names from the given SQL statement and is likely to
     * significantly improve performance, otherwise FALSE.
     *
     * Note: Qcache can handle ALL select statements - this function merely determines whether it needs
     *       table name hints.
     *
     * @param string $sql
     * @return bool
     */
    public static function cacheable($sql)
    {
        static $cacheable_l1c = [];

        $hash = hash('md5', $sql);

        // is the answer already known?
        if (array_key_exists($hash, $cacheable_l1c))
            return $cacheable_l1c[$hash];

        $qstr_tmp_lc = strtolower(trim($sql));

        return $cacheable_l1c[$hash] = (                  // Qcache should handle this if the statement...
            substr($qstr_tmp_lc, 0, 7) == 'select '   &&  // is a SELECT
            strpos($qstr_tmp_lc, ' from ', 7)         &&  // and has a FROM
            strpos($qstr_tmp_lc, ' join ', 7)         &&  // and has a JOIN
            ! strpos($qstr_tmp_lc, 'count(', 7)       &&  // and doesn't have a count (unsupported)
            ! strpos($qstr_tmp_lc, ' select ', 7)         // and doesn't have an embedded SELECT (unsupported)
        );
    }

    /**
     * Returns the names of all tables participating in the given SQL statement.
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
     * @param string[]  $db_connection_cache
     * @param string[]  $target_db_connector_sigs
     * @param array     $qcache_config
     */
    public function clearCache($db_connection_cache, $target_db_connector_sigs, $qcache_config)
    {
        $cache_conn = Qcache::getConnection($qcache_config, $db_connection_cache);

        foreach ($target_db_connector_sigs as $sig) {
            $cache_conn->truncateTable($sig . '_cache');
            $cache_conn->truncateTable($sig . '_logs');
            $this->deleteCacheFiles($qcache_config['qcache_folder'], $sig);
        }
    }

    /**
     * Rebuild Qcache database tables if the configuration has changed, qcache has been updated or tables are missing.
     *
     * @param string[]   $db_connection_cache
     * @param string[][] $target_db_connectors
     * @param array      $qcache_config
     * @throws exception\ConnectionException
     */
    public function verify($db_connection_cache, $target_db_connectors, $qcache_config)
    {
        $rebuild = false;

        $target_db_connector_sigs = array_keys($target_db_connectors);

        $folder_sig_file = $qcache_config['qcache_folder'] . DIRECTORY_SEPARATOR . self::FOLDER_SIG_FILE;
        if ($folder_sig = self::detectQcacheSourceFileChanges($folder_sig_file)) {

            foreach ($target_db_connector_sigs as $sig)
                self::deleteCacheFiles($qcache_config['qcache_folder'], $sig);

            @unlink($folder_sig_file);
            file_put_contents($folder_sig_file, $folder_sig);
            $rebuild = true;
        }

        $config_sig_file = $qcache_config['qcache_folder'] . DIRECTORY_SEPARATOR . self::CONFIG_SIG_FILE;
        if ($config_sig = self::detectQcacheConfigChanges($qcache_config, $config_sig_file)) {
            @unlink($config_sig_file);
            file_put_contents($config_sig_file, $config_sig);
            $rebuild = true;
        }

        $cache_conn = Qcache::getConnection($qcache_config, $db_connection_cache);

//echo "<pre>"; var_dump($cache_conn); echo "</pre>";

        $db_name = $db_connection_cache['name'];

        foreach ($target_db_connectors as $sig => $connector) {

            $sql = '';

            $table_name = $sig . '_cache';
            if ($rebuild || !$cache_conn->tableExists($db_name, $table_name))
                $sql .= $cache_conn->getCreateTableSQL_cache($table_name);

            $table_name = $sig . '_logs';
            if ($rebuild || !$cache_conn->tableExists($db_name, $table_name))
                $sql .= $cache_conn->getCreateTableSQL_logs($table_name);

            $target_conn = Qcache::getConnection($qcache_config, $connector);

            if ($target_conn->dbUsesCachedUpdatesTable()) {
                $table_name = $sig .'_table_update_times';
                if ($rebuild || !$cache_conn->tableExists($db_name, $table_name))
                    $sql .= $cache_conn->getCreateTableSQL_table_update_times($table_name);
            }

            if ($sql)
                $cache_conn->multi_write($sql);
        }
    }

    /**
     * Detects changes to the database schema and other files that are specific to this version of Qcache.
     * Returns a folder signature if changes are detected, otherwise FALSE.
     *
     * @var string $folder_sig_file
     * @return bool
     */
    private static function detectQcacheSourceFileChanges($folder_sig_file)
    {
        $folder_sig = self::getFolderSignature(realpath(__DIR__ . DIRECTORY_SEPARATOR . '..'));

        $reported_folder_sig = '';

        if (file_exists($folder_sig_file))
            $reported_folder_sig = file_get_contents($folder_sig_file);

        if ($reported_folder_sig != $folder_sig)
            return $folder_sig;

        return false;
    }

    /**
     * Detects changes to the configuration.
     * Returns a config signature if changes are detected, otherwise FALSE.
     *
     * @var array  $qcache_config
     * @var string $config_sig_file
     * @return bool
     */
    private static function detectQcacheConfigChanges($qcache_config, $config_sig_file)
    {
        $config_sig = serialize($qcache_config);

        $reported_config_sig = '';

        if (file_exists($config_sig_file))
            $reported_config_sig = file_get_contents($config_sig_file);

        if ($reported_config_sig != $config_sig)
            return $config_sig;

        return false;
    }

    /**
     * Starting from the given $folder, recursively concatenate the checksum of the contents of folders.
     *
     * @param string $folder
     * @return string
     */
    private static function getFolderSignature($folder)
    {
        if (!file_exists($folder))
            return '';

        $cs_str = '';

        foreach (scandir($folder) as $file)
            if ($file[0] != '.')
                if (is_dir($path = $folder . DIRECTORY_SEPARATOR . $file))
                    $cs_str .= self::getFolderSignature($path);

                else
                    if (($content = @file_get_contents($path)) !== false)
                        $cs_str .= dechex(crc32($content));

        return $cs_str;
    }

    /**
     * Deletes cache files in the given $dir (whose names start with the given $file_name_prefix).
     *
     * @param string $dir
     * @param string $prefix
     */
    private static function deleteCacheFiles($dir, $file_name_prefix)
    {
        if (!file_exists($dir))
            return;

        $slen = strlen($file_name_prefix);

        foreach (array_diff(scandir($dir), ['.', '..']) as $file)
            if (substr($file, 0, $slen) == $file_name_prefix)
                unlink($dir . DIRECTORY_SEPARATOR . $file);
    }
}