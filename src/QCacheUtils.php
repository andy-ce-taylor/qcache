<?php
/**
 * @noinspection SqlNoDataSourceInspection
 * @noinspection SqlDialectInspection
 */

namespace acet\qcache;

use acet\qcache\connector as Conn;

class QCacheUtils
{
    const CONFIG_SIG_FILE = 'config_signature.txt';
    const FOLDER_SIG_FILE = 'folder_signature.txt';

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
     * @param array    $qcache_config
     * @param string[] $db_connection_cache_data
     * @param string[] $db_connection_target_data
     * @param string   $qcache_folder
     */
    public function clearCache($qcache_config, $db_connection_cache_data, $db_connection_target_data)
    {
        $conn = QCache::getConnection($qcache_config, $db_connection_cache_data);

        $target_connection_sig = Conn\DbConnector::getSignature($db_connection_target_data);

        $conn->truncateTable($target_connection_sig . '_cache');
        $conn->truncateTable($target_connection_sig . '_logs');

        // delete cache files
        self::deleteCacheFiles($qcache_config['qcache_folder'], $target_connection_sig);
    }

    /**
     * Rebuild Qcache database tables if the configuration has changed, qcache has been updated or tables are missing.
     *
     * @param string[] $db_connection_cache_data
     * @param string[] $db_connection_target_data
     * @param array    $qcache_config
     * @throws exception\ConnectionException
     */
    public function verify($db_connection_cache_data, $db_connection_target_data, $qcache_config)
    {
        $target_connection_sig = Conn\DbConnector::getSignature($db_connection_target_data);

        $rebuild = false;

        $folder_sig_file = $qcache_config['qcache_folder'] . DIRECTORY_SEPARATOR . self::FOLDER_SIG_FILE;
        if ($folder_sig = self::detectQCacheFileChanges($folder_sig_file)) {
            self::deleteCacheFiles($qcache_config['qcache_folder'], $target_connection_sig);
            @unlink($folder_sig_file);
            file_put_contents($folder_sig_file, $folder_sig);
            $rebuild = true;
        }

        $config_sig_file = $qcache_config['qcache_folder'] . DIRECTORY_SEPARATOR . self::CONFIG_SIG_FILE;
        if ($config_sig = self::detectQCacheConfigChanges($qcache_config, $config_sig_file)) {
            @unlink($config_sig_file);
            file_put_contents($config_sig_file, $config_sig);
            $rebuild = true;
        }

        $conn = QCache::getConnection($qcache_config, $db_connection_cache_data);

        $db_name = $db_connection_cache_data['name'];

        $sql = '';

        $table_name = $target_connection_sig . '_cache';
        if ($rebuild || !$conn->tableExists($db_name, $table_name))
            $sql .= $conn->getCreateTableSQL_cache($table_name);

        $table_name = $target_connection_sig . '_logs';
        if ($rebuild || !$conn->tableExists($db_name, $table_name))
            $sql .= $conn->getCreateTableSQL_logs($table_name);

        if ($conn->dbUsesCachedUpdatesTable()) {
            $table_name = $target_connection_sig . '_table_update_times';
            if ($rebuild || !$conn->tableExists($db_name, $table_name))
                $sql .= $conn->getCreateTableSQL_table_update_times($table_name);
        }

        if ($sql)
            $conn->multi_write($sql);
    }

    /**
     * Detects changes to the database schema and other files that are specific to this version of QCache.
     * Returns a folder signature if changes are detected, otherwise FALSE.
     *
     * @var string $folder_sig_file
     * @return bool
     */
    private static function detectQCacheFileChanges($folder_sig_file)
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
    private static function detectQCacheConfigChanges($qcache_config, $config_sig_file)
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