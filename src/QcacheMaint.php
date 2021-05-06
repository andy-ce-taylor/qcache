<?php
/**
 * @noinspection SqlNoDataSourceInspection
 * @noinspection SqlDialectInspection
 */

namespace acet\qcache;

use acet\qcache\exception as QcEx;

class QcacheMaint
{
    /**
     * Removes cache records.
     *
     * @param string[] $db_connection_cache
     * @param string[] $target_db_connector_sigs
     * @param array $qcache_config ['qcache_folder'],[]
     * @throws QcEx\ConnectionException
     */
    public function clearCache($db_connection_cache, $target_db_connector_sigs, $qcache_config)
    {
        $cache_conn = Qcache::getConnection($qcache_config, $db_connection_cache);

        foreach ($target_db_connector_sigs as $sig) {
            try {
                $cache_conn->truncateTable($sig . '_cache_info');
            } catch (QcEx\TableWriteException $ex) {/* do nothing */}
            try {
                $cache_conn->truncateTable($sig . '_logs');
            } catch (QcEx\TableWriteException $ex) {/* do nothing */}

            $this->deleteCacheFiles($qcache_config['qcache_folder'], $sig);
        }
    }

    /**
     * Rebuild Qcache database tables if the configuration has changed, qcache has been updated or tables are missing.
     *
     * @param string[]   $db_connection_cache
     * @param string[][] $target_db_connectors
     * @param array      $qcache_config
     * @throws QcEx\ConnectionException|QcEx\TableWriteException
     */
    public function verify($db_connection_cache, $target_db_connectors, $qcache_config)
    {
        $rebuild = false;

        $target_db_connector_sigs = array_keys($target_db_connectors);

        $folder_sig_file = $qcache_config['qcache_folder'] . '/' . Constants::FOLDER_SIG_FILE;
        if ($folder_sig = self::detectQcacheSourceFileChanges($folder_sig_file)) {

            foreach ($target_db_connector_sigs as $sig) {
                self::deleteCacheFiles($qcache_config['qcache_folder'], $sig);
            }

            @unlink($folder_sig_file);
            file_put_contents($folder_sig_file, $folder_sig);
            $rebuild = true;
        }

        $config_sig_file = $qcache_config['qcache_folder'] . '/' . Constants::CONFIG_SIG_FILE;
        if ($config_sig = self::detectQcacheConfigChanges($qcache_config, $config_sig_file)) {
            @unlink($config_sig_file);
            file_put_contents($config_sig_file, $config_sig);
            $rebuild = true;
        }

        $cache_conn = Qcache::getConnection($qcache_config, $db_connection_cache);

//echo "<pre>"; var_dump($cache_conn); echo "</pre>";

        $db_name = $db_connection_cache['name'];

        foreach ($target_db_connectors as $sig => $connector) {

            $stmt = '';

            $table_name = $sig . '_cache_info';
            if ($rebuild || !$cache_conn->tableExists($db_name, $table_name)) {
                $stmt .= $cache_conn->getCreateTableSQL_cache_info($table_name);
            }

            $table_name = $sig . '_logs';
            if ($rebuild || !$cache_conn->tableExists($db_name, $table_name)) {
                $stmt .= $cache_conn->getCreateTableSQL_logs($table_name);
            }

            $target_conn = Qcache::getConnection($qcache_config, $connector);

            if ($target_conn->dbUsesCachedUpdatesTable()) {
                $table_name = $sig .'_table_update_times';
                if ($rebuild || !$cache_conn->tableExists($db_name, $table_name)) {
                    $stmt .= $cache_conn->getCreateTableSQL_table_update_times($table_name);
                }
            }

            if ($stmt) {
                $cache_conn->multi_write($stmt);
            }
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

        if (file_exists($folder_sig_file)) {
            $reported_folder_sig = file_get_contents($folder_sig_file);
        }

        if ($reported_folder_sig != $folder_sig) {
            return $folder_sig;
        }

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

        if (file_exists($config_sig_file)) {
            $reported_config_sig = file_get_contents($config_sig_file);
        }

        if ($reported_config_sig != $config_sig) {
            return $config_sig;
        }

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
        if (!file_exists($folder)) {
            return '';
        }

        $cs_str = '';

        foreach (scandir($folder) as $file) {
            if ($file[0] != '.') {
                if (is_dir($path = "$folder/$file")) {
                    $cs_str .= self::getFolderSignature($path);
                } elseif (($content = @file_get_contents($path)) !== false) {
                    $cs_str .= dechex(crc32($content));
                }
            }
        }

        return $cs_str;
    }

    /**
     * Deletes cache files in the given $dir (whose names start with the given $file_name_prefix).
     *
     * @param string $dir
     * @param string $filename_prefix
     */
    private static function deleteCacheFiles($dir, $filename_prefix)
    {
        if (!file_exists($dir)) {
            return;
        }

        $slen = strlen($filename_prefix);

        foreach (array_diff(scandir($dir), ['.', '..']) as $file) {
            if (substr($file, 0, $slen) == $filename_prefix) {
                unlink("$dir/$file");
            }
        }
    }

    /**
     * Removes excessive entries in $cache_recs together with their associated cache files.
     * Removal criteria:
     * - not used very often
     * - not used for a long time
     * - not resource intensive (query completes quickly)
     *
     * @param QCache $qc_instance
     */
    public static function maintenance(QCache $qc_instance)
    {
        $config = $qc_instance->getQcacheConfig();
        $use_db = $qc_instance->getCacheInfoToDb();

        // Calculate the high/low water marks
        $hwm = $config['max_qcache_records'] / (($use_db ? 0 : 1) + 2);
        $lwm = (int)($hwm * Constants::LWM_TO_HWM_RATIO);

        if (($num_cache_assets = $qc_instance->getNumCacheAssets()) < $hwm) {
            // limit not reached - nothing to do
            return;
        }

        // Remove excessive cache files
        $cache_info_recs = $qc_instance->getCacheInfoRecords();

        // Get hash indexed array of importance values
        $importances = [];
        foreach ($cache_info_recs as $ix => $rec) {
            $importances[$ix] = $rec[Constants::CACHE_INFO_REC_IMPORTANCE];
        }

        // Sort importances - least important last
        uasort(
            $importances,
            function ($a, $b) {
                $diff = $b - $a;
                return $diff < 0 ? -1 : ($diff > 0 ? 1 : 0);
            }
        );

        $db_connection_cache = $qc_instance->getDbConnectCache();
        $table_qc_cache_info = $qc_instance->getTableQcCacheInfo();
        $filename_prefix = $config['qcache_folder'] . '/' . $qc_instance->getTargetConnectionSignature();

        // Extract low importance elements (array position >= $lwm) and get their hashes
        $obsolete_elem_hashes = array_keys(array_slice($cache_info_recs, $lwm, null, true));

        // remove them
        foreach ($obsolete_elem_hashes as $hash) {
            $filename = $filename_prefix.$hash.'.';

            if ($use_db) {
                $db_connection_cache->delete($table_qc_cache_info, "hash='$hash'");
            } else {
                @unlink($filename . Constants::CACHE_INFO_FILE_EXT);
            }

            @unlink($filename . Constants::CACHE_FILE_EXT);
            @unlink($filename . Constants::STMT_FILE_EXT);


// uncomment if further processing of $cache_info_recs is needed
//          unset($cache_info_recs[$hash]);
        }
    }
}