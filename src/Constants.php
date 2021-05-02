<?php
namespace acet\qcache;

class Constants
{
    const STORAGE_TYPE_DB =               1;    // database
    const STORAGE_TYPE_FS =               2;    // filesystem

    const DFLT_CACHE_INFO_STORAGE_TYPE =  self::STORAGE_TYPE_FS;
    const DFLT_LOG_STORAGE_TYPE =         self::STORAGE_TYPE_FS;

    const CONFIG_SIG_FILE =               'config_signature.txt';
    const FOLDER_SIG_FILE =               'folder_signature.txt';

    const CACHE_FILE_EXT =                'cch';
    const STMT_FILE_EXT =                 'stm';
    const CACHE_INFO_FILE_EXT =           'dat';
    const EXCLUDE_STMT_FILE_EXT =         'xcl';

    const DFLT_GZ_COMPRESSION_LEVEL =     5;    // 0=no compression, 9=maximum compression, -1=native level

    const DFLT_MAX_QCACHE_TRANSACTIONS =  8000;

    // QUERY_STATUS_nnn reports the status of the last Qcache query
    const QUERY_STATUS_NULL =             0;    // not set
    const QUERY_STATUS_ERROR =            1;    // query not completed because of an error
    const QUERY_STATUS_EXCLUDED =         2;    // statement is excluded
    const QUERY_STATUS_DISABLED =         3;    // Qcache is disabled
    const QUERY_STATUS_CACHE_HIT =        4;    // a cached transaction was found
    const QUERY_STATUS_CACHE_MISS =       5;    // a new transaction was cached
    const QUERY_STATUS_CACHE_STALE =      6;    // a stale transaction was refreshed
}
