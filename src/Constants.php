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
    const CACHE_INFO_FILE_EXT =           'dat';
    const STMT_FILE_EXT =                 'stm';
    const EXCLUDE_STMT_FILE_EXT =         'xcl';

    const DFLT_GZ_COMPRESSION_LEVEL =     5;    // 0=no compression, 9=maximum compression, -1=native level

    const DFLT_MAX_QCACHE_TRANSACTIONS =  6000;
    const CLEAR_EXCESS_RND =              50;   // roughly every n cache operations, remove excessive assets
    const LWM_TO_HWM_RATIO =              0.9;

    // Weightings are used when computing cache importance
    const AT_FACTOR =                     1.0;  // access times (may need recency amplification (sines) rather than straight-line factors)
    const CA_FACTOR =                     3.5;  // Cumulative average (time cost)
    const IM_FACTOR =                     0.5;  // Impressions

    // Whole query exclusion ("SELECT * FROM table WHERE id=123"), or those with a matching start ("SELECT * FROM table WHERE id=")
    const EXCLUDE_QUERY_WHOLE =           0;
    const EXCLUDE_QUERY_STARTING =        1;

    // QUERY_STATUS_nnn signifies the status of the last Qcache query
    const QUERY_STATUS_NULL =             0;    // not set
    const QUERY_STATUS_ERROR =            1;    // query not completed because of an error
    const QUERY_STATUS_EXCLUDED =         2;    // statement is excluded
    const QUERY_STATUS_DISABLED =         3;    // Qcache is disabled
    const QUERY_STATUS_CACHE_MISS =       4;    // success - a new transaction was cached
    const QUERY_STATUS_CACHE_HIT =        5;    // success - a cached transaction was found
    const QUERY_STATUS_CACHE_STALE =      6;    // success - a stale transaction was refreshed

    // cache info record columns (if cache info storage is set to db, additional column 'hash' becomes the first column)
    const CACHE_INFO_COLUMNS =            'access_time, av_microsecs, impressions, importance, description, tables_csv';

    // Zero-based indexes into CACHE_INFO_COLUMNS - must stay synced
    const CACHE_INFO_REC_ACC_TIME =       0;
    const CACHE_INFO_REC_AV_MSECS =       1;
    const CACHE_INFO_REC_IMPRESSIONS =    2;
    const CACHE_INFO_REC_IMPORTANCE =     3;
    const CACHE_INFO_REC_DESCR =          4;
    const CACHE_INFO_REC_TABLES =         5;
}
