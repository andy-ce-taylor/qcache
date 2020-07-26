<?php
namespace acet\qcache;

class Constants
{
    const QCACHE_INFO_FILE_NAME       = 'qcache_info.json';
    const QCACHE_STATS_FILE_NAME      = 'qcache_stats.json';
    const QCACHE_LOG_FILE_NAME        = 'qcache.log';
    const MSSQL_TABLES_INFO_FILE_NAME = 'mssql_tables_info.json';
    const CLEAR_EXCESS_RND            = 100;
    const MAX_LOG_RECORDS             = 500;

    // Weightings are used when computing cache importance
    const AT_FACTOR = 1.0;  // access times (may need recency amplification (sines) rather than straight-line factors)
    const IM_FACTOR = 0.5;  // Impressions
    const CA_FACTOR = 3.5;  // Cumulative average (time cost)
}