<?php

namespace acet\qcache\exception;

class TableReadException extends QCacheException
{
    /**
     * TableReadException constructor.
     * @param string $sql
     * @param string $db_type
     */
    public function __construct($sql, $db_type)
    {
        parent::__construct("[$db_type] Table read exception");
    }
}