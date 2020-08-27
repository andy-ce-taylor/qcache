<?php

namespace acet\qcache\exception;

class TableWriteException extends QCacheException
{
    /**
     * TableWriteException constructor.
     * @param string $sql
     * @param string $db_type
     */
    public function __construct($sql, $db_type)
    {
        parent::__construct("[$db_type] Table write exception");
    }
}