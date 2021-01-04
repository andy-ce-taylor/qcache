<?php

namespace acet\qcache\exception;

class TableQueryException extends QcacheException
{
    /**
     * TableQueryException constructor.
     * @param string $description
     * @param string $sql
     * @param string $db_type
     * @param string $err_message
     */
    public function __construct($description, $sql, $db_type, $err_message='')
    {
        parent::__construct("[$db_type] $description $err_message");
    }
}