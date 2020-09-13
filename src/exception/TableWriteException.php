<?php

namespace acet\qcache\exception;

class TableWriteException extends TableQueryException
{
    /**
     * TableWriteException constructor.
     * @param string $sql
     * @param string $db_type
     * @param string $err_message
     */
    public function __construct($sql, $db_type, $err_message='')
    {
        parent::__construct("Table write exception", $sql, $db_type, $err_message);
    }
}