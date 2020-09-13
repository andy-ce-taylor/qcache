<?php

namespace acet\qcache\exception;

class TableReadException extends TableQueryException
{
    /**
     * TableReadException constructor.
     * @param string $sql
     * @param string $db_type
     * @param string $err_message
     */
    public function __construct($sql, $db_type, $err_message='')
    {
        parent::__construct("Table read exception", $sql, $db_type, $err_message);
    }
}