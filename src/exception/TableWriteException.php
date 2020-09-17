<?php

namespace acet\qcache\exception;

class TableWriteException extends TableQueryException
{
    /**
     * TableWriteException constructor.
     * @param string $table
     * @param string $sql
     * @param string $db_type
     * @param string $err_message
     */
    public function __construct($table, $sql, $db_type, $err_message='')
    {
        $err_message = "\"$table\"";

        parent::__construct("Table write error: ", $sql, $db_type, $err_message);
    }
}