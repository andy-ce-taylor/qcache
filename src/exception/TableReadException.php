<?php

namespace acet\qcache\exception;

class TableReadException extends TableQueryException
{
    /**
     * TableReadException constructor.
     * @param string $table
     * @param string $sql
     * @param string $db_type
     * @param string $err_message
     */
    public function __construct($table, $sql, $db_type, $err_message='')
    {
        if (stripos($err_message, "The user does not have permission") !== false)
            $err_message = "Permission denied for table \"$table\"";

        else $err_message = "\"$table\"";

        parent::__construct("Table read error: ", $sql, $db_type, $err_message);
    }
}