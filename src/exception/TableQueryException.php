<?php

namespace acet\qcache\exception;

class TableQueryException extends QCacheException
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
        $db_type = strtoupper($db_type);

        if ($err_message)
            $err_message = "\"$err_message\" ";

        parent::__construct("[$db_type] $description " . $err_message . "with query \"$sql\"");
    }
}