<?php

namespace acet\qcache\exception;

class ConnectionException extends QCacheException
{
    /**
     * ConnectionException constructor.
     * @param string $db_type
     * @param string $details
     */
    public function __construct($db_type, $details='')
    {
        parent::__construct("Unable to open $db_type connection.{$details}");
    }
}