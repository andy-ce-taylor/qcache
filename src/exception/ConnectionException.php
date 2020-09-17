<?php

namespace acet\qcache\exception;

class ConnectionException extends QCacheException
{
    /**
     * ConnectionException constructor.
     * @param string $db_type
     */
    public function __construct($db_type)
    {
        parent::__construct("Unable to open $db_type connection");
    }
}