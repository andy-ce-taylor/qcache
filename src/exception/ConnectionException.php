<?php

namespace acet\qcache\exception;

class ConnectionException extends QCacheException
{
    /**
     * ConnectionException constructor.
     * @param string $message
     */
    public function __construct($message="")
    {
        parent::__construct("Connection exception: $message");
    }
}