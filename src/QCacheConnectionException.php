<?php

namespace acet\qcache;

class QCacheConnectionException extends QCacheException
{
    public function __construct($message="")
    {
        parent::__construct("Connection exception: $message");
    }
}