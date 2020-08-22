<?php

namespace acet\qcache;

use Exception;

class QCacheException extends Exception
{
    public function __construct($message='')
    {
        parent::__construct("QCache: $message");
    }
}