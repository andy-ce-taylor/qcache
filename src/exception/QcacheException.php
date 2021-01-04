<?php

namespace acet\qcache\exception;

use Exception;

class QcacheException extends Exception
{
    public function __construct($message='')
    {
        parent::__construct("Qcache: $message");
    }
}