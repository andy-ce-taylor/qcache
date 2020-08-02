<?php
namespace acet\qcache;

class JsonEncodedFileIO
{
    /**
     * @param string  $file
     * @return mixed
     */
    public static function read($file)
    {
        if (file_exists($file)) {
            if ($content = file_get_contents($file)) {
                return json_decode($content, true);
            }
        }
        return null;
    }

    /**
     * @param string  $file
     * @param mixed   $info
     */
    public static function write($file, $info)
    {
        file_put_contents($file, json_encode($info, JSON_PRETTY_PRINT));
    }
}