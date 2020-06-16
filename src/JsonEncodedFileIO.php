<?php
namespace acet\qcache;

class JsonEncodedFileIO
{
    /**
     * @param string  $file
     * @return array
     */
    public static function readJsonEncodedArray($file)
    {
        if (file_exists($file)) {
            return json_decode(file_get_contents($file), true);
        }
        return [];
    }

    /**
     * @param string  $file
     * @param array   $info
     */
    public static function writeJsonEncodedArray($file, $info)
    {
        file_put_contents($file, json_encode($info, JSON_PRETTY_PRINT));
    }
}