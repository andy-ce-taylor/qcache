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
            if ($content = file_get_contents($file)) {
                return json_decode($content, true);
            }
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