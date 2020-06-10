<?php
namespace acet\qcache;

class SerializedFileIO
{
    /**
     * @param string  $file
     * @return mixed|null
     */
    public static function readSerializedArray($file)
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
    public static function writeSerializedArray($file, $info)
    {
        file_put_contents($file, json_encode($info, JSON_PRETTY_PRINT));
    }
}