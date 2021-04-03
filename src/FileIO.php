<?php
namespace acet\qcache;

class FileIO
{
    /**
     * @param string  $file
     * @param bool    $compressed
     * @param bool    $serialized
     * @return mixed
     */
    public static function read($file, $compressed, $serialized)
    {
        if (file_exists($file)) {

            if ($data = file_get_contents($file)) {

                if ($compressed) {
                    $data = gzinflate($data);
                }

                if ($serialized) {
                    $data = unserialize($data);
                }

                return $data;
            }
        }

        return null;
    }

    /**
     * @param string  $file
     * @param mixed   $data
     * @param int     $gz_compression_level
     * @param bool    $serialize
     */
    public static function write($file, $data, $gz_compression_level = 0, $serialize = false)
    {
        self::preProcess($data, $gz_compression_level, $serialize);

        file_put_contents($file, $data, LOCK_EX);
    }

    /**
     * @param string  $file
     * @param mixed   $data
     * @param int     $gz_compression_level
     * @param bool    $serialize
     */
    public static function append($file, $data, $gz_compression_level = 0, $serialize = false)
    {
        self::preProcess($data, $gz_compression_level, $serialize);

        file_put_contents($file, $data . "\n", FILE_APPEND | LOCK_EX);
    }

    /**
     * @param mixed  $data
     * @param int    $gz_compression_level
     * @param bool   $serialize
     */
    private static function preProcess(&$data, $gz_compression_level, $serialize)
    {
        if ($serialize) {
            $data = serialize($data);
        }

        if ($gz_compression_level) {
            $data = gzdeflate($data, $gz_compression_level);
        }
    }
}