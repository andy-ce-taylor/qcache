<?php
namespace acet\qcache;

class serializedDataFileIO
{
    /**
     * @param string  $file
     * @return mixed
     */
    public static function read($file)
    {
        if (file_exists($file))
            if ($contents = file_get_contents($file))
                return unserialize($contents);

        return null;
    }

    /**
     * @param string  $file
     * @param mixed   $data
     */
    public static function write($file, $data)
    {
        file_put_contents($file, serialize($data));
    }
}