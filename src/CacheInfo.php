<?php
/** @noinspection SqlNoDataSourceInspection */

namespace acet\qcache;

class CacheInfo extends QcacheUtils
{
    /** @var bool */
    private bool $from_db;

    /** @var string */
    private string $table;

    /** @var string */
    private string $folder;

    /** @var mixed */
    private $db_conn;

    /** @var string */
    private string $conn_sig;

    /**
     * Qcache constructor.
     *
     * @param bool $from_db
     * @param string $table
     * @param mixed $db_conn
     * @param string $conn_sig
     * @param string $folder
     */
    public function __construct(bool $from_db, string $table, $db_conn, string $conn_sig, string $folder)
    {
        $this->from_db = $from_db;
        $this->table = $table;
        $this->db_conn = $db_conn;
        $this->conn_sig = $conn_sig;
        $this->folder = $folder;
    }

    /**
     * Returns the specified record of cache information from the database or filesystem (according to settings).
     * Returns NULL if no such record exists.
     *
     * @param string $hash
     * @param string $cache_info_file
     * @return array|null
     */
    public function getCacheInfoRecord(string $hash, string $cache_info_file)
    {
        if ($this->from_db) {
            // retrieve cache info from database
            $columns = Constants::CACHE_INFO_COLUMNS;
            if ($cache_info = $this->db_conn->read(
                "SELECT $columns FROM $this->table WHERE hash='$hash'",
                false
            )) {
                return array_values($cache_info[0]);
            }
            return null;
        }

        // retrieve cache info from filesystem
        return $cache_info = FileIO::read($cache_info_file, false, true);
    }

    /**
     * Stores cache information to the database or filesystem (according to settings).
     *
     * @param string $upsert      - 'insert' | 'update'
     * @param string $hash
     * @param array $data
     * @param string $tables_csv
     * @param string $cache_info_file
     */
    public function storeCacheInfoRecord(string $upsert, string $hash, array $data, string $tables_csv, string $cache_info_file)
    {
        [$access_time, $av_microsecs, $impressions, $importance, $description] = $data;

        if ($this->from_db) {

            if ($upsert == 'insert') {
                $columns = Constants::CACHE_INFO_COLUMNS;
                $description_esc = $this->db_conn->escapeString($description);

                $this->db_conn->write(
                    "INSERT INTO $this->table (hash, $columns) " .
                    "VALUES ('$hash', $access_time, $av_microsecs, $impressions, $importance, $description_esc, '$tables_csv')"
                );

            } else { // $upsert == 'update'
                $this->db_conn->write(
                    "UPDATE $this->table ".
                    "SET access_time=$access_time,".
                    "av_microsecs=$av_microsecs,".
                    "impressions=$impressions,".
                    "importance=$importance ".
                    "WHERE hash='$hash'"
                );
            }

            return;
        }

        // store in filesystem
        FileIO::write(
            $cache_info_file,
            [$access_time, $av_microsecs, $impressions, $importance, $description, $tables_csv],
            0, true
        );
    }

    /**
     * Returns the current number of cache assets.
     *
     * @return int
     */
    public function getNumCacheAssets() :int
    {
        $num_assets = 0;

        foreach (array_diff(scandir($this->folder), ['.', '..']) as $file) {
            if (substr($file, -3) == Constants::CACHE_FILE_EXT) {
                $num_assets++;
            }
        }

        return $num_assets;
    }

    /**
     * Returns all cache information from the database or filesystem (according to settings).
     *
     * @return array
     */
    public function getCacheInfoRecords() :array
    {
        $cache_info_recs = [];

        if ($this->from_db) {
            // cache stored in database
            $columns = Constants::CACHE_INFO_COLUMNS;

            foreach ($this->db_conn->read("SELECT hash,$columns FROM $this->table", false) as $row) {
                $cache_info_recs[$row['hash']] = array_slice(array_values($row), 1);
            }

        } else {
            // cache stored in filesystem
            $folder = $this->folder;
            $sig_slen = strlen($this->conn_sig);

            foreach (array_diff(scandir($folder), ['.', '..']) as $file) {
                if (substr($file, -3) == Constants::CACHE_INFO_FILE_EXT) {
                    if ($cache_info = FileIO::read("$folder/$file", false, true)) {
                        $hash = substr($file, $sig_slen, -4);
                        $cache_info_recs[$hash] = $cache_info;
                    }
                }
            }
        }

        return $cache_info_recs;
    }

    /**
     * Returns all cache hashes from the database or filesystem (according to settings).
     *
     * @return string[]
     */
    public function getCacheInfoHashes() :array
    {
        $cache_info_hashes = [];

        if ($this->from_db) {
            foreach ($this->db_conn->read("SELECT hash FROM $this->table", false) as $row) {
                $cache_info_hashes[] = $row['hash'];
            }

        } else {
            // cache stored in filesystem
            $sig_slen = strlen($this->conn_sig);

            foreach (array_diff(scandir($this->folder), ['.', '..']) as $file) {
                if (substr($file, -3) == Constants::CACHE_INFO_FILE_EXT) {
                    // the 32 char hash appears directly after the sig
                    $cache_info_hashes[] = substr($file, $sig_slen, 32);
                }
            }
        }

        return $cache_info_hashes;
    }
}
