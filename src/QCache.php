<?php
/**
 * @noinspection SqlNoDataSourceInspection
 * @noinspection SqlDialectInspection
 */

namespace acet\qcache;

use Exception;

class QCache extends QCacheUtils
{
    /** @var bool */
    private $qcache_enabled;

    /** @var mixed */
    private $db_connection;

    /**
     * @param string  $db_type
     * @param string  $db_host
     * @param string  $db_user
     * @param string  $db_pass
     * @param string  $db_name
     * @param string  $temp_dir
     * @param bool    $qcache_enabled
     * @throws QCacheConnectionException
     */
    function __construct($db_type, $db_host, $db_user, $db_pass, $db_name, $temp_dir, $qcache_enabled=true) {
        if (empty($db_type) ||
            empty($db_host) ||
            empty($db_user) ||
            empty($db_pass) ||
            empty($db_name))
            throw new QCacheConnectionException("Missing database connection details");

        $this->qcache_enabled = $qcache_enabled;
        $this->db_connection = self::getConnection($db_type, $db_host, $db_user, $db_pass, $db_name, $temp_dir);
    }

    /**
     * @param string  $sql
     * @param mixed   $tables       - array of table names, or a tables csv string, or null
     * @param string $description
     *
     * @return SqlResultSet|false
     * @throws Exception
     */
    public function query($sql, $tables = null, $description = '')
    {
        if (!$this->qcache_enabled ||
            strpos($sql, ' FROM qc_') ||
            strpos($sql, ' INTO qc_') ||
            strpos($sql, 'UPDATE qc_') !== false)
            return false;

        $start_nanosecs = hrtime(true);
        $time_now = time();

        $hash = hash('md5', $sql = trim($sql));

        $sql_get_cache = "SELECT access_time, av_nanosecs, impressions, tables_csv, data FROM qc_cache WHERE hash='$hash'";

        if ($fileinfo = $this->db_connection->processQuery($sql_get_cache)) {
            
            // SQL statement has been seen before
            
            [$access_time, $av_nanosecs, $impressions, $tables_csv, $data] = array_values($fileinfo[0]);

            // check whether tables have changed since last access time
            if ($this->db_connection->haveTablesChanged($access_time, explode(',', $tables_csv))) {

                // Cache is stale

                // perform a fresh query and update cache
                $start_nanosecs = hrtime(true); // restart nanosecond timer
                $resultset = new SqlResultSet($this->db_connection->processQuery($sql));
                $elapsed_nanosecs = hrtime(true) - $start_nanosecs;

                $av_nanosecs = (float)($elapsed_nanosecs + $av_nanosecs * $impressions++) / $impressions;
                $data = $this->db_connection->sql_escape_string(serialize($resultset));

                $sql = "UPDATE qc_cache " .
                       "SET access_time=$access_time, av_nanosecs=$av_nanosecs, impressions=$impressions, data='$data' WHERE hash='$hash'";
                $this->db_connection->processUpdate($sql);

                $sql = "INSERT INTO qc_log (time, context, nanosecs, hash) " .
                       "VALUES ($time_now, 'db', $elapsed_nanosecs, '$hash')";
                $this->db_connection->processUpdate($sql);

                return $resultset;
            }

            // Cache is fresh - return a quick result from cache
            $resultset = unserialize($data);
            $elapsed_nanosecs = hrtime(true) - $start_nanosecs;

            $sql = "INSERT INTO qc_log (time, context, nanosecs, hash) VALUES ($time_now, 'qc', $elapsed_nanosecs, '$hash')";
            $this->db_connection->processUpdate($sql);

            return $resultset;
        }

        // previously unseen SQL statement
        
        if (is_null($tables))
            if (($tables = QCacheUtils::getTables($sql)) == false)
                throw new Exception("Bad SELECT statement");

        $tables_csv = is_array($tables) ? implode(',', $tables) : $tables;

        $start_nanosecs = hrtime(true); // restart nanosecond timer
        $resultset = new SqlResultSet($this->db_connection->processQuery($sql));
        $elapsed_nanosecs = hrtime(true) - $start_nanosecs;

        $description = $this->db_connection->sql_escape_string($description);
        $script = $this->db_connection->sql_escape_string($sql);
        $data = $this->db_connection->sql_escape_string(serialize($resultset));
        
        $sql = "INSERT INTO qc_cache (hash, access_time, script, av_nanosecs, impressions, description, tables_csv, data) " .
               "VALUES ('$hash', $time_now, '$script', $elapsed_nanosecs, 1, '$description', '$tables_csv', '$data')";
        $this->db_connection->processUpdate($sql);

        $sql = "INSERT INTO qc_log (time, context, nanosecs, hash) VALUES ($time_now, 'db', $elapsed_nanosecs, '$hash')";
        $this->db_connection->processUpdate($sql);

        return $resultset;
    }

    /**
     * @param string  $db_type
     * @param string  $db_host
     * @param string  $db_user
     * @param string  $db_pass
     * @param string  $db_name
     * @param string  $temp_dir
     * @return DbConnectorMySQL|DbConnectorMSSQL
     * @throws QCacheConnectionException
     */
    public static function getConnection($db_type, $db_host, $db_user, $db_pass, $db_name, $temp_dir)
    {
        switch ($db_type) {
            case 'mysql':
                return new DbConnectorMySQL($db_host, $db_user, $db_pass, $db_name);

            case 'mssql':
                return new DbConnectorMSSQL($db_host, $db_user, $db_pass, $db_name, $temp_dir);
        }

        // whoops! unsupported database type
        return null;
    }

    /**
     * @return DbConnectorMySQL|DbConnectorMSSQL
     */
    public function getDbConnection()
    {
        return $this->db_connection;
    }
}