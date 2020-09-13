<?php

namespace acet\qcache\connector;

use acet\qcache\exception as QcEx;
use acet\qcache\SqlResultSet;

interface DbConnectorInterface
{
    /**
     * @param string $str
     * @return string
     */
    public function escapeString($str);

    /**
     * @param mixed $data
     * @return mixed
     */
    public function escapeBinData($data);

    /**
     * Returns the difference (in seconds) between database timestamps and the current system time.
     *
     * @return int
     */
    public function getDbTimeOffset();

    public function dbUsesCachedUpdatesTable();

    /**
     * Returns the database engine specific SQL command which will be used to produce an
     * array of records with specific fields (name/value pairs).
     * If $selector or $selector_values is empty, all records are returned.
     *   Analogous to "SELECT field1, field2 FROM source"
     * Otherwise, $selector and $selector_values are used to filter records.
     *   Analogous to "SELECT field1, field2 FROM source WHERE selector IN selector_values"
     *
     * @param string           $source
     * @param string[]|string  $fields - For all fields, use '*'
     * @param string           $selector
     * @param string[]         $selector_values
     * @param int              $limit     - 0 = no limit
     *
     * @return string
     */
    public function prepareSimpleSQL($source, $fields, $selector, $selector_values, $limit=0);

    /**
     * Process a table read request, such as SELECT, and return the response.
     * @param string  $sql
     * @param bool    $return_resultset
     * @return SqlResultSet|array
     */
    public function read($sql, $return_resultset=true);

    /**
     * @param string  $sql
     * @return bool
     */
    public function write($sql);

    /**
     * Delete all rows from the given table.
     *
     * @param string $table
     * @return bool
     */
    public function truncateTable($table);

    /**
     * Process multiple queries.
     * @param string $sql
     * @return bool
     * @throws QcEx\TableWriteException
     */
    public function multi_write($sql);

    /**
     * @param mixed $resultset
     * @return bool
     */
    public function freeResultset($resultset);

    /**
     * Returns the change times for the given tables.
     *
     * @param string[]  $tables
     * @return int[]|false
     */
    public function getTableTimes($tables);

    /**
     * Return TRUE if the given table exists, otherwise FALSE.
     *
     * @param string $schema
     * @param string $table
     * @return bool
     */
    public function tableExists($schema, $table);

    /**
     * Returns SQL to create the cache table.
     * @param string  $table_name
     * @return string
     */
    public function getCreateTableSQL_cache($table_name);

    /**
     * Returns SQL to create the logs table.
     * @param string  $table_name
     * @return string
     */
    public function getCreateTableSQL_logs($table_name);

    /**
     * Returns SQL to create the update times table.
     * @param string  $table_name
     * @return string
     */
    public function getCreateTableSQL_table_update_times($table_name);

    /**
     * Returns the primary keys for the given table.
     * @return string[]
     */
    public function getPrimary($table);

    /**
     * Returns the names of all external tables.
     * @return string[]
     */
    public function getTableNames();

    /**
     * Returns the names of all columns in the given external table.
     * @return string[]
     */
    public function getColumnNames($table);
}