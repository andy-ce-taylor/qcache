<?php
namespace acet\qcache;

interface DbConnectorInterface
{
    /**
     * Returns the difference (in seconds) between database timestamps and the current system time.
     *
     * @return int
     */
    public function getDbTimeOffset();

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
     * @param string  $sql
     * @return array
     */
    public function processQuery($sql);

    /**
     * Returns the change times for the given tables.
     *
     * @param string[]  $tables
     * @return int[]|false
     */
    public function getTableTimes($tables);
}