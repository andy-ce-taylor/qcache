<?php

namespace acet\qcache\connector;

/**
 * @method getTableTimes($db_connection_cache, string[] $table_names)
 * @method getDbTimeOffset()
 */
abstract class DbChangeDetectionAbs
{
    /**
     * Returns TRUE if any tables have changed since the given unix epoch time.
     *
     * If $table_names is given, only those tables are checked, otherwise all.
     *
     * Assumes table changes if the information isn't available or the query fails.
     *
     * @param int       $since        - unix time to test against
     * @param string[]  $table_names  - the names of tables to check (optional)
     * @param mixed     $db_connection_cache
     * @return bool
     */
    public function detectTableChanges($since, $table_names, $db_connection_cache)
    {
        if (($table_change_times = $this->getTableChangeTimes($table_names, $db_connection_cache)) === false) {
            // information isn't available or query failed
            return true;
        }

        foreach ($table_change_times as $table => $change_time) {
            if ($change_time > $since) {
                return true;
            }
        }

        return false;
    }

    /**
     * Returns the names of tables that have changed since the given unix time.
     *
     * If $table_names is given, only those tables are checked, otherwise all.
     *
     * Returns the pertinent array of $table_names (all, or those specified) if
     * the information isn't available or the query fails.
     *
     * @param int       $since        - unix time to test against
     * @param string[]  $table_names  - the names of tables to check
     * @param mixed     $db_connection_cache
     *
     * @return string[]
     */
    public function getChangedTables($since, $table_names, $db_connection_cache)
    {
        // can it be assumed that all tables have changed?
        if (($table_change_times = $this->getTableChangeTimes($table_names, $db_connection_cache)) === false) {
            return $table_names;
        }

        $changed_tables = [];

        foreach ($table_change_times as $table => $change_time) {
            if ($change_time > $since) {
                $changed_tables[] = $table;
            }
        }

        return $changed_tables;
    }

    /**
     * Returns the unix change time of the given tables.
     *
     * Returns FALSE if the information isn't available.
     *
     * @param string[]  $table_names  - the names of tables to check
     * @param mixed     $db_connection_cache
     *
     * @return int[]|false
     */
    public function getTableChangeTimes($table_names, $db_connection_cache)
    {
        static $table_times_l1c = [];

        // process tables whose times have not been cached locally
        if ($uncached_table_names = array_keys(array_diff_key(array_flip($table_names), $table_times_l1c))) {

            if (($table_times = $this->getTableTimes($db_connection_cache, $uncached_table_names)) === false) {
                return false;
            }

            // add the db time offset to each table change time
            $time_offset = $this->getDbTimeOffset();

            foreach ($table_times as $k => &$t) {
                $table_times_l1c[$k] = $t = $t + $time_offset;
            }

        } else {
            $table_times = array_intersect_key($table_times_l1c, array_flip($table_names));
        }

        return $table_times;
    }
}