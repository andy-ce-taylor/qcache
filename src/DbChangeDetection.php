<?php
namespace acet\qcache;

abstract class DbChangeDetection
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
     *
     * @return bool
     */
    public function haveTablesChanged($since, $table_names=[])
    {
        if (($table_change_times = $this->getTableChangeTimes($table_names)) === false) {
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
     * @param string[]  $table_names  - the names of tables to check (optional)
     *
     * @return string[]
     */
    public function getChangedTables($since, $table_names=null)
    {
        // can it be assumed that all tables have changed?
        if (($table_change_times = $this->getTableChangeTimes($table_names)) === false)
            return $table_names;

        $changed_tables = [];

        foreach ($table_change_times as $table => $change_time)
            if ($change_time > $since)
                $changed_tables[] = $table;

        return $changed_tables;
    }

    /**
     * Returns the unix change time of the given tables.
     *
     * Returns FALSE if the information isn't available.
     *
     * @param string[]  $table_names  - the names of tables to check
     *
     * @return int[]|false
     */
    public function getTableChangeTimes($table_names)
    {
        static $table_times_l1c;

        if (!$table_times_l1c) {
            if (($table_times_l1c = $this->getTableTimes()) === false)
                return false;

            // add the db time offset to each table change time
            $time_offset = $this->getDbTimeOffset();
            foreach ($table_times_l1c as &$t)
                $t += $time_offset;
        }

        return array_intersect_key($table_times_l1c, array_flip($table_names));
    }
}