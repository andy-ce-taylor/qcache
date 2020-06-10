<?php
namespace acet\qcache;

abstract class DbChangeDetection
{
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
        if (($table_change_times = $this->getTableChangeTimes($table_names)) === false) {

            // ToDo: implement TTL caching for use as a backup method

            // for now, we must assume that all the tables have changed
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
     * Returns the unix time of table changes.
     *
     * If $table_names is given, only those tables are checked, otherwise all.
     *
     * Returns FALSE if the information isn't available or the query fails.
     *
     * @param string[]  $table_names  - the names of tables to check (optional)
     *
     * @return int[]|false
     */
    public function getTableChangeTimes($table_names=null)
    {
        $time_offset = $this->getDbTimeOffset();

        if (is_string($table_names)) {
            $table_names = array($table_names);
        }

        $table_times = $this->getTableTimes($table_names);

        // add the db time offset to each table change time
        return array_map(
            function($time) use ($time_offset) {
                return $time + $time_offset;
            },
            $table_times
        );
    }
}