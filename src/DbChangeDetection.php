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

            // for now, we must assume that all tables have changed
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
        static $table_times_l1c = [];

        $time_offset = $this->getDbTimeOffset();

        if (is_string($table_names)) {
            $table_names = array($table_names);
        }

        $ret = [];

        foreach ($table_names as $k => $name) {
            if (array_key_exists($name, $table_times_l1c)) {
                $ret[$name] = $table_times_l1c[$name];
                unset($table_names[$k]);
            }
        }

        if (!empty($table_names)) {
            $table_times = $this->getTableTimes($table_names);

            // add the db time offset to each table change time
            $table_times = array_map(
                function ($time) use ($time_offset) {
                    return $time + $time_offset;
                },
                $table_times
            );

            $table_times_l1c = array_merge($table_times_l1c, $table_times);
            $ret = array_merge($ret, $table_times);
        }

        return $ret;
    }
}