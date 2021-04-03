<?php
/**
 * @noinspection SqlNoDataSourceInspection
 * @noinspection SqlDialectInspection
 */

namespace acet\qcache;

class QcacheUtils
{
    /**
     * Returns a description suitable for passing to Qcache::query.
     *
     * @param string $file  - use __FILE__
     * @param string $func  - use __FUNCTION__
     * @param string $line  - use __LINE__
     * @return string       - e.g. "my_file.php[123]::MyClass::myMethod"
     */
    public static function getDescription($file, $func, $line)
    {
        $class = basename($file, '.php');
        return "{$file}[$line]::$class::$func";
    }

    /**
     * Cleans and returns the given SQL statement.
     * Returns FALSE if it doesn't look like a SELECT statement.
     *
     * @param string $stmt
     * @return string|bool
     */
    public static function getCleanedSelectStmt($stmt)
    {
        // trim and remove tabs/newlines/carriage-returns
        $cleaned_stmt = str_replace(["\t", "\n", "\r"], ' ', trim($stmt));

        // return FALSE if not a SELECT statement
        if (substr(strtoupper($cleaned_stmt), 0, 7) != 'SELECT ') {
            return false;
        }

        return $cleaned_stmt;
    }

    /**
     * Examines the SQL statement and determines whether Qcache would be successful in parsing table names from the
     * given SQL statement (returns TRUE) or whether table names should be supplied as an argument (returns FALSE).
     *
     * Note: Qcache can handle ALL select statements.
     *       This function merely determines whether it needs the developer to supply table name hints.
     *
     * @param string  $stmt
     * @param string  $hash
     * @return bool
     */
    public static function isCacheable($stmt, $hash = '')
    {
        static $cacheable_l1c = [];

        if (!$hash) {
            $hash = hash('md5', $stmt);
        }

        // is the answer already known?
        if (array_key_exists($hash, $cacheable_l1c))
            return $cacheable_l1c[$hash];

        $stmt_lc = strtolower(trim($stmt));

        // Qcache can handle this if the statement...
        return $cacheable_l1c[$hash] =
            substr($stmt_lc, 0, 7) == 'select ' &&   // is a SELECT
            strpos($stmt_lc, ' from ', 7)       &&   // and has a FROM clause
            !strpos($stmt_lc, ' select ', 7);        // and doesn't have an embedded SELECT (unsupported)
    }

    /**
     * Returns the names of all tables participating in the given SELECT statement.
     *
     * @param string  $stmt
     * @param string  $hash
     * @return string[]|bool
     */
    public static function findTableNames($stmt, $hash = '')
    {
        if (!self::isCacheable($stmt, $hash)) {
            return false;
        }

        // work in lowercase
        $stmt_lc = strtolower($stmt);

        // expect table names between FROM and... [first JOIN | LIMIT | WHERE] (whichever is first)
        $tables_str = '';

        // find JOINed tables
        $first_join_pos = false;
        $join_p = $from = strpos($stmt_lc, ' from ') + 6;
        while ($join_p = strpos($stmt_lc, ' join ', $join_p)) {
            if (!$first_join_pos) {
                $tables_str = trim(substr($stmt, $from, $join_p - $from)) . ',';
                $first_join_pos = $join_p;
            }
            $join_p += 6;
            $on_p = strpos($stmt_lc, ' on ', $join_p);
            $tables_str .= substr($stmt, $join_p, $on_p - $join_p) . ',';
        }

        // if no luck with JOINs, find WHERE/LIMIT
        if (!$first_join_pos) {
            if (($where_p = strpos($stmt_lc, ' where ', $from)) === false) {
                $where_p = PHP_INT_MAX;
            }

            if (($limit_p = strpos($stmt_lc, ' limit ', $from)) === false) {
                $limit_p = PHP_INT_MAX;
            }

            if ($first_p = min($where_p, $limit_p)) {
                $tables_str = trim(substr($stmt, $from, $first_p - $from));
            }
        } else {
            $tables_str = trim($tables_str, ', ');
        }

        // split $tables_str into individual tables
        $tables_str = trim(str_replace(["'","`",'"'], '', $tables_str));

        $tables = [];
        foreach (explode(',', $tables_str) as $str) {
            if ($p = strpos($str = trim($str), ' ')) {
                $str = substr($str, 0, $p);
            }

            $tables[] = $str;
        }

        if ($tables) {
            return $tables;
        }

        // Oops! - SELECT with no tables found
        return false;
    }
}