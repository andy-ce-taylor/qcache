<?php
namespace acet\qcache;

/**
 * Class SqlResultSet
 * @package acet\qcache
 *
 * This class closely mimics the functionality of the result sets used by popular native RDBMSs.
 * It is primarily used to return results (whether from the cache or the database) to the caller.
 */
class SqlResultSet
{
    const RES_FMT_NUM   = 1;
    const RES_FMT_ASSOC = 2;
    const RES_FMT_BOTH  = 3;
    
    public $rows;
    public $num_rows;
    public $field_count;

    private $row_ix = 0;

    /**
     * @param array $data
     */
    public function __construct($data)
    {
        $this->rows = $data;
        $this->num_rows = count($data);
        $this->field_count = $this->num_rows ? count($data[0]) : 0;
    }

    /**
     * Seeks to an arbitrary result pointer specified by the offset in the result set.
     *
     * Equivalents for MySQL, MsSQL: mysqli_result::data_seek, mssql_data_seek
     *
     * @param int $offset   : Must be between zero and the total number of rows minus one.
     * @return bool
     */
    public function data_seek(int $offset)
    {
        if ($offset >= 0 && $offset < $this->num_rows) {
            $this->row_ix = $offset;
            return true;
        }

        return false;
    }

    /**
     * Fetches all result rows as an associative array, a numeric array, or both.
     *
     * Equivalents for MySQL, MsSQL: mysqli_result::fetch_all, sqlsrv_fetch_array
     *
     * @param int $result_type  : RES_FMT_NUM | RES_FMT_ASSOC | RES_FMT_BOTH
     * @return array
     */
    public function fetch_all(int $result_type = self::RES_FMT_NUM)
    {
        if ($this->row_ix == $this->num_rows) {
            return [];
        }

        $rows = [];

        while ($this->row_ix < $this->num_rows) {
            $rows[] = $this->fetch_array($result_type);
        }

        return $rows;
    }

    /**
     * Fetch a result row as an associative array, a numeric array, or both.
     *
     * Equivalents for MySQL, MsSQL: mysqli_result::fetch_array, sqlsrv_fetch_array
     *
     * @param int $result_type  : RES_FMT_NUM | RES_FMT_ASSOC | RES_FMT_BOTH
     * @return array|null
     */
    public function fetch_array(int $result_type = self::RES_FMT_BOTH)
    {
        if ($this->row_ix == $this->num_rows) {
            return null;
        }

        $row_assoc = $this->fetch_assoc();

        $row = [];

        if ($result_type == self::RES_FMT_NUM || $result_type == self::RES_FMT_BOTH) {
            $row = array_values($row_assoc);
        }

        if ($result_type == self::RES_FMT_ASSOC || $result_type == self::RES_FMT_BOTH) {
            $row = array_merge($row, $row_assoc);
        }

        return $row;
    }

    /**
     * Returns all values from the next row in an associative array [ 'col1'=>'val1', 'col2'=>'val2', ... ].
     *
     * Equivalents for MySQL, MsSQL: mysqli_result::fetch_assoc, sqlsrv_fetch_array
     *
     * @return array|null
     */
    public function fetch_assoc()
    {
        if ($this->row_ix == $this->num_rows) {
            return null;
        }

        return $this->rows[$this->row_ix++];
    }

    /**
     * Returns all values from the next row in a numerically indexed array [ 0=>'val1', 1=>'val2', ... ].
     *
     * Equivalents for MySQL, MsSQL: mysqli_result::fetch_row, sqlsrv_fetch_array
     *
     * @return array|null
     */
    public function fetch_row()
    {
        if ($this->row_ix == $this->num_rows) {
            return null;
        }

        return array_values($this->fetch_assoc());
    }

    /**
     * Release resources.
     *
     * Equivalents for MySQL, MsSQL: mysqli_free_result, mssql_free_result
     *
     */
    public function free_result()
    {
        $this->rows = [];
        $this->num_rows = 0;
        $this->row_ix = 0;
    }
}
