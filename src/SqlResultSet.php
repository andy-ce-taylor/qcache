<?php
namespace acet\qcache;

class SqlResultSet
{
    public $rows;
    public $num_rows;
    public $field_count;

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
     * @return array|null
     */
    public function fetch_row()
    {
        if ($this->num_rows == 0) {
            return null;
        }

        $row = $this->rows[--$this->num_rows];

        return array_values($row);
    }

    /**
     * @return array|null
     */
    public function fetch_assoc()
    {
        if ($this->num_rows == 0) {
            return null;
        }

        $row = $this->rows[--$this->num_rows];

        return $row;
    }

    /**
     * @param int $type
     * @return array|null
     */
    public function fetch_array($type=MYSQLI_BOTH)
    {
        if ($this->num_rows == 0) {
            return null;
        }

        $row = $this->rows[--$this->num_rows];

        $ret = [];
        if ($type == MYSQLI_NUM || $type == MYSQLI_BOTH) {
            $ret = array_values($row);
        }

        if ($type == MYSQLI_ASSOC || $type == MYSQLI_BOTH) {
            $ret = array_merge($ret, $row);
        }

        return $ret;
    }

    /**
     * @param int $type
     * @return array|null
     */
    public function fetch_all($type=MYSQLI_BOTH)
    {
        if ($this->num_rows == 0) {
            return null;
        }

        $rows = [];

        while ($this->num_rows) {
            $rows[] = $this->fetch_array($type);
        }

        return $rows;
    }

    /**
     * Release resources.
     */
    public function free_result()
    {
        $this->rows = [];
        $this->num_rows = 0;
    }
}
