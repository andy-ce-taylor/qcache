# QCache

#### Database query caching

#### Requirements
QCache requires the following:

* PHP 5.4+, PHP 7+

#### Installation
QCache is installed via Composer. To add a dependency to QCache in your project, either

Run the following to use the latest stable version

    composer require acet/qcache
or if you want the latest master version

    composer require acet/qcache:dev-master
You can also manually edit your composer.json file

{
    "require": {
       "acet/qcache": "*"
    }
}

#### Example
```

$max_cache_files = 1000;

qc = new QCache(
    'mysql',
    $host_name, $user_name, $password, $database_name,
    '/path_to/my_cache_folder',
    $max_cache_files
);

$sql = "SELECT * FROM contact";
$tables = ['contact'];
$description = "An optional description";

/** SqlResultSet $result */
$result = $this->qc->query($sql, $tables, __FILE__, __LINE__, $description);

while ($row = $result->fetch_assoc()) {
    ...
}

$result->free_result();
```
