# QCache

#### Database query caching

QCache is a query caching library which works with MySQL and MSSQL databases (more to follow).

PHP scripts spend much of their time querying databases using SELECT statements. QCache can
replace those calls, making time-hungry database operations perform faster and more efficiently.

QCache checks the tables used in a SELECT statement to see whether anything has changed since
it last looked (_rows added_/_updated_/_dropped_).

If a change is detected or there is no saved cache file corresponding to the SQL statement, the
database is queried, the time is recorded and the result set is cached before being passed back
to the calling program.

If no changes are detected, there is no need for the database to repeat the operation using the
exact same query and producing an identical result set) so a cached result is returned to the
calling program.

The result (whether from the cache or the database) is passed back to the caller in object
`QCache::SqlResultSet` which mimics the functionality of the result sets used by most native
databases (`fetch_row()`, `fetch_assoc()` etc.).

#### Requirements
QCache requires the following:

* PHP 5.4+, PHP 7+

#### Installation
QCache is installed via Composer.

Run the following to use the latest stable version:
```
    composer require acet/QCache
```    
or if you want the latest master version:
```
    composer require acet/QCache:dev-master
```
You can also manually edit your composer.json file:
```
    {
        "require": {
           "acet/QCache": "*"
        }
    }
```

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
