# Qcache

#### Database query caching

Qcache is a query caching library which works with MySQL, MSSQL and SQLite databases.

PHP scripts spend much of their time querying databases using SELECT statements. Qcache can
replace those calls, making time-hungry database operations perform faster and more efficiently.

#### How it works

Qcache checks the tables used in a SELECT statement to see whether anything has changed since
it last looked (_rows added_/_updated_/_dropped_).

If a change is detected or there is no cache entry corresponding to the SQL statement, the
database is queried, the time is recorded and the result set is cached before being passed back
to the calling program.

If no change is detected, there is no need for the database to repeat the operation using the
exact same query and producing an identical result set, so a cached result can be returned to the
calling program.

The result (whether from the cache or the database query) is passed back to the caller in object
`Qcache::SqlResultSet` which mimics the functionality of the result sets used by most native
databases (`fetch_row()`, `fetch_assoc()` etc.).

#### Requirements
Qcache requires the following:

* PHP 7.1+

#### Installation
Qcache is installed via Composer.

Run the following to use the latest stable version:
```
    composer require acet/qcache
```    
or if you want the latest master version:
```
    composer require acet/qcache:dev-master
```
You can also manually edit your composer.json file:
```
    {
        "require": {
           "acet/Qcache": "*"
        }
    }
```

#### Example
```
```

