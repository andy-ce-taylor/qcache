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
$conn = new DbConnectorMySQL($host, $username, $password, $db_name);

$qcache_enabled = true;

$qc = new QCache($conn, "/tmp/qcache", $qcache_enabled);

$affected_tables = ["my_table"];

$results = $this->qc->query("SELECT * FROM my_table", $affected_tables);

```
