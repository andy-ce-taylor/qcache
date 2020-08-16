# Change Log

### 1.1.4
- Selective caching to either DB or file according to performance constraints.
- Auto rebuild tables on schema change.
- Improved T-SQL support.
- Various optimizations.

### 1.1.3
- QCache to create it's own tables.
- Refactored various sections of code.

### 1.1.2
- Fixed improperly rendered string.

### 1.1.1
- Implemented function clearCache.

### 1.1.0
- Non-file based caching.

### 1.0.12
- Minor typos fixed.

### 1.0.11
- General improvements.
- Improvements to SqlResultset.
- Added methods cacheable() & getTables().

### 1.0.10
- Removed blank lines from log file.

### 1.0.9
- Limit the size of the log file.
- Improved ms converter.

### 1.0.8
- Improved css for simple cache monitor.

### 1.0.7
- Further enhancements.

### 1.0.6
- Improvements to the simple monitor.

### 1.0.5
- Further improvements.

### 1.0.4
- Improvements to the simple monitor.
- Fixed potential null return value problem.

### 1.0.3
- Improved monitoring and fixed an error in performance stats capture.

### 1.0.2
- Added missing function 'rmdir_plus'

### 1.0.1
- Added composer dependancy 'ext-json'.

### 1.0.1
- Added milliseconds columns for both cached and db accesses.

### 1.0.0
- Improvements to the simple cache monitor.

### 0.0.4
- Added a simple cache monitor.

### 0.0.3
- Implemented method refreshCaches (disabled until further tests completed).
- Added method to clear caches and a stub for refreshing caches which can be called during housekeeping.

### 0.0.2
- Empty resultset returns empty array (instead of null).

### 0.0.1
- Improved exception handling.
- Fixed db table times cache file write issue.

### 0.0.0
- Initial release.
