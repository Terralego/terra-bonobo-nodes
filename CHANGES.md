
0.5.6 / 2020-12-17
==================

  * Switch pyfiles to bygfiles

0.5.5 / 2020-12-16
==================

  * Add VERSION.md to package


0.5.4 / 2020-12-16
==================

  * Add changelog to package


0.5.3 / 2020-12-16
==================

  * Add node to get the min value of an array
  * Reverse TransitTimeOneToMany for deals with oneway on motorway

0.5.2 / 2020-12-08
==================

  * Remove version fix


0.5.1 / 2020-09-24
==================

  * Fix gitlab-ci
  * fix es test
  * fix linting

0.5.0 / 2020-09-23
==================

## /!\ Breaking changes:

Compatible with last version of elasticsearch (i.e. 7.9.1)

## Fixes

  * Fix elasticsearch python lib version
  * Fix more warnings
  * Removing f-string without placeholders for string
  * Fixing ambiguous variable name
  * include type name

0.4.0 / 2020-01-09
==================

### Improves

* Index name from Option used instead of an hardcoded one

0.3.9 / 2019-11-26
==================

### Fixes

* Fix and improve runlevel manegement of nodes that
  uses context and buffer
* SubDivideGeom take only valid geometries

0.3.8 / 2019-11-26
==================

### Fixes

* Still fix runlevel when importing data

0.3.7 / 2019-11-25
==================

### Fixes

* Fix runlevel when importing data

0.3.6 / 2019-11-22
==================

### Fixes

* When doing a bulk import explicit the used layer

0.3.5 / 2019-11-22
==================

### Fixes

* Fixes runlevel on LoadFeatureInLayer node

0.3.4 / 2019-11-21
==================

### Improve

* Improve documentation

0.3.3 / 2019-11-14
==================

### Improve

* Do not pop identifier when extracting SQL data

0.3.2 / 2019-11-13
==================

### Improve

* Setup CI pipeline and fix all tests

0.3.1 / 2019-11-13
==================

### Improves

* Improve nodes documentation

### Fixes

* Do not drop original data using IdentifierFromProperty node
