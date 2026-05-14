# Oubliette

Experimental document [layer](https://apple.github.io/foundationdb/layer-concept.html) for Foundation DB.

## Features
* Documents are serialized using MessagePack (like BSON, but better)
* HTTP API
* Emergent schema: schema evolution is automatic, the type of each field is defined by the first inserted document containing that field.
  Once defined, field types are enforced.
* Lisp-like query language: `(and (eq .foo "chlos") (gt .bar.baz 137))` (WIP)
* Alternative way to query your data: special syntax for building low-level query plans. Maximum control, minimum guesswork from CBO. 
* Secondary indexes, including compound indexes. Automatic asynchronous materialization.

## Usage
* Create the database and collection:
  ```
  curl -s -X POST --json '{}' localhost:4800/_manage/testdb/testcollection/create
  ```
* Insert some documents:
  ```
  curl -s -X PUT --json '{"docs": [{"foo": "137, "bar": true, "baz": "chlos"}]}' localhost:4800/testdb/testcollection
  ```
* Create index:
  ```
  curl -vks -X POST --json '{"name": "idx_foo", "fields": [{"field": ".foo"}]}' localhost:4800/_manage/testdb/testcollection/create_index
  ```
* Query the data:
  ```
  curl -vs -X POST --json '{"plan": "(scan ())"}' localhost:4800/testdb/testcollection
  ```
  In example above, plan `(scan ())` has no filtering expression, so it will return all documents in collection. Other examples of plan expressions are:
  - Fullscan with condition: `(scan (eq .foo 137))`, where .foo - path to field (nested field will go like .foo.bar.baz etc)
  - Index scan, simple equality: `(ixscan (eq idx_foo (137)))`, where idx_foo is the index name, and `(137)` is the index field value (several values in case of compound indexes).
  - Index scan, closed interval of values: `(ixscan (interval idx_foo (42) (137)))` - return documents where .foo (indexed field in idx_foo index) is in interval [42, 137]
  - Same, left-open interval: `(ixscan (left_interval idx_foo (42) (137)))` - (42, 137]
  - Same, right-open interval: `(ixscan (right_interval idx_foo (42) (137)))` - [42, 137)
  - Same, open interval: `(ixscan (open_interval idx_foo (42) (137)))` - (42, 137)
