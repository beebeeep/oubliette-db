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


## Query planner
Oubliette leverages classic Volcano iterator model for query execution. In the nutshell, the query execution plan consists of number of operators reading data directly from the database and producing stream of documents. Those streams can be then somehow filtered, joined or combined with each other according to the query plan, thus producing the stream of documents matching the query.

The query planner job is to take query string, written in some, presumably, human-readable format (like SQL), parse it, and then convert it into execution plan, using some dark magic of math, bold assumptions, and blatant guesswork from cost-based optimizers.
While this is a respectable area of computer science and it won't harm to try to dig in and try to reimplement what your grandfather did in 1970s during his time in IBM; I decided to do something different instead: create somewhat less-than-terrible syntax for defining execution plans on the low level and expose it to the end users. So that users can get maximum control and cannot blame database optimizer when the performance is not up to their expectations.

So here it is, the syntax:

### Simple scans:
`(scan (<predicate>))`, where `<predicate>` is lisp-like expression return true or false, for example: `(eq .foo 137)` (field `.foo` equals 137).
You can use comparison operators `ge`, `gt`, `le`, `lt`, `eq`, multi-match `in` operator, plus boolean operators `and`, `or`, and `not`.

Examples:
```
(scan ())                ; no filter - return all documents in collection
(scan (eq .foo 137))     ; return all documents where .foo equals 137
(scan                    ; return all documents where:
  (and
    (in .foo 42 137)     ; .foo is either 42 or 137 
    (eq .bar "chlos")    ; and .bar equals string "chlos"
    (gt .baz.baq 0)))`   ; and nested field with path .baz.baq is greater than zero
``` 

### Index scans
Fullscans are bad, but I've got you - you have indexes. You can query documents:
* By specific index value: `(ixscan (eq idx_foo (137))` (return all documens where field indexed by index `idx_foo` equals 137 (compound undexes will have more values, like `(137 42 "chlos")`. 
* By interval of values: `(ixscan (interval idx_foo (42) (137)))` (return all documens where indexed value is in interval [42, 137]). `interval` means closed interval, you can also use `left-interval`, `right-interval` and `open-interval` for left-open, right-open and open intervals, respectively.

Examples:
```
(ixscan (eq idx_foo_bar (137 "chlos")))
(ixscan (interval idx_foo_bar (137 "chlos") (420 "zigzag")))
```

### Filtering and combining
`scan` and `ixscan` operators are the only "producing" operators, i.e. those who can inject documents into pipeline by reading the from the DB. However, you can combine and filter them to create more complicated queries.

`(union <subplan1> <subplan2> ... <subplanN>)` simply joins the documents returned by `<subplan1>` - `<subplanN>`, iterating over each sub-plan sequentially.

`(filter <subplan> <predicate>)` filters the documents returned by `<subplan>` using `<predicate>` (predicate has the same syntax as in `scan`).

Example:
```
(filter
  (union
    (ixscan (eq  idx_foo (2)))
    (scan (eq .bar "chlos")))
  (eq .baz.baq 0))
```
