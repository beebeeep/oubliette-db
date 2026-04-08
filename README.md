# Oubliette

Experimental document [layer](https://apple.github.io/foundationdb/layer-concept.html) for Foundation DB.

## Features
* Lisp-like query language: `(and (eq .foo "chlos") (gt .bar.baz 137))`
* Secondary indexes
* Emergent schema: schema evolution is automatic, the type of each field is defined by the first inserted document containing that field.
  Once defined, field types are enforced.
