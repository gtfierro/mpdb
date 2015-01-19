## MPDB

The MsgPack DataBase is a collection-based key-value store designed to be used
by embedded clients with limited processing power and memory.

### MPDB Operations

All communication with MPDB is via [MsgPack](http://msgpack.org/) maps.
Currently, MPDB only supports `map`, `string`, `int`, `uint`, `int64` and
`uint64`, and then only the latter 5 for actual values that can be stored. Keys
can only be strings. Because this database has been designed for embedded
clients running Lua and because `float` are not natively supported by Lua, we
do currently support `float` or `double`.

* `oper` is which operation is being sent
* `nodeid` is the unique node identifier. For IPv6, this is derived from the
  hex representation of the last 4 bytes of the global address.
* `echo` is a monotonically increasing number used to implement reliable
  delivery over UDP
* `data` is a key-value map representing the data to be stored
* `keys` is a list of string keys describing what data the client wants
  returned

MPDB uses a form of "collections". All key/value pairs are stored in "buckets",
which are essentially discrete, local namespaces. For the `PERSIST` and
`GETPERSIST` operations, the collection is implicitly determined to be the Node
ID of the client accessing the data. 

For all other operations, any key can be prefixed with a collection name, delineated
by a period. Non-prefixed keys are assumed to be part of the global collection (called "global" to
avoid confusion), and only 1 prefix is allowed per key.

| Full Key | Key | Collection |
| -------- | --- | ---------- |
| "abc"    | "abc" | "global" |
| "col.abc" | "abc" | "col" |
| "col.nest.abc" | "nest.abc" | "col" |

#### `PERSIST`

| Key | Value |
|-----|-------|
|`oper` | `PERSIST` |
|`nodeid` | own node id |
|`echo` | echo tag |
|`data` | data (nested) |

`PERSIST` stores private key/value pairs for a single node, described by
`nodeid`. Only that `nodeid` can access or change these values. Any prefixes
on keys will be treated as part of the key name and NOT as a collection.

`PERSIST` returns a single key/value pair: `result` with the value of an error
if there was one. If the error is an empty string, you can assume that the
operation was successful.

#### `GETPERSIST`

| Key | Value |
|-----|-------|
|`oper` | `GETPERSIST` |
|`nodeid` | own node id |
|`echo` | echo tag |
|`keys` | list of keys |

`GETPERSIST` returns a map of key/value pairs corresponding to the list of keys
sent in the query. All keys will be included in the returned map, and will have
`nil` values if they were not found in the database. If there was an error
in the transaction, the usual error message format will be returned (see `PERSIST`).

#### `INSERT`

| Key | Value |
|-----|-------|
|`oper` | `INSERT`
|`nodeid` | own node id |
|`echo` | echo tag |
|`data` | data (nested) |

`INSERT` stores key/value pairs for arbitrary collections (see the top of this
section). A key can have up to a single prefix, but the `data` map in this
message can have keys that belong to different collections.

The `result` map will be returned

#### `GET`

| Key | Value |
| --- | ----- |
|`oper` | `GET` |
|`nodeid` | own node id |
|`echo` | echo tag |
|`keys` | list of keys |

`GET` returns a map of key/value pairs corresponding to the list of keys sent
in the query. All keys will be included in the returned map, and will have
`nil` values if they were not found in the database. If there was an error in
the transaction, the usual error message format will be returned (see
`PERSIST`). Each key can have a different prefix; that is, querying multiple
collections within the same message is permitted. Each key will be prefixed
with its collection in the returned map.

#### `GETBUCKET`

| Key | Value |
| --- | ----- |
|`oper` | `GET` |
|`nodeid` | own node id |
|`echo` | echo tag |
|`collection` | name of collection |

`GETBUCKET` returns a map of all key/value pairs for the given collection. Each
key will be prefixed with teh collection name. If there was an error in the
transaction, the usual error message format will be returned (see `PERSIST`).
Each key can have a different prefix; that is, querying multiple collections
within the same message is permitted.

### Server

The storage mechanism is backed by [Bolt](https://github.com/boltdb/bolt), so
it is a single file with transactions and ACID semantics on the server side.
The server code is built entirely in Go in order to facilitate handling many
concurrent clients while still making the code straightforward to read.

`server.go` contains the client-facing code, parses the incoming requests and
handles the reliable UDP protocol. Currently, it listens on port 7000 for
UDP/IPv6, though this will be configurable in an upcoming version.

`db.go` contains the database code for each of the operations supported by
MPDB. Some test cases can be found in `db_test.go`, and can be run with `go
test`.

`decode.go` contains a mostly zero-copy MsgPack decoder.

### Client

A functional implementation of an MPDB client can be found [here](https://github.com/SoftwareDefinedBuildings/ioet_contrib/blob/master/lib/mpdb.lua)

The client behavior should become apparent after reading this document.
