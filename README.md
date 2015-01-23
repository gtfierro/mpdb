## MPDB

The MsgPack DataBase is a collection-based key-value store designed to be used
by embedded clients with limited processing power and memory.

### MPDB Operations

All communication with MPDB is via [MsgPack](http://msgpack.org/) maps.
Currently, MPDB only supports `map`, `string`, `int`, `uint`, `int64` and
`uint64`, and then only the latter 5 for actual values that can be stored. Keys
can only be strings. Because this database has been designed for embedded
clients running Lua and because `float` are not natively supported by Lua, we
do not currently support `float` or `double`.

* `oper` is which operation is being sent
* `nodeid` is the unique node identifier. For IPv6, this is derived from the
  last 4 bytes of the global address.
* `echo` is a monotonically increasing number used to implement reliable
  delivery over UDP
* `data` is a key-value map representing the data to be stored
* `keys` is a list of string keys describing what data the client wants
  returned

MPDB uses a form of "collections". All key/value pairs are stored in "buckets",
which are essentially discrete, local namespaces. For the `PERSIST` and
`GETPERSIST` operations, the collection is implicitly determined to be the Node
ID of the client accessing the data. 

For all other operations, any key can be prefixed with a collection name,
delineated by a period. Non-prefixed keys are assumed to be part of the global
collection (called "global" to avoid confusion), and only 1 prefix is allowed
per key.

| Full Key | Key | Collection |
| -------- | --- | ---------- |
| "abc"    | "abc" | "global" |
| "col.abc" | "abc" | "col" |
| "col.nest.abc" | "nest.abc" | "col" |

All responses will contain an `echo` field corresponding to which message
request they correspond to. Incoming message with `echo = X` will have a
response with `echo = X`.

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

`PERSIST` returns three key/value pairs: `result`, which will be empty (in the
case of an insert command), `error` which will contain an error message if
there is one, and `echo`, which contains the echo tag corresponding to this
transaction. If the error is an empty string, you can assume that the operation
was successful.

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
|`oper` | `GETBUCKET` |
|`nodeid` | own node id |
|`echo` | echo tag |
|`collection` | name of collection |

`GETBUCKET` returns a map of all key/value pairs for the given collection. Each
key will be prefixed with teh collection name. If there was an error in the
transaction, the usual error message format will be returned (see `PERSIST`).
Each key can have a different prefix; that is, querying multiple collections
within the same message is permitted.

### Reliable UDP

To achieve reliable transport, MPDB includes a monotonically increasing `echo`
tag that is consistent for each request/response transaction pair.

Server-side, MPDB assumes that if it receives multiple messages, then all those
messages will have different echo tags, and that the messages were sent in
order of increasing echo tag. Messages will be served in that order as well.
The server will attempt to serve messages with consecutive echo tags, but if
the server time-out is hit before the server receives a message with the
desired echo tag, it will serve the next available message until the missing
message receives (if it ever does). Assuming the server has received message
with echo tag `X`, the server timeout timer will be started for echo tag `X+1`
upon receiving message `X+2` (if `X+1` was never received).

Client-side, each successive message will be sent with a unique, consecutive,
monotonically increasing echo tag. For a sent message, if the client does not
receive a response from the server with the corresponding echo tag within the
client time-out window, the client can choose to resend the message as many
times as it wants. Consecutive execution order is only guaranteed if the server
receives the client's message within the server time-out window. Client echo tags
should start at `1`.

Each client is considered separately, so multiple clients do not have to
coordinate echo tags.

The server only keeps track of N messages into the future of the last confirmed
echo tag (this is 10 by default).

Parameters:
* server time out (STO) -- 3 seconds default
* client time out (CTO) -- 1 second default
* num cached messages -- 10 default

Example:

* server receives echo tags `1, 2, 3`
* server processes messages with tags `1, 2, 3`
* client receives responses with tags `1, 2, 3`
* server receives message with tag `5`. Because server has not seen message
  `4`, the server time-out timer is started
* client does not receive a response for message `4`, so it resends
* server receives message `4`, and then processes messages `4, 5` and responds
  to the client

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

A functional implementation of an MPDB client can be found
[here](https://github.com/SoftwareDefinedBuildings/ioet_contrib/blob/master/lib/mpdb.lua)

The client behavior should become apparent after reading this document.
