As there's a pause in the chat, converted an existing better-sqlite3 node app to pglite:

```json
{
    "dependencies": {
      "@electric-sql/pglite": "canary",
      "electric-sql": "canary"
  }
}
```

```ts
/* PGlite */
import { PGlite } from "@electric-sql/pglite";
import { electrify } from 'electric-sql/pglite'
import { schema } from './generated/client/index.js'
import { ElectricConfig } from "electric-sql/config"
import { insecureAuthToken } from 'electric-sql/auth'

/* create an electric conn, database, and client */
const config: ElectricConfig = {
    url: process.env.ELECTRIC_SERVICE || 'http://electric:5133',
    debug: true
}
const pglite = new PGlite()
const electric = await electrify(pglite, schema, config)
await electric.connect(insecureAuthToken({ "sub": "insecure" }))
```

And at runtime:

```txt
> electric-pglite-client@0.0.1 app:start
> node dist/index.js

Using Postgres version: PostgreSQL 15devel on aarch64-apple-darwin22.6.0, compiled by emcc (Emscripten gcc/clang-like replacement + linker emulating GNU ld) 3.1.56 (cf90417346b78455089e64eb909d71d091ecc055), 32-bit
applying migration: 0
applying migration: 1
applying migration: 2
no lsn retrieved from store
connecting to electric server
server returned an error while establishing connection: WebSocket is not defined
connectAndStartRetryHandler was cancelled: WebSocket is not defined
connectAndStartRetryHandler was cancelled: WebSocket is not defined
file://.../pglite/node_modules/electric-sql/dist/sockets/web.js:9
    const ws = new WebSocket(opts.url, [this.protocolVsn]);
               ^

ReferenceError: WebSocket is not defined
    at WebSocketWeb.makeSocket (file://.../pglite/node_modules/electric-sql/dist/sockets/web.js:9:16)
    at WebSocketWeb.open (file://.../pglite/node_modules/electric-sql/dist/sockets/genericSocket.js:25:24)
    at file://.../pglite/node_modules/electric-sql/dist/satellite/client.js:228:19
    at new Promise (<anonymous>)
    at SatelliteClient.connect (file://.../pglite/node_modules/electric-sql/dist/satellite/client.js:188:12)
    at SatelliteProcess._connect (file://.../pglite/node_modules/electric-sql/dist/satellite/process.js:602:25)
    at BackOff.request (file://.../pglite/node_modules/electric-sql/dist/satellite/process.js:559:18)
    at BackOff.<anonymous> (/.../pglite/node_modules/exponential-backoff/dist/backoff.js:76:51)
    at step (/.../pglite/node_modules/exponential-backoff/dist/backoff.js:33:23)
    at Object.next (/.../pglite/node_modules/exponential-backoff/dist/backoff.js:14:53)

Node.js v20.12.2
```

Tried adding ws dependency, referring to it in index.ts:

```ts
/* TODO? does this work? */
import { WebSocket } from 'ws';
```

And still get the ReferenceError.

----

```json
{
  "query": "SELECT k,v FROM lww WHERE k = 0; INSERT INTO lww (k,v) VALUES (1,'0') ON CONFLICT (k) DO UPDATE SET v = lww.v || ' ' || '0'; INSERT INTO lww (k,v) VALUES (2,'0') ON CONFLICT (k) DO UPDATE SET v = lww.v || ' ' || '0'; SELECT k,v FROM lww WHERE k = 1; SELECT k,v FROM lww WHERE k = 2; INSERT INTO lww (k,v) VALUES (3,'0') ON CONFLICT (k) DO UPDATE SET v = lww.v || ' ' || '0'; SELECT k,v FROM lww WHERE k = 3;"
}
```

```json

[
  {
    "rows": [],
    "fields": [
      {
        "name": "k",
        "dataTypeID": 23
      },
      {
        "name": "v",
        "dataTypeID": 25
      }
    ]
  },
  {
    "rows": [],
    "fields": []
  },
  {
    "rows": [],
    "fields": []
  },
  {
    "rows": [
      {
        "k": 1,
        "v": "0 0"
      }
    ],
    "fields": [
      {
        "name": "k",
        "dataTypeID": 23
      },
      {
        "name": "v",
        "dataTypeID": 25
      }
    ]
  },
  {
    "rows": [
      {
        "k": 2,
        "v": "0 0 0 0"
      }
    ],
    "fields": [
      {
        "name": "k",
        "dataTypeID": 23
      },
      {
        "name": "v",
        "dataTypeID": 25
      }
    ]
  },
  {
    "rows": [],
    "fields": []
  },
  {
    "rows": [
      {
        "k": 3,
        "v": "0"
      }
    ],
    "fields": [
      {
        "name": "k",
        "dataTypeID": 23
      },
      {
        "name": "v",
        "dataTypeID": 25
      }
    ],
    "affectedRows": 3
  }
]
```
