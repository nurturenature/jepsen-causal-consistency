import assert from "assert";
import dotenv from "dotenv";
import express, { Express, Request, Response } from "express";

dotenv.config();

/* ElectricSQL */
import Database from 'better-sqlite3'
import { electrify } from 'electric-sql/node'
import { schema } from './generated/client/index.js'
import { ElectricConfig } from "electric-sql/config";
import { insecureAuthToken } from 'electric-sql/auth'

/* create an electric conn, database, and client */
const config: ElectricConfig = {
    url: 'http://hosthost:5133',
    debug: true
}
const e_conn = new Database('electric.db')
e_conn.pragma('journal_mode = WAL')
const electric = await electrify(e_conn, schema, config)
await electric.connect(insecureAuthToken({ "sub": "insecure" }))

/* sync databases */
const { synced: gset } = await electric.db.gset.sync()
await gset
const { synced: lww } = await electric.db.lww.sync()
await lww

/* create a conn for db transactions, see https://www.sqlite.org/isolation.html */
/* a shared conn has no isolation */
const txn_conn = new Database('electric.db')
txn_conn.pragma('journal_mode = WAL')

/* webserver */
const port = process.env.PORT || 3000;
const app: Express = express();
app.use(express.json())

app.get("/gset/r/:k", async (req: Request, res: Response) => {
    const value = await electric.db.gset.findMany({
        where: {
            k: parseInt(req.params.k, 10)
        }
    })
    if (value == null) {
        res.send({
            'k': req.params.k,
            'v': null
        });
    } else {
        res.send(value);
    }
});

app.get("/gset/w/:id/:k/:v", async (req: Request, res: Response) => {
    const result = await electric.db.gset.upsert({
        create: {
            id: parseInt(req.params.id, 10),
            k: parseInt(req.params.k, 10),
            v: parseInt(req.params.v, 10)
        },
        update: {
            k: parseInt(req.params.k, 10),
            v: parseInt(req.params.v, 10)
        },
        where: {
            id: parseInt(req.params.id, 10)
        }
    })
    res.send(result);
});

app.get("/gset/list", async (req: Request, res: Response) => {
    const result = await electric.db.gset.findMany()
    res.send(result);
});

app.post("/gset/electric-findMany", async (req: Request, res: Response) => {
    const result = await electric.db.gset.findMany(req.body)
    res.send(result)
});

app.post("/gset/electric-createMany", async (req: Request, res: Response) => {
    const result = await electric.db.gset.createMany(req.body)
    res.send(result)
});

app.post("/gset/better-sqlite3", (req: Request, res: Response) => {
    const insert = txn_conn.prepare(
        'INSERT INTO gset (id,k,v) VALUES (@id, @k, @v)');

    const select = txn_conn.prepare(
        'SELECT k,v FROM gset WHERE k = @k');

    const result = Array()

    const txn = txn_conn.transaction((mops) => {
        for (const mop of mops)
            switch (mop.f) {
                case 'r':
                    const read = <any>select.all(mop)
                    if (read.length == 0) {
                        result.push({ 'f': 'r', 'k': mop.k, 'v': null })
                    } else {
                        result.push({ 'f': 'r', 'k': mop.k, 'v': read })
                    }
                    break;
                case 'w':
                    const write = insert.run(mop);
                    assert(write.changes == 1)
                    result.push(mop)
                    break;
            }
    });

    try {
        txn(req.body.value)
        res.send({ 'type': 'ok', 'value': result })
    } catch (e) {
        res.send({ 'type': 'info', 'error': e })
    }
});

app.get("/lww/list", async (req: Request, res: Response) => {
    const result = await electric.db.lww.findMany()
    res.send(result);
});

app.post("/lww/better-sqlite3", (req: Request, res: Response) => {
    const upsert = txn_conn.prepare(
        'INSERT INTO lww (k,v) VALUES (@k,@v) ON CONFLICT (k) DO UPDATE SET v = v || \' \' || @v');
    const select = txn_conn.prepare(
        'SELECT k,v FROM lww WHERE k = @k');

    const result = Array()

    const txn = txn_conn.transaction((mops) => {
        for (const mop of mops)
            switch (mop.f) {
                case 'r':
                    const read = <any>select.get(mop)
                    if (read == undefined) {
                        result.push({ 'f': 'r', 'k': mop.k, 'v': null })
                    } else {
                        result.push({ 'f': 'r', 'k': read.k, 'v': read.v })
                    }
                    break;
                case 'append':
                    const write = upsert.run(mop);
                    assert(write.changes == 1)
                    result.push(mop)
                    break;
            }
    });

    try {
        txn(req.body.value)
        res.send({ 'type': 'ok', 'value': result })
    } catch (e) {
        res.send({ 'type': 'info', 'error': e })
    }
});

app.post("/control/disconnect", async (req: Request, res: Response) => {
    console.log('[electricsql]: disconnect request received.')

    electric.disconnect()
    console.log('[electricsql]: ElectricSQL disconnected.')
});

app.post("/control/connect", async (req: Request, res: Response) => {
    console.log('[electricsql]: connect request received.')

    await electric.connect()
    console.log('[electricsql]: ElectricSQL connected.')
});

app.post("/control/stop", async (req: Request, res: Response) => {
    console.log('[electricsql]: stop request received.')

    electric.disconnect()
    console.log('[electricsql]: ElectricSQL disconnected.')

    await electric.close()
    console.log('[electricsql]: ElectricSQL closed.')

    e_conn.close()
    console.log('[electricsql]: ElectricSQL conn closed.')
    txn_conn.close()
    console.log('[electricsql]: txn conn closed.')

    process.exit(0)
});

app.get("/control/pragma", async (req: Request, res: Response) => {
    res.send([
        {
            "description": "ElectricSQL connection",
            "name": e_conn.name,
            "compile_options": e_conn.pragma('compile_options'),
            "journal_mode": e_conn.pragma('journal_mode', { simple: true }),
            "locking_mode": e_conn.pragma('locking_mode', { simple: true }),
            "read_uncommitted": e_conn.pragma('read_uncommitted', { simple: true })
        },
        {
            "description": "transaction connection",
            "name": txn_conn.name,
            "compile_options": txn_conn.pragma('compile_options'),
            "journal_mode": txn_conn.pragma('journal_mode', { simple: true }),
            "locking_mode": txn_conn.pragma('locking_mode', { simple: true }),
            "read_uncommitted": txn_conn.pragma('read_uncommitted', { simple: true })
        }]);
});

const webserver = app.listen(port, () => {
    console.log(`[electricsql]: Server is listening at http://localhost:${port}`);
});
