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

/* create a database and client */
const config: ElectricConfig = {
    url: 'http://electricsql:5133',
    auth: {
        token: insecureAuthToken({ "sub": "insecure" })
    },
    debug: true
}
const conn = new Database('electric.db')
conn.pragma('journal_mode = WAL')
const electric = await electrify(conn, schema, config)

/* sync databases */
const { synced: gset } = await electric.db.gset.sync()
await gset

const { synced: lww_register } = await electric.db.lww_register.sync()
await lww_register

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

app.post("/lww_register/electric-findMany", async (req: Request, res: Response) => {
    const result = await electric.db.lww_register.findMany(req.body)
    res.send(result)
});

app.post("/lww_register/electric-upsert", async (req: Request, res: Response) => {
    const result = await electric.db.lww_register.upsert(req.body)
    res.send(result)
});

app.post("/gset/better-sqlite3", (req: Request, res: Response) => {
    const insert = conn.prepare(
        'INSERT INTO gset (id,k,v) VALUES (@id, @k, @v)');

    const select = conn.prepare(
        'SELECT k,v FROM gset WHERE k = @k');

    const result = Array()

    const txn = conn.transaction((mops) => {
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

app.post("/lww_register/better-sqlite3", (req: Request, res: Response) => {
    const insert = conn.prepare(
        'INSERT INTO lww_register (k,v) VALUES (@k, @v) ON CONFLICT(k) DO UPDATE SET v = @v');

    const select = conn.prepare(
        'SELECT k,v FROM lww_register WHERE k = @k');

    const result = Array()

    const txn = conn.transaction((mops) => {
        for (const mop of mops)
            switch (mop.f) {
                case 'r':
                    const read = <any>select.get(mop)
                    if (read == undefined) {
                        result.push({ 'f': 'r', 'k': mop.k, 'v': null })
                    } else {
                        result.push({ 'f': 'r', 'k': mop.k, 'v': read.v })
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

app.post("/control/stop", async (req: Request, res: Response) => {
    console.log('[electricsql]: stop request received.')

    await electric.close()
    console.log('[electricsql]: ElectricSQL closed.')

    conn.close()
    console.log('[electricsql]: DB conn closed.')

    process.exit(0)
});

const webserver = app.listen(port, () => {
    console.log(`[electricsql]: Server is listening at http://localhost:${port}`);
});
