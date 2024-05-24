import dotenv from "dotenv"
import express, { Express, Request, Response } from "express"

dotenv.config();

/* electric-sqlite */
import Database from 'better-sqlite3'
import { electrify } from 'electric-sql/node'
import { schema } from './generated/client/index.js'
import { ElectricConfig } from "electric-sql/config"
import { insecureAuthToken } from 'electric-sql/auth'

/* create an electric conn, database, and client */
const config: ElectricConfig = {
    url: process.env.ELECTRIC_SERVICE || 'http://electric:5133',
    debug: true
}
const e_conn = new Database('electric.db')
e_conn.pragma('journal_mode = WAL')
const electric = await electrify(e_conn, schema, config)
await electric.connect(insecureAuthToken({ "sub": "insecure" }))

/* sync databases */
const { synced: dummy } = await electric.db.dummy.sync()
await dummy
const { synced: lww } = await electric.db.lww.sync()
await lww

/* create a conn for db transactions, see https://www.sqlite.org/isolation.html */
/* a shared conn has no isolation */
const txn_conn = new Database('electric.db')
txn_conn.pragma('journal_mode = WAL')

/* webserver */
const port = process.env.CLIENT_PORT || 8089
const app: Express = express();
app.use(express.json())

app.get("/lww/list", async (req: Request, res: Response) => {
    const result = await electric.db.lww.findMany()
    res.send(result);
});

app.post("/lww/updateMany", async (req: Request, res: Response) => {
    const result = await electric.db.lww.updateMany(req.body)
    res.send(result)
});

app.post("/lww/findMany", async (req: Request, res: Response) => {
    const result = await electric.db.lww.findMany(req.body)
    res.send(result)
});

app.post("/lww/electric-findUnique", async (req: Request, res: Response) => {
    const result = await electric.db.lww.findUnique(req.body)
    res.send(result)
});

const foo = electric.db.dummy.update({
    where: { dummy: 0 },
    data: {
        v: null,
        lww_lww_dummyTodummy: {
            update: {
                data: { v: "0" },
                where: { k: 0 }
            }
        }
    },
    include: { lww_lww_dummyTodummy: true }
})

app.post("/lww/electric-update", async (req: Request, res: Response) => {
    const result = await electric.db.dummy.update(req.body)
    res.send(result)
});

app.post("/lww/electric-upsert", async (req: Request, res: Response) => {
    const result = await electric.db.lww.upsert(req.body)
    res.send(result)
});

app.get("/control/disconnect", async (req: Request, res: Response) => {
    console.log('[electric-sqlite]: disconnect request received.')

    electric.disconnect()
    console.log('[electric-sqlite]: electric-sqlite disconnected.')

    res.send("disconnected")
});

app.get("/control/connect", async (req: Request, res: Response) => {
    console.log('[electric-sqlite]: connect request received.')

    await electric.connect()
    console.log('[electric-sqlite]: electric-sqlite connected.')

    res.send("connected")
});

app.get("/control/pragma", async (req: Request, res: Response) => {
    res.send([
        {
            "description": "electric-sqlite connection",
            "name": e_conn.name,
            "compile_options": e_conn.pragma('compile_options'),
            "journal_mode": e_conn.pragma('journal_mode', { simple: true }),
            "locking_mode": e_conn.pragma('locking_mode', { simple: true }),
            "read_uncommitted": e_conn.pragma('read_uncommitted', { simple: true })
        }]);
});

const webserver = app.listen(port, () => {
    console.log(`[electric-sqlite]: Server is listening at http://localhost:${port}`);
});
