import express, { Express, Request, Response } from "express";
import dotenv from "dotenv";

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

/* sync database */
const { synced } = await electric.db.lww_registers.sync()
await synced

/* webserver */
const port = process.env.PORT || 3000;
const app: Express = express();
app.use(express.json())

app.get("/r/:k", async (req: Request, res: Response) => {
    const value = await electric.db.lww_registers.findUnique({
        where: {
            k: parseInt(req.params.k, 10)
        }
    })
    res.send(value);
});

app.get("/w/:k/:v", async (req: Request, res: Response) => {
    const result = await electric.db.lww_registers.upsert({
        create: {
            k: parseInt(req.params.k, 10),
            v: parseInt(req.params.v, 10)
        },
        update: {
            v: parseInt(req.params.v, 10)
        },
        where: {
            k: parseInt(req.params.k, 10)
        }
    })
    res.send(result);
});

app.get("/list", async (res: Response) => {
    const result = await electric.db.lww_registers.findMany()
    res.send(result);
});

app.get("/list-sql", async (res: Response) => {
    const result = await electric.db.raw({ sql: "SELECT * FROM lww_registers;" })
    res.send(result);
});

app.post("/better-sqlite3", (req: Request, res: Response) => {
    const insert = conn.prepare(
        'INSERT INTO lww_registers (k,v) VALUES (@k, @v) ON CONFLICT(k) DO UPDATE SET v = @v');

    const select = conn.prepare(
        'SELECT k,v FROM lww_registers WHERE k = @k');

    const txn = conn.transaction((mops) => {
        for (const mop of mops)
            switch (mop.f) {
                case 'r':
                    select.get(mop);
                    break;
                case 'w':
                    insert.run(mop);
                    break;
            }
    });

    const result = txn(req.body.sql)
    res.send(result)
});

app.listen(port, () => {
    console.log(`[server]: Server is running at http://localhost:${port}`);
});
