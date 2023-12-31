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
    url: 'http://electric:5133',
    auth: {
        token: await insecureAuthToken({ user_id: 'insecure' })
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
const app: Express = express();
const port = process.env.PORT || 3000;

app.get("/r/:key", async (req: Request, res: Response) => {
    const value = await electric.db.lww_registers.findUnique({
        where: {
            key: parseInt(req.params.key, 10)
        }
    })
    res.send(value);
});

app.get("/w/:key/:value", async (req: Request, res: Response) => {
    const result = await electric.db.lww_registers.upsert({
        create: {
            key: parseInt(req.params.key, 10),
            value: parseInt(req.params.value, 10)
        },
        update: {
            value: parseInt(req.params.value, 10)
        },
        where: {
            key: parseInt(req.params.key, 10)
        }
    })
    res.send(result);
});

app.get("/list", async (req: Request, res: Response) => {
    const result = await electric.db.lww_registers.findMany()
    res.send(result);
});

app.get("/list-sql", async (req: Request, res: Response) => {
    const result = await electric.db.raw({ sql: "SELECT * FROM lww_registers;" })
    res.send(result);
});

app.listen(port, () => {
    console.log(`[server]: Server is running at http://localhost:${port}`);
});
