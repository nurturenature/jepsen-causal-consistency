import express, { Express, Request, Response } from "express";
import dotenv from "dotenv";

dotenv.config();

/* ElectricSQL */
import Database from 'better-sqlite3'
import { electrify } from 'electric-sql/node'
import { schema } from './generated/client/index.js'

import jwt from 'jsonwebtoken'
function unsignedJWT(sub: string, customClaims?: object) {
    const claims = { "iat": 1703740031, "exp": 1803743631 } || {}

    return jwt.sign({ ...claims, sub: sub }, '', { algorithm: 'none' })
}
const config = {
    url: "http://electricsql:5133",
    auth: {
        token: unsignedJWT("insecure")
    },
    debug: true
}

const conn = new Database('electric.db')
conn.pragma('journal_mode = WAL')
const electric = await electrify(conn, schema, config)
const { db } = electric
const shape = await db.lww_registers.sync()
await shape.synced

const app: Express = express();
const port = process.env.PORT || 3000;

app.get("/r/:key", async (req: Request, res: Response) => {
    const value = await db.lww_registers.findUnique({
        where: {
            key: parseInt(req.params.key, 10)
        }
    })
    res.send(value);
});

app.get("/list", async (req: Request, res: Response) => {
    const result = await db.lww_registers.findMany()
    res.send(result);
});

app.get("/w/:key/:value", async (req: Request, res: Response) => {
    const result = await db.lww_registers.upsert({
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

app.listen(port, () => {
    console.log(`[server]: Server is running at http://localhost:${port}`);
});
