import dotenv from "dotenv"
import express, { Express, Request, Response } from "express"

dotenv.config();

/* PGlite */
import { PGlite } from "@electric-sql/pglite";
import { electrify } from 'electric-sql/pglite'
import { schema } from './generated/client/index.js'
import { ElectricConfig } from "electric-sql/config"
import { insecureAuthToken } from 'electric-sql/auth'

/* TODO? does this work? */
import { WebSocket } from 'ws';

/* create an electric conn, database, and client */
const config: ElectricConfig = {
    url: process.env.ELECTRIC_SERVICE || 'http://electric:5133',
    debug: true
}
const pglite = new PGlite()
const electric = await electrify(pglite, schema, config)
await electric.connect(insecureAuthToken({ "sub": "insecure" }))

/* sync database */
const { synced: lww } = await electric.db.lww.sync()
await lww

/* webserver */
const port = process.env.CLIENT_PORT || 8089
const app: Express = express();
app.use(express.json())

app.get("/lww/list", async (req: Request, res: Response) => {
    const result = await electric.db.lww.findMany()
    res.send(result);
});

app.post("/lww/electric-findUnique", async (req: Request, res: Response) => {
    const result = await electric.db.lww.findUnique(req.body)
    res.send(result)
});

app.post("/lww/electric-findMany", async (req: Request, res: Response) => {
    const result = await electric.db.lww.findMany(req.body)
    res.send(result)
});

app.post("/lww/electric-upsert", async (req: Request, res: Response) => {
    const result = await electric.db.lww.upsert(req.body)
    res.send(result)
});

app.post("/lww/pglite-exec", async (req: Request, res: Response) => {
    const result = await pglite.exec(req.body)
    res.send(result)
});

app.get("/control/disconnect", (req: Request, res: Response) => {
    console.log('[pglite]: disconnect request received.')

    electric.disconnect()
    console.log('[pglite]: PGlite disconnected.')

    res.send("disconnected")
});

app.get("/control/connect", async (req: Request, res: Response) => {
    console.log('[pglite]: connect request received.')

    await electric.connect()
    console.log('[pglite]: PGlite connected.')

    res.send("connected")
});

app.get("/control/properties", async (req: Request, res: Response) => {
    res.send([
        {
            "description": "PGlite properties",
            "ready": pglite.ready,
            "closed": pglite.closed,
            "waitReady": await pglite.waitReady
        }]);
});

const webserver = app.listen(port, () => {
    console.log(`[pglite]: Server is listening at http://localhost:${port}`);
});
