import assert from "assert"
import dotenv from "dotenv"
import express, { Express, Request, Response } from "express"

dotenv.config();

/* local SQLite3 */
import Database from 'better-sqlite3'

/* create a conn for db transactions, see https://www.sqlite.org/isolation.html */
/* a shared conn has no isolation */
const txn_conn = new Database(process.env.LOCAL_SQLITE3_DATABASE || '/var/jepsen/shared/db/local.db')
txn_conn.pragma('journal_mode = WAL')

/* webserver */
const port = process.env.CLIENT_PORT || 8089
const app: Express = express();
app.use(express.json())

app.post("/lww/better-sqlite3", (req: Request, res: Response) => {
    const upsert = txn_conn.prepare(
        'INSERT INTO lww (k,v) VALUES (@k,@v) ON CONFLICT (k) DO UPDATE SET v = concat_ws(\' \',lww.v,@v)');
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

app.get("/control/pragma", async (req: Request, res: Response) => {
    res.send([
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
    console.log(`[local-sqlite3]: Server is listening at http://localhost:${port}`);
});
