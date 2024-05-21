<table style="text-align: center">
    <thead >
        <tr >
            <th  style="text-align: center" colspan="4">ElectricSQL Tests</th>
        </tr>
        <tr>
            <th style="text-align: center">workload</th>
            <th style="text-align: center">db</th>
            <th style="text-align: center">client<br />API</th>
            <th style="text-align: center">min/max<br />txn len</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>electric-sqlite</td>
            <td>SQLite3</td>
            <td>generated</td>
            <td>1 / 1</td>
        </tr>
        <tr>
            <td>electric-pglite</td>
            <td>PGlite</td>
            <td>generated</td>
            <td>1 / 1</td>
        </tr>
        <tr>
            <td>better-sqlite</td>
            <td>SQLite3</td>
            <td>better-sqlite3</td>
            <td>2 / 4</td>
        </tr>
        <tr>
            <td>pgexec-pglite</td>
            <td>PGlite</td>
            <td>PGlite.exec</td>
            <td>2 / 4</td>
        </tr>
        <tr>
            <td>local-sqlite</td>
            <td>single shared<br />SQLite3</td>
            <td>better-sqlite3</td>
            <td>2 / 4</td>
        </tr>
    </tbody>
</table>