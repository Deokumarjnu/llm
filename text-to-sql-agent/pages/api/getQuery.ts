import { createDbClient } from "./dbClient";

export async function execute(sql: string) {
    return await new Promise((resolve, reject) => {
        const client = createDbClient();
        client.connect()
        .then(() => {
        return client.query(sql);
        })
        .then((result) => {
        resolve(result.rows);
        })
        .catch((error) => {
        console.log({ error });
        reject(error);
        })
        .finally(() => {
        client.end();
        });
    });
  }