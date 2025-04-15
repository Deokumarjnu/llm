import { createDbClient } from "./dbClient";

export async function execute(sql: string, params: any[] = []) {
  return await new Promise((resolve, reject) => {
    const client = createDbClient();
    client
      .connect()
      .then(() => {
        return client.query(sql, params); // Use parameterized queries to prevent SQL injection
      })
      .then((result) => {
        resolve(result.rows);
      })
      .catch((error) => {
        console.error("Database query error:", error);
        reject(error);
      })
      .finally(() => {
        client.end();
      });
  });
}