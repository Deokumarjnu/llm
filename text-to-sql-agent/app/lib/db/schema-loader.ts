import { createDbClient } from "@/pages/api/dbClient";

export async function loadDatabaseSchema() {
  const client = createDbClient();

  await client.connect();

  const result = await client.query(`
    SELECT table_name, column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'public'
    ORDER BY table_name, ordinal_position;
  `);

  await client.end();

  // Group columns by table
  const schema: Record<string, string[]> = {};
  result.rows.forEach(({ table_name, column_name, data_type }) => {
    const col = `${column_name} (${data_type})`;
    schema[table_name] = schema[table_name] || [];
    schema[table_name].push(col);
  });

  return schema;
}