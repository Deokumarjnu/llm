import { loadDatabaseSchema } from "./schema-loader";

export async function generateSchemaChunks(): Promise<string[]> {
  const schema = await loadDatabaseSchema();
  const chunks: string[] = [];

  for (const [table, columns] of Object.entries(schema)) {
    const description = `Table: ${table}\nColumns:\n- ${columns.join("\n- ")}`;
    chunks.push(description);
  }

  return chunks;
}
