import { OpenAIEmbeddings } from "@langchain/openai";
import { MemoryVectorStore } from "langchain/vectorstores/memory";
import { Client } from "pg";

const client = new Client({
  connectionString: process.env.DATABASE_URL,
});
await client.connect();

export async function generateSchemaChunks(): Promise<string[]> {
  const { rows } = await client.query(`
    SELECT table_name, column_name
    FROM information_schema.columns
    WHERE table_schema = 'public'
  `);

  const grouped: Record<string, string[]> = {};
  rows.forEach(({ table_name, column_name }) => {
    if (!grouped[table_name]) grouped[table_name] = [];
    grouped[table_name].push(column_name);
  });

  return Object.entries(grouped).map(
    ([table, cols]) => `Table: ${table}, Columns: ${cols.join(", ")}`
  );
}

let store: MemoryVectorStore;

export async function getVectorStore(): Promise<MemoryVectorStore> {
  if (store) return store;
  const schemaChunks = await generateSchemaChunks();
  const embeddings = new OpenAIEmbeddings();
  store = await MemoryVectorStore.fromTexts(
    schemaChunks,
    schemaChunks.map(() => ({})),
    embeddings
  );
  return store;
}