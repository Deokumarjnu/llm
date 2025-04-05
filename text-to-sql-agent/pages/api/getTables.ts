"use server";

import { NextApiRequest, NextApiResponse } from 'next';
import { createDbClient } from './dbClient';

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  const client = createDbClient();

  try {
    // Connect to the database
    await client.connect();

    // Query to get the list of tables
    const result = await client.query(`
      SELECT table_name
      FROM information_schema.tables
      WHERE table_schema = 'pg_tables'
    `);

    // Return the list of table names
    res.status(200).json(result.rows);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: 'Failed to fetch tables' });
  } finally {
    // Close the connection
    await client.end();
  }
}