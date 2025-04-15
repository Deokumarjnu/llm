"use server";

import { NextApiRequest, NextApiResponse } from "next";
import { createDbClient } from "./dbClient";
import { TABLE_DESCRIPTIONS } from "../utils/constant";

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  const client = createDbClient();
  
  // Extract the filter parameters if present
  const { table, column, value, filterType } = req.query;
  const useNameFilter = filterType === 'name';
  
  try {
    // Connect to the database
    await client.connect();

    // Query to get the schema details for all tables
    const tableResult = await client.query(`
      SELECT table_name, column_name, data_type
      FROM information_schema.columns
      WHERE table_schema = 'public'
      ORDER BY table_name, ordinal_position;
    `);

    // Query to get foreign key relationships
    const fkResult = await client.query(`
      SELECT
        tc.table_name, 
        kcu.column_name, 
        ccu.table_name AS foreign_table_name,
        ccu.column_name AS foreign_column_name 
      FROM 
        information_schema.table_constraints AS tc 
        JOIN information_schema.key_column_usage AS kcu
          ON tc.constraint_name = kcu.constraint_name
          AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
          ON ccu.constraint_name = tc.constraint_name
          AND ccu.table_schema = tc.table_schema
      WHERE tc.constraint_type = 'FOREIGN KEY'
      AND tc.table_schema = 'public';
    `);

    // Group columns by table name
    const schema = tableResult.rows.reduce((acc: any, row: any) => {
      const tableName = row.table_name;
      
      if (!acc[tableName]) {
        acc[tableName] = {
          columns: [],
          description: TABLE_DESCRIPTIONS[tableName as keyof typeof TABLE_DESCRIPTIONS] || `Table containing ${tableName} data`,
          // Add table synonyms - institutions and schools are the same
          synonyms: tableName === 'institutions' ? ['schools'] : []
        };
      }
      acc[tableName].columns.push({ column: row.column_name, type: row.data_type });
      return acc;
    }, {});

    // Create an alias entry for "schools" pointing to "institutions"
    if (schema['institutions']) {
      schema['schools'] = {
        ...schema['institutions'],
        isAlias: true,
        actualTable: 'institutions'
      };
    }

    // Process foreign keys to build relationships
    fkResult.rows.forEach((row: any) => {
      const tableName = row.table_name;
      if (schema[tableName]) {
        if (!schema[tableName].relationships) {
          schema[tableName].relationships = [];
        }
        schema[tableName].relationships.push({
          column: row.column_name,
          foreignTable: row.foreign_table_name,
          foreignColumn: row.foreign_column_name
        });
      }
    });

    // Filter data if parameters are provided
    if (table && column && value) {
      // Execute a query to get the filtered data
      const actualTable = schema[table as string]?.isAlias ? schema[table as string].actualTable : table;
      
      if (actualTable) {
        try {
          // Use parameterized query to prevent SQL injection
          const filterOperator = useNameFilter ? 'ILIKE' : '=';
          const filterValue = useNameFilter ? `%${value}%` : value;
          
          const filteredData = await client.query(
            `SELECT * FROM ${actualTable} WHERE ${column} ${filterOperator} $1`,
            [filterValue]
          );
          
          res.status(200).json({ 
            schema, 
            filteredData: filteredData.rows 
          });
          return;
        } catch (filterError) {
          console.error("Error filtering data:", filterError);
          // Continue to return the schema without filtered data
        }
      }
    }

    res.status(200).json(schema);
  } catch (error) {
    console.error("Error fetching schema:", error);
    res.status(500).json({ error: "Failed to fetch schema" });
  } finally {
    // Close the database connection
    await client.end();
  }
}