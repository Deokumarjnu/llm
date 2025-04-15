"use server";

import { ChatOpenAI } from "@langchain/openai";
import { createReactAgent } from "@langchain/langgraph/prebuilt";
import { tool } from "@langchain/core/tools";
import { z } from "zod";
import {
  mapStoredMessagesToChatMessages,
  StoredMessage,
} from "@langchain/core/messages";
import { execute } from "./getQuery";

export async function message(
  messages: StoredMessage[],
  relevantTableNames: string[]
): Promise<string> {
  // Keep only the last 20 messages to avoid token overflow
  const trimmedMessages = messages.slice(-20);
  const deserialized = mapStoredMessagesToChatMessages(trimmedMessages);

  const getFromDB = tool(
    async (input: { sql: string; params?: unknown[] }) => {
      if (input?.sql && input.sql.includes("course attendance")) {
        input.sql = input.sql.replace(
          /WHERE\s+.*?\s*;/i,
          "WHERE course_attendance_filters IS NOT NULL;"
        );
      }
      if (input?.sql) {
        try {
          // First try the exact query
          console.log("Executing SQL:", input.sql, "with params:", input.params);
          const result = await execute(input.sql, input.params || []);
          return JSON.stringify(result);
        } catch (error) {
          // Log the error for debugging
          console.error("Error executing SQL:", input.sql, error);

          // If the query fails due to name matching issues, try with fuzzy matching
          const errorMsg = error instanceof Error ? error.message : "";
          if (
            errorMsg.includes("column not found") ||
            errorMsg.includes("does not exist") ||
            errorMsg.includes("syntax error")
          ) {
            // Convert exact name matches to ILIKE pattern matches
            const fallbackSQL = input.sql
              .replace(
                /([a-zA-Z_]+)\.name\s*=\s*'([^']+)'/gi,
                "$1.name ILIKE '%$2%'"
              )
              .replace(
                /([a-zA-Z_]+)\.first_name\s*=\s*'([^']+)'/gi,
                "$1.first_name ILIKE '%$2%'"
              )
              .replace(
                /([a-zA-Z_]+)\.last_name\s*=\s*'([^']+)'/gi,
                "$1.last_name ILIKE '%$2%'"
              );

            console.log("Fallback SQL generated:", fallbackSQL);

            if (fallbackSQL !== input.sql) {
              try {
                console.log("Executing fallback SQL:", fallbackSQL, "with params:", input.params);
                const fallbackResult = await execute(
                  fallbackSQL,
                  input.params || []
                );
                return JSON.stringify({
                  rows: fallbackResult,
                  note: "Used fuzzy name matching since exact match failed",
                  original_query: input.sql,
                  fallback_query: fallbackSQL,
                });
              } catch (fallbackError) {
                console.error("Error executing fallback SQL:", fallbackSQL, fallbackError);
                // If even the fallback fails, return the original error
                throw error;
              }
            } else {
              throw error;
            }
          } else {
            throw error;
          }
        }
      }
      return null;
    },
    {
      name: "get_from_db",
      description: `Get data from a database. The schema for the relevant tables includes:
${relevantTableNames
        .map(
          (table) =>
            `Table "${table}": ${messages
              .filter(
                (m) =>
                  m.type === "system" &&
                  "content" in m &&
                  typeof m.content === "string" &&
                  m.content.includes(table)
              )
              .map((m) => ("content" in m ? m.content : ""))
              .join(", ")}`
        )
        .join("\n")}

Important notes for SQL generation:
1. The terms "schools" and "institutions" refer to the same table called "institutions"
2. When the user refers to entities by name instead of ID, use appropriate JOIN operations and name matching
3. For example, instead of "WHERE school_id = 5", use "JOIN institutions ON institutions.id = table.institution_id WHERE institutions.name = 'School Name'"
4. Use wildcards with LIKE/ILIKE for flexible name matching when needed (e.g., WHERE name ILIKE '%Springfield%')
5. Prefer simple queries with clear conditions
6. If the question mentions “course attendance”, check for rows where course_attendance_filters IS NOT NULL.
7. If the question mentions "daily attendance”, check for rows where daily_attendance_filters IS NOT NULL.
8. If the question mentions "period attendance”, check for rows where period_attendance_filters IS NOT NULL.
`,
schema: z.object({
        sql: z.string().describe("SQL query to get data from a PostgreSQL database."),
        params: z
          .array(z.unknown())
          .optional()
          .describe("Parameters for the SQL query."),
      }),
    }
  );

  const agent = createReactAgent({
    llm: new ChatOpenAI({
      modelName: "gpt-4", // <-- Token-friendly model (128k context)
      temperature: 0,
      openAIApiKey: process.env.OPENAI_API_KEY,
    }),
    tools: [getFromDB],
  });

  const response = await agent.invoke({
    messages: deserialized,
  });

  const lastMessageContent = response.messages[response.messages.length - 1].content;
  if (typeof lastMessageContent === "string") {
    return lastMessageContent;
  } else {
    throw new Error("The content of the last message is not a string.");
  }
}
