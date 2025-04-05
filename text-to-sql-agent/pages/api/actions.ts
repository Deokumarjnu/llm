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

export async function message(messages: StoredMessage[], tables: string[]) {
  const deserialized = mapStoredMessagesToChatMessages(messages);

  const getFromDB = tool(
    async (input) => {
      if (input?.sql) {
        console.log({ sql: input.sql });

        const result = await execute(input.sql);

        return JSON.stringify(result);
      }
      return null;
    },
    {
      name: "get_from_db",
      description: `Get data from a database, the database has the following schema:
      ${tables.map((table) => `- ${table}`).join("\n")}`,
      schema: z.object({
        sql: z
          .string()
          .describe(
            "SQL query to get data from a PostgreSQL database. Always put quotes around the field and table arguments."
          ),
      }),
    }
  );

  const agent = createReactAgent({
    llm: new ChatOpenAI({
      modelName: "gpt-4", // Specify the OpenAI model (e.g., gpt-3.5-turbo or gpt-4)
      temperature: 0, // Adjust temperature for deterministic responses
      openAIApiKey: process.env.OPENAI_API_KEY, // Use your OpenAI API key
    }),
    tools: [getFromDB],
  });

  const response = await agent.invoke({
    messages: deserialized,
  });

  return response.messages[response.messages.length - 1].content;
}