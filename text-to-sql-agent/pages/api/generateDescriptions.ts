import { NextApiRequest, NextApiResponse } from "next";
import { ChatOpenAI } from "@langchain/openai";
import { HumanMessage } from "@langchain/core/messages";

// Number of tables to process in a single batch (increased for maximum throughput)
const BATCH_SIZE = 5;

interface Table {
    name: string;
    columns: {
        name: string;
        type: string;
    }[];
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
    try {
        const { tables } = req.body;
        
        // Initialize the ChatOpenAI model with GPT-4 and maximum concurrency
        const model = new ChatOpenAI({
            modelName: "gpt-4",  // Using GPT-4 for highest quality descriptions
            temperature: 0.2,
            openAIApiKey: process.env.OPENAI_API_KEY,
            maxConcurrency: 20  // Maximum concurrency (adjust based on OpenAI rate limits)
        });
        
        // Create batches of tables for processing
        const batches = [];
        for (let i = 0; i < tables.length; i += BATCH_SIZE) {
            batches.push(tables.slice(i, i + BATCH_SIZE));
        }
        
        // Process batches in parallel
        const batchResults = await Promise.all(batches.map(async (batch) => {
            // Process each table in the batch concurrently
            return Promise.all(batch.map(async (table: Table) => {
                const { name, columns } = table;
                
                // Format columns information for the prompt
                const columnsInfo = columns.map((col: any) => 
                    `${col.name} (${col.type})`
                ).join(', ');
                
                // Optimized prompt for GPT-4
                const promptContent = `As a database expert, provide a concise, informative description for this database table:

Table Name: ${name}
Columns: ${columnsInfo}

Describe the table's purpose, what data it likely contains, and potential relationships with other tables. Be precise and informative. Limit to 75 words.`;
                
                try {
                    const response = await model.invoke([new HumanMessage(promptContent)]);
                    return {
                        tableName: name,
                        description: response.content.toString().trim(),
                    };
                } catch (error) {
                    console.error(`Error generating description for table ${name}:`, error);
                    return {
                        tableName: name,
                        description: `Failed to generate description: ${error instanceof Error ? error.message : 'Unknown error'}`,
                    };
                }
            }));
        }));
        
        // Flatten batch results into a single array
        const tableDescriptions = batchResults.flat();
        
        res.status(200).json(tableDescriptions);
    } catch (error) {
        console.error("Error generating table descriptions:", error);
        res.status(500).json({ 
            error: "Failed to generate table descriptions",
            details: error instanceof Error ? error.message : String(error)
        });
    }
}