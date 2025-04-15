import { generateSQLQuery } from "@/app/lib/langchain/sql-generator";
import { NextResponse } from "next/server";

export async function POST(req: Request) {
  const { question, context } = await req.json();
  
  // Pass the conversation context to the SQL generator
  const result = await generateSQLQuery(question, context);
  
  return NextResponse.json(result);
}