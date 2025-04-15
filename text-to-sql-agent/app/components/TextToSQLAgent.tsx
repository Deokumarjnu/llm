'use client';

import { useState, useEffect } from "react";
import { TABLE_DESCRIPTIONS } from "../../pages/utils/constant";

export default function TextToSQLAgent() {
  const [question, setQuestion] = useState("");
  const [messages, setMessages] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [tables, setTables] = useState<string[]>([]);
  const [selectedTable, setSelectedTable] = useState<string | null>(null);
  const [showTableInfo, setShowTableInfo] = useState(false);
  // Add conversation context state
  const [conversationContext, setConversationContext] = useState<{
    previousQuestions: string[];
    previousResults: any[];
    entities: {
      district_id?: number;
      school_id?: number;
      school_name?: string;
      district_name?: string;
    };
  }>({
    previousQuestions: [],
    previousResults: [],
    entities: {}
  });

  // Fetch available tables on component mount
  useEffect(() => {
    async function fetchTables() {
      try {
        const res = await fetch("/api/getTables", {
          method: "GET",
          headers: { "Content-Type": "application/json" },
        });
        if (res.ok) {
          const data = await res.json();
          setTables(Object.keys(data));
        }
      } catch (err) {
        console.error("Error fetching tables:", err);
      }
    }

    fetchTables();
  }, []);

  // Helper function to extract entities from SQL result
  function extractEntitiesFromResult(data: any) {
    const entities = { ...conversationContext.entities };
    
    // Extract district_id if present
    if (data.sql && data.sql.toLowerCase().includes('district_id')) {
      const districtMatch = data.sql.match(/district_id\s*=\s*(\d+)/i);
      if (districtMatch && districtMatch[1]) {
        entities.district_id = parseInt(districtMatch[1]);
      }
    }
    
    // Extract school_id if present
    if (data.sql && data.sql.toLowerCase().includes('institution_id')) {
      const schoolMatch = data.sql.match(/institution_id\s*=\s*(\d+)/i);
      if (schoolMatch && schoolMatch[1]) {
        entities.school_id = parseInt(schoolMatch[1]);
      }
    }
    
    // Extract school name from note or question
    if (data.note && data.note.includes('school')) {
      const schoolMatch = data.note.match(/school\s+["']?([^"']+)["']?/i);
      if (schoolMatch && schoolMatch[1]) {
        entities.school_name = schoolMatch[1].trim();
      }
    }
    
    return entities;
  }

  async function sendMessage() {
    if (!question.trim()) return;

    const userMessage = { type: "user", content: question };
    setMessages((prev) => [...prev, userMessage]);
    setQuestion("");
    setLoading(true);

    // Check for conversational phrases first
    const conversationalPhrasePattern = /^(?:nice|great|awesome|good|thanks|thank you|cool|perfect|excellent|fine|ok|okay|yes|no|sure|got it)[\s!.]*$/i;
    if (conversationalPhrasePattern.test(question.trim())) {
      // Return a friendly response without making an API call
      const responses = [
        "I'm glad I could help! What else would you like to know about your database?",
        "Thanks for the feedback! Feel free to ask me another database question.",
        "You're welcome! What other data would you like to explore?",
        "Great! What would you like to query next?",
        "I'm here to help with your database queries. What would you like to know?"
      ];
      const randomResponse = responses[Math.floor(Math.random() * responses.length)];
      
      setMessages((prev) => [
        ...prev,
        { type: "ai", content: randomResponse }
      ]);
      setLoading(false);
      return;
    }

    try {
      // Add the conversation context to the request
      const res = await fetch("/api/query", {
        method: "POST",
        body: JSON.stringify({ 
          question,
          context: conversationContext
        }),
        headers: { "Content-Type": "application/json" },
      });
      const data = await res.json();

      // Check if we received a conversational note from the backend
      if (data.note && data.note.includes("Conversational input detected")) {
        const aiMessage = {
          type: "ai",
          content: "I understand you're trying to have a conversation. How can I help you with your database today? Try asking me something like 'Show me all districts' or 'How many students are in Elementary School?'"
        };
        setMessages((prev) => [...prev, aiMessage]);
        setLoading(false);
        return;
      }

      // Format the response based on the type of data received
      let responseContent = "";
      
      if (data.error) {
        responseContent = `❌ Error: ${data.error}`;
      } else {        
        // Add any notes from the backend
        if (data.note) {
          responseContent += `Note: ${data.note}\n\n`;
        }
        
        // Format the rows nicely based on content
        if (data.rows && data.rows.length > 0) {
          // If it's a count result
          if (data.rows.length === 1 && data.rows[0].student_count !== undefined) {
            responseContent += `Found ${data.rows[0].student_count} student(s)`;
          } 
          // If it's a list of items
          else if (data.rows.length > 0) {
            responseContent += `Results (${data.rows.length} rows):\n`;
            responseContent += data.rows.map((r: any) =>
              Object.entries(r)
                .map(([k, v]) => `${k}: ${v}`)
                .join(", ")
            ).join("\n");
          } 
          // Empty results
          else {
            responseContent += "No results found for your query.";
          }
        } else {
          responseContent += "No results found for your query.";
        }
      }

      const aiMessage = {
        type: "ai",
        content: responseContent,
      };
      
      setMessages((prev) => [...prev, aiMessage]);
      
      // Extract entities and update conversation context
      const updatedEntities = extractEntitiesFromResult(data);
      
      setConversationContext(prevContext => ({
        previousQuestions: [...prevContext.previousQuestions, question],
        previousResults: [...prevContext.previousResults, data],
        entities: { ...prevContext.entities, ...updatedEntities }
      }));
      
    } catch (err) {
      setMessages((prev) => [
        ...prev,
        { type: "ai", content: "❌ Error fetching response. Please try again or rephrase your question." },
      ]);
    } finally {
      setLoading(false);
    }
  }

  function getTableDescription(tableName: string) {
    return TABLE_DESCRIPTIONS[tableName as keyof typeof TABLE_DESCRIPTIONS] || 
      `No description available for table "${tableName}"`;
  }

  function toggleTableInfo(table: string | null = null) {
    if (table) {
      setSelectedTable(table);
    }
    setShowTableInfo(!showTableInfo);
  }

  function addTableToQuestion(tableName: string) {
    const tablePrefix = question ? " " : "";
    setQuestion(prev => `${prev}${tablePrefix}${tableName}`);
    setShowTableInfo(false);
  }

  return (
    <div className="flex flex-col h-screen justify-between">
      <header className="bg-white p-2 border-b">
        <div className="flex items-center justify-between">
          <h1 className="text-black font-bold text-lg ml-2">Text-to-SQL Agent</h1>
          <button
            onClick={() => toggleTableInfo()}
            className="px-3 py-1 bg-blue-500 text-white rounded-md text-sm"
          >
            Table Info
          </button>
        </div>
      </header>

      {showTableInfo && (
        <div className="absolute top-14 right-0 w-80 bg-white border shadow-lg rounded-md z-10 max-h-[80vh] overflow-auto">
          <div className="p-3 border-b bg-gray-50 font-medium flex justify-between items-center">
            <div>Database Tables</div>
            <button 
              onClick={() => setShowTableInfo(false)}
              className="text-gray-500 hover:text-gray-700"
            >
              ✕
            </button>
          </div>
          <div className="p-1">
            {tables.map((table) => (
              <div key={table} className="mb-2 border-b pb-2">
                <div className="flex justify-between items-center px-2">
                  <div className="font-medium text-blue-600">{table}</div>
                  <button 
                    onClick={() => addTableToQuestion(table)}
                    className="text-xs bg-blue-50 text-blue-600 px-2 py-1 rounded"
                  >
                    Use
                  </button>
                </div>
                <div className="px-2 text-xs text-gray-600 mt-1">
                  {getTableDescription(table).substring(0, 120)}...
                </div>
                <button 
                  onClick={() => toggleTableInfo(table)}
                  className="text-xs text-blue-500 px-2 mt-1"
                >
                  View details
                </button>
              </div>
            ))}
          </div>
        </div>
      )}

      {selectedTable && showTableInfo && (
        <div className="absolute top-1/2 left-1/2 transform -translate-x-1/2 -translate-y-1/2 w-3/4 bg-white border shadow-2xl rounded-md z-20">
          <div className="p-3 border-b bg-gray-50 font-medium flex justify-between items-center">
            <div>Table: {selectedTable}</div>
            <button 
              onClick={() => setSelectedTable(null)}
              className="text-gray-500 hover:text-gray-700"
            >
              ✕
            </button>
          </div>
          <div className="p-4">
            <h3 className="font-medium mb-2">Description:</h3>
            <p className="mb-4 text-sm">{getTableDescription(selectedTable)}</p>
            <div className="flex justify-end">
              <button 
                onClick={() => addTableToQuestion(selectedTable)}
                className="bg-blue-500 text-white px-3 py-1 rounded-md text-sm mr-2"
              >
                Use in Query
              </button>
              <button 
                onClick={() => setSelectedTable(null)}
                className="bg-gray-300 px-3 py-1 rounded-md text-sm"
              >
                Close
              </button>
            </div>
          </div>
        </div>
      )}

      <div className="flex flex-col h-full overflow-y-auto p-4 space-y-4">
        {messages.map((msg, idx) => {
          const isUser = msg.type === "user";

          return (
            <div key={idx} className={`flex ${isUser ? "justify-start" : "justify-end"} w-full`}>
              <div className={`flex items-start ${isUser ? "" : "flex-row-reverse"}`}>
                <div className={`h-8 w-8 rounded-full flex items-center justify-center text-white text-sm ${isUser ? "bg-orange-400" : "bg-green-400"}`}>
                  {isUser ? "Me" : "AI"}
                </div>
                <div className={`ml-2 p-3 rounded-xl shadow text-sm whitespace-pre-wrap ${isUser ? "bg-white" : "bg-indigo-100"}`}>
                  {msg.content}
                </div>
              </div>
            </div>
          );
        })}
      </div>

      <div className="bg-gray-100 p-4">
        <div className="flex items-center h-16 rounded-xl bg-white w-full px-4">
          <input
            type="text"
            disabled={loading}
            value={question}
            onChange={(e) => setQuestion(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && sendMessage()}
            className="flex w-full border rounded-xl focus:outline-none focus:border-indigo-300 pl-4 h-10"
            placeholder="Ask your database something..."
          />
          <button
            onClick={sendMessage}
            className="ml-4 flex items-center justify-center bg-indigo-500 hover:bg-indigo-600 rounded-xl text-white px-4 py-2"
            disabled={loading}
          >
            {loading ? "Loading..." : "Send"}
          </button>
        </div>
      </div>
    </div>
  );
}
