import Home from "./home";

export default async function Page() {
  // Construct the absolute URL for the API endpoint
  const baseUrl = process.env.NEXT_PUBLIC_API_URL || "http://localhost:3000";
  const response = await fetch(`${baseUrl}/api/getTables`, {
    cache: "no-store", // Ensures fresh data on every request
  });

  const tables = await response.json();

  // Ensure tables is always an array of strings
  const sanitizedTables = Array.isArray(tables)
    ? tables.map((table) => table.table_name)
    : [];
  
  return <Home initialTables={sanitizedTables} />;
}