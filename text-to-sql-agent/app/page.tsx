import TextToSQLAgent from "./components/TextToSQLAgent";

// Server Component - does not use React hooks
export default function Page() {
  // Don't use useState in Server Component
  return <TextToSQLAgent />;
}