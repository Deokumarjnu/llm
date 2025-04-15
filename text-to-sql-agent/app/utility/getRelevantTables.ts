export function getRelevantTables(
  query: string,
  allTables: Record<string, { columns: Array<{ column: string; type: string }>}>,
  limit = 20
) {
  const queryLower = query.toLowerCase();
  
  // Score tables based on query relevance
  const scoredTables = Object.entries(allTables).map(([table, data]) => {
    const { columns } = data;
    
    // Base score for direct table name mention
    let score = queryLower.includes(table.toLowerCase()) ? 10 : 0;
    
    // Add score for keyword matches
    if (queryLower.includes('school')) score += table.toLowerCase().includes('school') ? 5 : 0;
    if (queryLower.includes('district')) score += table.toLowerCase().includes('district') ? 5 : 0;
    
    // Add score for column matches
    score += columns.reduce(
      (acc, col) => queryLower.includes(col.column.toLowerCase()) ? acc + 1 : acc,
      0
    );
    
    return { table, score, columns };
  });
  
  // Get top scored tables
  const topTables = scoredTables
    .sort((a, b) => b.score - a.score)
    .slice(0, Math.max(1, limit / 2))
    .filter(t => t.score > 0);
  
  // Find related tables
  const relatedTableNames = new Set<string>();
  
  // Add related tables to the list if not already included
  const additionalTables = scoredTables
    .filter(t => relatedTableNames.has(t.table) && !topTables.find(tt => tt.table === t.table))
    .slice(0, Math.max(1, limit / 2));
  
  return [...topTables, ...additionalTables]
    .slice(0, limit);
}