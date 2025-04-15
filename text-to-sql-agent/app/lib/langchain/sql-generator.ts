import { getVectorStore } from "./vector-store";
import { OpenAI } from "@langchain/openai";
import { Client } from "pg";
import { TABLE_DESCRIPTIONS } from "../../../pages/utils/constant";

const client = new Client({
  connectionString: process.env.DATABASE_URL,
});
await client.connect();

// Parse table descriptions to extract relationships
function parseTableRelationships() {
  const relationships = new Map();
  
  // Extract relationships from table descriptions
  for (const [tableName, description] of Object.entries(TABLE_DESCRIPTIONS)) {
    // Parse the description to identify potential relationships
    const relatedTablesMatches = description.match(/relates to (?:a |an |')?([a-z_]+)(?:'| table| tables)?/gi) || [];
    const foreignKeyMatches = description.match(/(?:via|through) ['"]?([a-z_]+_id)['"]?/gi) || [];
    
    const relatedTables: string[] = [];
    
    // Extract table names from relationship matches
    relatedTablesMatches.forEach(match => {
      const tableName = match.match(/relates to (?:a |an |')?([a-z_]+)/i)?.[1];
      if (tableName && !tableName.includes(' ')) {
        relatedTables.push(tableName.toLowerCase());
      }
    });
    
    // Extract foreign key fields
    const foreignKeys = foreignKeyMatches.map(match => 
      match.match(/(?:via|through) ['"]?([a-z_]+_id)['"]?/i)?.[1]?.toLowerCase()
    ).filter(Boolean);
    
    relationships.set(tableName, { relatedTables, foreignKeys });
  }
  
  return relationships;
}

// Find the best table to query based on the user question
function findRelevantTables(question: string, topN = 3) {
  const normalizedQuestion = question.toLowerCase();
  const relevanceScores = [];
  
  for (const [tableName, description] of Object.entries(TABLE_DESCRIPTIONS)) {
    // Check if table name is mentioned directly
    const tableNameMentioned = normalizedQuestion.includes(tableName.toLowerCase());
    
    // Check for keywords in the description that match the question
    const keywords = description.toLowerCase().split(/\s+/);
    const keywordMatches = keywords.filter(word => 
      word.length > 3 && normalizedQuestion.includes(word)
    ).length;
    
    // Calculate a relevance score
    const score = (tableNameMentioned ? 10 : 0) + keywordMatches;
    
    if (score > 0 || tableNameMentioned) {
      relevanceScores.push({ tableName, score });
    }
  }
  
  // Sort by relevance score and get top N results
  return relevanceScores
    .sort((a, b) => b.score - a.score)
    .slice(0, topN)
    .map(item => item.tableName);
}

// Check if a question is a follow-up to a previous question
function isFollowUpQuestion(question: string) {
  const followupPatterns = [
    /^(and|also|what about|how about|can you|now|then)\s+/i,
    /^(show|list|get|find|tell me|give me)\s+/i,
    /^(what|where|who|how many)\s+/i,
    /those/i,
    /them$/i,
    /\bfrom there\b/i,
    /\bthese\b/i,
    /\bthat\b/i
  ];
  
  return followupPatterns.some(pattern => pattern.test(question.trim()));
}

// Apply context to the current question based on previous context
function applyContext(question: string, context: any) {
  if (!context || Object.keys(context.entities).length === 0) {
    return question;
  }

  let enhancedQuestion = question;
  
  // Check if the question is specifically looking for a named school
  const hasSpecificSchoolName = /(?:Elementary|Middle|High|Charter)\s+School/.test(question);
  
  // Check if the question explicitly mentions a district or school
  const mentionsDistrict = /district(?:_|\s+)?id|district\s+\d+|districtid/i.test(question);
  const mentionsSchool = /\bschool\b|\binstitution\b/i.test(question);

  // For follow-up questions about students that don't specify a school
  if (question.toLowerCase().includes('student') && !mentionsSchool && !hasSpecificSchoolName && context.entities.school_name) {
    enhancedQuestion = `${enhancedQuestion} in school ${context.entities.school_name}`;
  }
  
  // ONLY apply district context if we don't have a specific school name
  if ((question.toLowerCase().includes('school') || question.toLowerCase().includes('institution')) 
      && !mentionsDistrict && context.entities.district_id && !hasSpecificSchoolName) {
    enhancedQuestion = `${enhancedQuestion} for district_id ${context.entities.district_id}`;
  }

  // If question appears to be a follow-up without context
  if (isFollowUpQuestion(question)) {
    // Add the most relevant context, but don't override specific school names
    if (context.entities.school_name && !mentionsSchool && !hasSpecificSchoolName) {
      enhancedQuestion = `${enhancedQuestion} for school ${context.entities.school_name}`;
    } else if (context.entities.district_id && !mentionsDistrict && !hasSpecificSchoolName) {
      enhancedQuestion = `${enhancedQuestion} for district_id ${context.entities.district_id}`;
    }
  }

  console.log(`Enhanced question: "${enhancedQuestion}" (original: "${question}")`);
  return enhancedQuestion;
}

export async function generateSQLQuery(question: string, queryContext?: any) {
  const vectorStore = await getVectorStore();
  const relationships = parseTableRelationships();
  
  // Check if input is just a conversational phrase
  const conversationalPhrasePattern = /^(?:nice|great|awesome|good|thanks|thank you|cool|perfect|excellent|fine|ok|okay|yes|no|sure|got it)[\s!.]*$/i;
  if (conversationalPhrasePattern.test(question.trim())) {
    return {
      sql: "",
      note: "Conversational input detected. Please enter a database query.",
      rows: []
    };
  }
  
  // Pre-process question to standardize references to institutions/schools
  // But preserve proper names containing "school" - like "Elementary School"
  const schoolNamePattern = /([A-Z][a-z]+\s+)?[A-Z][a-z]*\s+School/g;
  const schoolNames = [...question.matchAll(schoolNamePattern)].map(m => m[0]);
  
  // Store the school names before replacement to restore them later
  const schoolNamesMap = new Map();
  let tempQuestion = question;
  
  // Replace school names with placeholders to protect them
  schoolNames.forEach((name, index) => {
    const placeholder = `__SCHOOL_NAME_${index}__`;
    schoolNamesMap.set(placeholder, name);
    tempQuestion = tempQuestion.replace(name, placeholder);
  });
  
  // Now do the regular standardization on the protected text
  const standardizedQuestion = tempQuestion
    .replace(/\bschools\b/gi, 'institutions')
    .replace(/\bschool\b/gi, 'institution');
  
  // Restore the original school names
  let finalQuestion = standardizedQuestion;
  schoolNamesMap.forEach((name, placeholder) => {
    finalQuestion = finalQuestion.replace(placeholder, name);
  });
  
  // DEBUG: Log the original and transformed questions
  console.log(`Original question: "${question}"`);
  console.log(`After processing: "${finalQuestion}"`);
  
  // Apply conversation context if available, but ONLY if it's a follow-up question
  const isThisAFollowUp = isFollowUpQuestion(finalQuestion);
  console.log(`Is this a follow-up question? ${isThisAFollowUp}`);
  
  let processedQuestion;
  if (queryContext && isThisAFollowUp) {
    processedQuestion = applyContext(finalQuestion, queryContext);
  } else {
    // If it's not a follow-up, don't apply context automatically
    processedQuestion = finalQuestion;
  }
  
  // Special handling for Elementary School queries - they shouldn't have district constraints
  if (question.includes("Elementary School") && processedQuestion.includes("district_id")) {
    // Remove any district_id constraints that might have been added from context
    processedQuestion = finalQuestion; // Revert to the version without context
    console.log(`Removed district context for Elementary School query: "${processedQuestion}"`);
  }
  
  // Find relevant tables for this question
  const relevantTables = findRelevantTables(processedQuestion);
  
  // Specific pattern handling for common query types
  const normalizedQuestion = processedQuestion.toLowerCase();
  
  // NEW PATTERN: Handle queries about students in a specific school with district filter
  // This pattern separates the school name from district conditions
  const studentsInSchoolWithDistrictPattern = /(?:how many |number of |count |list |show |get |find )?(?:students|users)(?:.*)(?:in|at|for|of|from)\s+(?:school|institution)?\s*(?:"|')?([^?"']+?)(?:"|')?(?:\s*for|\s*with|\s*where|\s*in|\s*and)\s+(?:district(?:_|\s*)?id|districtid)\s*(?:=|:|\s+)?\s*(\d+)/i;
  const studentsInSchoolWithDistrictMatch = normalizedQuestion.match(studentsInSchoolWithDistrictPattern);
  
  if (studentsInSchoolWithDistrictMatch && studentsInSchoolWithDistrictMatch[1] && studentsInSchoolWithDistrictMatch[2]) {
    // Extract the school name and district ID
    const schoolName = studentsInSchoolWithDistrictMatch[1].trim();
    const districtId = studentsInSchoolWithDistrictMatch[2].trim();
    
    try {
      // First find the school by name AND district ID
      const schoolLookupSQL = `SELECT id FROM institutions WHERE name ILIKE '%${schoolName}%' AND district_id = ${districtId}`;
      const schoolResult = await client.query(schoolLookupSQL);
      
      if (schoolResult.rows.length > 0) {
        // If school found, try to find students linked to this school
        const schoolId = schoolResult.rows[0].id;
        
        // Check if we want a list or a count
        const wantsList = normalizedQuestion.match(/(?:list|show|get|find)/i) !== null;
        
        // Look for a table that links users/students to institutions
        try {
          // Try institutions_users and join with users for details
          if (wantsList) {
            // Return detailed list of students
            const listSQL = `
              SELECT u.id, u.first_name, u.last_name, u.email
              FROM institutions_users iu
              JOIN users u ON iu.user_id = u.id
              WHERE iu.institution_id = ${schoolId} 
                AND iu.deleted_at IS NULL
              ORDER BY u.last_name, u.first_name
            `;
            const listResult = await client.query(listSQL);
            
            return {
              sql: listSQL,
              rows: listResult.rows,
              note: `List of students in ${schoolName} (ID: ${schoolId}) in district ${districtId}`
            };
          } else {
            // Return just the count
            const countSQL = `
              SELECT COUNT(*) as student_count 
              FROM institutions_users 
              WHERE institution_id = ${schoolId} AND deleted_at IS NULL
            `;
            const countResult = await client.query(countSQL);
            
            return {
              sql: countSQL,
              rows: countResult.rows,
              note: `Count of students in ${schoolName} (ID: ${schoolId}) in district ${districtId}`
            };
          }
        } catch (err1) {
          try {
            // Try schools_users (alternate pattern)
            if (wantsList) {
              const listSQL = `
                SELECT u.id, u.first_name, u.last_name, u.email
                FROM schools_users su
                JOIN users u ON su.user_id = u.id
                WHERE su.school_id = ${schoolId}
                ORDER BY u.last_name, u.first_name
              `;
              const listResult = await client.query(listSQL);
              
              return {
                sql: listSQL,
                rows: listResult.rows,
                note: `List of students in ${schoolName} (ID: ${schoolId}) in district ${districtId}`
              };
            } else {
              const countSQL = `
                SELECT COUNT(*) as student_count 
                FROM schools_users 
                WHERE school_id = ${schoolId}
              `;
              const countResult = await client.query(countSQL);
              
              return {
                sql: countSQL,
                rows: countResult.rows,
                note: `Count of students in ${schoolName} (ID: ${schoolId}) in district ${districtId}`
              };
            }
          } catch (err2) {
            try {
              // Try courses_users through courses linked to institutions
              if (wantsList) {
                const listSQL = `
                  SELECT DISTINCT u.id, u.first_name, u.last_name, u.email
                  FROM courses c
                  JOIN courses_users cu ON c.id = cu.course_id
                  JOIN users u ON cu.user_id = u.id
                  WHERE c.institution_id = ${schoolId}
                  ORDER BY u.last_name, u.first_name
                `;
                const listResult = await client.query(listSQL);
                
                return {
                  sql: listSQL,
                  rows: listResult.rows,
                  note: `List of students in ${schoolName} (ID: ${schoolId}) in district ${districtId} based on course enrollment`
                };
              } else {
                const countSQL = `
                  SELECT COUNT(DISTINCT cu.user_id) as student_count
                  FROM courses c
                  JOIN courses_users cu ON c.id = cu.course_id
                  WHERE c.institution_id = ${schoolId}
                `;
                const countResult = await client.query(countSQL);
                
                return {
                  sql: countSQL,
                  rows: countResult.rows,
                  note: `Count of students in ${schoolName} (ID: ${schoolId}) in district ${districtId} based on course enrollment`
                };
              }
            } catch (err3) {
              // Return the school info at least
              return {
                sql: schoolLookupSQL,
                rows: schoolResult.rows,
                note: `Found school "${schoolName}" in district ${districtId} but could not determine student information.`
              };
            }
          }
        }
      } else {
        // No school found with that name in that district
        return {
          sql: schoolLookupSQL,
          rows: [],
          note: `No institution found matching name: "${schoolName}" in district ${districtId}`
        };
      }
    } catch (err) {
      console.error(`Failed school lookup:`, err instanceof Error ? err.message : err);
    }
  }
  
  // Regular pattern for students in a school without district specification
  const studentsInSchoolPattern = /(?:how many |number of |count |list |show |get |find )?(?:students|users)(?:.*)(?:in|at|for|of|from)\s+(?:school|institution)?\s*(?:"|')?([^?"']+?)(?:"|')?(?:\s*\?)?$/i;
  const studentsInSchoolMatch = normalizedQuestion.match(studentsInSchoolPattern);

  if (studentsInSchoolMatch && studentsInSchoolMatch[1] && !studentsInSchoolWithDistrictMatch) {
    // Extract the school name
    const schoolName = studentsInSchoolMatch[1].trim();
    
    try {
      // First find the school by name
      const schoolLookupSQL = `SELECT id FROM institutions WHERE name ILIKE '%${schoolName}%'`;
      const schoolResult = await client.query(schoolLookupSQL);
      
      if (schoolResult.rows.length > 0) {
        // If school found, get students linked to this school
        const schoolId = schoolResult.rows[0].id;
        
        // Check if we want a list or a count
        const wantsList = normalizedQuestion.match(/(?:list|show|get|find)/i) !== null;
        
        // Look for a table that links users/students to institutions
        try {
          // Try institutions_users first (most common pattern)
          if (wantsList) {
            // Return detailed list of students
            const listSQL = `
              SELECT u.id, u.first_name, u.last_name, u.email
              FROM institutions_users iu
              JOIN users u ON iu.user_id = u.id
              WHERE iu.institution_id = ${schoolId} 
                AND iu.deleted_at IS NULL
              ORDER BY u.last_name, u.first_name
            `;
            const listResult = await client.query(listSQL);
            
            return {
              sql: listSQL,
              rows: listResult.rows,
              note: `List of students in ${schoolName} (ID: ${schoolId})`
            };
          } else {
            // Return just the count
            const countSQL = `
              SELECT COUNT(*) as student_count 
              FROM institutions_users 
              WHERE institution_id = ${schoolId} AND deleted_at IS NULL
            `;
            const countResult = await client.query(countSQL);
            
            return {
              sql: countSQL,
              rows: countResult.rows,
              note: `Count of students in ${schoolName} (ID: ${schoolId})`
            };
          }
        } catch (err1) {
          // Remaining fallback approaches...
          try {
            // Try schools_users (alternate pattern)
            if (wantsList) {
              const listSQL = `
                SELECT u.id, u.first_name, u.last_name, u.email
                FROM schools_users su
                JOIN users u ON su.user_id = u.id
                WHERE su.school_id = ${schoolId}
                ORDER BY u.last_name, u.first_name
              `;
              const listResult = await client.query(listSQL);
              
              return {
                sql: listSQL,
                rows: listResult.rows,
                note: `List of students in ${schoolName} (ID: ${schoolId})`
              };
            } else {
              const countSQL = `
                SELECT COUNT(*) as student_count 
                FROM schools_users 
                WHERE school_id = ${schoolId}
              `;
              const countResult = await client.query(countSQL);
              
              return {
                sql: countSQL,
                rows: countResult.rows,
                note: `Count of students in ${schoolName} (ID: ${schoolId})`
              };
            }
          } catch (err2) {
            try {
              // Try courses_users through courses linked to institutions
              if (wantsList) {
                const listSQL = `
                  SELECT DISTINCT u.id, u.first_name, u.last_name, u.email
                  FROM courses c
                  JOIN courses_users cu ON c.id = cu.course_id
                  JOIN users u ON cu.user_id = u.id
                  WHERE c.institution_id = ${schoolId}
                  ORDER BY u.last_name, u.first_name
                `;
                const listResult = await client.query(listSQL);
                
                return {
                  sql: listSQL,
                  rows: listResult.rows,
                  note: `List of students in ${schoolName} (ID: ${schoolId}) based on course enrollment`
                };
              } else {
                const countSQL = `
                  SELECT COUNT(DISTINCT cu.user_id) as student_count
                  FROM courses c
                  JOIN courses_users cu ON c.id = cu.course_id
                  WHERE c.institution_id = ${schoolId}
                `;
                const countResult = await client.query(countSQL);
                
                return {
                  sql: countSQL,
                  rows: countResult.rows,
                  note: `Count of students in ${schoolName} (ID: ${schoolId}) based on course enrollment`
                };
              }
            } catch (err3) {
              // Return the school info at least
              return {
                sql: schoolLookupSQL,
                rows: schoolResult.rows,
                note: `Found school "${schoolName}" but could not determine student information.`
              };
            }
          }
        }
      } else {
        // No school found with that name
        return {
          sql: schoolLookupSQL,
          rows: [],
          note: `No institution found matching name: ${schoolName}`
        };
      }
    } catch (err) {
      console.error(`Failed school lookup:`, err instanceof Error ? err.message : err);
    }
  }
  
  // Handle school/institution queries with district ID or name pattern
  // This pattern captures both numeric IDs and text names
  const districtPattern = /(?:schools|institutions|school|institution)(?:.*)(?:(?:associated|related|belonging|linked|connected)(?:.*)(?:district(?:_|\s+)?id|districtid|district)\s*(?:=|:|\s+)?\s*(?:"|')?([\w\s-]+)(?:"|')?|(?:district|district_id|districtid)\s*(?:=|:|\s+)?\s*(?:"|')?([\w\s-]+)(?:"|')?)/i;
  const districtMatch = normalizedQuestion.match(districtPattern);
  
  if (districtMatch) {
    // Extract the district identifier (could be ID number or name) from either of the capture groups
    const districtIdentifier = districtMatch[1] || districtMatch[2];
    
    if (districtIdentifier) {
      // Check if the district identifier is numeric (likely an ID) or text (likely a name)
      const isNumericId = /^\d+$/.test(districtIdentifier.trim());
      
      try {
        let directSQL;
        if (isNumericId) {
          // For numeric IDs, use direct equality
          directSQL = `SELECT * FROM institutions WHERE district_id = ${districtIdentifier.trim()}`;
        } else {
          // For names, use ILIKE for case-insensitive partial matching
          // First try to get the district ID from the district name
          const districtLookupSQL = `SELECT id FROM districts WHERE name ILIKE '%${districtIdentifier.trim()}%'`;
          try {
            const districtResult = await client.query(districtLookupSQL);
            if (districtResult.rows.length > 0) {
              // If district found by name, use its ID to find related institutions
              const districtId = districtResult.rows[0].id;
              directSQL = `SELECT * FROM institutions WHERE district_id = ${districtId}`;
            } else {
              // If no district found, fall back to showing a message
              return {
                sql: districtLookupSQL,
                rows: [],
                note: `No district found matching name: ${districtIdentifier}`
              };
            }
          } catch (lookupErr) {
            // If district lookup fails, just return the error
            return {
              sql: districtLookupSQL,
              error: `Failed to find district with name: ${districtIdentifier}. Error: ${lookupErr instanceof Error ? lookupErr.message : lookupErr}`
            };
          }
        }
        
        // Execute the query to find institutions in the district
        const { rows } = await client.query(directSQL);
        return {
          sql: directSQL,
          rows,
          note: `Query for institutions associated with district ${isNumericId ? 'ID' : 'name'}: ${districtIdentifier}`
        };
      } catch (err) {
        if (err instanceof Error) {
          console.error(`Failed district query:`, err.message);
        } else {
          console.error(`Failed district query:`, err);
        }
      }
    }
  }
  
  // Handle simple listing queries directly
  if (/all\s+(?:the\s+)?(\w+)(?:\s+(?:in|from|at)\s+(?:the\s+)?(?:database|db))?/i.test(normalizedQuestion)) {
    const entityMatch = normalizedQuestion.match(/all\s+(?:the\s+)?(\w+)(?:\s+(?:in|from|at)\s+(?:the\s+)?(?:database|db))?/i);
    const entity = entityMatch ? entityMatch[1].toLowerCase() : null;
    
    if (entity) {
      const possibleTableNames = [
        entity,                    // As is
        entity + 's',              // Plural
        entity.replace(/s$/, '')   // Singular
      ];
      
      // Try each possible table name
      for (const tableName of possibleTableNames) {
        if (TABLE_DESCRIPTIONS[tableName as keyof typeof TABLE_DESCRIPTIONS]) {
          try {
            const directSQL = `SELECT * FROM ${tableName}`;
            const { rows } = await client.query(directSQL);
            return {
              sql: directSQL,
              rows,
              note: `Direct query for all ${tableName}`
            };
          } catch (err) {
            if (err instanceof Error) {
              console.error(`Failed direct query for ${tableName}:`, err.message);
            } else {
              console.error(`Failed direct query for ${tableName}:`, err);
            }
          }
        }
      }
    }
  }
  
  // Handle specific entity by ID patterns
  const idPatternMatch = normalizedQuestion.match(/(\w+)\s+(?:with|having|where)?\s+(?:id|number)?\s*(?:is|=|:)?\s*(\d+)/i);
  if (idPatternMatch) {
    const [_, entity, idValue] = idPatternMatch;
    const singularEntity = entity.toLowerCase().replace(/s$/, '');
    const pluralEntity = singularEntity + 's';
    
    // Check if we have this table in our descriptions
    const tableName = TABLE_DESCRIPTIONS[singularEntity as keyof typeof TABLE_DESCRIPTIONS] ? singularEntity :
                     (TABLE_DESCRIPTIONS[pluralEntity as keyof typeof TABLE_DESCRIPTIONS] ? pluralEntity : null);
    
    if (tableName) {
      try {
        const directSQL = `SELECT * FROM ${tableName} WHERE id = ${idValue}`;
        const { rows } = await client.query(directSQL);
        return {
          sql: directSQL,
          rows,
          note: `Direct query for ${tableName} with id ${idValue}`
        };
      } catch (err) {
        if (err instanceof Error) {
          console.error(`Failed direct query for ${tableName} with ID:`, err.message);
        } else {
          console.error(`Failed direct query for ${tableName} with ID:`, err);
        }
      }
    }
  }
  
  // Handle "related to" patterns
  const relatedToMatch = normalizedQuestion.match(/(\w+)(?:\s+related\s+to|\s+in|\s+for|\s+of|\s+from)\s+(\w+)\s+(?:"|')?([^"']+)(?:"|')?/i);
  if (relatedToMatch) {
    const [_, targetEntity, relatedEntity, relatedIdentifier] = relatedToMatch;
    
    // Special case for "schools/institutions related to district X"
    if ((targetEntity.toLowerCase() === 'schools' || targetEntity.toLowerCase() === 'institutions') && 
        (relatedEntity.toLowerCase() === 'district' || relatedEntity.toLowerCase() === 'districts')) {
        
        // Check if the identifier is numeric (likely an ID) or text (likely a name)
        const isNumericId = /^\d+$/.test(relatedIdentifier.trim());
        
        try {
          let directSQL;
          if (isNumericId) {
            // For numeric IDs, use direct equality
            directSQL = `SELECT * FROM institutions WHERE district_id = ${relatedIdentifier.trim()}`;
          } else {
            // For names, use ILIKE for case-insensitive partial matching
            // First try to get the district ID from the district name
            const districtLookupSQL = `SELECT id FROM districts WHERE name ILIKE '%${relatedIdentifier.trim()}%'`;
            const districtResult = await client.query(districtLookupSQL);
            
            if (districtResult.rows.length > 0) {
              // If district found by name, use its ID to find related institutions
              const districtId = districtResult.rows[0].id;
              directSQL = `SELECT * FROM institutions WHERE district_id = ${districtId}`;
            } else {
              // If no district found, fall back to showing a message
              return {
                sql: districtLookupSQL,
                rows: [],
                note: `No district found matching name: ${relatedIdentifier}`
              };
            }
          }
          
          // Execute the query to find institutions in the district
          const { rows } = await client.query(directSQL);
          return {
            sql: directSQL,
            rows,
            note: `Schools/institutions in district ${isNumericId ? 'ID' : 'name'}: ${relatedIdentifier}`
          };
        } catch (err) {
          if (err instanceof Error) {
            console.error(`Failed district query:`, err.message);
          } else {
            console.error(`Failed district query:`, err);
          }
        }
    }
    
    // Try variations of plural/singular for both entities
    const targetVariations = [
      targetEntity.toLowerCase(),
      targetEntity.toLowerCase().replace(/s$/, ''),
      targetEntity.toLowerCase().replace(/s$/, '') + 's'
    ];
    
    const relatedVariations = [
      relatedEntity.toLowerCase(),
      relatedEntity.toLowerCase().replace(/s$/, ''),
      relatedEntity.toLowerCase().replace(/s$/, '') + 's'
    ];
    
    // Find the most likely table names
    let targetTable = null;
    let relatedTable = null;
    
    for (const targetVar of targetVariations) {
      if (TABLE_DESCRIPTIONS[targetVar as keyof typeof TABLE_DESCRIPTIONS]) {
        targetTable = targetVar;
        break;
      }
    }
    
    for (const relatedVar of relatedVariations) {
      if (TABLE_DESCRIPTIONS[relatedVar as keyof typeof TABLE_DESCRIPTIONS]) {
        relatedTable = relatedVar;
        break;
      }
    }
    
    if (targetTable && relatedTable) {
      // Check if there's a foreign key pattern we can use
      const foreignKeyField = `${relatedTable.replace(/s$/, '')}_id`;
      
      try {
        // Check if relatedIdentifier is numeric for direct ID comparison
        const isNumericId = /^\d+$/.test(relatedIdentifier.trim());
        if (isNumericId) {
          const directSQL = `SELECT * FROM ${targetTable} WHERE ${foreignKeyField} = ${relatedIdentifier.trim()}`;
          const { rows } = await client.query(directSQL);
          return {
            sql: directSQL,
            rows,
            note: `Direct "related to" query for ${targetTable} related to ${relatedTable} ${relatedIdentifier}`
          };
        } else {
          // For non-numeric identifiers, try to find the ID first
          const lookupSQL = `SELECT id FROM ${relatedTable} WHERE name ILIKE '%${relatedIdentifier.trim()}%'`;
          try {
            const lookupResult = await client.query(lookupSQL);
            if (lookupResult.rows.length > 0) {
              const relatedId = lookupResult.rows[0].id;
              const joinSQL = `
                SELECT ${targetTable}.*
                FROM ${targetTable}
                JOIN ${relatedTable} ON ${targetTable}.${foreignKeyField} = ${relatedTable}.id
                WHERE ${relatedTable}.id = ${relatedId}
              `;
              const { rows } = await client.query(joinSQL);
              return {
                sql: joinSQL,
                rows,
                note: `Join query for ${targetTable} related to ${relatedTable} with name: "${relatedIdentifier}" (ID: ${relatedId})`
              };
            } else {
              return {
                sql: lookupSQL,
                rows: [],
                note: `No ${relatedTable} found with name similar to "${relatedIdentifier}"`
              };
            }
          } catch (lookupErr) {
            console.error(`Failed lookup for ${relatedTable}:`, lookupErr instanceof Error ? lookupErr.message : lookupErr);
          }
        }
      } catch (directErr) {
        // If direct query fails, try to find a join path
        try {
          const joinSQL = `
            SELECT ${targetTable}.*
            FROM ${targetTable}
            JOIN ${relatedTable} ON ${targetTable}.${foreignKeyField} = ${relatedTable}.id
            WHERE ${relatedTable}.name ILIKE '%${relatedIdentifier.trim()}%'
          `;
          const { rows } = await client.query(joinSQL);
          return {
            sql: joinSQL,
            rows,
            note: `Join query for ${targetTable} related to ${relatedTable} with name: "${relatedIdentifier}"`
          };
        } catch (joinErr) {
          console.error("Join query failed:", joinErr instanceof Error ? joinErr.message : joinErr);
        }
      }
    }
  }
  
  // Special handler for intervention types with attendance filters
  // Additional pattern specifically for "give me only X type intervention" format
  const interventionTypePattern = /(?:all|get|show|list|find|give me)\s+(?:the\s+)?(?:interventions?|alerts?)(?:\s+(?:with|having|that\s+have|of|for|using|type))?\s+(?:type\s+)?(?:(daily|period|course)\s+attendance|(?:course|period|daily)-attendance)/i;
  const simpleInterventionTypePattern = /(?:all|get|show|list|find|give me)\s+(?:(daily|period|course)(?:\s+attendance)?)\s+(?:type\s+)?(?:interventions?|alerts?)/i;
  const directInterventionTypePattern = /(?:all|get|show|list|find|give me)(?:\s+only)?\s+(?:intervention|alert)s?\s+(?:type\s+)?(?:which|that)?\s+(?:(daily|period|course)\s+attendance)/i;
  const onlyCoursePattern = /(?:all|get|show|list|find|give me)\s+(?:only)?\s+(?:(?:intervention|alert)s?\s+which\s+is\s+|(?:intervention|alert)s?\s+with\s+|)(?:course\s+attendance)\s+(?:intervention|alert|type)?/i;
  const onlyTypePattern = /(?:all|get|show|list|find|give me)\s+(?:only)?\s+(?:intervention|alert)s?\s+which\s+(?:are|is)\s+(course|daily|period)(?:\s+attendance)?/i;
  
  // Check if the user is asking for interventions
  const isInterventionQuery = (
    normalizedQuestion.includes('intervention') || 
    normalizedQuestion.includes('alert')
  ) && (
    normalizedQuestion.includes('course') || 
    normalizedQuestion.includes('daily') || 
    normalizedQuestion.includes('period') ||
    normalizedQuestion.includes('attendance')
  );
  
  // Direct check for "give me only course attendance intervention" or similar
  if (isInterventionQuery) {
    console.log("Processing intervention query:", normalizedQuestion);
    
    // Determine which type of intervention is being requested
    let interventionType = null;
    
    // Check against all patterns to identify the intervention type
    const typeFromPattern = 
      (interventionTypePattern.exec(normalizedQuestion) || [])[1] ||
      (simpleInterventionTypePattern.exec(normalizedQuestion) || [])[1] ||
      (directInterventionTypePattern.exec(normalizedQuestion) || [])[1] ||
      (onlyCoursePattern.test(normalizedQuestion) ? 'course' : null) ||
      (onlyTypePattern.exec(normalizedQuestion) || [])[1];
    
    if (typeFromPattern) {
      interventionType = typeFromPattern.toLowerCase();
    }
    // Use keyword matching as fallback when no specific pattern matches
    else if ((normalizedQuestion.includes('course') && !normalizedQuestion.includes('daily') && !normalizedQuestion.includes('period')) || 
              normalizedQuestion.match(/only.*course/i)) {
      interventionType = 'course';
    } else if ((normalizedQuestion.includes('period') && !normalizedQuestion.includes('daily') && !normalizedQuestion.includes('course')) || 
              normalizedQuestion.match(/only.*period/i)) {
      interventionType = 'period';
    } else if ((normalizedQuestion.includes('daily') && !normalizedQuestion.includes('period') && !normalizedQuestion.includes('course')) || 
              normalizedQuestion.match(/only.*daily/i)) {
      interventionType = 'daily';
    }
    
    console.log(`Detected intervention type: ${interventionType || 'unspecified'}`);
    
    // Direct execution for course attendance interventions based on the sample data
    if (interventionType === 'course') {
      try {
        console.log("Direct course attendance intervention query");
        const directSQL = `
          SELECT * FROM interventions 
          WHERE course_attendance_filters IS NOT NULL 
          ORDER BY created_at DESC
        `;
        const results = await client.query(directSQL);
        if (results.rows.length > 0) {
          return {
            sql: directSQL,
            rows: results.rows,
            note: `Found ${results.rows.length} interventions with Course Attendance filters`
          };
        } else {
          return {
            sql: directSQL,
            rows: [],
            note: "No interventions found with Course Attendance filters"
          };
        }
      } catch (err) {
        if (err instanceof Error) {
          console.error("Error with direct course attendance query:", err.message);
        } else {
          console.error("Error with direct course attendance query:", err);
        }
      }
    }
    
    // Try multiple possible table names for interventions
    const possibleTableNames = ["districts_interventions"]; 

    let tableExists = false;
    for (const tableName of possibleTableNames) {
      try {
        console.log(`Checking table existence: ${tableName}`);
        const checkSQL = `SELECT 1 FROM ${tableName} LIMIT 1`;
        await client.query(checkSQL);
        console.log(`Table exists: ${tableName}`);
        tableExists = true;
        break; // Exit loop if a valid table is found
      } catch (checkErr) {
        console.log(`Table ${tableName} does not exist or is not accessible`);
      }
    }

    if (!tableExists) {
      console.error("The 'interventions' table does not exist in the database.");
      return {
        sql: "",
        rows: [],
        note: "The 'interventions' table does not exist in the database. Please check the database schema or contact the database administrator to ensure the required table is created and accessible."
      };
    }

    // Proceed with querying the 'interventions' table
    for (const tableName of possibleTableNames) {
      try {
        console.log(`Trying table name: ${tableName}`);
        let interventionSQL;

        if (interventionType === 'course') {
          interventionSQL = `
            SELECT * FROM ${tableName} 
            WHERE course_attendance_filters IS NOT NULL 
            ORDER BY created_at DESC
          `;
        } else if (interventionType === 'period') {
          interventionSQL = `
            SELECT * FROM ${tableName} 
            WHERE period_attendance_filters IS NOT NULL 
            ORDER BY created_at DESC
          `;
        } else if (interventionType === 'daily') {
          interventionSQL = `
            SELECT * FROM ${tableName} 
            WHERE daily_attendance_filters IS NOT NULL 
            ORDER BY created_at DESC
          `;
        } else {
          // If no specific type, get interventions with any attendance filters
          interventionSQL = `
            SELECT * FROM ${tableName} 
            WHERE course_attendance_filters IS NOT NULL 
               OR period_attendance_filters IS NOT NULL 
               OR daily_attendance_filters IS NOT NULL
            ORDER BY created_at DESC
          `;
        }

        // Try executing the query
        const results = await client.query(interventionSQL);

        let typeText = interventionType
          ? `${interventionType.charAt(0).toUpperCase() + interventionType.slice(1)} Attendance`
          : 'any attendance type';

        return {
          sql: interventionSQL,
          rows: results.rows,
          note: `Found ${results.rows.length} interventions with ${typeText} filters`
        };
      } catch (err) {
        if (err instanceof Error) {
          console.error(`Error querying ${tableName}:`, err.message);
        } else {
          console.error(`Error querying ${tableName}:`, err);
        }
        // Continue to the next table name
      }
    }
    
    // Last resort - try a completely different approach with a hardcoded query
    // based on the sample data provided
    try {
      console.log("Attempting last resort query for interventions");
      // Based on the sample data, we know the interventions table exists
      // and has course_attendance_filters, daily_attendance_filters columns
      const sampleSQL = `
        SELECT * FROM interventions 
        WHERE ${interventionType === 'course' ? 'course_attendance_filters IS NOT NULL' : 
               interventionType === 'period' ? 'period_attendance_filters IS NOT NULL' : 
               interventionType === 'daily' ? 'daily_attendance_filters IS NOT NULL' : 
               '(course_attendance_filters IS NOT NULL OR period_attendance_filters IS NOT NULL OR daily_attendance_filters IS NOT NULL)'}
      `;
      
      const sampleResult = await client.query(sampleSQL);
      
      if (sampleResult.rows.length > 0) {
        let typeText = interventionType ? 
                      `${interventionType.charAt(0).toUpperCase() + interventionType.slice(1)} Attendance` : 
                      'any attendance type';
        
        return {
          sql: sampleSQL,
          rows: sampleResult.rows,
          note: `Found ${sampleResult.rows.length} interventions with ${typeText} filters`
        };
      } else {
        // Try without filtering if no results
        const basicSQL = `SELECT * FROM interventions LIMIT 100`;
        const basicResult = await client.query(basicSQL);
        
        return {
          sql: basicSQL,
          rows: basicResult.rows,
          note: `Returning all interventions (up to 100). Please review to identify intervention type.`
        };
      }
    } catch (finalErr) {
      if (finalErr instanceof Error) {
        console.error("Final intervention query attempts failed:", finalErr);
      } else {
        console.error("Final intervention query attempts failed:", finalErr);
      }
      return {
        sql: "",
        error: "Could not retrieve intervention data after multiple attempts",
        rows: [],
        note: "Try using a more specific query or check the database structure."
      };
    }
  }
  
  // When processing the query string, detect special patterns for students in schools
  const studentsAssociatedPattern = /(?:students|users)\s+(?:associated|connected|linked|related)\s+(?:with|to)\s+(?:institutions|schools|school|institution)\s+(?:named|called|name)?\s*(?:"|')?([^"']+)(?:"|')?/i;
  const studentsAssociatedMatch = normalizedQuestion.match(studentsAssociatedPattern);
  
  if (studentsAssociatedMatch && studentsAssociatedMatch[1]) {
    const schoolName = studentsAssociatedMatch[1].trim();
    console.log(`Looking for students associated with school named: "${schoolName}"`);
    
    try {
      // First find all schools matching this name pattern
      const schoolLookupSQL = `SELECT id FROM institutions WHERE name ILIKE '%${schoolName}%'`;
      const schoolResult = await client.query(schoolLookupSQL);
      
      if (schoolResult.rows.length > 0) {
        // We found matching schools - now get students from these schools
        const schoolIds = schoolResult.rows.map(row => row.id);
        console.log(`Found ${schoolIds.length} matching schools with IDs: ${schoolIds.join(', ')}`);
        
        // Query to get only the first_name of students
        const studentSQL = `
          SELECT DISTINCT u.first_name
          FROM users u
          JOIN institutions_users iu ON u.id = iu.user_id
          WHERE iu.institution_id IN (${schoolIds.join(',')})
            AND iu.deleted_at IS NULL
          ORDER BY u.first_name
        `;
        
        const studentResult = await client.query(studentSQL);
        return {
          sql: studentSQL,
          rows: studentResult.rows,
          note: `Found ${studentResult.rows.length} students associated with schools named "${schoolName}" (first names only)`
        };
      } else {
        return {
          sql: schoolLookupSQL,
          rows: [],
          note: `No schools found matching name: "${schoolName}"`
        };
      }
    } catch (err) {
      console.error("Error querying students:", err instanceof Error ? err.message : err);
      return {
        sql: "",
        rows: [],
        note: `An error occurred while querying students for school "${schoolName}".`
      };
    }
  }

  // Handle queries for reports related to a specific student
  const reportsForStudentPattern = /(?:all|get|show|list|find)\s+(?:the\s+)?reports\s+(?:for|of|about)\s+(?:student|user)\s+(?:"|')?([^"']+)(?:"|')?/i;
  const reportsForStudentMatch = normalizedQuestion.match(reportsForStudentPattern);

  if (reportsForStudentMatch && reportsForStudentMatch[1]) {
    const studentName = reportsForStudentMatch[1].trim();
    console.log(`Looking for reports for student named: "${studentName}"`);

    try {
      // Query to find the student by name
      const studentLookupSQL = `
        SELECT id FROM users 
        WHERE first_name ILIKE '%${studentName}%' OR last_name ILIKE '%${studentName}%'
      `;
      const studentResult = await client.query(studentLookupSQL);

      if (studentResult.rows.length > 0) {
        // We found the student(s) - now get reports for these students
        const studentIds = studentResult.rows.map(row => row.id);
        console.log(`Found ${studentIds.length} matching students with IDs: ${studentIds.join(', ')}`);

        // Query to get reports for the student(s)
        const reportsSQL = `
          SELECT gr.*
          FROM generated_reports gr
          JOIN users u ON gr.user_id = u.id
          WHERE u.id IN (${studentIds.join(',')})
          ORDER BY gr.created_at DESC
        `;

        const reportsResult = await client.query(reportsSQL);
        return {
          sql: reportsSQL,
          rows: reportsResult.rows,
          note: `Found ${reportsResult.rows.length} reports for student(s) named "${studentName}"`
        };
      } else {
        return {
          sql: studentLookupSQL,
          rows: [],
          note: `No students found matching name: "${studentName}"`
        };
      }
    } catch (err) {
      console.error("Error querying reports for student:", err instanceof Error ? err.message : err);
      return {
        sql: "",
        rows: [],
        note: `An error occurred while querying reports for student "${studentName}".`
      };
    }
  }

  // Also check for "all students in Elementary School" pattern
  const studentsInSchoolPatternAlt = /(?:all|every|the|show|list|get|find)\s+(?:students|users)\s+(?:in|at|from|of)\s+(?:"|')?([^"']+?)(?:"|')?(?:\s+schools?|\s+institutions?)?(?:\s*\?)?$/i;
  const studentsInSchoolMatchAlt = normalizedQuestion.match(studentsInSchoolPatternAlt);

  if (studentsInSchoolMatchAlt && studentsInSchoolMatchAlt[1]) {
    const schoolName = studentsInSchoolMatchAlt[1].trim();
    console.log(`Looking for students in school named: "${schoolName}"`);
    
    try {
      // Find schools matching this name pattern
      const schoolLookupSQL = `SELECT id, name FROM institutions WHERE name ILIKE '%${schoolName}%'`;
      const schoolResult = await client.query(schoolLookupSQL);
      
      if (schoolResult.rows.length > 0) {
        // We found matching schools - now get students
        const schoolIds = schoolResult.rows.map(row => row.id);
        
        // Try all potential joins to find students
        const studentSQL = `
          SELECT u.id, u.first_name, u.last_name, u.email, i.name as school_name
          FROM users u
          JOIN institutions_users iu ON u.id = iu.user_id
          JOIN institutions i ON iu.institution_id = i.id
          WHERE i.id IN (${schoolIds.join(',')})
            AND iu.deleted_at IS NULL
          ORDER BY i.name, u.last_name, u.first_name
        `;
        
        try {
          const studentResult = await client.query(studentSQL);
          return {
            sql: studentSQL,
            rows: studentResult.rows,
            note: `Students in schools named "${schoolName}"`
          };
        } catch (studentErr) {
          // Try alternate paths if this fails
          try {
            const altStudentSQL = `
              SELECT DISTINCT u.id, u.first_name, u.last_name, u.email, i.name as school_name
              FROM users u
              LEFT JOIN institutions_users iu ON u.id = iu.user_id AND iu.deleted_at IS NULL
              LEFT JOIN schools_users su ON u.id = su.user_id
              LEFT JOIN courses_users cu ON u.id = cu.user_id
              LEFT JOIN courses c ON cu.course_id = c.id
              JOIN institutions i ON 
                (iu.institution_id = i.id) OR 
                (su.school_id = i.id) OR 
                (c.institution_id = i.id)
              WHERE i.id IN (${schoolIds.join(',')})
              ORDER BY i.name, u.last_name, u.first_name
            `;
            const altStudentResult = await client.query(altStudentSQL);
            return {
              sql: altStudentSQL,
              rows: altStudentResult.rows,
              note: `Students in schools named "${schoolName}" (via multiple join paths)`
            };
          } catch (altErr) {
            // Return the schools at least
            return {
              sql: schoolLookupSQL,
              rows: schoolResult.rows,
              note: `Found ${schoolResult.rows.length} schools matching "${schoolName}" but couldn't find associated students.`
            };
          }
        }
      } else {
        return {
          sql: schoolLookupSQL,
          rows: [],
          note: `No schools found matching name: "${schoolName}"`
        };
      }
    } catch (err) {
      console.error("Error in alt school lookup:", err instanceof Error ? err.message : err);
    }
  }

  // Use vector search to find relevant schema info
  const relevant = await vectorStore.similaritySearch(question, 4);
  const context = relevant.map((r) => r.pageContent).join("\n");
  
  // Enhanced prompt with table descriptions for the most likely relevant tables
  let tableDescriptions = "";
  relevantTables.forEach(tableName => {
    if (TABLE_DESCRIPTIONS[tableName as keyof typeof TABLE_DESCRIPTIONS]) {
      tableDescriptions += `Table "${tableName}": ${TABLE_DESCRIPTIONS[tableName as keyof typeof TABLE_DESCRIPTIONS]}\n\n`;
    }
  });

  // Add institutions table description if asking about schools and it's not already included
  if (normalizedQuestion.includes('school') && !relevantTables.includes('institutions')) {
    tableDescriptions += `Table "institutions": ${TABLE_DESCRIPTIONS['institutions']}\n\n`;
  }

  // Add users table description if asking about students and it's not already included
  if ((normalizedQuestion.includes('student') || normalizedQuestion.includes('count')) 
      && !relevantTables.includes('users')) {
    tableDescriptions += `Table "users": ${TABLE_DESCRIPTIONS['users']}\n\n`;
  }

  // Add districts table description if asking about districts and it's not already included
  if (normalizedQuestion.includes('district') && !relevantTables.includes('districts')) {
    tableDescriptions += `Table "districts": ${TABLE_DESCRIPTIONS['districts']}\n\n`;
  }

  const prompt = `
You are an expert SQL generator that creates precise queries to answer user questions about a database. Use the schema and detailed table descriptions below to craft your SQL:

${tableDescriptions}
${context}

Generate a SQL query to answer: "${processedQuestion}"

Important notes:
- The terms "schools" and "institutions" refer to the same table called "institutions" 
- All tables that appear in the SELECT or JOIN clauses MUST be explicitly included in the FROM clause
- Use appropriate JOINs when data needs to be combined from multiple tables
- For simple listing queries (e.g., "all courses"), use a straightforward SELECT * FROM tablename
- For filtered queries, use proper WHERE clauses with valid boolean expressions
- Use LIMIT only when explicitly asked to restrict the number of results
- When the user refers to entities by name instead of ID, use LIKE with wildcards ('%Name%')
- If the question refers to a relationship between entities, use appropriate JOINs based on foreign keys
- For queries about "schools in district X" where X is a name (not ID), use: SELECT institutions.* FROM institutions JOIN districts ON institutions.district_id = districts.id WHERE districts.name ILIKE '%X%'
- For queries about "schools in district X" where X is a numeric ID, use: SELECT * FROM institutions WHERE district_id = X
- For queries about students in a specific school, filter institutions by name and join with the appropriate user/student table
- Include proper WHERE clauses for name filtering (e.g., institutions.name ILIKE '%School Name%')
- IMPORTANT: DO NOT change names like "Elementary School" to "Elementary Institution" in search conditions - keep them exactly as provided
- When searching for school names that include the word "School", maintain the exact name format

Only output the SQL query without explanations.
`;

  const model = new OpenAI({ temperature: 0 });
  const sql = (await model.invoke(prompt)).trim();

  // Post-process the SQL to ensure schools is properly mapped to institutions
  // BUT DON'T replace "school" in search strings that might be part of school names
  let processedSQL = sql
                        .replace(/\bFROM\s+schools\b/gi, 'FROM institutions')
                        .replace(/\bJOIN\s+schools\b/gi, 'JOIN institutions');
  
  // Add direct handling for "Elementary School" in SQL
  if (question.toLowerCase().includes('elementary school')) {
    if (processedSQL.includes('WHERE')) {
      if (!processedSQL.toLowerCase().includes('elementary')) {
        processedSQL = processedSQL.replace(/WHERE/i, `WHERE institutions.name ILIKE '%Elementary School%' AND`);
      }
    } else {
      processedSQL = processedSQL.replace(/FROM institutions/i, `FROM institutions WHERE institutions.name ILIKE '%Elementary School%'`);
    }
  }
  
  // Check if query is asking about a specific school by name but doesn't have a filter
  if (!processedSQL.toLowerCase().includes('ilike') && 
      !processedSQL.toLowerCase().includes('like') && 
      normalizedQuestion.includes('school') && 
      !normalizedQuestion.includes('all')) {
      
    // Look for a school name pattern in the question
    const schoolNameMatch = normalizedQuestion.match(/school\s+(?:named|called)?\s*(?:"|')?([^"'?]+)(?:"|')?/i);
    if (schoolNameMatch && schoolNameMatch[1]) {
      const schoolName = schoolNameMatch[1].trim();
      
      // Add a WHERE clause for the school name if not already present
      if (processedSQL.toLowerCase().includes('from institutions') && 
          !processedSQL.toLowerCase().includes('institution') && 
          !processedSQL.toLowerCase().includes('school')) {
        
        if (processedSQL.toLowerCase().includes('where')) {
          processedSQL = processedSQL.replace(/WHERE/i, `WHERE institutions.name ILIKE '%${schoolName}%' AND`);
        } else {
          processedSQL = processedSQL.replace(/FROM institutions/i, `FROM institutions WHERE institutions.name ILIKE '%${schoolName}%'`);
        }
      }
    }
  }
  
  // Special handling for searching institution names containing "School"
  // Don't replace the word "School" in search conditions for institution names
  processedSQL = processedSQL.replace(
    /institutions\.name\s+ILIKE\s+'%([^%]*)institution([^%]*)'%/gi,
    (match, p1, p2) => `institutions.name ILIKE '%${p1}school${p2}%'`
  );
  
  // Fix cases where "Elementary School" might have been converted to "Elementary Institution"
  processedSQL = processedSQL.replace(
    /institutions\.name\s+ILIKE\s+'%([^%]*)institution%'/gi,
    (match, prefix) => {
      // Check if the original question contains a school name
      const schoolNameMatch = normalizedQuestion.match(/(?:named|called|find|for)?\s*(?:"|')?([^"'?]+?school[^"'?]*)(?:"|')?/i);
      if (schoolNameMatch && schoolNameMatch[1]) {
        return `institutions.name ILIKE '%${schoolNameMatch[1].trim()}%'`;
      }
      return match;
    }
  );
  
  // Fix for "Elementary School" pattern specifically
  if (normalizedQuestion.includes("Elementary School") && 
      processedSQL.includes("Elementary Institution")) {
    processedSQL = processedSQL.replace(/Elementary Institution/gi, "Elementary School");
  }
  
  // Special post-processing for district-related queries
  if (normalizedQuestion.includes('district') && 
      (normalizedQuestion.includes('school') || normalizedQuestion.includes('institution'))) {
    // Extract district ID if it exists in the query
    const districtIdMatch = normalizedQuestion.match(/district\s*(?:_|\s+)?id\s*(?:=|:)?\s*(\d+)/i);
    
    if (districtIdMatch && districtIdMatch[1]) {
      const districtId = districtIdMatch[1];
      
      // If the generated SQL doesn't have a WHERE district_id condition, add it
      if (!processedSQL.toLowerCase().includes('district_id')) {
        if (processedSQL.toLowerCase().includes('where')) {
          processedSQL = processedSQL.replace(/WHERE/i, `WHERE district_id = ${districtId} AND`);
        } else if (processedSQL.toLowerCase().includes('from institutions')) {
          processedSQL = processedSQL.replace(/FROM institutions/i, `FROM institutions WHERE district_id = ${districtId}`);
        }
      }
    }
    // For district name queries, ensure we have a proper JOIN to districts
    else {
      const districtNameMatch = normalizedQuestion.match(/district\s+(?:named|called|with name|name)?\s*(?:"|')?([^"']+)(?:"|')?/i);
      if (districtNameMatch && districtNameMatch[1]) {
        const districtName = districtNameMatch[1].trim();
        
        if (!processedSQL.toLowerCase().includes('join districts')) {
          if (processedSQL.toLowerCase().includes('from institutions')) {
            processedSQL = `SELECT institutions.* FROM institutions JOIN districts ON institutions.district_id = districts.id WHERE districts.name ILIKE '%${districtName}%'`;
          }
        }
      }
    }
  }
  
  try {
    console.log("Executing SQL:", processedSQL);
    const { rows } = await client.query(processedSQL);
    return { sql: processedSQL, rows };
  } catch (err: any) {
    if (err instanceof Error) {
      console.error("SQL Error:", err.message);
    } else {
      console.error("SQL Error:", err);
    }
    
    // Implement a tiered fallback strategy
    const fallbacks = [
      // New Fallback 0: Try specific school name query if appropriate
      async () => {
        if (normalizedQuestion.includes('student') && normalizedQuestion.includes('school')) {
          // Extract school name if present
          const schoolNameMatch = normalizedQuestion.match(/(?:in|at|for|of)\s+(?:school|institution)?\s*(?:"|')?([^?"']+?)(?:"|')?(?:\s*\?)?$/i);
          
          if (schoolNameMatch && schoolNameMatch[1]) {
            const schoolName = schoolNameMatch[1].trim();
            
            // Try to find the school first
            const schoolLookupSQL = `SELECT id FROM institutions WHERE name ILIKE '%${schoolName}%'`;
            const schoolResult = await client.query(schoolLookupSQL);
            
            if (schoolResult.rows.length > 0) {
              const schoolId = schoolResult.rows[0].id;
              
              // Try to find students linked to this school
              try {
                const studentQuerySQL = `
                  SELECT COUNT(*) as student_count 
                  FROM institutions_users 
                  WHERE institution_id = ${schoolId}
                `;
                const countResult = await client.query(studentQuerySQL);
                
                return {
                  sql: studentQuerySQL,
                  rows: countResult.rows,
                  note: `Fallback count of students in ${schoolName} (ID: ${schoolId})`
                };
              } catch (countErr) {
                // Return the school info at least
                return {
                  sql: `SELECT * FROM institutions WHERE id = ${schoolId}`,
                  rows: await client.query(`SELECT * FROM institutions WHERE id = ${schoolId}`).then(r => r.rows),
                  note: `Found school "${schoolName}" but could not count students`
                };
              }
            }
          }
        }
        throw new Error("School-student query fallback failed");
      },
      
      // Fallback 0: Try district-specific query if contains district and schools/institutions
      async () => {
        if (normalizedQuestion.includes('district') && 
            (normalizedQuestion.includes('school') || normalizedQuestion.includes('institution'))) {
          
          // Check for district ID
          const districtIdMatch = normalizedQuestion.match(/district\s*(?:_|\s+)?id\s*(?:=|:)?\s*(\d+)/i) || 
                                 normalizedQuestion.match(/district\s+(\d+)/i);
          
          if (districtIdMatch && districtIdMatch[1]) {
            const districtId = districtIdMatch[1];
            const directSQL = `SELECT * FROM institutions WHERE district_id = ${districtId}`;
            const { rows } = await client.query(directSQL);
            return {
              sql: directSQL,
              rows,
              note: `Fallback to direct district query with ID: ${districtId}`
            };
          }
          
          // Check for district name
          const districtNameMatch = normalizedQuestion.match(/district\s+(?:called|named)?\s*(?:"|')?([^"']+)(?:"|')?/i);
          if (districtNameMatch && districtNameMatch[1]) {
            const districtName = districtNameMatch[1].trim();
            
            // First find the district ID by name
            try {
              const lookupSQL = `SELECT id FROM districts WHERE name ILIKE '%${districtName}%'`;
              const districtResult = await client.query(lookupSQL);
              
              if (districtResult.rows.length > 0) {
                const districtId = districtResult.rows[0].id;
                const directSQL = `SELECT * FROM institutions WHERE district_id = ${districtId}`;
                const { rows } = await client.query(directSQL);
                return {
                  sql: directSQL,
                  rows,
                  note: `Fallback to direct district query with name: ${districtName} (ID: ${districtId})`
                };
              } 
            } catch (lookupErr) {
              // Continue to next fallback if lookup fails
            }
          }
        }
        throw new Error("District query fallback failed");
      },
      
      // Fallback 1: Try simplified version of the query without JOINs
      async () => {
        if (processedSQL.toLowerCase().includes('join')) {
          const fromTable = processedSQL.match(/FROM\s+([a-z_]+)/i)?.[1];
          if (fromTable) {
            const simpleSQL = `SELECT * FROM ${fromTable} LIMIT 100`;
            const { rows } = await client.query(simpleSQL);
            return { 
              sql: simpleSQL, 
              rows,
              note: "Simplified query - removed JOINs" 
            };
          }
        }
        throw new Error("Cannot simplify JOIN query");
      },
      
      // Fallback 2: Try with fuzzy name matching if there are exact matches
      async () => {
        const fuzzySQL = processedSQL
          .replace(/([a-zA-Z_]+)\.name\s*=\s*'([^']+)'/gi, "$1.name ILIKE '%$2%'")
          .replace(/([a-zA-Z_]+)\.first_name\s*=\s*'([^']+)'/gi, "$1.first_name ILIKE '%$2%'")
          .replace(/([a-zA-Z_]+)\.last_name\s*=\s*'([^']+)'/gi, "$1.last_name ILIKE '%$2%'");
          
        if (fuzzySQL !== processedSQL) {
          const { rows } = await client.query(fuzzySQL);
          return { 
            sql: fuzzySQL, 
            rows,
            note: "Used fuzzy name matching" 
          };
        }
        throw new Error("No fuzzy matches available");
      },
      
      // Fallback 3: Use the most relevant table with a simple query
      async () => {
        if (relevantTables.length > 0) {
          const mainTable = relevantTables[0];
          const simpleSQL = `SELECT * FROM ${mainTable} LIMIT 100`;
          const { rows } = await client.query(simpleSQL);
          return {
            sql: simpleSQL,
            rows,
            note: `Fallback to simple query on most relevant table: ${mainTable}`
          };
        }
        throw new Error("No relevant tables identified");
      }
    ];
    
    // Try each fallback in order
    for (const fallback of fallbacks) {
      try {
        return await fallback();
      } catch (fallbackErr) {
        // Try next fallback
        continue;
      }
    }
    
    // If all fallbacks failed, return the original error
    return { sql: processedSQL, error: err instanceof Error ? err.message : err };
  }
}