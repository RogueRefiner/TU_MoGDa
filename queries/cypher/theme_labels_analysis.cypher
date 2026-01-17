// Answers: Show themes with their labels in multiple languages
// Use this to see how themes are labeled across different languages
MATCH (t:Theme)-[:HAS_LABEL]->(tl:ThemeLabel)
RETURN t.uri as theme_uri, tl.language as language, tl.title as label
ORDER BY t.uri, tl.language;
