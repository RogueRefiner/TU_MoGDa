// Answers: Themes with multilingual label coverage
// Use this to identify which themes have translations in multiple languages
MATCH (t:Theme)-[:HAS_LABEL]->(tl:ThemeLabel)
WITH t, collect(DISTINCT tl.language) as languages, count(DISTINCT tl) as label_count
RETURN t.uri as theme_uri, label_count as language_count, languages
ORDER BY label_count DESC;
