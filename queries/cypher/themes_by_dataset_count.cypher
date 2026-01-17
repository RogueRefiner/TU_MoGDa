// Answers: Find the most popular themes across all datasets
// Use this to understand which themes are most frequently used
MATCH (t:Theme)<-[:HAS_THEME]-(d:Dataset)
RETURN t.uri as theme_uri, count(d) as dataset_count
ORDER BY dataset_count DESC
LIMIT 20;
