// Answers: Publishers ranked by theme diversity in their datasets
// Use this to find publishers with the most diverse dataset topics
MATCH (p:Publisher)<-[:PUBLISHED_BY]-(d:Dataset)-[:HAS_THEME]->(t:Theme)
RETURN p.uri as publisher_uri, count(DISTINCT t) as theme_count, count(DISTINCT d) as dataset_count
ORDER BY theme_count DESC
LIMIT 15;
