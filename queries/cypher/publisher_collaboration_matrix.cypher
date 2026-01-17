// Answers: Publishers that share datasets published by the same publisher
// Use this to identify publisher relationships through shared datasets
MATCH (p:Publisher)<-[:PUBLISHED_BY]-(d:Dataset)-[:HAS_THEME]->(t:Theme)<-[:HAS_THEME]-(d2:Dataset)-[:PUBLISHED_BY]->(p2:Publisher)
WHERE p.uri < p2.uri
WITH p, p2, count(DISTINCT t) as shared_theme_count, count(DISTINCT d) + count(DISTINCT d2) as dataset_involvement
RETURN p.uri as publisher1_uri, p2.uri as publisher2_uri, shared_theme_count, dataset_involvement
ORDER BY shared_theme_count DESC
LIMIT 20;
