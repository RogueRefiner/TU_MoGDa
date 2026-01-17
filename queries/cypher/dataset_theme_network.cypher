// Answers: Connected datasets that share themes
// Use this to find related datasets through their shared themes
MATCH (d1:Dataset)-[:HAS_THEME]->(t:Theme)<-[:HAS_THEME]-(d2:Dataset)
WHERE d1.uri < d2.uri
WITH d1, d2, count(DISTINCT t) as shared_theme_count
MATCH (d1)-[:HAS_TITLE]->(t1:Title)
MATCH (d2)-[:HAS_TITLE]->(t2:Title)
RETURN d1.uri as dataset1_uri, t1.value as dataset1_title, 
       d2.uri as dataset2_uri, t2.value as dataset2_title, 
       shared_theme_count
ORDER BY shared_theme_count DESC
LIMIT 30;
