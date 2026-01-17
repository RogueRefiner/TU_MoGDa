// Answers: show publishers linked to datasets via PUBLISHED_BY.
// Use this to inspect publisher-dataset relationships.
MATCH (p:Publisher)-[r:PUBLISHED_BY]-(d:Dataset)
RETURN p, r, d
LIMIT 50;
