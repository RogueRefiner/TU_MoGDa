// Answers: show sample dataset nodes with one-hop relationships.
// Use this to visualize a slice of the graph.
MATCH (d:Dataset)-[r]->(n)
RETURN d, r, n
LIMIT 50;
