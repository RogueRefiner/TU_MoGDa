# Report Evidence Notes (Group 32)

## What I ran (Neo4j / Cypher)
- SHOW CONSTRAINTS;
- MATCH (n) RETURN labels(n) AS label, count(*) AS cnt ORDER BY cnt DESC;
- MATCH (:Dataset)-[:PUBLISHED_BY]->(:Publisher) RETURN count(*) AS rels;

## Quick sanity results (from my run)
- Dataset nodes: 1500
- Publisher nodes: 769
- Theme nodes: 32
- PUBLISHED_BY rels: 1500

## SPARQL queries used
1) queries/datasets_publishers_themes.sparql
2) queries/enrich_datasets.sparql

## Screenshots (saved under docs/screenshots/)
- show_constraints.png  (SHOW CONSTRAINTS output)
- counts_by_label.png   (labels + counts output)
- published_by_count.png (relationship count output)
- (optional) sample_queries.png (a few Cypher query results)