# Report Evidence Notes – Group 32

## Cypher commands executed (Neo4j)

SHOW CONSTRAINTS;

MATCH (n)
RETURN labels(n) AS label, count(*) AS cnt
ORDER BY cnt DESC;

MATCH (:Dataset)-[:PUBLISHED_BY]->(:Publisher)
RETURN count(*) AS rels;

MATCH (:Dataset)-[:HAS_THEME]->(:Theme)
RETURN count(*) AS rels;

## Results summary

- Dataset nodes: ~500
- Publisher nodes: ~274
- Theme nodes: ~21
- PUBLISHED_BY relationships: ~500
- HAS_THEME relationships: ~824

## SPARQL queries used

1) datasets_publishers_themes.sparql  
(Initial data extraction from data.europa.eu SPARQL endpoint)

2) Second enrichment SPARQL query  
(Additional metadata used to enrich datasets / relationships)

## Screenshots

- docs/screenshots/show_constraints.png
- docs/screenshots/counts_by_label.png
- docs/screenshots/published_by_count.png
- docs/screenshots/dataset_theme_count.png