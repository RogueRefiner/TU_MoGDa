// Creates a ThemeLabel node for a specific title and language if it does not exist.
MERGE (tl:ThemeLabel {title: $title, language: $language});

// Connects a Theme to its label with HAS_LABEL if missing.
MATCH (t:Theme {uri: $theme_uri})
MATCH (tl:ThemeLabel {title: $title, language: $language})
MERGE (t)-[:HAS_LABEL]->(tl);
