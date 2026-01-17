// Ensures Publisher nodes have a unique uri.
CREATE CONSTRAINT publisher_uri_unique IF NOT EXISTS
FOR (p:Publisher)
REQUIRE p.uri IS UNIQUE;

// Ensures Dataset nodes have a unique uri.
CREATE CONSTRAINT dataset_uri_unique IF NOT EXISTS
FOR (d:Dataset)
REQUIRE d.uri IS UNIQUE;

// Ensures Theme nodes have a unique uri.
CREATE CONSTRAINT theme_uri_unique IF NOT EXISTS
FOR (t:Theme)
REQUIRE t.uri IS UNIQUE;

// Ensures ThemeLabel nodes are unique per title and language.
CREATE CONSTRAINT theme_label_unique IF NOT EXISTS
FOR (tl:ThemeLabel)
REQUIRE (tl.title, tl.language) IS UNIQUE;
