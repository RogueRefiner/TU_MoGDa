CREATE CONSTRAINT publisher_uri_unique IF NOT EXISTS
FOR (p:publisher)
REQUIRE p.uri IS UNIQUE;

CREATE CONSTRAINT dataset_uri_unique IF NOT EXISTS
FOR (d:Dataset)
REQUIRE d.uri IS UNIQUE;

CREATE CONSTRAINT theme_uri_unique IF NOT EXISTS
FOR (t:Theme)
REQUIRE t.uri IS UNIQUE;