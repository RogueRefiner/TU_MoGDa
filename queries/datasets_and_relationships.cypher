// Creates a Dataset node keyed by its uri if it does not exist.
MERGE (d:Dataset {uri: $uri});

// Creates a Title node for the dataset title value if it does not exist.
MERGE (t:Title {value: $value});

// Connects a Dataset to its Title with HAS_TITLE if missing.
MATCH (d:Dataset {uri: $dataset_uri})
MATCH (t:Title {value: $title_value})
MERGE (d)-[:HAS_TITLE]->(t);

// Creates a Publisher node keyed by its uri if it does not exist.
MERGE (p:Publisher {uri: $uri});

// Connects a Dataset to its Publisher with PUBLISHED_BY if missing.
MATCH (d:Dataset {uri: $dataset_uri})
MATCH (p:Publisher {uri: $publisher_uri})
MERGE (d)-[:PUBLISHED_BY]->(p);

// Creates a Theme node keyed by its uri if it does not exist.
MERGE (t:Theme {uri: $uri});

// Connects a Dataset to a Theme with HAS_THEME if missing.
MATCH (d:Dataset {uri: $dataset_uri})
MATCH (t:Theme {uri: $theme_uri})
MERGE (d)-[:HAS_THEME]->(t);

// Creates a LandingPage node keyed by its url if it does not exist.
MERGE (lp:LandingPage {url: $url});

// Connects a Dataset to its LandingPage with HAS_LANDING_PAGE if missing.
MATCH (d:Dataset {uri: $dataset_uri})
MATCH (lp:LandingPage {url: $landing_page_url})
MERGE (d)-[:HAS_LANDING_PAGE]->(lp);

// Creates a DownloadURL node keyed by its url if it does not exist.
MERGE (du:DownloadURL {url: $url});

// Connects a Dataset to its DownloadURL with HAS_DOWNLOAD_URL if missing.
MATCH (d:Dataset {uri: $dataset_uri})
MATCH (du:DownloadURL {url: $download_url})
MERGE (d)-[:HAS_DOWNLOAD_URL]->(du);

