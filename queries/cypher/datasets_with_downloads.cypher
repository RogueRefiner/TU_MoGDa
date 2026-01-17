// Answers: Datasets that have both landing pages and download URLs
// Use this to find datasets with complete access information
MATCH (d:Dataset)-[:HAS_LANDING_PAGE]->(lp:LandingPage)
MATCH (d)-[:HAS_DOWNLOAD_URL]->(du:DownloadURL)
MATCH (d)-[:HAS_TITLE]->(t:Title)
MATCH (d)-[:PUBLISHED_BY]->(p:Publisher)
RETURN 
  d.uri AS dataset_uri, 
  t.value AS title, 
  p.uri AS publisher_uri, 
  lp.url AS landing_page, 
  du.url AS download_url
LIMIT 50;