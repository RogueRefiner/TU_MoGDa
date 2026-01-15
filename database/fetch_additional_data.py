import csv
import sys
import time
from pathlib import Path
from SPARQLWrapper import SPARQLWrapper, JSON

sys.path.insert(0, str(Path(__file__).parent.parent))

from logging_utils.app_logger import AppLogger

logger = AppLogger()

OUTPUT_FILE = "../data/enriched_datasets.csv"
INITIAL_DATASETS_FILE = "../data/datasets_publishers_themes.csv"
SPARQL_ENDPOINT = "https://data.europa.eu/sparql"


def get_initial_datasets(sparql: SPARQLWrapper):
    """
    Fetch initial datasets from the SPARQL endpoint.
    Returns all matching datasets, not just the first one.
    """
    query = f"""
        PREFIX dct: <http://purl.org/dc/terms/>
        PREFIX dcat: <http://www.w3.org/ns/dcat#>
        PREFIX adms: <http://www.w3.org/ns/adms#>

        SELECT ?dataset (SAMPLE(?title) AS ?datasetTitle) (SAMPLE(?pub) AS ?publisher) (GROUP_CONCAT(DISTINCT ?theme; separator="|") AS ?themes)
        WHERE {{
        ?dataset dcat:distribution ?dist .
        ?dist dct:format <http://publications.europa.eu/resource/authority/file-type/CSV> .
        ?dataset a dcat:Dataset ;
                dct:title ?title ;
                dct:publisher ?pub ;
                dcat:theme ?theme .
        FILTER(lang(?title) = "en")
        }}
        GROUP BY ?dataset
        LIMIT 500    
    """

    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)

    try:
        results = sparql.query().convert()
        if results["results"]["bindings"]:
            datasets = [
                {k: v.get("value", "") for k, v in result.items()}
                for result in results["results"]["bindings"]
            ]
            logger.success(f"Fetched {len(datasets)} datasets from SPARQL endpoint")
            return datasets
        else:
            logger.warning("No datasets found in SPARQL query results")
    except Exception as e:
        logger.error(f"Error querying initial datasets: {e}")

    return []


def get_dataset_details(dataset_uri: str) -> dict:
    """Fetch additional details for a specific dataset."""
    sparql = SPARQLWrapper(SPARQL_ENDPOINT)

    query = f"""
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX dcat: <http://www.w3.org/ns/dcat#>
    PREFIX adms: <http://www.w3.org/ns/adms#>

    SELECT ?issued ?status ?accessURL ?byteSize ?downloadURL ?landingPage 
           (GROUP_CONCAT(DISTINCT ?keyword; separator=", ") AS ?keywords)
    WHERE {{
      BIND(<{dataset_uri}> AS ?dataset)

      OPTIONAL {{ ?dataset dct:issued ?issued . }}
      OPTIONAL {{ ?dataset adms:status ?status . }}
      OPTIONAL {{ ?dataset dcat:landingPage ?landingPage . }}
      OPTIONAL {{ ?dataset dcat:keyword ?keyword . }}

      {{
        OPTIONAL {{ ?dataset dcat:accessURL ?accessURL . }}
        OPTIONAL {{ ?dataset dcat:downloadURL ?downloadURL . }}
        OPTIONAL {{ ?dataset dcat:byteSize ?byteSize . }}
      }}
      UNION
      {{
        ?dataset dcat:distribution ?dist .
        OPTIONAL {{ ?dist dcat:accessURL ?accessURL . }}
        OPTIONAL {{ ?dist dcat:downloadURL ?downloadURL . }}
        OPTIONAL {{ ?dist dcat:byteSize ?byteSize . }}
      }}
    }}
    GROUP BY ?issued ?status ?accessURL ?byteSize ?downloadURL ?landingPage
    """

    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)

    try:
        results = sparql.query().convert()
        if results["results"]["bindings"]:
            res = results["results"]["bindings"][0]
            return {k: v.get("value", "") for k, v in res.items()}
    except Exception as e:
        logger.error(f"Error querying {dataset_uri}: {e}")

    return {}


def save_initial_datasets():
    """Fetch initial datasets from SPARQL and save to CSV."""
    logger.info(f"Fetching initial datasets from {SPARQL_ENDPOINT}...")
    sparql = SPARQLWrapper(SPARQL_ENDPOINT)

    datasets = get_initial_datasets(sparql)

    if not datasets:
        logger.error("No datasets fetched. Aborting.")
        return

    try:
        fieldnames = ["dataset", "datasetTitle", "publisher", "themes"]
        with open(
            INITIAL_DATASETS_FILE, mode="w", encoding="utf-8", newline=""
        ) as outfile:
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()

            for dataset in datasets:
                writer.writerow(dataset)

        logger.success(f"Saved {len(datasets)} datasets to {INITIAL_DATASETS_FILE}")
    except Exception as e:
        logger.error(f"Error saving initial datasets: {e}")


def run_enrichment():
    """Enrich datasets with additional details from SPARQL."""
    try:
        with open(INITIAL_DATASETS_FILE, mode="r", encoding="utf-8") as infile:
            reader = csv.DictReader(infile)
            fieldnames = [
                "dataset",
                "issued",
                "status",
                "accessURL",
                "byteSize",
                "downloadURL",
                "landingPage",
                "keywords",
            ]

            with open(OUTPUT_FILE, mode="w", encoding="utf-8", newline="") as outfile:
                writer = csv.DictWriter(outfile, fieldnames=fieldnames)
                writer.writeheader()

                row_count = 0
                for row in reader:
                    dataset_uri = row["dataset"]
                    logger.info(f"Processing: {dataset_uri}...")

                    details = get_dataset_details(dataset_uri)
                    row.update(details)

                    filtered_row = {k: v for k, v in row.items() if k in fieldnames}

                    writer.writerow(filtered_row)
                    row_count += 1
                    time.sleep(0.5)

        logger.success(
            f"Enrichment complete. Processed {row_count} datasets. Results saved to {OUTPUT_FILE}"
        )
    except Exception as e:
        logger.error(f"Error during enrichment: {e}")


if __name__ == "__main__":
    logger.info("Starting dataset fetching process")
    save_initial_datasets()
    # logger.info("\nStarting enrichment process...")
    # run_enrichment()
    # logger.success("Process completed successfully!")
