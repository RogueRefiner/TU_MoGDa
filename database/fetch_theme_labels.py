import csv
import sys
import re
from pathlib import Path
from SPARQLWrapper import SPARQLWrapper, JSON

sys.path.insert(0, str(Path(__file__).parent.parent))

from logging_utils.app_logger import AppLogger

logger = AppLogger()

INPUT_CSV = "../data/datasets_publishers_themes.csv"
OUTPUT_CSV = "../data/datasets_with_theme_labels.csv"
SPARQL_ENDPOINT = "https://publications.europa.eu/webapi/rdf/sparql"


def extract_theme_uris(themes_str: str) -> list[str]:
    if not themes_str or not isinstance(themes_str, str):
        return []

    theme_list = re.split(r"[\s|]+", themes_str.strip())
    return [t for t in theme_list if t.startswith("http")]


def fetch_theme_labels(sparql: SPARQLWrapper, theme_uris: list[str]) -> dict:
    if not theme_uris:
        return {}

    values_clause = " ".join([f"<{uri}>" for uri in theme_uris])

    query = f"""
        PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
        
        SELECT ?theme ?labelEN ?labelIT ?labelDE
        WHERE {{
          VALUES ?theme {{ {values_clause} }}
          
          ?theme skos:prefLabel ?labelEN .
          FILTER(lang(?labelEN) = "en")
          
          OPTIONAL {{
            ?theme skos:prefLabel ?labelIT .
            FILTER(lang(?labelIT) = "it")
          }}

          OPTIONAL {{
            ?theme skos:prefLabel ?labelDE .
            FILTER(lang(?labelDE) = "de")
          }}
        }}
    """

    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)

    theme_labels = {}

    try:
        results = sparql.query().convert()

        for binding in results["results"]["bindings"]:
            theme_uri = binding.get("theme", {}).get("value", "")
            label_en = binding.get("labelEN", {}).get("value", "")
            label_it = binding.get("labelIT", {}).get("value", "")
            label_de = binding.get("labelDE", {}).get("value", "")

            if theme_uri:
                theme_labels[theme_uri] = {
                    "en": label_en,
                    "it": label_it,
                    "de": label_de,
                }
                logger.debug(
                    f"Fetched labels for {theme_uri}: EN='{label_en}', IT='{label_it}', DE='{label_de}'"
                )

        logger.success(f"Fetched labels for {len(theme_labels)} themes")
        return theme_labels

    except Exception as e:
        logger.error(f"Error querying theme labels: {e}")
        return {}


def process_datasets():
    try:
        with open(INPUT_CSV, "r", encoding="utf-8") as infile:
            reader = csv.DictReader(infile)
            datasets = list(reader)

        logger.info(f"Loaded {len(datasets)} datasets from {INPUT_CSV}")

        unique_themes = set()
        for row in datasets:
            themes_str = row.get("themes", "")
            theme_uris = extract_theme_uris(themes_str)
            unique_themes.update(theme_uris)

        logger.info(f"Found {len(unique_themes)} unique theme URIs")

        sparql = SPARQLWrapper(SPARQL_ENDPOINT)
        theme_labels = fetch_theme_labels(sparql, list(unique_themes))

        if not theme_labels:
            logger.warning("No theme labels fetched. Continuing with empty labels.")

        fieldnames = [
            "dataset",
            "themes",
            "theme_labels_en",
            "theme_labels_it",
            "theme_labels_de",
        ]

        with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as outfile:
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()

            for idx, row in enumerate(datasets):
                themes_str = row.get("themes", "")
                theme_uris = extract_theme_uris(themes_str)

                labels_en = []
                labels_it = []
                labels_de = []

                for theme_uri in theme_uris:
                    if theme_uri in theme_labels:
                        labels = theme_labels[theme_uri]
                        if labels.get("en"):
                            labels_en.append(labels["en"])
                        if labels.get("it"):
                            labels_it.append(labels["it"])
                        if labels.get("de"):
                            labels_de.append(labels["de"])

                enriched_row = {
                    "dataset": row.get("dataset", ""),
                    "themes": themes_str,
                    "theme_labels_en": " | ".join(labels_en),
                    "theme_labels_it": " | ".join(labels_it),
                    "theme_labels_de": " | ".join(labels_de),
                }

                writer.writerow(enriched_row)

                if (idx + 1) % 50 == 0:
                    logger.info(f"Processed {idx + 1}/{len(datasets)} datasets")

        logger.success(f"Saved {len(datasets)} enriched datasets to {OUTPUT_CSV}")

    except Exception as e:
        logger.error(f"Error processing datasets: {e}")


if __name__ == "__main__":
    logger.info("Starting theme label enrichment process")
    process_datasets()
    logger.success("Theme label enrichment completed")
