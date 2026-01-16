import pandas as pd
from database.database_manager import load_db_config
from database.models import (
    Dataset,
    DatasetTitle,
    Publisher,
    Theme,
    LandingPage,
    DownloadURL,
)
from logging_utils.app_logger import AppLogger
import re

DEBUG = True


def load_and_combine_datasets(
    initial_csv_path: str, enriched_csv_path: str
) -> list[Dataset]:
    logger = AppLogger()
    datasets = []

    try:
        initial_df = pd.read_csv(initial_csv_path)
        enriched_df = pd.read_csv(enriched_csv_path)

        logger.info(f"Loaded {len(initial_df)} rows from initial CSV")
        logger.info(f"Loaded {len(enriched_df)} rows from enriched CSV")

        merged_df = initial_df.merge(
            enriched_df, on="dataset", how="left", suffixes=("_initial", "_enriched")
        )

        logger.info(f"Combined datasets into {len(merged_df)} rows")

        for idx, row in merged_df.iterrows():
            try:
                theme_uris = [
                    Theme(uri=theme.strip())
                    for theme in str(row.get("themes", "")).split("|")
                    if theme.strip()
                ]

                landing_page = None
                if (
                    pd.notna(row.get("landingPage"))
                    and str(row.get("landingPage", "")).strip()
                ):
                    try:
                        landing_page = LandingPage(
                            url=str(row.get("landingPage")).strip()
                        )
                    except Exception as e:
                        logger.warning(
                            f"Failed to parse landingPage at row {idx + 2}: {e}"
                        )

                download_url = None
                if (
                    pd.notna(row.get("downloadURL"))
                    and str(row.get("downloadURL", "")).strip()
                ):
                    try:
                        download_url = DownloadURL(
                            url=str(row.get("downloadURL")).strip()
                        )
                    except Exception as e:
                        logger.warning(
                            f"Failed to parse downloadURL at row {idx + 2}: {e}"
                        )

                dataset = Dataset(
                    uri=row["dataset"],
                    title=DatasetTitle(
                        value=row.get("datasetTitle", row.get("title", ""))
                    ),
                    publisher=Publisher(uri=row.get("publisher", "")),
                    themes=theme_uris,
                    landing_page=landing_page,
                    download_url=download_url,
                    issued=row.get("issued") if pd.notna(row.get("issued")) else None,
                    status=row.get("status") if pd.notna(row.get("status")) else None,
                    access_url=(
                        row.get("accessURL") if pd.notna(row.get("accessURL")) else None
                    ),
                    byte_size=(
                        int(row.get("byteSize"))
                        if pd.notna(row.get("byteSize"))
                        and str(row.get("byteSize", "")).strip()
                        else None
                    ),
                    keywords=(
                        str(row.get("keywords", "")).split(", ")
                        if pd.notna(row.get("keywords"))
                        and str(row.get("keywords", "")).strip()
                        else None
                    ),
                )
                datasets.append(dataset)

            except Exception as e:
                logger.error(f"Failed to parse dataset at row {idx + 2}: {e}")
                continue

        logger.success(
            f"Created {len(datasets)} Dataset objects from combined CSV data"
        )
        return datasets

    except Exception as e:
        logger.error(f"Failed to load and combine CSV files: {e}")
        return []


def load_theme_labels(theme_labels_csv_path: str) -> dict:
    logger = AppLogger()
    theme_labels_map = {}

    try:
        df = pd.read_csv(theme_labels_csv_path)
        logger.info(f"Loaded theme labels from {theme_labels_csv_path}")

        for idx, row in df.iterrows():
            try:
                themes_str = row.get("themes", "")
                theme_uris = [
                    t
                    for t in re.split(r"[\s|]+", themes_str.strip())
                    if t.startswith("http")
                ]

                for theme_uri in theme_uris:
                    if theme_uri not in theme_labels_map:
                        theme_labels_map[theme_uri] = {}

                    label_en = row.get("theme_labels_en", "")
                    label_it = row.get("theme_labels_it", "")
                    label_de = row.get("theme_labels_de", "")

                    if label_en and isinstance(label_en, str):
                        labels_list = [
                            l.strip() for l in str(label_en).split("|") if l.strip()
                        ]
                        if labels_list:
                            theme_labels_map[theme_uri]["en"] = labels_list[0]

                    if label_it and isinstance(label_it, str):
                        labels_list = [
                            l.strip() for l in str(label_it).split("|") if l.strip()
                        ]
                        if labels_list:
                            theme_labels_map[theme_uri]["it"] = labels_list[0]

                    if label_de and isinstance(label_de, str):
                        labels_list = [
                            l.strip() for l in str(label_de).split("|") if l.strip()
                        ]
                        if labels_list:
                            theme_labels_map[theme_uri]["de"] = labels_list[0]

            except Exception as e:
                logger.warning(f"Failed to parse theme labels at row {idx + 2}: {e}")
                continue

        logger.success(f"Loaded labels for {len(theme_labels_map)} unique themes")
        return theme_labels_map

    except Exception as e:
        logger.error(f"Failed to load theme labels CSV: {e}")
        return {}


if __name__ == "__main__":
    logger = AppLogger()

    database_manager = load_db_config()

    if DEBUG:
        database_manager.clear_graph()

    if not database_manager.load_constraints():
        logger.error("Failed to load constraints. Exiting.")
        database_manager.close()
        exit(1)

    datasets = load_and_combine_datasets(
        "data/datasets_publishers_themes.csv", "data/enriched_datasets.csv"
    )

    if not datasets:
        logger.error("No datasets loaded. Exiting.")
        database_manager.close()
        exit(1)

    stats = database_manager.create_dataset_nodes_and_relationships(datasets)

    logger.info("Graph creation statistics:")
    logger.info(f"Datasets created: {stats['datasets_created']}")
    logger.info(f"Titles created: {stats['titles_created']}")
    logger.info(f"Publishers created: {stats['publishers_created']}")
    logger.info(f"Themes created: {stats['themes_created']}")
    logger.info(f"Landing pages created: {stats['landing_pages_created']}")
    logger.info(f"Download URLs created: {stats['download_urls_created']}")
    logger.info(f"HAS_TITLE relationships: {stats['has_title_relationships']}")
    logger.info(f"PUBLISHED_BY relationships: {stats['published_by_relationships']}")
    logger.info(f"HAS_THEME relationships: {stats['has_theme_relationships']}")
    logger.info(
        f"HAS_LANDING_PAGE relationships: {stats['has_landing_page_relationships']}"
    )
    logger.info(
        f"HAS_DOWNLOAD_URL relationships: {stats['has_download_url_relationships']}"
    )

    if stats["errors"]:
        logger.error(f"Encountered {len(stats['errors'])} errors during creation")
        for error in stats["errors"][:5]:
            logger.error(f"  - {error}")

    logger.info("Loading theme labels...")
    theme_labels_map = load_theme_labels("data/datasets_with_theme_labels.csv")

    if theme_labels_map:
        label_stats = database_manager.create_theme_label_nodes_and_relationships(
            theme_labels_map
        )

        logger.info("Theme label creation statistics:")
        logger.info(f"Theme labels created: {label_stats['theme_labels_created']}")
        logger.info(
            f"HAS_LABEL relationships: {label_stats['has_label_relationships']}"
        )

        if label_stats["errors"]:
            logger.error(
                f"Encountered {len(label_stats['errors'])} errors during label creation"
            )
            for error in label_stats["errors"][:5]:
                logger.error(f"  - {error}")
    else:
        logger.warning("No theme labels loaded. Skipping theme label node creation.")

    database_manager.close()
    logger.success("Process completed")
