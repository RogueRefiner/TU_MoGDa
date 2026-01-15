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

    database_manager.close()
    logger.success("Process completed")
