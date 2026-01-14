import pandas as pd
from database.database_manager import load_db_config
from database.models import Dataset, DatasetTitle, Publisher, Theme
from logging_utils.app_logger import AppLogger


def load_datasets_from_csv(csv_path: str) -> list[Dataset]:
    logger = AppLogger()
    datasets = []

    try:
        df = pd.read_csv(csv_path)
        logger.info(f"Loaded {len(df)} rows from CSV file")

        for idx, row in df.iterrows():
            try:
                theme_uris = [
                    Theme(uri=theme.strip())
                    for theme in str(row["themes"]).split("|")
                    if theme.strip()
                ]

                dataset = Dataset(
                    uri=row["dataset"],
                    title=DatasetTitle(value=row["datasetTitle"]),
                    publisher=Publisher(uri=row["publisher"]),
                    themes=theme_uris,
                )
                datasets.append(dataset)

            except Exception as e:
                logger.error(f"Failed to parse dataset at row {idx + 2}: {e}")
                continue

        logger.success(f"Created {len(datasets)} Dataset objects from CSV")
        return datasets

    except Exception as e:
        logger.error(f"Failed to load CSV file: {e}")
        return []


if __name__ == "__main__":
    logger = AppLogger()

    database_manager = load_db_config()

    if not database_manager.load_constraints():
        logger.error("Failed to load constraints. Exiting.")
        database_manager.close()
        exit(1)

    datasets = load_datasets_from_csv("database/datasets_publishers_themes.csv")

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
    logger.info(f"HAS_TITLE relationships: {stats['has_title_relationships']}")
    logger.info(f"PUBLISHED_BY relationships: {stats['published_by_relationships']}")
    logger.info(f"HAS_THEME relationships: {stats['has_theme_relationships']}")

    if stats["errors"]:
        logger.error(f"Encountered {len(stats['errors'])} errors during creation")
        for error in stats["errors"][:5]:
            logger.error(f"  - {error}")

    database_manager.close()
    logger.success("Process completed")
