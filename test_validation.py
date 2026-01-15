import pandas as pd
from database.database_manager import load_db_config
from database.models import Dataset, DatasetTitle, Publisher, Theme
from logging_utils.app_logger import AppLogger


def load_and_validate_datasets(csv_path: str) -> tuple[list[Dataset], list[dict]]:
    logger = AppLogger()
    valid_datasets = []
    errors = []

    try:
        df = pd.read_csv(csv_path)
        logger.info(f"Loaded {len(df)} rows from test CSV file")

        for idx, row in df.iterrows():
            row_num = idx + 2
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
                valid_datasets.append(dataset)
                logger.debug(f"Row {row_num}: Successfully validated dataset")

            except AssertionError as e:
                error_msg = f"Row {row_num}: Validation assertion failed - {e}"
                logger.error(error_msg)
                errors.append(
                    {
                        "row": row_num,
                        "error_type": "AssertionError",
                        "message": str(e),
                        "dataset_uri": row.get("dataset", "N/A"),
                    }
                )

            except ValueError as e:
                error_msg = f"Row {row_num}: Value error - {e}"
                logger.error(error_msg)
                errors.append(
                    {
                        "row": row_num,
                        "error_type": "ValueError",
                        "message": str(e),
                        "dataset_uri": row.get("dataset", "N/A"),
                    }
                )

            except Exception as e:
                error_msg = (
                    f"Row {row_num}: Failed to parse dataset - {type(e).__name__}: {e}"
                )
                logger.error(error_msg)
                errors.append(
                    {
                        "row": row_num,
                        "error_type": type(e).__name__,
                        "message": str(e),
                        "dataset_uri": row.get("dataset", "N/A"),
                    }
                )

        logger.success(
            f"Validated {len(valid_datasets)} datasets, encountered {len(errors)} errors"
        )
        return valid_datasets, errors

    except Exception as e:
        logger.error(f"Failed to load CSV file: {e}")
        return [], [{"error_type": "CSV Load Error", "message": str(e)}]


if __name__ == "__main__":
    logger = AppLogger()
    logger.info("TEST: Validation Errors with test_datasets_with_errors.csv")

    valid_datasets, validation_errors = load_and_validate_datasets(
        "data/test_datasets_with_errors.csv"
    )

    logger.info("VALIDATION SUMMARY")
    logger.info(f"Valid datasets: {len(valid_datasets)}")
    logger.info(f"Validation errors: {len(validation_errors)}\n")

    if validation_errors:
        logger.info("Error Details:")
        for i, error in enumerate(validation_errors, 1):
            logger.info(f"Error {i}:")
            logger.info(f"  Type: {error.get('error_type', 'Unknown')}")
            logger.info(f"  Row: {error.get('row', 'N/A')}")
            logger.info(f"  Dataset URI: {error.get('dataset_uri', 'N/A')}")
            logger.info(f"  Message: {error.get('message', 'No message')}")

    logger.info("NEO4J CONSTRAINT TEST")

    if valid_datasets:
        database_manager = load_db_config()

        if database_manager.load_constraints():
            logger.info("Constraints loaded successfully")

            logger.info(f"Attempting to create {len(valid_datasets)} datasets...")
            stats = database_manager.create_dataset_nodes_and_relationships(
                valid_datasets
            )

            logger.info("Graph creation statistics:\n")
            logger.info(f"Datasets created: {stats['datasets_created']}")
            logger.info(f"Titles created: {stats['titles_created']}")
            logger.info(f"Publishers created: {stats['publishers_created']}")
            logger.info(f"Themes created: {stats['themes_created']}")
            logger.info(f"HAS_TITLE relationships: {stats['has_title_relationships']}")
            logger.info(
                f"PUBLISHED_BY relationships: {stats['published_by_relationships']}"
            )
            logger.info(f"HAS_THEME relationships: {stats['has_theme_relationships']}")

            if stats["errors"]:
                logger.error(
                    f"\nEncountered {len(stats['errors'])} errors during Neo4j creation:"
                )
                for error in stats["errors"][:10]:
                    logger.error(f"  - {error}")
        else:
            logger.error("Failed to load constraints")

        database_manager.close()
    else:
        logger.warning("No valid datasets to test with Neo4j constraints")

    logger.info("Check validation_errors.log for detailed validation error logs")
