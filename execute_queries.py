import sys
from pathlib import Path
from typing import List, Dict, Any
import json

sys.path.insert(0, str(Path(__file__).parent.parent))

from database.database_manager import load_db_config
from logging_utils.app_logger import AppLogger

logger = AppLogger()

QUERIES_DIR = Path(__file__).parent / "queries/cypher"


def load_query_file(file_path: Path) -> str:
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
            # Remove comments and empty lines
            lines = [line.split("//")[0].strip() for line in content.split("\n")]
            query = "\n".join([line for line in lines if line])
            return query
    except Exception as e:
        logger.error(f"Failed to load query from {file_path}: {e}")
        return ""


def format_result(record: Dict[str, Any]) -> Dict[str, Any]:
    formatted = {}
    for key, value in record.items():
        if isinstance(value, (str, int, float, bool, type(None))):
            formatted[key] = value
        elif isinstance(value, list):
            formatted[key] = str(value)
        else:
            formatted[key] = str(value)
    return formatted


def execute_query(driver, query_name: str, query: str, limit: int = 100) -> List[Dict]:
    try:
        with driver.session() as session:
            result = session.run(query)
            records = []
            for i, record in enumerate(result):
                if i >= limit:
                    break
                records.append(dict(record))
            return records
    except Exception as e:
        logger.error(f"Error executing query '{query_name}': {e}")
        return []


def run_all_queries() -> None:
    database_manager = load_db_config()

    try:
        query_files = sorted(QUERIES_DIR.glob("*.cypher"))

        if not query_files:
            logger.warning(f"No Cypher query files found in {QUERIES_DIR}")
            return

        logger.info(f"Found {len(query_files)} query files")
        all_results = {}

        for query_file in query_files:
            query_name = query_file.stem
            logger.info(f"\nExecuting query: {query_name}")
            logger.info(f"{'='*80}")

            query = load_query_file(query_file)
            if not query:
                logger.warning(f"Skipping empty query: {query_name}")
                continue

            results = execute_query(database_manager.driver, query_name, query)
            all_results[query_name] = results

            if results:
                logger.success(f"Query returned {len(results)} results")

                if len(results) > 0:
                    logger.info(f"\nFirst result:")
                    first_record = format_result(results[0])
                    for key, value in first_record.items():
                        logger.info(f"  {key}: {value}")

                    if len(results) > 1:
                        logger.info(f"\n... and {len(results) - 1} more results")
            else:
                logger.warning(f"Query returned no results")

        logger.info("QUERY EXECUTION SUMMARY")

        for query_name, results in all_results.items():
            logger.info(f"{query_name}: {len(results)} results")

        output_file = Path("query_results.json")
        try:
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(all_results, f, indent=2, default=str)
            logger.success(f"All results saved to {output_file}")
        except Exception as e:
            logger.error(f"Failed to save results to JSON: {e}")

    finally:
        database_manager.close()
        logger.success("All queries completed")


if __name__ == "__main__":
    logger.info("Starting Cypher query execution")
    run_all_queries()
