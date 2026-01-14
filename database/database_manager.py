from dataclasses import dataclass, field
import os
from dotenv import load_dotenv
from neo4j import GraphDatabase, Neo4jDriver
from logging_utils.app_logger import AppLogger
from database.models import Dataset
from typing import List
from pathlib import Path


@dataclass
class DatabaseManager:
    uri: str
    username: str
    password: str
    driver: Neo4jDriver = field(init=False)
    logger: AppLogger = field(default_factory=AppLogger)

    def __post_init__(self) -> None:
        try:
            self.driver = GraphDatabase.driver(
                self.uri, auth=(self.username, self.password)
            )
            self.driver.verify_authentication()
            self.driver.verify_connectivity()
            self.logger.success(
                f"Connected to Neo4j database at {self.uri} as {self.username} - AppLogger id={id(self.logger)}"
            )
        except Exception as e:
            self.logger.error(f"Failed to connect to Neo4j database: {e}")

    def close(self) -> None:
        if self.driver is not None:
            self.driver.close()
            self.logger.info("Neo4j database connection closed")

    def load_constraints(self) -> bool:
        try:
            constraints_file = Path("queries/neo4j_constraints.cypher")
            if not constraints_file.exists():
                self.logger.error(f"Constraints file not found: {constraints_file}")
                return False

            constraints_content = constraints_file.read_text()

            statements = [
                stmt.strip() for stmt in constraints_content.split(";") if stmt.strip()
            ]

            with self.driver.session() as session:
                for statement in statements:
                    try:
                        session.run(statement)
                        self.logger.info(f"Executed constraint: {statement[:50]}...")
                    except Exception as e:
                        self.logger.error(f"Failed to execute constraint: {e}")
                        return False

                self.logger.success(
                    f"Neo4j constraints loaded successfully ({len(statements)} statements)"
                )
                return True
        except Exception as e:
            self.logger.error(f"Failed to load constraints: {e}")
            return False

    def create_dataset_nodes_and_relationships(self, datasets: List[Dataset]) -> dict:
        stats = {
            "datasets_created": 0,
            "titles_created": 0,
            "publishers_created": 0,
            "themes_created": 0,
            "has_title_relationships": 0,
            "published_by_relationships": 0,
            "has_theme_relationships": 0,
            "errors": [],
        }

        try:
            with self.driver.session() as session:
                for dataset in datasets:
                    try:
                        session.run(
                            "MERGE (d:Dataset {uri: $uri})",
                            {"uri": dataset.uri},
                        )
                        stats["datasets_created"] += 1

                        session.run(
                            "MERGE (t:Title {value: $value})",
                            {"value": dataset.title.value},
                        )
                        stats["titles_created"] += 1

                        session.run(
                            "MATCH (d:Dataset {uri: $dataset_uri}) "
                            "MATCH (t:Title {value: $title_value}) "
                            "MERGE (d)-[:HAS_TITLE]->(t)",
                            {
                                "dataset_uri": dataset.uri,
                                "title_value": dataset.title.value,
                            },
                        )
                        stats["has_title_relationships"] += 1

                        session.run(
                            "MERGE (p:Publisher {uri: $uri})",
                            {"uri": dataset.publisher.uri},
                        )
                        stats["publishers_created"] += 1

                        session.run(
                            "MATCH (d:Dataset {uri: $dataset_uri}) "
                            "MATCH (p:Publisher {uri: $publisher_uri}) "
                            "MERGE (d)-[:PUBLISHED_BY]->(p)",
                            {
                                "dataset_uri": dataset.uri,
                                "publisher_uri": dataset.publisher.uri,
                            },
                        )
                        stats["published_by_relationships"] += 1

                        for theme in dataset.themes:
                            session.run(
                                "MERGE (t:Theme {uri: $uri})",
                                {"uri": theme.uri},
                            )
                            stats["themes_created"] += 1

                            session.run(
                                "MATCH (d:Dataset {uri: $dataset_uri}) "
                                "MATCH (t:Theme {uri: $theme_uri}) "
                                "MERGE (d)-[:HAS_THEME]->(t)",
                                {
                                    "dataset_uri": dataset.uri,
                                    "theme_uri": theme.uri,
                                },
                            )
                            stats["has_theme_relationships"] += 1

                    except Exception as e:
                        error_msg = (
                            f"Failed to create nodes for dataset {dataset.uri}: {e}"
                        )
                        self.logger.error(error_msg)
                        stats["errors"].append(error_msg)

            self.logger.success(
                f"Loaded {stats['datasets_created']} datasets with relationships"
            )
            return stats

        except Exception as e:
            error_msg = f"Failed to create dataset nodes and relationships: {e}"
            self.logger.error(error_msg)
            stats["errors"].append(error_msg)
            return stats


def load_db_config() -> DatabaseManager:
    load_dotenv()

    uri = os.getenv("NEO4J_URI", "neo4j://localhost")
    username = os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "password")

    return DatabaseManager(uri=uri, username=username, password=password)
