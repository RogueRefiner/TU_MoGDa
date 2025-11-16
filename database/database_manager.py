from dataclasses import dataclass, field
import os
from dotenv import load_dotenv
from neo4j import GraphDatabase, Neo4jDriver
from logging_utils.app_logger import AppLogger


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


def load_db_config() -> DatabaseManager:
    load_dotenv()

    uri = os.getenv("NEO4J_URI", "neo4j://localhost")
    username = os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "password")

    return DatabaseManager(uri=uri, username=username, password=password)
