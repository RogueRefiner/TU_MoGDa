import sys
from typing import Any
from pathlib import Path
from loguru import logger as loguru_logger


class AppLogger:
    _instance: "AppLogger" = None
    _initialized: bool = False

    def __new__(cls: type["AppLogger"], *args: Any, **kwargs: Any) -> "AppLogger":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        if AppLogger._initialized:
            return

        # remove default handler to sys.stderr
        loguru_logger.remove()

        loguru_logger.add(
            sys.stdout,
            level="DEBUG",
            colorize=True,
            backtrace=True,
            diagnose=False,
            format=(
                "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
                "<level>{level: <8}</level> | "
                "<cyan>{module}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
                "<level>{message}</level>"
            ),
        )

        loguru_logger.add(
            Path(__file__).parent / "validation_errors.log",
            level="ERROR",
            format="{time:YYYY-MM-DD HH:mm:ss} - {level} - {message}",
        )

        self.logger = loguru_logger

        self.logger.info(f"Logger initialized (AppLogger id={id(self)})")

        AppLogger._initialized = True

    def trace(self, msg: str) -> None:
        self.logger.opt(depth=1).trace(msg)

    def debug(self, msg: str) -> None:
        self.logger.opt(depth=1).debug(msg)

    def info(self, msg: str) -> None:
        self.logger.opt(depth=1).info(msg)

    def success(self, msg: str) -> None:
        self.logger.opt(depth=1).success(msg)

    def warning(self, msg: str) -> None:
        self.logger.opt(depth=1).warning(msg)

    def error(self, msg: str) -> None:
        self.logger.opt(depth=1).error(msg)

    def critical(self, msg: str) -> None:
        self.logger.opt(depth=1).critical(msg)
