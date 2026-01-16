from pydantic import BaseModel, field_validator
from typing import List, Optional
from logging_utils.app_logger import AppLogger

_validation_logger = AppLogger()


def _log_validation_error(error_msg: str) -> None:
    _validation_logger.error(error_msg)


class DatasetTitle(BaseModel):
    value: str

    @field_validator("value")
    @classmethod
    def validate_value(cls, v: str) -> str:
        try:
            assert v is not None, "DatasetTitle.value cannot be None"
            assert isinstance(v, str), "DatasetTitle.value must be a string"
            assert len(v.strip()) > 0, "DatasetTitle.value cannot be empty"
        except AssertionError as e:
            error_msg = f"DatasetTitle validation failed: {e}"
            _log_validation_error(error_msg)
            raise
        return v


class Publisher(BaseModel):
    uri: str

    @field_validator("uri")
    @classmethod
    def validate_uri(cls, v: str) -> str:
        try:
            assert v is not None, "Publisher.uri cannot be None"
            assert isinstance(v, str), "Publisher.uri must be a string"
            assert len(v.strip()) > 0, "Publisher.uri cannot be empty"
        except AssertionError as e:
            error_msg = f"Publisher.uri validation failed: {e}"
            _log_validation_error(error_msg)
            raise
        return v


class Theme(BaseModel):
    uri: str

    @field_validator("uri")
    @classmethod
    def validate_uri(cls, v: str) -> str:
        try:
            assert v is not None, "Theme.uri cannot be None"
            assert isinstance(v, str), "Theme.uri must be a string"
            assert len(v.strip()) > 0, "Theme.uri cannot be empty"
        except AssertionError as e:
            error_msg = f"Theme.uri validation failed: {e}"
            _log_validation_error(error_msg)
            raise
        return v


class LandingPage(BaseModel):
    url: str

    @field_validator("url")
    @classmethod
    def validate_url(cls, v: str) -> str:
        try:
            assert v is not None, "LandingPage.url cannot be None"
            assert isinstance(v, str), "LandingPage.url must be a string"
            assert len(v.strip()) > 0, "LandingPage.url cannot be empty"
        except AssertionError as e:
            error_msg = f"LandingPage.url validation failed: {e}"
            _log_validation_error(error_msg)
            raise
        return v


class DownloadURL(BaseModel):
    url: str

    @field_validator("url")
    @classmethod
    def validate_url(cls, v: str) -> str:
        try:
            assert v is not None, "DownloadURL.url cannot be None"
            assert isinstance(v, str), "DownloadURL.url must be a string"
            assert len(v.strip()) > 0, "DownloadURL.url cannot be empty"
        except AssertionError as e:
            error_msg = f"DownloadURL.url validation failed: {e}"
            _log_validation_error(error_msg)
            raise
        return v


class ThemeLabel(BaseModel):
    title: str
    language: str

    @field_validator("title")
    @classmethod
    def validate_title(cls, v: str) -> str:
        try:
            assert v is not None, "ThemeLabel.title cannot be None"
            assert isinstance(v, str), "ThemeLabel.title must be a string"
            assert len(v.strip()) > 0, "ThemeLabel.title cannot be empty"
        except AssertionError as e:
            error_msg = f"ThemeLabel.title validation failed: {e}"
            _log_validation_error(error_msg)
            raise
        return v

    @field_validator("language")
    @classmethod
    def validate_language(cls, v: str) -> str:
        try:
            assert v is not None, "ThemeLabel.language cannot be None"
            assert isinstance(v, str), "ThemeLabel.language must be a string"
            assert len(v.strip()) > 0, "ThemeLabel.language cannot be empty"
        except AssertionError as e:
            error_msg = f"ThemeLabel.language validation failed: {e}"
            _log_validation_error(error_msg)
            raise
        return v


class Dataset(BaseModel):
    uri: str
    title: DatasetTitle
    publisher: Publisher
    themes: List[Theme]
    landing_page: Optional[LandingPage] = None
    download_url: Optional[DownloadURL] = None
    issued: Optional[str] = None
    status: Optional[str] = None
    access_url: Optional[str] = None
    byte_size: Optional[int] = None
    keywords: Optional[List[str]] = None

    @field_validator("uri")
    @classmethod
    def validate_uri(cls, v: str) -> str:
        try:
            assert v is not None, "Dataset.uri cannot be None"
            assert isinstance(v, str), "Dataset.uri must be a string"
            assert len(v.strip()) > 0, "Dataset.uri cannot be empty"
        except AssertionError as e:
            error_msg = f"Dataset.uri validation failed: {e}"
            _log_validation_error(error_msg)
            raise
        return v
