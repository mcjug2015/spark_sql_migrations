import logging
from unittest import mock

from spark_sql_migrations import custom_logging
from spark_sql_migrations.custom_logging import build_config, get_log_file_path, is_logging_configured, setup_logging


def test_get_log_file_path_from_env(monkeypatch):
    monkeypatch.setenv(custom_logging.LOG_FILE_ENV_VAR, "/fake/from/env.log")

    assert get_log_file_path() == "/fake/from/env.log"


@mock.patch("spark_sql_migrations.custom_logging.os.getcwd", return_value="/fake/cwd")
def test_get_log_file_path_defaults_under_cwd(getcwd, monkeypatch):
    monkeypatch.delenv(custom_logging.LOG_FILE_ENV_VAR, raising=False)

    assert get_log_file_path() == "/fake/cwd/local_log.log"
    getcwd.assert_called_once()


@mock.patch("spark_sql_migrations.custom_logging.get_log_file_path", return_value="/fake/built.log")
def test_build_config_takes_filename_from_helper(get_log_file_path_mock):
    config = build_config()

    assert config["handlers"]["file"]["filename"] == "/fake/built.log"
    assert config["root"]["handlers"] == ["stderr", "stdout", "file"]
    get_log_file_path_mock.assert_called_once()


@mock.patch("spark_sql_migrations.custom_logging.logging.getLogger")
def test_is_logging_configured_with_handlers(get_logger):
    get_logger.return_value.handlers = ["stderr", "stdout", "file"]

    assert is_logging_configured() is True
    get_logger.assert_called_once_with()


@mock.patch("spark_sql_migrations.custom_logging.logging.getLogger")
def test_is_logging_configured_without_handlers(get_logger):
    get_logger.return_value.handlers = []

    assert is_logging_configured() is False
    get_logger.assert_called_once_with()


@mock.patch("spark_sql_migrations.custom_logging.logging.config.dictConfig")
@mock.patch("spark_sql_migrations.custom_logging.build_config", return_value={"fake": "config"})
@mock.patch("spark_sql_migrations.custom_logging.is_logging_configured", return_value=False)
def test_setup_logging_configures_when_unconfigured(is_configured, build_config_mock, dict_config):
    result = setup_logging()

    assert result is logging
    dict_config.assert_called_once_with({"fake": "config"})
    build_config_mock.assert_called_once()
    is_configured.assert_called_once()


@mock.patch("spark_sql_migrations.custom_logging.is_logging_configured", return_value=True)
def test_setup_logging_returns_logging_when_already_configured(is_configured):
    result = setup_logging()

    assert result is logging
    is_configured.assert_called_once()
