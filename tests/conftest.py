import pytest
from pytest import Config, FixtureRequest, Parser


def pytest_configure(config: Config):
    config.addinivalue_line("markers", "control: tests for Control")
    config.addinivalue_line("markers", "logger: tests for Logger")


def pytest_addoption(parser: Parser):
    parser.addoption("--mysql-host", default="192.168.1.102", help="host")
    parser.addoption("--mysql-port", default=3306, help="port")
    parser.addoption("--mysql-user", default="root", help="user")
    parser.addoption("--mysql-password", default="123456", help="password")
    parser.addoption("--mysql-db", default="test", help="db")

    parser.addoption("--postgresql-host", default="192.168.1.102", help="host")
    parser.addoption("--postgresql-port", default=5432, help="port")
    parser.addoption("--postgresql-user", default="root", help="user")
    parser.addoption("--postgresql-password", default="root", help="password")
    parser.addoption("--postgresql-database", default="test", help="database")


@pytest.fixture(scope="module")
def mysql_config(request: FixtureRequest) -> dict:
    return {
        "host": request.config.getoption("--mysql-host"),
        "port": request.config.getoption("--mysql-port"),
        "user": request.config.getoption("--mysql-user"),
        "password": request.config.getoption("--mysql-password"),
        "db": request.config.getoption("--mysql-db"),
    }


@pytest.fixture(scope="module")
def postgresql_config(request: FixtureRequest) -> dict:
    return {
        "host": request.config.getoption("--postgresql-host"),
        "port": request.config.getoption("--postgresql-port"),
        "user": request.config.getoption("--postgresql-user"),
        "password": request.config.getoption("--postgresql-password"),
        "database": request.config.getoption("--postgresql-database"),
    }
