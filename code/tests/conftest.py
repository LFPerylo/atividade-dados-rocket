import pytest
from src.common.spark_session import build_local_spark_session


@pytest.fixture(scope="session")
def spark():
    session = build_local_spark_session()
    yield session
    session.stop()
