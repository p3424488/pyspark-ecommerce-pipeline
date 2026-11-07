
import pytest

from src.spark_session import create_spark_session


@pytest.fixture(scope="session")
def spark():
    session = create_spark_session("EcommercePipelineTests")
    yield session
    session.stop()
