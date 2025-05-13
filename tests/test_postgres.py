import pytest

from src.scripts.postgres import PostgresClient

DB_URI = "postgresql://admin:password@localhost:5432/mydatabase"


@pytest.fixture(scope="function")
def postgres_client():
    """
    Provides a real PostgresClient using the local PostgreSQL DB,
    and ensures proper teardown.
    """
    client = PostgresClient(db_uri=DB_URI)
    yield client
    client.close()


@pytest.fixture(autouse=True)
def clean_test_schema(postgres_client):
    """Clean up test_schema before and after each test."""
    schema = "test_schema"
    if postgres_client.schema_exists(schema):
        postgres_client.execute_query(f"DROP SCHEMA {schema} CASCADE;")


def test_create_schema(postgres_client):
    schema_name = "test_schema"
    postgres_client.create_schema(schema_name=schema_name)
    result = postgres_client.schema_exists(schema_name=schema_name)
    assert result


def test_create_table(postgres_client):
    columns = {"id": "SERIAL PRIMARY KEY", "name": "VARCHAR(255)"}
    schema_name = "test_schema"
    table_name = "test_table"
    postgres_client.create_table(
        schema_name=schema_name, table_name=table_name, columns=columns
    )
    result = postgres_client.table_exists(
        schema_name=schema_name, table_name=table_name
    )
    assert result


@pytest.mark.parametrize(
    "strategy, data, expected",
    [
        ("skip", [(1, "Alice"), (2, "Bob"), (1, "John")], (2, "Alice")),
        ("update", [(1, "Alice"), (2, "Bob"), (1, "John")], (2, "John")),
    ],
)
def test_insert_data(postgres_client, strategy, data, expected):
    # Setup table
    columns = {"id": "INT PRIMARY KEY", "name": "VARCHAR(255)"}
    schema_name = "test_schema"
    table_name = "test_table"
    postgres_client.create_schema(schema_name=schema_name)
    postgres_client.create_table(
        schema_name=schema_name, table_name=table_name, columns=columns
    )

    # Insert data
    postgres_client.insert_data(
        schema_name=schema_name,
        table_name=table_name,
        data=data,
        strategy=strategy,
    )

    # Check inserted rows
    nb_elements = postgres_client.count_elements(
        schema_name=schema_name, table_name=table_name
    )

    check_name = postgres_client.execute_query(
        "SELECT name FROM test_schema.test_table WHERE id = '1';"
    )
    assert check_name[0][0] == expected[1]
    assert nb_elements == expected[0]
