import psycopg2

from config import logger


class PostgresClient:
    def __init__(self, db_uri) -> None:
        """
        Initialize the Postgres client and run initial setup.
        """
        self.db_uri = db_uri
        self.connection = None
        self.cursor = None
        self.data = None
        self.connect()

    def connect(self) -> None:
        """
        Establish a connection to the PostgreSQL database.
        """
        try:
            self.connection = psycopg2.connect(self.db_uri)
            self.cursor = self.connection.cursor()
            logger.info("Connected to PostgreSQL database.")
        except Exception as e:
            logger.error(f"Error connecting to the database: {e}")
            raise

    def schema_exists(self, schema_name: str) -> bool:
        """
        Check if a schema exists in the PostgreSQL database.

        Returns:
            True if the schema exists, False otherwise.
        """
        try:
            check_schema_sql = """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.schemata
                    WHERE schema_name = %s
                );
            """
            self.cursor.execute(check_schema_sql, (schema_name,))
            return self.cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"Error checking existence of schema '{schema_name}': {e}")
            raise

    def table_exists(self, schema_name: str, table_name: str) -> bool:
        """
        Check if a table exists in the specified schema.

        Returns:
            True if the table exists, False otherwise.
        """
        try:
            check_table_sql = """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = %s
                    AND table_name = %s
                );
            """
            self.cursor.execute(check_table_sql, (schema_name, table_name))
            return self.cursor.fetchone()[0]
        except Exception as e:
            logger.error(
                f"Error checking existence of table '{schema_name}.{table_name}': {e}"
            )
            raise

    def create_schema(self, schema_name="public") -> None:
        """
        Create a schema in the PostgreSQL database.
        """
        try:
            if self.schema_exists(schema_name):
                logger.info(f"Schema '{schema_name}' already exists.")
                return
            create_schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema_name};"
            self.cursor.execute(create_schema_sql)
            self.connection.commit()
            logger.info(f"Schema '{schema_name}' created successfully.")
        except Exception as e:
            logger.error(f"Error creating schema '{schema_name}': {e}")
            raise

    def create_table(
        self, schema_name="public", table_name="users", columns=None
    ) -> None:
        """
        Create a table in the PostgreSQL database.
        """
        self.create_schema(schema_name=schema_name)

        if columns is None:
            # If columns are not provided, use the previously defined ones
            if not hasattr(self, "columns"):
                raise ValueError("Columns must be defined before inserting data.")
            columns = self.columns

        try:
            if self.table_exists(schema_name, table_name):
                logger.info(f"Table '{schema_name}.{table_name}' already exists.")
                return
            self.columns = list(columns.keys())
            column_definitions = ", ".join(
                [f"{col} {dtype}" for col, dtype in columns.items()]
            )
            create_table_sql = (
                f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name}"
                f"({column_definitions});"
            )
            self.cursor.execute(create_table_sql)
            self.connection.commit()
            logger.info(f"Table '{schema_name}.{table_name}' created successfully.")
        except Exception as e:
            logger.error(f"Error creating table '{schema_name}.{table_name}': {e}")
            raise

    def insert_data(
        self, schema_name="public", table_name="users", data=None, columns=None
    ) -> None:
        """
        Insert data into the PostgreSQL table.
        """
        if self.data is None:
            raise ValueError("Data must be provided to insert into the table.")

        if columns is None:
            # If columns are not provided, use the previously defined ones
            if not hasattr(self, "columns"):
                raise ValueError("Columns must be defined before inserting data.")
            columns = self.columns

        try:
            columns_str = ", ".join(columns)
            placeholders = ", ".join(["%s"] * len(columns))

            insert_sql = (
                f"INSERT INTO {schema_name}.{table_name}"
                f"({columns_str}) VALUES ({placeholders});"
            )
            self.cursor.executemany(insert_sql, self.data)
            self.connection.commit()
            logger.info(
                f"Inserted {len(self.data)} rows into '{schema_name}.{table_name}'."
            )
        except Exception as e:
            logger.error(f"Error inserting data into '{schema_name}.{table_name}': {e}")
            raise

    def fetch_data(self, schema_name="public", table_name="users") -> None:
        """
        Fetch data from the PostgreSQL table.
        """
        try:
            fetch_sql = f"SELECT * FROM {schema_name}.{table_name};"
            self.cursor.execute(fetch_sql)
            rows = self.cursor.fetchall()
            logger.info(f"Fetched {len(rows)} rows from '{table_name}'.")
            for row in rows:
                print(row)
                pass
        except Exception as e:
            logger.error(f"Error fetching data from '{table_name}': {e}")
            raise

    def delete_table(self, schema_name="public", table_name="users") -> None:
        """
        Delete the PostgreSQL table.
        """
        try:
            delete_sql = f"DROP TABLE IF EXISTS {schema_name}.{table_name};"
            self.cursor.execute(delete_sql)
            self.connection.commit()
            logger.info(f"Table '{schema_name}.{table_name}' deleted successfully.")
        except Exception as e:
            logger.error(f"Error deleting table '{schema_name}.{table_name}': {e}")
            raise

    def count_elements(self, schema_name="public", table_name="users"):
        """
        Count the number of elements in the PostgreSQL table.
        """
        try:
            count_sql = f"SELECT COUNT(*) FROM {schema_name}.{table_name};"
            self.cursor.execute(count_sql)
            count = self.cursor.fetchone()[0]
            logger.info(f"Count of elements in '{schema_name}.{table_name}': {count}")
            return count
        except Exception as e:
            logger.error(
                f"Error counting elements in '{schema_name}.{table_name}': {e}"
            )
            raise

    def close(self) -> None:
        """
        Close the cursor and connection to the PostgreSQL database.
        """
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
                logger.info("PostgreSQL connection closed.")
        except Exception as e:
            logger.error(f"Error closing PostgreSQL connection: {e}")
