import os

import psycopg
from dotenv import load_dotenv


# Load environment variables from the project root.
load_dotenv()


# Read database connection settings.
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}


def test_connection():
    connection = None

    try:
        connection = psycopg.connect(
            **DB_CONFIG,
            connect_timeout=10
        )

        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT current_database(), current_user;"
            )

            database_name, database_user = cursor.fetchone()

        print("PostgreSQL connection successful!")
        print(f"Database: {database_name}")
        print(f"User: {database_user}")

    except psycopg.Error as error:
        print("PostgreSQL connection failed.")
        print(f"Error: {error}")

    finally:
        if connection is not None:
            connection.close()


if __name__ == "__main__":
    test_connection()