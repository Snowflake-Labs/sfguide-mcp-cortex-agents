#!/usr/bin/env python3

import os
import snowflake.connector
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Snowflake connection parameters
SNOWFLAKE_PAT = os.getenv('SNOWFLAKE_PAT')  # Programmatic access token
SNOWFLAKE_ACCOUNT_URL = os.getenv('SNOWFLAKE_ACCOUNT_URL')

def execute_cortex_search():
    try:
        # Create a connection
        conn = snowflake.connector.connect(
            user='your-username',
            password=SNOWFLAKE_PAT,
            account=SNOWFLAKE_ACCOUNT_URL,
            warehouse='your-warehouse',
            database='your-database',
            schema='your-schema'
        )

        # Create a cursor
        cursor = conn.cursor()

        # Execute the Cortex search query
        sql_query = """
        SELECT *
        FROM TABLE(
            CORTEX_SEARCH_DATA_SCAN(
                SERVICE_NAME => 'sales_conversation_search'
            )
        );
        """

        cursor.execute(sql_query)

        # Fetch and print results
        for row in cursor:
            print(row)

    except snowflake.connector.errors.ProgrammingError as e:
        print(f"Error executing query: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Clean up
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    execute_cortex_search() 