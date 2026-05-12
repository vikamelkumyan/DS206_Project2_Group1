import uuid
import os
from dotenv import load_dotenv

# Requirement 12: Generates a unique UUID
def generate_execution_id():
    return str(uuid.uuid4())

# Requirement 10: Reads an SQL script
def load_sql_script(file_path):
    try:
        with open(file_path, 'r') as file:
            return file.read()
    except FileNotFoundError:
        print(f"Warning: SQL file not found at {file_path}")
        return None

# Requirement 10: Parses database configs
def parse_db_config():
    # Force load the .env from the project root
    base_dir = os.path.dirname(os.path.abspath(__file__))
    load_dotenv(os.path.join(base_dir, '.env'))
    
    return {
        'server': os.getenv('MSSQL_SERVER', 'localhost'),
        'user': os.getenv('MSSQL_USER', 'sa'),
        'password': os.getenv('MSSQL_SA_PASSWORD', ''),
        'database': 'ORDER_DDS',
        'port': os.getenv('MSSQL_PORT', '1433')
    }