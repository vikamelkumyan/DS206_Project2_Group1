import pymssql
import pandas as pd
import os
import sys

# This line ensures 'utils' can always be found
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import parse_db_config

def connect_to_db():
    cfg = parse_db_config()
    return pymssql.connect(
        server=cfg['server'],
        user=cfg['user'],
        password=cfg['password'],
        database=cfg['database'],
        port=cfg['port']
    )

def task_ingest_excel_sheet(file_path, sheet_name, table_name):
    """Requirement 11: Ingests a specific Excel sheet into a staging table."""
    try:
        # Load the specific tab from your Excel file
        df = pd.read_excel(file_path, sheet_name=sheet_name)
        
        # Standardize columns (remove spaces)
        df.columns = [c.replace(' ', '') for c in df.columns]
        
        conn = connect_to_db()
        cursor = conn.cursor()
        
        # Clear staging (Reproducibility)
        cursor.execute(f"TRUNCATE TABLE dbo.stg_{table_name}")
        
        # Create insert statement
        cols = ", ".join(df.columns)
        placeholders = ", ".join(["%s"] * len(df.columns))
        sql = f"INSERT INTO dbo.stg_{table_name} ({cols}) VALUES ({placeholders})"
        
        # Handle empty/NaN values for SQL
        df = df.where(pd.notnull(df), None)
        
        cursor.executemany(sql, df.values.tolist())
        conn.commit()
        conn.close()
        
        print(f"Successfully loaded {sheet_name} into stg_{table_name}")
        return {'success': True}
    except Exception as e:
        print(f"Error loading {sheet_name}: {e}")
        return {'success': False}

# Test block - You can uncomment this to test one sheet
if __name__ == "__main__":
    task_ingest_excel_sheet('raw_data_source.xlsx', 'Customers', 'Customers')