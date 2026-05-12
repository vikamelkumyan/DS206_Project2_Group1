import sys
import os

# Adds the root directory (where utils.py lives) to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils import generate_execution_id
from tasks import task_ingest_excel_sheet

class DimensionalDataFlow:
    def __init__(self, source_path):
        # Requirement 12: Unique execution ID for each run
        self.execution_id = generate_execution_id()
        self.source_path = source_path
        self.status = "Pending"

    def exec(self, start_date, end_date):
        print(f"--- Starting Pipeline Execution: {self.execution_id} ---")
        self.status = "Running"
        
        # List of sheets to ingest (Requirement 11 + 12)
        tables_to_ingest = ['Customers', 'Products', 'Orders', 'Categories', 'Suppliers', 'Employees']        
        try:
            for table in tables_to_ingest:
                print(f"Action: Ingesting {table}...")
                task_ingest_excel_sheet(self.source_path, table, table)
            
            self.status = "Completed"
            print(f"--- Result: {self.status} ---")
            
        except Exception as e:
            self.status = "Failed"
            print(f"--- Flow Interrupted: {e} ---")

if __name__ == "__main__":
    # Ensure the path works regardless of where you run it from
    base_path = os.path.dirname(os.path.abspath(__file__))
    excel_file = os.path.join(base_path, '..', 'raw_data_source.xlsx')
    
    flow = DimensionalDataFlow(excel_file)
    flow.exec()