import sys
import os

# Adds the root directory (where utils.py lives) to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils import generate_execution_id
from logging import setup_dimensional_logger

if __package__:
    from .tasks import task_ingest_excel_sheet
else:
    from tasks import task_ingest_excel_sheet

class DimensionalDataFlow:
    def __init__(self, source_path):
        # Requirement 12: Unique execution ID for each run
        self.execution_id = generate_execution_id()
        self.source_path = source_path
        self.status = "Pending"
        self.logger = setup_dimensional_logger(self.execution_id)

    def exec(self, start_date, end_date):
        self.logger.info(
            "Starting dimensional data pipeline for start_date=%s and end_date=%s",
            start_date,
            end_date,
        )
        self.status = "Running"
        
        # List of sheets to ingest (Requirement 11 + 12)
        tables_to_ingest = ['Customers', 'Products', 'Orders', 'Categories', 'Suppliers', 'Employees']        
        try:
            for table in tables_to_ingest:
                self.logger.info("Starting ingestion for %s", table)
                result = task_ingest_excel_sheet(self.source_path, table, table)
                self.logger.info("Finished ingestion for %s with result=%s", table, result)
            
            self.status = "Completed"
            self.logger.info("Dimensional data pipeline completed with status=%s", self.status)
            
        except Exception as e:
            self.status = "Failed"
            self.logger.exception("Dimensional data pipeline failed with status=%s: %s", self.status, e)
            raise

if __name__ == "__main__":
    # Ensure the path works regardless of where you run it from
    base_path = os.path.dirname(os.path.abspath(__file__))
    excel_file = os.path.join(base_path, '..', 'raw_data_source.xlsx')
    
    flow = DimensionalDataFlow(excel_file)
    flow.exec("1996-01-01", "1998-12-31")
