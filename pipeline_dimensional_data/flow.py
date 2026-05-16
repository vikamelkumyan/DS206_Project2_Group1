import os
import sys

# Adds the root directory (where utils.py lives) to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils import generate_execution_id
from logging import setup_dimensional_logger

if __package__:
    from .tasks import (
        task_create_dimensional_database,
        task_create_dimensional_tables,
        task_create_staging_raw_tables,
        task_ingest_all_staging_raw_tables,
        task_update_all_dimensions,
        task_update_fact,
        task_update_fact_error,
    )
else:
    from tasks import (
        task_create_dimensional_database,
        task_create_dimensional_tables,
        task_create_staging_raw_tables,
        task_ingest_all_staging_raw_tables,
        task_update_all_dimensions,
        task_update_fact,
        task_update_fact_error,
    )


class DimensionalDataFlow:
    def __init__(self, source_path=None):
        # Requirement 12: Unique execution ID for each run
        self.execution_id = generate_execution_id()
        self.source_path = source_path or os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "raw_data_source.xlsx",
        )
        self.status = "Pending"
        self.logger = setup_dimensional_logger(self.execution_id)

    def _run_task(self, task_name, task_callable):
        self.logger.info("Starting task=%s", task_name)
        result = task_callable()
        self.logger.info("Finished task=%s with result=%s", task_name, result)

        if not result.get("success"):
            self.status = "Failed"
            self.logger.error("Stopping pipeline after failed task=%s", task_name)
            return result

        return result

    def exec(self, start_date, end_date):
        self.logger.info(
            "Starting dimensional data pipeline for start_date=%s and end_date=%s",
            start_date,
            end_date,
        )
        self.status = "Running"

        tasks = [
            ("create_dimensional_database", task_create_dimensional_database),
            ("create_staging_raw_tables", task_create_staging_raw_tables),
            ("create_dimensional_tables", task_create_dimensional_tables),
            (
                "ingest_all_staging_raw_tables",
                lambda: task_ingest_all_staging_raw_tables(self.source_path),
            ),
            ("update_all_dimensions", task_update_all_dimensions),
            ("update_fact", lambda: task_update_fact(start_date, end_date)),
            ("update_fact_error", lambda: task_update_fact_error(start_date, end_date)),
        ]

        try:
            for task_name, task_callable in tasks:
                result = self._run_task(task_name, task_callable)
                if not result.get("success"):
                    return result

            self.status = "Completed"
            self.logger.info("Dimensional data pipeline completed with status=%s", self.status)
            return {"success": True, "execution_id": self.execution_id, "status": self.status}

        except Exception as e:
            self.status = "Failed"
            self.logger.exception("Dimensional data pipeline failed with status=%s: %s", self.status, e)
            return {
                "success": False,
                "execution_id": self.execution_id,
                "status": self.status,
                "error": str(e),
            }


if __name__ == "__main__":
    # Ensure the path works regardless of where you run it from
    base_path = os.path.dirname(os.path.abspath(__file__))
    excel_file = os.path.join(base_path, '..', 'raw_data_source.xlsx')
    
    flow = DimensionalDataFlow(excel_file)
    flow.exec("1996-01-01", "1998-12-31")
