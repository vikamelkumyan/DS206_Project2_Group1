from logging import setup_dimensional_logger
from utils import generate_execution_id

from .config import RAW_DATA_SOURCE_PATH
from .tasks import (
    task_ingest_all_source_tables,
    task_update_all_dimensions,
    task_update_fact,
    task_update_fact_error,
)


class DimensionalDataFlow:
    def __init__(self, source_path=None):
        # Requirement 12: Unique execution ID for each run
        self.execution_id = generate_execution_id()
        self.source_path = source_path or RAW_DATA_SOURCE_PATH
        self.status = "Pending"
        self.logger = setup_dimensional_logger(self.execution_id)

    def _run_task(self, task_name, task_callable):
        self.logger.info("Starting task=%s", task_name)
        result = task_callable()
        self.logger.info(
            "Finished task=%s success=%s",
            result.get("task", task_name),
            result.get("success"),
        )
        self._log_task_details(task_name, result)

        if not result.get("success"):
            self.status = "Failed"
            self.logger.error(
                "Failed task=%s error=%s",
                result.get("task", task_name),
                result.get("error"),
            )
            self.logger.error("Stopping pipeline after failed task=%s", task_name)
            return result

        return result

    def _log_task_details(self, task_name, result):
        if not result.get("success"):
            return

        if task_name == "ingest_all_source_tables":
            for table_count in result.get("table_counts", []):
                self.logger.info(
                    "Loaded source sheet=%s into table=%s row_count=%s",
                    table_count["sheet"],
                    table_count["table"],
                    table_count["rows"],
                )
            return

        if task_name == "update_all_dimensions":
            for dimension in result.get("dimensions", []):
                self.logger.info(
                    "Updated dimension target_table=%s from source_table=%s using query=%s",
                    dimension["target_table"],
                    dimension["source_table"],
                    dimension["query"],
                )
            return

        if task_name in {"update_fact", "update_fact_error"}:
            self.logger.info(
                "Updated target_table=%s from source_orders_table=%s and source_order_details_table=%s "
                "using query=%s for start_date=%s and end_date=%s",
                result.get("target_table"),
                result.get("source_orders_table"),
                result.get("source_order_details_table"),
                result.get("query"),
                result.get("start_date"),
                result.get("end_date"),
            )

    def exec(self, start_date, end_date):
        self.logger.info(
            "Starting dimensional data pipeline for start_date=%s and end_date=%s",
            start_date,
            end_date,
        )
        self.status = "Running"

        tasks = [
            (
                "ingest_all_source_tables",
                lambda: task_ingest_all_source_tables(self.source_path),
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
