import argparse
import sys

from pipeline_dimensional_data.config import RAW_DATA_SOURCE_PATH
from pipeline_dimensional_data.flow import DimensionalDataFlow


def main():
    parser = argparse.ArgumentParser(description="DS206 Project Pipeline CLI")
    parser.add_argument("--start_date", type=str, required=True)
    parser.add_argument("--end_date", type=str, required=True)
    args = parser.parse_args()

    pipeline = DimensionalDataFlow(source_path=RAW_DATA_SOURCE_PATH)
    result = pipeline.exec(args.start_date, args.end_date)
    return 0 if result.get("success") else 1

if __name__ == "__main__":
    sys.exit(main())
