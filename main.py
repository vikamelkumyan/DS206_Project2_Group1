import argparse
import sys
import os

root_dir = os.path.dirname(os.path.abspath(__file__))
subfolder_path = os.path.join(root_dir, 'pipeline_dimensional_data')
if subfolder_path not in sys.path:
    sys.path.insert(0, subfolder_path)

try:
    from flow import DimensionalDataFlow
except ImportError as e:
    print(f"Import Error: {e}")
    sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="DS206 Project Pipeline CLI")
    parser.add_argument("--start_date", type=str, required=True)
    parser.add_argument("--end_date", type=str, required=True)
    args = parser.parse_args()

    try:
        # Initializing with source_path as required by your __init__
        pipeline = DimensionalDataFlow(source_path="data")
        
        print(f"Starting pipeline: {args.start_date} to {args.end_date}")
        
        # Now passing the two required date arguments
        pipeline.exec(args.start_date, args.end_date)
        
        print("Pipeline finished successfully.")
    except Exception as e:
        print(f"Error during execution: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
