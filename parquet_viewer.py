import pandas as pd
import argparse
from pathlib import Path

def view_parquet(file_path, num_rows=5, show_info=True, show_schema=True):
    """
    View contents of a Parquet file
    
    Parameters:
    file_path (str): Path to Parquet file
    num_rows (int): Number of rows to display (default=5)
    show_info (bool): Whether to show DataFrame info
    show_schema (bool): Whether to show data schema
    """
    try:
        # Read Parquet file
        df = pd.read_parquet(file_path)
        
        # Print file information
        print(f"\n{'='*50}")
        print(f"File: {file_path}")
        print(f"{'='*50}")
        
        # Show basic information
        if show_info:
            print("\nDataFrame Info:")
            print(f"Number of rows: {len(df)}")
            print(f"Number of columns: {len(df.columns)}")
            print("\nMemory usage:")
            print(df.info(memory_usage='deep'))
            
        # Show schema
        if show_schema:
            print("\nSchema:")
            for column in df.columns:
                print(f"{column}: {df[column].dtype}")
        
        # Show data preview
        print(f"\nFirst {num_rows} rows:")
        print(df.head(num_rows))
        
        return df
        
    except Exception as e:
        print(f"Error reading parquet file: {str(e)}")
        return None

def main():
    parser = argparse.ArgumentParser(description='View Parquet file contents')
    parser.add_argument('file', help='Path to Parquet file')
    parser.add_argument('--rows', type=int, default=5, help='Number of rows to display (default: 5)')
    parser.add_argument('--no-info', action='store_true', help='Skip showing DataFrame info')
    parser.add_argument('--no-schema', action='store_true', help='Skip showing schema')
    
    args = parser.parse_args()
    
    if not Path(args.file).exists():
        print(f"Error: File {args.file} does not exist")
        return
        
    view_parquet(args.file, args.rows, not args.no_info, not args.no_schema)

if __name__ == "__main__":
    main()
