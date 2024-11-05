import pandas as pd
import os
from pathlib import Path
import argparse

def convert_csv_to_parquet(input_path, output_path=None):
    """
    Convert a CSV file to Parquet format.
    
    Parameters:
    input_path (str): Path to input CSV file
    output_path (str): Optional path for output Parquet file. If not provided,
                      will use the same name as input file with .parquet extension
    
    Returns:
    str: Path to the created Parquet file
    """
    try:
        # Read CSV file
        df = pd.read_csv(input_path)
        
        # If output path is not provided, create one
        if output_path is None:
            output_path = str(Path(input_path).with_suffix('.parquet'))
            
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True)
        
        # Write to Parquet format
        df.to_parquet(output_path, index=False)
        
        return output_path
    
    except Exception as e:
        raise Exception(f"Error converting {input_path}: {str(e)}")

def batch_convert_csv_to_parquet(input_dir, output_dir=None):
    """
    Convert all CSV files in a directory to Parquet format.
    
    Parameters:
    input_dir (str): Directory containing CSV files
    output_dir (str): Optional directory for output Parquet files
    
    Returns:
    list: List of paths to created Parquet files
    """
    converted_files = []
    
    # Create output directory if specified and doesn't exist
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    
    # Process all CSV files in the input directory
    for csv_file in Path(input_dir).glob('**/*.csv'):
        if output_dir:
            # Create relative path structure in output directory
            rel_path = csv_file.relative_to(input_dir)
            output_path = str(Path(output_dir) / rel_path.with_suffix('.parquet'))
        else:
            output_path = None
            
        try:
            parquet_file = convert_csv_to_parquet(str(csv_file), output_path)
            converted_files.append(parquet_file)
            print(f"Successfully converted: {csv_file} -> {parquet_file}")
        except Exception as e:
            print(f"Failed to convert {csv_file}: {str(e)}")
    
    return converted_files

def main():
    parser = argparse.ArgumentParser(description='Convert CSV files to Parquet format')
    parser.add_argument('input', help='Input CSV file or directory')
    parser.add_argument('--output', help='Output Parquet file or directory (optional)')
    
    args = parser.parse_args()
    
    input_path = Path(args.input)
    
    if input_path.is_file():
        # Convert single file
        try:
            output_file = convert_csv_to_parquet(str(input_path), args.output)
            print(f"Successfully converted: {input_path} -> {output_file}")
        except Exception as e:
            print(f"Conversion failed: {str(e)}")
    
    elif input_path.is_dir():
        # Convert all CSV files in directory
        converted_files = batch_convert_csv_to_parquet(str(input_path), args.output)
        print(f"\nConverted {len(converted_files)} files")
    
    else:
        print(f"Error: {args.input} is not a valid file or directory")

if __name__ == "__main__":
    main()
