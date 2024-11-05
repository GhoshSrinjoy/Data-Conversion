import pandas as pd
import os
from pathlib import Path
import math
import sys
from tqdm import tqdm

class CSVSplitterConverter:
    def __init__(self, chunk_size_mb=250):
        """
        Initialize the splitter converter
        
        Parameters:
        chunk_size_mb (float): Target size for each chunk in megabytes
        """
        self.chunk_size_mb = float(chunk_size_mb)
        self.bytes_per_mb = 1024 * 1024
    
    def estimate_rows_per_chunk(self, csv_path, chunk_size_mb):
        """
        Estimate how many rows should be in each chunk based on file size
        and desired chunk size
        """
        try:
            # Read a small sample to estimate row size
            sample_size = 1000
            sample_df = pd.read_csv(csv_path, nrows=sample_size, encoding='utf-8', on_bad_lines='skip')
            
            # Calculate average row size in bytes
            sample_memory = sample_df.memory_usage(deep=True).sum()
            avg_row_size = sample_memory / len(sample_df)
            
            # Calculate rows per chunk
            target_chunk_size = chunk_size_mb * self.bytes_per_mb
            rows_per_chunk = max(1000, int(target_chunk_size / avg_row_size))
            
            return rows_per_chunk
        
        except Exception as e:
            print(f"Error estimating chunk size: {str(e)}")
            # Return a safe default if estimation fails
            return 10000
    
    def get_total_rows(self, csv_path):
        """Count total rows in CSV file"""
        try:
            # Use pandas to count rows with proper encoding
            with pd.read_csv(csv_path, encoding='utf-8', chunksize=10000) as reader:
                total_rows = sum(len(chunk) for chunk in reader)
            return total_rows
        except Exception as e:
            print(f"Warning: Could not count exact rows due to {str(e)}")
            print("Proceeding with chunk-based processing...")
            return None
    
    def split_and_convert(self, input_csv, output_dir, chunk_size_mb=None):
        """
        Split large CSV file into smaller chunks and convert to Parquet
        
        Parameters:
        input_csv (str): Path to input CSV file
        output_dir (str): Directory to save output files
        chunk_size_mb (float): Optional override for chunk size in MB
        
        Returns:
        list: Paths to created Parquet files
        """
        if chunk_size_mb is not None:
            self.chunk_size_mb = float(chunk_size_mb)
            
        # Create output directory if it doesn't exist
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Get total rows and calculate chunks
        total_rows = self.get_total_rows(input_csv)
        rows_per_chunk = self.estimate_rows_per_chunk(input_csv, self.chunk_size_mb)
        
        if total_rows:
            total_chunks = math.ceil(total_rows / rows_per_chunk)
            print(f"\nTotal rows: {total_rows:,}")
        else:
            total_chunks = None
            print("\nProcessing in chunks (total rows unknown)")
            
        print(f"Processing {input_csv}")
        print(f"Estimated rows per chunk: {rows_per_chunk:,}")
        if total_chunks:
            print(f"Expected number of chunks: {total_chunks}")
        
        created_files = []
        
        try:
            # Process the file in chunks
            chunk_iterator = pd.read_csv(
                input_csv,
                chunksize=rows_per_chunk,
                encoding='utf-8',
                on_bad_lines='skip',
                iterator=True
            )
            
            # Use tqdm with or without total
            pbar = tqdm(chunk_iterator, total=total_chunks if total_chunks else None, 
                       desc="Converting chunks")
            
            for chunk_num, chunk in enumerate(pbar):
                # Generate output filename
                output_file = output_dir / f"chunk_{chunk_num:04d}.parquet"
                
                # Convert chunk to parquet
                chunk.to_parquet(output_file, index=False)
                created_files.append(output_file)
                
                # Calculate and display actual chunk size
                chunk_size = os.path.getsize(output_file) / self.bytes_per_mb
                pbar.set_postfix({'Current chunk size': f'{chunk_size:.2f}MB'})
            
            print(f"\nCreated {len(created_files)} files:")
            for file in created_files:
                size_mb = os.path.getsize(file) / self.bytes_per_mb
                print(f"- {file.name}: {size_mb:.2f} MB")
                
            return created_files
            
        except Exception as e:
            print(f"Error during conversion: {str(e)}")
            return created_files

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Split large CSV file and convert to Parquet')
    parser.add_argument('input_csv', help='Path to input CSV file')
    parser.add_argument('output_dir', help='Directory to save output files')
    parser.add_argument('--chunk-size', type=float, default=250.0,
                      help='Target size for each chunk in megabytes (default: 250.0)')
    
    args = parser.parse_args()
    
    splitter = CSVSplitterConverter(chunk_size_mb=args.chunk_size)
    splitter.split_and_convert(args.input_csv, args.output_dir)

if __name__ == "__main__":
    main()