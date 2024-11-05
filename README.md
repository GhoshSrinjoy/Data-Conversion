## This script provides several features:
1. Can convert a single CSV file or batch process an entire directory
2. Preserves the original data structure
3. Maintains directory hierarchy when batch processing
4. Includes error handling and progress feedback
5. Command-line interface for easy use

You can use it in several ways:

1. Convert a single file:
```python
convert_csv_to_parquet('data.csv')
```

2. Convert all CSV files in a directory:
```python
batch_convert_csv_to_parquet('input_directory', 'output_directory')
```

3. From command line:
```bash
# Convert single file
python script.py input.csv --output output.parquet

# Convert entire directory
python script.py input_directory --output output_directory
```
