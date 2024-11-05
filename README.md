# CSV-Parquet Conversion Tools

A collection of Python utilities for handling CSV and Parquet files, specifically designed for data engineers and analysts working with large datasets.

## Tools Included

1. **csv_to_parquet.py**: Basic CSV to Parquet converter
2. **parquet_viewer.py**: Parquet file viewer and inspector
3. **csv_splitter_converter.py**: Large CSV file splitter and converter

## Installation

### Prerequisites
```bash
pip install pandas pyarrow tqdm
```

### Setup
1. Clone or download the repository
2. Ensure all requirements are installed
3. Make sure Python 3.7+ is installed on your system

## Tool Details

### 1. CSV to Parquet Converter (csv_to_parquet.py)
Basic converter for transforming CSV files to Parquet format.

#### Features
- Single file conversion
- Batch directory conversion
- Preserves data types and structure
- Maintains directory hierarchy

#### Usage
```bash
# Convert single file
python csv_to_parquet.py data.csv

# Specify output location
python csv_to_parquet.py data.csv --output converted/output.parquet

# Convert entire directory
python csv_to_parquet.py input_directory --output output_directory
```

#### Python API
```python
from csv_to_parquet import convert_csv_to_parquet

# Convert single file
convert_csv_to_parquet('data.csv', 'output.parquet')

# Convert directory
batch_convert_csv_to_parquet('input_directory', 'output_directory')
```

### 2. Parquet Viewer (parquet_viewer.py)
Tool for inspecting and viewing Parquet file contents.

#### Features
- View file contents
- Display schema information
- Show memory usage
- Configurable row display

#### Usage
```bash
# Basic view
python parquet_viewer.py file.parquet

# View with 10 rows
python parquet_viewer.py file.parquet --rows 10

# View without schema
python parquet_viewer.py file.parquet --no-schema

# View without additional info
python parquet_viewer.py file.parquet --no-info
```

#### Python API
```python
from parquet_viewer import view_parquet

# View file with default settings
df = view_parquet('file.parquet')

# Customize view
df = view_parquet('file.parquet', 
                  num_rows=10,
                  show_info=True,
                  show_schema=True)
```

### 3. CSV Splitter and Converter (csv_splitter_converter.py)
Tool for handling large CSV files by splitting them into smaller chunks and converting to Parquet format.

#### Features
- Split large CSV files into manageable chunks
- Automatic chunk size estimation
- Progress tracking
- UTF-8 encoding support
- Handles problematic rows
- Supports decimal chunk sizes

#### Usage
```bash
# Split with default chunk size (250MB)
python csv_splitter_converter.py large_file.csv output_directory

# Split with custom chunk size (e.g., 100MB)
python csv_splitter_converter.py large_file.csv output_directory --chunk-size 100

# Split with small chunk size (e.g., 0.1MB)
python csv_splitter_converter.py large_file.csv output_directory --chunk-size 0.1
```

#### Python API
```python
from csv_splitter import CSVSplitterConverter

# Initialize with desired chunk size (in MB)
splitter = CSVSplitterConverter(chunk_size_mb=250)

# Split and convert
created_files = splitter.split_and_convert(
    'large_file.csv',
    'output_directory'
)
```

## Common Use Cases

1. **Converting Small to Medium CSV Files**
   ```bash
   python csv_to_parquet.py data.csv
   ```

2. **Inspecting Converted Files**
   ```bash
   python parquet_viewer.py output.parquet
   ```

3. **Handling Large CSV Files**
   ```bash
   python csv_splitter_converter.py large_file.csv chunks --chunk-size 100
   ```

## Error Handling

### Common Issues and Solutions

1. **Memory Errors**
   - Use csv_splitter_converter.py for large files
   - Reduce chunk size using --chunk-size parameter

2. **Encoding Issues**
   - The tools use UTF-8 encoding by default
   - Bad lines are skipped and logged

3. **Data Type Issues**
   - Parquet viewer will show schema information
   - Check data types using --show-schema option

## Best Practices

1. **For Large Files**
   - Start with a larger chunk size and adjust down if needed
   - Monitor system memory during conversion
   - Use progress indicators to track conversion status

2. **For Data Validation**
   - Use parquet_viewer to inspect converted files
   - Compare row counts between source and destination
   - Verify data types are preserved correctly

## Contributing

Feel free to submit issues and enhancement requests!

## License

This project is licensed under the Apache License - see the LICENSE file for details.

## Support

For support and questions, please open an issue in the repository.
