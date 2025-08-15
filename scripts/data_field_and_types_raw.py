import pandas as pd
import os
import json
from pathlib import Path
from typing import Dict, Any

def analyze_csv_files(data_directory: str) -> Dict[str, Dict[str, Any]]:
    """
    Analyze all CSV files in the specified directory and extract headers with their data types.
    
    Args:
        data_directory (str): Path to the directory containing CSV files
        
    Returns:
        Dict[str, Dict[str, Any]]: Dictionary with filename as key and column info as value
    """
    results = {}
    
    # Get all CSV files in the directory
    csv_files = [f for f in os.listdir(data_directory) if f.endswith('.csv')]
    
    print(f"Found {len(csv_files)} CSV files in {data_directory}")
    print("-" * 60)
    
    for csv_file in csv_files:
        file_path = os.path.join(data_directory, csv_file)
        print(f"\nAnalyzing: {csv_file}")
        
        try:
            # Read the CSV file
            df = pd.read_csv(file_path)
            
            # Get basic info about the dataset
            file_info = {
                'file_name': csv_file,
                'total_rows': len(df),
                'total_columns': len(df.columns),
                'columns': {}
            }
            
            # Analyze each column
            for column in df.columns:
                column_info = {
                    'dtype': str(df[column].dtype),
                    'non_null_count': df[column].count(),
                    'null_count': df[column].isnull().sum(),
                    'unique_count': df[column].nunique(),
                    'sample_values': df[column].dropna().head(3).tolist() if not df[column].empty else []
                }
                
                # Try to infer better data type
                if df[column].dtype == 'object':
                    # Check if it's datetime
                    try:
                        pd.to_datetime(df[column].dropna().head(100), errors='raise')
                        column_info['suggested_type'] = 'datetime'
                    except:
                        # Check if it's numeric but stored as string
                        try:
                            pd.to_numeric(df[column].dropna().head(100), errors='raise')
                            column_info['suggested_type'] = 'numeric'
                        except:
                            column_info['suggested_type'] = 'string'
                elif df[column].dtype in ['int64', 'float64']:
                    column_info['suggested_type'] = 'numeric'
                else:
                    column_info['suggested_type'] = str(df[column].dtype)
                
                file_info['columns'][column] = column_info
            
            results[csv_file] = file_info
            
            # Print summary for this file
            print(f"  - Rows: {file_info['total_rows']:,}")
            print(f"  - Columns: {file_info['total_columns']}")
            print(f"  - Headers: {list(df.columns)}")
            
        except Exception as e:
            print(f"  Error reading {csv_file}: {str(e)}")
            results[csv_file] = {'error': str(e)}
    
    return results

def print_detailed_analysis(results: Dict[str, Dict[str, Any]]):
    """
    Print detailed analysis of all CSV files.
    
    Args:
        results (Dict[str, Dict[str, Any]]): Results from analyze_csv_files function
    """
    print("\n" + "="*80)
    print("DETAILED ANALYSIS OF ALL CSV FILES")
    print("="*80)
    
    for filename, file_info in results.items():
        if 'error' in file_info:
            print(f"\n❌ {filename}: {file_info['error']}")
            continue
            
        print(f"\n📄 {filename}")
        print(f"   Total Rows: {file_info['total_rows']:,}")
        print(f"   Total Columns: {file_info['total_columns']}")
        print(f"   Columns and Data Types:")
        
        for col_name, col_info in file_info['columns'].items():
            null_percentage = (col_info['null_count'] / file_info['total_rows']) * 100
            print(f"     • {col_name}")
            print(f"       - Current Type: {col_info['dtype']}")
            print(f"       - Suggested Type: {col_info['suggested_type']}")
            print(f"       - Non-null: {col_info['non_null_count']:,} ({100-null_percentage:.1f}%)")
            print(f"       - Unique Values: {col_info['unique_count']:,}")
            if col_info['sample_values']:
                print(f"       - Sample Values: {col_info['sample_values']}")

def save_results_to_json(results: Dict[str, Dict[str, Any]], output_file: str):
    """
    Save the analysis results to a JSON file.
    
    Args:
        results (Dict[str, Dict[str, Any]]): Results from analyze_csv_files function
        output_file (str): Path to save the JSON file
    """
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False, default=str)
    print(f"\n💾 Results saved to: {output_file}")

def create_summary_table(results: Dict[str, Dict[str, Any]]):
    """
    Create a summary table of all files and their column counts.
    
    Args:
        results (Dict[str, Dict[str, Any]]): Results from analyze_csv_files function
    """
    print("\n" + "="*80)
    print("SUMMARY TABLE")
    print("="*80)
    print(f"{'File Name':<35} {'Rows':<12} {'Columns':<10} {'Status'}")
    print("-" * 80)
    
    total_files = 0
    total_columns = 0
    successful_files = 0
    
    for filename, file_info in results.items():
        total_files += 1
        if 'error' in file_info:
            print(f"{filename:<35} {'ERROR':<12} {'N/A':<10} ❌")
        else:
            rows = f"{file_info['total_rows']:,}"
            cols = file_info['total_columns']
            total_columns += cols
            successful_files += 1
            print(f"{filename:<35} {rows:<12} {cols:<10} ✅")
    
    print("-" * 80)
    print(f"Total Files: {total_files}")
    print(f"Successful: {successful_files}")
    print(f"Total Unique Columns: {total_columns}")

if __name__ == "__main__":
    # Configuration
    DATA_DIRECTORY = r"F:\olist-ecommerce-pipeline\data\raw"
    OUTPUT_JSON = r"F:\olist-ecommerce-pipeline\data\processed\csv_analysis_results.json"
    
    print("🔍 Starting CSV Analysis...")
    print(f"📁 Data Directory: {DATA_DIRECTORY}")
    
    # Check if directory exists
    if not os.path.exists(DATA_DIRECTORY):
        print(f"❌ Error: Directory {DATA_DIRECTORY} does not exist!")
        exit(1)
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(OUTPUT_JSON), exist_ok=True)
    
    # Analyze all CSV files
    results = analyze_csv_files(DATA_DIRECTORY)
    
    # Print detailed analysis
    print_detailed_analysis(results)
    
    # Create summary table
    create_summary_table(results)
    
    # Save results to JSON
    save_results_to_json(results, OUTPUT_JSON)
    
    print(f"\n🎉 Analysis complete! Check {OUTPUT_JSON} for detailed results.")