import json
import gzip
import pandas as pd
from typing import List, Dict, Any
from s3_utils import get_s3_client
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
import io

logger = logging.getLogger(__name__)

def process_inventory_chunk(chunk_df: pd.DataFrame, path_depth: int) -> pd.DataFrame:
    """Process a chunk of inventory data."""
    # Split the path and get the path at the specified depth
    chunk_df['PathParts'] = chunk_df['Key'].str.split('/')
    chunk_df['PathAtDepth'] = chunk_df['PathParts'].apply(
        lambda parts: '/'.join(parts[:path_depth]) if len(parts) >= path_depth else None
    )
    
    # Group by the path at the specified depth
    grouped = chunk_df.groupby('PathAtDepth').agg({
        'Size': ['sum', 'count']
    }).reset_index()
    
    # Rename columns for clarity
    grouped.columns = ['path', 'total_size', 'object_count']
    
    # Add is_folder flag
    chunk_df['IsDeeper'] = chunk_df['PathParts'].apply(lambda parts: len(parts) > path_depth)
    has_children = chunk_df.groupby('PathAtDepth')['IsDeeper'].any()
    grouped['is_folder'] = grouped['path'].map(has_children)
    
    return grouped

def process_inventory_file(file_info: Dict[str, Any], bucket_name: str, path_depth: int, s3_client) -> List[Dict[str, Any]]:
    """Process a single inventory file."""
    try:
        # Get the inventory file
        file_obj = s3_client.get_object(Bucket=bucket_name, Key=file_info['key'])
        
        # Read and process the inventory file in chunks
        results = []
        chunk_size = 100000  # Process 100k rows at a time
        
        with gzip.open(file_obj['Body'], mode='rt') as buffer:
            # Read CSV without header to handle unnamed columns
            for chunk in pd.read_csv(buffer, header=None, chunksize=chunk_size):
                # Get the actual number of columns
                num_columns = len(chunk.columns)
                
                # Define base columns that we know exist in S3 inventory
                base_columns = ['Bucket', 'Key', 'Size', 'LastModifiedDate', 'StorageClass']
                
                # Create column names based on actual number of columns
                if num_columns == len(base_columns):
                    chunk.columns = base_columns
                elif num_columns > len(base_columns):
                    additional_columns = [f'Unnamed_{i}' for i in range(len(base_columns), num_columns)]
                    chunk.columns = base_columns + additional_columns
                else:
                    chunk.columns = base_columns[:num_columns]
                
                # Convert Size column to numeric and fill NaN with 0
                if 'Size' in chunk.columns:
                    chunk['Size'] = pd.to_numeric(chunk['Size'], errors='coerce').fillna(0)
                else:
                    chunk['Size'] = 0
                
                # Ensure Key column exists
                if 'Key' not in chunk.columns:
                    continue
                
                # Process the chunk
                grouped = process_inventory_chunk(chunk, path_depth)
                
                # Convert to list of dictionaries
                for _, row in grouped.iterrows():
                    results.append({
                        'bucket': bucket_name,
                        'source': file_info.get('source', ''),
                        'path': row['path'],
                        'total_size': int(row['total_size']),
                        'object_count': int(row['object_count']),
                        'is_folder': bool(row['is_folder'])
                    })
        
        return results
    except Exception as e:
        logger.error(f"Error processing inventory file {file_info['key']}: {str(e)}")
        return []

def get_path_size(bucket_name: str, manifest_keys: List[str], path_depth: int, s3_client=None) -> List[Dict[str, Any]]:
    """
    Calculate sizes for objects and folders at a specific path depth in the S3 bucket.
    
    Args:
        bucket_name (str): Name of the S3 bucket
        manifest_keys (List[str]): List of manifest keys to process
        path_depth (int): The depth in the S3 path to analyze (1-based)
        s3_client: Optional boto3 S3 client. If not provided, one will be created.
    
    Returns:
        List[Dict[str, Any]]: List of dictionaries containing path, size, and object count
    """
    if s3_client is None:
        s3_client = get_s3_client()
    
    try:
        all_results = []
        
        # Process each manifest file
        for manifest_key in manifest_keys:
            # Extract source from manifest path
            source = manifest_key.split('/')[0] if manifest_key else ''
            
            # Get the manifest file
            response = s3_client.get_object(Bucket=bucket_name, Key=manifest_key)
            manifest = json.loads(response['Body'].read().decode('utf-8'))
            
            # Add source information to each file info
            for file_info in manifest['files']:
                file_info['source'] = source
            
            # Process inventory files in parallel
            with ThreadPoolExecutor(max_workers=10) as executor:
                # Create a partial function with the common arguments
                process_func = partial(process_inventory_file, 
                                     bucket_name=bucket_name,
                                     path_depth=path_depth,
                                     s3_client=s3_client)
                
                # Submit all files for processing
                future_to_file = {
                    executor.submit(process_func, file_info): file_info 
                    for file_info in manifest['files']
                }
                
                # Collect results as they complete
                for future in as_completed(future_to_file):
                    file_info = future_to_file[future]
                    try:
                        results = future.result()
                        all_results.extend(results)
                    except Exception as e:
                        logger.error(f"Error processing {file_info['key']}: {str(e)}")
        
        # Combine results for the same path
        if all_results:
            # Convert to DataFrame for efficient grouping
            results_df = pd.DataFrame(all_results)
            grouped = results_df.groupby(['path', 'source']).agg({
                'total_size': 'sum',
                'object_count': 'sum',
                'is_folder': 'any'
            }).reset_index()
            
            # Convert back to list of dictionaries
            final_results = []
            for _, row in grouped.iterrows():
                final_results.append({
                    'bucket': bucket_name,
                    'source': row['source'],
                    'path': row['path'],
                    'total_size': int(row['total_size']),
                    'object_count': int(row['object_count']),
                    'is_folder': bool(row['is_folder'])
                })
            
            # Sort results by path
            final_results.sort(key=lambda x: x['path'])
            return final_results
        
        return []
        
    except Exception as e:
        logger.error(f"Error calculating path sizes: {str(e)}")
        raise

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Calculate sizes for objects and folders at a specific path depth in S3')
    parser.add_argument('bucket_name', help='Name of the S3 bucket')
    parser.add_argument('manifest_keys', nargs='+', help='One or more manifest keys to process')
    parser.add_argument('--depth', type=int, required=True, help='Path depth to analyze (1-based)')
    
    args = parser.parse_args()
    
    try:
        results = get_path_size(args.bucket_name, args.manifest_keys, args.depth)
        
        # Print results in a formatted table
        print(f"\nResults for path depth {args.depth}:")
        print("-" * 80)
        print(f"{'Path':<50} {'Size':>12} {'Objects':>8} {'Type':>8}")
        print("-" * 80)
        
        for result in results:
            size_gb = result['total_size'] / (1024**3)  # Convert to GB
            print(f"{result['path']:<50} {size_gb:>11.2f}GB {result['object_count']:>8} {'Folder' if result['is_folder'] else 'Object':>8}")
            
    except Exception as e:
        print(f"Error: {str(e)}")
        exit(1) 