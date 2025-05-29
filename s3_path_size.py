import json
import gzip
import pandas as pd
from typing import List, Dict, Any
from s3_utils import get_s3_client
import logging

logger = logging.getLogger(__name__)

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
        Each dictionary has the following keys:
        - path: The path at the specified depth
        - total_size: Total size of all objects under this path
        - object_count: Number of objects under this path
        - is_folder: Boolean indicating if this is a folder (has children)
    """
    if s3_client is None:
        s3_client = get_s3_client()
    
    try:
        results = []
        for manifest_key in manifest_keys:
            # Get the manifest file
            response = s3_client.get_object(Bucket=bucket_name, Key=manifest_key)
            manifest = json.loads(response['Body'].read().decode('utf-8'))
            
            # Process each inventory file
            for file_info in manifest['files']:
                # Get the inventory file
                file_obj = s3_client.get_object(Bucket=bucket_name, Key=file_info['key'])
                
                # Read and process the inventory file
                with gzip.open(file_obj['Body'], mode='rt') as buffer:
                    # Read CSV without header to handle unnamed columns
                    df = pd.read_csv(buffer, header=None)
                    
                    # Based on the error message, we can see the actual column structure:
                    # Column 0: Bucket name
                    # Column 1: Key (object path)
                    # Column 2: Size
                    # Column 3: LastModifiedDate
                    # Column 4: StorageClass
                    # Column 5: Unnamed column
                    
                    # Rename columns based on their position
                    df.columns = ['Bucket', 'Key', 'Size', 'LastModifiedDate', 'StorageClass', 'Unnamed']
                    
                    # Convert Size column to numeric
                    df['Size'] = pd.to_numeric(df['Size'], errors='coerce')
                    
                    # Split the path and get the path at the specified depth
                    df['PathParts'] = df['Key'].str.split('/')
                    df['PathAtDepth'] = df['PathParts'].apply(
                        lambda parts: '/'.join(parts[:path_depth]) if len(parts) >= path_depth else None
                    )
                    
                    # Group by the path at the specified depth
                    grouped = df.groupby('PathAtDepth').agg({
                        'Size': ['sum', 'count']
                    }).reset_index()
                    
                    # Rename columns for clarity
                    grouped.columns = ['path', 'total_size', 'object_count']
                    
                    # Add is_folder flag (True if there are objects deeper than this path)
                    df['IsDeeper'] = df['PathParts'].apply(lambda parts: len(parts) > path_depth)
                    has_children = df.groupby('PathAtDepth')['IsDeeper'].any()
                    grouped['is_folder'] = grouped['path'].map(has_children)
                    
                    # Convert to list of dictionaries
                    for _, row in grouped.iterrows():
                        results.append({
                            'path': row['path'],
                            'total_size': int(row['total_size']),
                            'object_count': int(row['object_count']),
                            'is_folder': bool(row['is_folder'])
                        })
        
        # Sort results by path
        results.sort(key=lambda x: x['path'])
        
        return results
        
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