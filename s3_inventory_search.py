import json
import gzip
import pandas as pd
import argparse
from s3_utils import get_s3_client
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any
import io
from s3_inventory_utils import get_latest_inventory_manifests
from tqdm import tqdm
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.panel import Panel
from rich import print as rprint
import sys

# Initialize Rich console
console = Console()

def format_size(size_bytes: int) -> str:
    """Format size in bytes to human readable format."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"

def is_folder_match(key: str, search_string: str) -> bool:
    """
    Check if the search string matches a folder name in the path.
    
    :param key: S3 object key
    :param search_string: String to search for
    :return: True if search string matches a folder name
    """
    # Split the path into components
    parts = key.split('/')
    # Check each part except the last one (which is the file name)
    return any(search_string.lower() in part.lower() for part in parts[:-1])

def get_folder_path(key: str, search_string: str) -> str:
    """
    Get the folder path that contains the search string.
    
    :param key: S3 object key
    :param search_string: String to search for
    :return: Folder path containing the search string
    """
    parts = key.split('/')
    for i, part in enumerate(parts):
        if search_string.lower() in part.lower():
            # Return the path up to and including the matching folder
            return '/'.join(parts[:i+1])
    return key

def process_inventory_file(s3_client, bucket_name: str, file_info: Dict[str, Any], 
                         search_string: str, columns: List[str], dtypes: Dict[str, str],
                         progress: Progress, task_id: int) -> pd.DataFrame:
    """
    Process a single inventory file and return matching objects.
    """
    try:
        # Get the file
        file_obj = s3_client.get_object(Bucket=bucket_name, Key=file_info["key"])
        
        # Process the file in chunks
        chunk_size = 100000
        matching_chunks = []
        
        with gzip.open(file_obj["Body"], mode='rt') as buffer:
            for chunk in pd.read_csv(buffer, 
                                   dtype=dtypes, 
                                   low_memory=False,
                                   chunksize=chunk_size):
                chunk.columns = columns
                
                # Check for folder matches
                folder_matches = chunk[chunk['Key'].apply(lambda x: is_folder_match(x, search_string))]
                if not folder_matches.empty:
                    folder_matches.loc[:, 'Folder_Path'] = folder_matches['Key'].apply(
                        lambda x: get_folder_path(x, search_string)
                    )
                    matching_chunks.append(folder_matches)
                
                # Also check for direct matches in case there are no folder matches
                direct_matches = chunk[chunk['Key'].str.contains(search_string, case=False, na=False)]
                if not direct_matches.empty and folder_matches.empty:
                    matching_chunks.append(direct_matches)
                
                # Update progress
                progress.update(task_id, advance=len(chunk))
        
        if matching_chunks:
            return pd.concat(matching_chunks)
        return pd.DataFrame()
        
    except Exception as e:
        console.print(f"[red]Error processing file {file_info['key']}: {e}[/red]")
        return pd.DataFrame()

def display_results(folder_summary: pd.DataFrame, is_folder_match: bool):
    """Display results in a formatted table."""
    if is_folder_match:
        table = Table(title="Folder Search Results", show_header=True, header_style="bold magenta")
        table.add_column("Folder Path", style="cyan")
        table.add_column("Total Size", justify="right", style="green")
        table.add_column("File Count", justify="right", style="blue")
        
        for _, row in folder_summary.iterrows():
            table.add_row(
                row['Folder_Path'],
                format_size(row['Total_Size']),
                str(row['File_Count'])
            )
    else:
        table = Table(title="Object Search Results", show_header=True, header_style="bold magenta")
        table.add_column("Object Key", style="cyan")
        table.add_column("Size", justify="right", style="green")
        table.add_column("Last Modified", style="blue")
        
        for _, row in folder_summary.iterrows():
            table.add_row(
                row['Key'],
                format_size(row['Size']),
                row['LastModifiedDate']
            )
    
    console.print(table)

def search_inventory(bucket_name: str, manifest_keys: List[str], search_string: str, s3_client=None) -> Dict[str, Any]:
    """
    Search through S3 inventory files for objects matching the search string.
    
    Args:
        bucket_name (str): Name of the S3 bucket
        manifest_keys (List[str]): List of manifest keys to process
        search_string (str): String to search for in object keys
        s3_client: Optional boto3 S3 client. If not provided, one will be created.
    
    Returns:
        Dict[str, Any]: Dictionary containing search results and metadata
    """
    if s3_client is None:
        s3_client = get_s3_client()
    
    try:
        all_matches = []
        print(f"Processing {len(manifest_keys)} manifest keys: {manifest_keys}")  # Debug log
        
        for manifest_key in manifest_keys:
            if not manifest_key or not isinstance(manifest_key, str):
                print(f"Skipping invalid manifest key: {manifest_key}")  # Debug log
                continue
                
            # Extract source from manifest path (e.g., "inventory/2024-03-21/manifest.json" -> "inventory")
            source = manifest_key.split('/')[0] if manifest_key else ''
            print(f"Processing manifest: {manifest_key}, source: {source}")  # Debug log
            
            try:
                # Get the manifest file
                print(f"Fetching manifest from bucket {bucket_name}, key {manifest_key}")  # Debug log
                response = s3_client.get_object(Bucket=bucket_name, Key=manifest_key)
                manifest = json.loads(response['Body'].read().decode('utf-8'))
                print(f"Found manifest with {len(manifest['files'])} files")
                
                # Process each inventory file
                for file_info in manifest['files']:
                    try:
                        inventory_key = file_info['key']
                        print(f"Processing inventory file: {inventory_key}")  # Debug log
                        
                        # Get the inventory file
                        file_obj = s3_client.get_object(Bucket=bucket_name, Key=inventory_key)
                        
                        # Read and process the inventory file
                        with gzip.open(file_obj['Body'], mode='rt') as buffer:
                            # Read CSV without header to handle unnamed columns
                            df = pd.read_csv(buffer, header=None)
                            
                            # Get the actual number of columns
                            num_columns = len(df.columns)
                            print(f"Found {num_columns} columns in inventory file")  # Debug log
                            
                            # Define base columns that we know exist in S3 inventory
                            base_columns = ['Bucket', 'Key', 'Size', 'LastModifiedDate', 'StorageClass']
                            
                            # Create column names based on actual number of columns
                            if num_columns == len(base_columns):
                                df.columns = base_columns
                            elif num_columns > len(base_columns):
                                # If we have more columns than expected, add them as Unnamed_X
                                additional_columns = [f'Unnamed_{i}' for i in range(len(base_columns), num_columns)]
                                df.columns = base_columns + additional_columns
                            else:
                                # If we have fewer columns than expected, only use the columns we have
                                df.columns = base_columns[:num_columns]
                            
                            # Convert Size column to numeric and fill NaN with 0
                            if 'Size' in df.columns:
                                df['Size'] = pd.to_numeric(df['Size'], errors='coerce').fillna(0)
                            else:
                                # If Size column is not present, add it with zeros
                                df['Size'] = 0
                            
                            # Search for matches
                            if 'Key' in df.columns:
                                # First, find all objects that contain the search string
                                print(f"Searching for '{search_string}' in {len(df)} objects")  # Debug log
                                matches = df[df['Key'].str.contains(search_string, case=False, na=False)].copy()
                                print(f"Found {len(matches)} initial matches")  # Debug log
                                
                                if not matches.empty:
                                    # Add source information before processing
                                    matches['Source'] = source
                                    
                                    # For each matching object, find its containing folder
                                    def get_folder_path(key, search_str):
                                        parts = key.split('/')
                                        for i, part in enumerate(parts):
                                            if search_str.lower() in part.lower():
                                                # Return the path up to and including the matching folder
                                                folder_path = '/'.join(parts[:i+1])
                                                print(f"Found matching folder: {folder_path}")  # Debug log
                                                return folder_path
                                        return key

                                    matches['Folder_Path'] = matches['Key'].apply(
                                        lambda x: get_folder_path(x, search_string)
                                    )
                                    
                                    # Group by folder path and source
                                    folder_matches = matches.groupby(['Folder_Path', 'Source']).agg({
                                        'Size': ['sum', 'count']
                                    }).reset_index()
                                    
                                    # Rename columns
                                    folder_matches.columns = ['Folder_Path', 'Source', 'Total_Size', 'File_Count']
                                    
                                    # Add bucket information
                                    folder_matches['Bucket'] = bucket_name
                                    
                                    print(f"Grouped into {len(folder_matches)} unique folders")  # Debug log
                                    print("Folder matches:")  # Debug log
                                    for _, row in folder_matches.iterrows():
                                        print(f"  {row['Folder_Path']} - {row['Total_Size']} bytes, {row['File_Count']} files")  # Debug log
                                    
                                    # Convert to list of dictionaries and handle NaN values
                                    folder_results = folder_matches.to_dict('records')
                                    for result in folder_results:
                                        # Convert any remaining NaN values to None
                                        for key, value in result.items():
                                            if pd.isna(value):
                                                result[key] = None
                                    
                                    all_matches.extend(folder_results)
                                else:
                                    print(f"No matches found for '{search_string}'")  # Debug log
                            else:
                                # If Key column is not present, skip this file
                                print(f"Skipping file {inventory_key} - no Key column found")
                                continue
                            
                            print(f"Found {len(matches)} matches in {inventory_key}")  # Debug log
                            
                    except Exception as e:
                        print(f"Error processing inventory file {file_info['key']}: {str(e)}")
                        continue
            except Exception as e:
                print(f"Error processing manifest {manifest_key}: {str(e)}")
                continue
        
        if not all_matches:
            print("No matches found")  # Debug log
            return {
                'type': 'folders',
                'results': [],
                'total_folders': 0,
                'total_size': 0
            }
        
        # Convert to DataFrame for easier processing
        matches_df = pd.DataFrame(all_matches)
        print(f"Total matches found: {len(matches_df)}")
        
        # Calculate totals
        total_size = int(matches_df['Total_Size'].sum())
        total_folders = len(matches_df)
        
        print(f"Returning {total_folders} folder matches")  # Debug log
        return {
            'type': 'folders',
            'results': all_matches,
            'total_folders': total_folders,
            'total_size': total_size
        }
        
    except Exception as e:
        print(f"Search failed: {str(e)}")
        raise Exception(f"Search failed: {str(e)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Search S3 inventory for objects containing a specific string')
    parser.add_argument('bucket_name', help='Name of the S3 bucket')
    parser.add_argument('manifest_key', nargs='?', help='Key of the manifest file to process')
    parser.add_argument('search_string', nargs='?', help='String to search for in object keys')
    parser.add_argument('--workers', type=int, default=4, help='Number of parallel workers (default: 4)')
    parser.add_argument('--list-manifests', action='store_true', help='List latest inventory manifests for all accounts')
    
    args = parser.parse_args()
    
    try:
        if args.list_manifests:
            manifests = get_latest_inventory_manifests([args.bucket_name])
            for bucket, keys in manifests.items():
                console.print(Panel(f"[bold blue]Manifests for bucket {bucket}:[/bold blue]"))
                for key in keys:
                    console.print(f"  [cyan]{key}[/cyan]")
        else:
            if not args.manifest_key or not args.search_string:
                parser.error("manifest_key and search_string are required when not using --list-manifests")
            search_inventory(args.bucket_name, [args.manifest_key], args.search_string)
    except Exception as e:
        console.print(Panel(f"[red]Error: {e}[/red]", title="Error"))
        sys.exit(1) 