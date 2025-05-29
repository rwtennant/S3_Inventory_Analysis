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

def search_inventory(bucket_name, manifest_key, search_string, s3_client=None):
    """Search for objects in the specified manifest."""
    if s3_client is None:
        from s3_utils import get_s3_client
        s3_client = get_s3_client()
    
    try:
        # Get the manifest file
        response = s3_client.get_object(Bucket=bucket_name, Key=manifest_key)
        manifest = json.loads(response['Body'].read().decode('utf-8'))
        
        # Get the inventory files
        inventory_files = []
        for file in manifest['files']:
            response = s3_client.get_object(Bucket=bucket_name, Key=file['key'])
            inventory_files.append(pd.read_csv(response['Body']))
        
        # Combine all inventory files
        inventory_df = pd.concat(inventory_files, ignore_index=True)
        
        # Search for objects matching the search string
        if search_string:
            mask = inventory_df['Key'].str.contains(search_string, case=False, na=False)
            results = inventory_df[mask]
        else:
            results = inventory_df
        
        return results
        
    except Exception as e:
        print(f"Error searching inventory: {str(e)}")
        raise

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
            search_inventory(args.bucket_name, args.manifest_key, args.search_string)
    except Exception as e:
        console.print(Panel(f"[red]Error: {e}[/red]", title="Error"))
        sys.exit(1) 