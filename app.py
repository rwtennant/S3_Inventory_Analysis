from flask import Flask, render_template, request, jsonify, send_file
from s3_inventory_search import search_inventory
from s3_inventory_utils import get_latest_inventory_manifests
from s3_utils import get_s3_client
import pandas as pd
import os
import json
from datetime import datetime
import boto3
import traceback
import logging
from dotenv import load_dotenv
from s3_path_size import get_path_size
import io

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# File paths for storing history
BUCKET_HISTORY_FILE = 'bucket_history.json'
MANIFEST_CACHE_FILE = 'manifest_cache.json'

def load_bucket_history():
    """Load the list of previously used buckets."""
    if os.path.exists(BUCKET_HISTORY_FILE):
        with open(BUCKET_HISTORY_FILE, 'r') as f:
            return json.load(f)
    return []

def save_bucket_history(buckets):
    """Save the list of previously used buckets."""
    with open(BUCKET_HISTORY_FILE, 'w') as f:
        json.dump(buckets, f)

def load_manifest_cache():
    """Load the manifest cache."""
    if os.path.exists(MANIFEST_CACHE_FILE):
        with open(MANIFEST_CACHE_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_manifest_cache(cache):
    """Save the manifest cache."""
    with open(MANIFEST_CACHE_FILE, 'w') as f:
        json.dump(cache, f, indent=4)

def clear_manifest_cache():
    """Clear the manifest cache."""
    if os.path.exists(MANIFEST_CACHE_FILE):
        os.remove(MANIFEST_CACHE_FILE)
        logger.info("Manifest cache cleared")

@app.route('/')
def index():
    """Render the main page."""
    return render_template('index.html')

@app.route('/api/buckets', methods=['GET'])
def get_buckets():
    """Get the list of previously used buckets."""
    return jsonify(load_bucket_history())

@app.route('/api/buckets', methods=['POST'])
def add_bucket():
    """Add a new bucket to the history."""
    data = request.get_json()
    bucket_names = data.get('bucket_names', [])
    
    # Load existing history
    buckets = load_bucket_history()
    
    # Add new buckets if they don't exist
    for bucket in bucket_names:
        if bucket not in buckets:
            buckets.append(bucket)
    
    # Save updated history
    save_bucket_history(buckets)
    
    return jsonify(buckets)

@app.route('/api/buckets/<bucket_name>', methods=['DELETE'])
def delete_bucket(bucket_name):
    """Remove a bucket from the history."""
    try:
        # Load existing history
        buckets = load_bucket_history()
        
        # Remove the bucket if it exists
        if bucket_name in buckets:
            buckets.remove(bucket_name)
            # Save updated history
            save_bucket_history(buckets)
            return jsonify({'message': f'Bucket {bucket_name} removed successfully'})
        else:
            return jsonify({'error': f'Bucket {bucket_name} not found in history'}), 404
            
    except Exception as e:
        logger.error(f"Error removing bucket {bucket_name}: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/manifests', methods=['POST'])
def get_manifests():
    """Get the latest manifests for the specified buckets."""
    data = request.get_json()
    bucket_names = data.get('bucket_names', [])
    
    if not bucket_names:
        return jsonify({'error': 'No bucket names provided'}), 400
    
    try:
        logger.info(f"Fetching manifests for {len(bucket_names)} buckets")
        
        # Initialize S3 client using environment variables
        try:
            s3_client = get_s3_client()
            logger.debug("S3 client initialized successfully")
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Failed to initialize S3 client: {error_msg}")
            if "Missing required AWS credentials" in error_msg:
                return jsonify({'error': 'AWS credentials are missing. Please check your .env file.'}), 401
            elif "Invalid AWS credentials" in error_msg:
                return jsonify({'error': 'Invalid AWS credentials. Please check your AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.'}), 401
            elif "AWS session token has expired" in error_msg:
                return jsonify({'error': 'AWS session token has expired. Please refresh your credentials.'}), 401
            elif "Access denied" in error_msg:
                return jsonify({'error': 'Access denied. Please check if your AWS credentials have the necessary permissions.'}), 403
            else:
                return jsonify({'error': f'Failed to initialize S3 client: {error_msg}'}), 500
        
        # Load existing cache
        manifest_cache = load_manifest_cache()
        logger.debug(f"Loaded manifest cache with {len(manifest_cache)} buckets")
        
        # Get latest manifests using the provided client
        latest_manifests = get_latest_inventory_manifests(bucket_names, s3_client)
        
        if not latest_manifests:
            logger.warning("No manifests found for any buckets")
            return jsonify({'error': 'No manifests found. Please check if the buckets have S3 inventory configured.'}), 404
        
        # Update cache with new manifests
        current_time = datetime.now().isoformat()
        for dest_bucket in bucket_names:  # Use selected bucket as destination bucket
            # Create a new bucket object if it doesn't exist
            if dest_bucket not in manifest_cache:
                manifest_cache[dest_bucket] = {}
            
            # Get manifests for this destination bucket
            for source_bucket, manifests in latest_manifests.items():
                for manifest in manifests:
                    # Add the manifest to the destination bucket's account
                    manifest_data = {
                        'key': manifest['key'],
                        'added_date': current_time,
                        'last_used': current_time
                    }
                    manifest_cache[dest_bucket][source_bucket] = manifest_data
                    logger.debug(f"Added manifest to cache: {manifest['key']}")  # Simplified logging
        
        # Save updated cache
        save_manifest_cache(manifest_cache)
        logger.debug("Updated manifest cache")
        
        # Convert the cache format to match the expected response format
        response_data = {}
        for bucket in bucket_names:
            if bucket in manifest_cache:
                response_data[bucket] = [
                    {
                        'key': manifest['key'],
                        'added_date': manifest['added_date'].replace('T', ' ').split('.')[0],
                        'source_bucket': source_bucket  # Add source bucket information
                    }
                    for source_bucket, manifest in manifest_cache[bucket].items()
                ]
                logger.debug(f"Added {len(response_data[bucket])} manifests to response for bucket {bucket}")
            else:
                response_data[bucket] = []
                logger.debug(f"No manifests found for bucket {bucket}")
        
        logger.info(f"Successfully retrieved manifests for {len(response_data)} buckets")
        return jsonify(response_data)
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Error fetching manifests: {error_msg}")
        return jsonify({'error': f'An unexpected error occurred: {error_msg}'}), 500

@app.route('/api/manifests/cached', methods=['GET'])
def get_cached_manifests():
    """Get cached manifests for specified buckets."""
    bucket_names = request.args.getlist('bucket')
    if not bucket_names:
        return jsonify({'error': 'No bucket names provided'}), 400
    
    try:
        manifest_cache = load_manifest_cache()
        result = {}
        
        for bucket in bucket_names:
            if bucket in manifest_cache:
                result[bucket] = [
                    {
                        'key': manifest['key'],
                        'added_date': manifest['added_date'].replace('T', ' ').split('.')[0],
                        'source_bucket': source_bucket  # Add source bucket information
                    }
                    for source_bucket, manifest in manifest_cache[bucket].items()
                ]
                logger.info(f"Found {len(result[bucket])} manifests for bucket {bucket}")
            else:
                result[bucket] = []
                logger.info(f"No manifests found for bucket {bucket}")
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error getting cached manifests: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/search', methods=['POST'])
def search():
    """Search for objects in the specified manifests."""
    data = request.get_json()
    bucket_name = data.get('bucket_name')
    manifest_keys = data.get('manifest_keys', [])
    search_string = data.get('search_string')
    
    if not all([bucket_name, manifest_keys, search_string]):
        return jsonify({'error': 'Missing required parameters'}), 400
    
    try:
        # Initialize S3 client using environment variables
        s3_client = get_s3_client()
        
        # Update last used timestamp for manifests
        manifest_cache = load_manifest_cache()
        if bucket_name in manifest_cache:
            for manifest in manifest_keys:
                if manifest in manifest_cache[bucket_name]:
                    manifest_cache[bucket_name][manifest]['last_used'] = datetime.now().isoformat()
            save_manifest_cache(manifest_cache)
        
        try:
            results = search_inventory(bucket_name, manifest_keys, search_string, s3_client)
            logger.debug(f"Search returned results of type: {results.get('type')}")
            logger.debug(f"Search results structure: {json.dumps(results, indent=2)}")
            
            # Results are already in the correct format, just return them
            return jsonify(results)
        except Exception as e:
            logger.error(f"Search failed: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500
    except Exception as e:
        error_msg = str(e)
        if "Missing required AWS credentials" in error_msg:
            return jsonify({'error': 'AWS credentials are missing. Please check your .env file.'}), 401
        elif "Invalid AWS credentials" in error_msg:
            return jsonify({'error': 'Invalid AWS credentials. Please check your AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.'}), 401
        elif "AWS session token has expired" in error_msg:
            return jsonify({'error': 'AWS session token has expired. Please refresh your credentials.'}), 401
        elif "Access denied" in error_msg:
            return jsonify({'error': 'Access denied. Please check if your AWS credentials have the necessary permissions.'}), 403
        else:
            logger.error(f"Error searching inventory: {error_msg}")
            return jsonify({'error': error_msg}), 500

@app.route('/api/path-size', methods=['POST'])
def calculate_path_size():
    try:
        data = request.get_json()
        bucket_name = data.get('bucket_name')
        manifest_keys = data.get('manifest_keys')
        path_depth = data.get('path_depth')

        if not all([bucket_name, manifest_keys, path_depth]):
            return jsonify({'error': 'Missing required parameters'}), 400

        if not isinstance(path_depth, int) or path_depth < 1:
            return jsonify({'error': 'Path depth must be a positive integer'}), 400

        results = get_path_size(bucket_name, manifest_keys, path_depth)
        
        # Calculate totals
        total_size = sum(result['total_size'] for result in results)
        total_paths = len(results)

        return jsonify({
            'results': results,
            'total_size': total_size,
            'total_paths': total_paths
        })

    except Exception as e:
        logger.error(f"Error calculating path size: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/manifests/clear-cache', methods=['POST'])
def clear_cache():
    """Clear the manifest cache."""
    try:
        clear_manifest_cache()
        return jsonify({'message': 'Manifest cache cleared successfully'})
    except Exception as e:
        logger.error(f"Error clearing manifest cache: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/download-csv', methods=['POST'])
def download_csv():
    """Download search results as a CSV file."""
    data = request.get_json()
    bucket_name = data.get('bucket_name')
    manifest_keys = data.get('manifest_keys', [])
    search_string = data.get('search_string')
    operation_type = data.get('operation_type')
    path_depth = data.get('path_depth')
    
    if not all([bucket_name, manifest_keys]):
        return jsonify({'error': 'Missing required parameters'}), 400
    
    try:
        # Initialize S3 client using environment variables
        s3_client = get_s3_client()
        
        # Get results based on operation type
        if operation_type == 'search':
            if not search_string:
                return jsonify({'error': 'Search string is required for search operation'}), 400
            results = search_inventory(bucket_name, manifest_keys, search_string, s3_client)
            
            # Convert results to DataFrame
            if results['type'] == 'folders':
                df = pd.DataFrame(results['results'])
                # Rename columns for better readability
                df = df.rename(columns={
                    'Folder_Path': 'Path',
                    'Total_Size': 'Size',
                    'File_Count': 'Object Count'
                })
            else:
                df = pd.DataFrame(results['results'])
                # Rename columns for better readability
                df = df.rename(columns={
                    'Key': 'Path',
                    'Size': 'Size',
                    'LastModifiedDate': 'Last Modified'
                })
        else:
            # For path size calculation
            if not path_depth:
                return jsonify({'error': 'Path depth is required for path size calculation'}), 400
            results = get_path_size(bucket_name, manifest_keys, path_depth)
            df = pd.DataFrame(results)
            # Rename columns for better readability
            df = df.rename(columns={
                'path': 'Path',
                'total_size': 'Size',
                'object_count': 'Object Count',
                'is_folder': 'Is Folder'
            })
        
        # Create CSV in memory
        output = io.StringIO()
        df.to_csv(output, index=False)
        output.seek(0)
        
        # Generate filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"s3_inventory_results_{timestamp}.csv"
        
        return send_file(
            io.BytesIO(output.getvalue().encode('utf-8')),
            mimetype='text/csv',
            as_attachment=True,
            download_name=filename
        )
        
    except Exception as e:
        error_msg = str(e)
        if "Missing required AWS credentials" in error_msg:
            return jsonify({'error': 'AWS credentials are missing. Please check your .env file.'}), 401
        elif "Invalid AWS credentials" in error_msg:
            return jsonify({'error': 'Invalid AWS credentials. Please check your AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.'}), 401
        elif "AWS session token has expired" in error_msg:
            return jsonify({'error': 'AWS session token has expired. Please refresh your credentials.'}), 401
        elif "Access denied" in error_msg:
            return jsonify({'error': 'Access denied. Please check if your AWS credentials have the necessary permissions.'}), 403
        else:
            logger.error(f"Error downloading CSV: {error_msg}")
            return jsonify({'error': error_msg}), 500

if __name__ == '__main__':
    app.run(debug=True) 