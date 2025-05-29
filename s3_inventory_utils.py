import pandas as pd
from typing import List, Dict
from s3_utils import get_s3_client
import re
from botocore.exceptions import ClientError
import logging

logger = logging.getLogger(__name__)

def get_latest_inventory_manifests(bucket_names, s3_client=None):
    """Find the latest S3 inventory manifest keys for given buckets."""
    if s3_client is None:
        from s3_utils import get_s3_client
        s3_client = get_s3_client()
    
    latest_manifests = {}
    
    for bucket_name in bucket_names:
        try:
            logger.info(f"Checking bucket: {bucket_name}")
            
            # List objects in the bucket, looking for manifest files
            inventory_objects = s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=''  # We'll look at all objects and filter for manifests
            )
            
            if 'Contents' not in inventory_objects:
                logger.warning(f"No objects found in bucket: {bucket_name}")
                continue
            
            # Filter for manifest files
            manifest_files = []
            for obj in inventory_objects['Contents']:
                key = obj['Key']
                if key.endswith('manifest.json'):
                    manifest_data = {
                        'key': key,
                        'last_modified': obj['LastModified'].isoformat()
                    }
                    logger.debug(f"Found manifest: {key}")  # Changed to debug level and simplified
                    manifest_files.append(manifest_data)
            
            if manifest_files:
                # Group manifests by source bucket
                manifests_by_source = {}
                for manifest in manifest_files:
                    # Extract source bucket from the path
                    # Typical path format: prefix/source-bucket/inventory-id/manifest.json
                    parts = manifest['key'].split('/')
                    if len(parts) >= 3:
                        source_bucket = parts[-3]  # Get the source bucket name from the path
                        if source_bucket not in manifests_by_source:
                            manifests_by_source[source_bucket] = []
                        manifests_by_source[source_bucket].append(manifest)
                
                # For each source bucket, find the latest manifest
                for source_bucket, manifests in manifests_by_source.items():
                    latest_manifest = max(manifests, key=lambda x: x['last_modified'])
                    logger.debug(f"Latest manifest for {source_bucket}: {latest_manifest['key']}")  # Changed to debug level and simplified
                    
                    if source_bucket not in latest_manifests:
                        latest_manifests[source_bucket] = []
                    latest_manifests[source_bucket].append(latest_manifest)
            else:
                logger.warning(f"No manifest files found in bucket: {bucket_name}")
                
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            
            if error_code == 'NoSuchBucket':
                logger.error(f"Bucket {bucket_name} does not exist. Please check the bucket name.")
                raise Exception(f"Bucket {bucket_name} does not exist. Please check the bucket name.")
            elif error_code == 'AccessDenied':
                logger.error(f"Access denied to bucket {bucket_name}. Please check your AWS credentials and permissions.")
                raise Exception("Access denied. Please check if your AWS credentials have the necessary permissions.")
            elif error_code == 'InvalidAccessKeyId':
                logger.error(f"Invalid AWS access key ID. Please check your AWS_ACCESS_KEY_ID.")
                raise Exception("Invalid AWS credentials. Please check your AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.")
            elif error_code == 'SignatureDoesNotMatch':
                logger.error(f"Invalid AWS secret access key. Please check your AWS_SECRET_ACCESS_KEY.")
                raise Exception("Invalid AWS credentials. Please check your AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.")
            elif error_code == 'ExpiredToken':
                logger.error(f"AWS session token has expired. Please refresh your credentials.")
                raise Exception("AWS session token has expired. Please refresh your credentials.")
            else:
                logger.error(f"AWS API error for bucket {bucket_name}: {error_code} - {error_message}")
                raise Exception(f"AWS API error: {error_code} - {error_message}")
        except Exception as e:
            logger.error(f"Unexpected error processing bucket {bucket_name}: {str(e)}")
            raise
    
    logger.info(f"Found manifests for {len(latest_manifests)} source buckets")  # Simplified summary
    return latest_manifests 