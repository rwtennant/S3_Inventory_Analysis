from dotenv import load_dotenv
import os
import boto3
import logging
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

def get_s3_client():
    """
    Initialize and return an authenticated S3 client using credentials from environment variables.
    
    :return: boto3 S3 client
    :raises: Exception with detailed error message if credentials are missing or invalid
    """
    # Load environment variables from .env file
    load_dotenv()

    # Read AWS access keys from environment variables
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    aws_session_token = os.getenv('AWS_SESSION_TOKEN')

    # Log credential status (without exposing sensitive data)
    logger.debug("Loading AWS credentials from environment variables")
    logger.debug(f"AWS Access Key ID present: {'Yes' if aws_access_key_id else 'No'}")
    logger.debug(f"AWS Secret Access Key present: {'Yes' if aws_secret_access_key else 'No'}")
    logger.debug(f"AWS Session Token present: {'Yes' if aws_session_token else 'No'}")

    # Check for missing credentials
    missing_credentials = []
    if not aws_access_key_id:
        missing_credentials.append("AWS_ACCESS_KEY_ID")
    if not aws_secret_access_key:
        missing_credentials.append("AWS_SECRET_ACCESS_KEY")
    
    if missing_credentials:
        raise Exception(f"Missing required AWS credentials: {', '.join(missing_credentials)}. Please check your .env file.")

    try:
        # Initialize AWS session
        aws_session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token
        )
        
        # Initialize and return S3 client
        return aws_session.client('s3')
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        
        if error_code == 'InvalidClientTokenId':
            raise Exception("Invalid AWS credentials. Please check your AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.")
        elif error_code == 'ExpiredToken':
            raise Exception("AWS session token has expired. Please refresh your credentials.")
        elif error_code == 'AccessDenied':
            raise Exception("Access denied. Please check if your AWS credentials have the necessary permissions.")
        else:
            raise Exception(f"AWS API error: {error_code} - {error_message}")
    except Exception as e:
        raise Exception(f"Failed to initialize S3 client: {str(e)}") 