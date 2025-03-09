import json
import os
import boto3
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from flipkart_po_parser import FlipkartPOParser, EmailConfig, ClientConfig
import base64
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize AWS clients
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
email_table = dynamodb.Table('email_credentials')
user_table = dynamodb.Table('user_configs')

# Encryption key (should be stored in AWS Secrets Manager in production)
ENCRYPTION_KEY = os.environ.get('ENCRYPTION_KEY', 'qXYqPLe9MjJ5AmfBKslHi3AontJCFiCRAiPipKkLrfg=')
# Ensure the key is properly formatted for Fernet
if not ENCRYPTION_KEY.endswith('='):
    ENCRYPTION_KEY += '=' * (-len(ENCRYPTION_KEY) % 4)
fernet = Fernet(ENCRYPTION_KEY.encode())

def decrypt_password(encrypted_password: str) -> str:
    """Decrypt the password using Fernet"""
    try:
        return fernet.decrypt(encrypted_password.encode()).decode()
    except Exception as e:
        logger.error(f"Error decrypting password: {e}")
        raise

def get_email_credentials() -> Dict:
    """Fetch email credentials from DynamoDB"""
    try:
        response = email_table.scan()
        items = response.get('Items', [])
        if not items:
            raise ValueError("No email credentials found")
        
        # Get the first email credential and decrypt the password
        email_cred = items[0]
        email_cred['password'] = decrypt_password(email_cred['password'])
        return email_cred
    except Exception as e:
        logger.error(f"Error fetching email credentials: {e}")
        raise

def get_flipkart_users() -> List[Dict]:
    """Fetch users who have FLIPKART channel enabled"""
    try:
        response = user_table.scan()
        users = []
        
        for item in response.get('Items', []):
            # Check if user has FLIPKART channel
            channels = item.get('channels', [])
            flipkart_channel = next(
                (channel for channel in channels if channel['name'] == 'FLIPKART'),
                None
            )
            
            if flipkart_channel:
                user_data = {
                    'client_id': item['user_id'],
                    'base_user_id': item['base_user_id'],
                    'token': item['token'],
                    'user_name': item['user_name'],
                    'sender_email': flipkart_channel['sender_email']
                }
                users.append(user_data)
        
        return users
    except Exception as e:
        logger.error(f"Error fetching Flipkart users: {e}")
        raise

def upload_to_s3(file_path: str, client_id: str) -> str:
    """Upload file to S3 and return the S3 key"""
    try:
        file_name = os.path.basename(file_path)
        s3_key = f"po_files/{client_id}/{datetime.now().strftime('%Y/%m/%d')}/{file_name}"
        
        s3_client.upload_file(file_path, os.environ['S3_BUCKET'], s3_key)
        logger.info(f"File uploaded to S3: {s3_key}")
        return s3_key
    except Exception as e:
        logger.error(f"Error uploading to S3: {e}")
        raise

def cleanup_old_files(client_id: str, days: int = 7) -> None:
    """Remove files older than specified days from S3"""
    try:
        cutoff_date = datetime.now() - timedelta(days=days)
        prefix = f"po_files/{client_id}/"
        
        # List objects in the bucket
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=os.environ['S3_BUCKET'], Prefix=prefix):
            for obj in page.get('Contents', []):
                # Extract date from path
                path_parts = obj['Key'].split('/')
                if len(path_parts) >= 4:  # Ensure we have enough parts
                    try:
                        file_date = datetime.strptime(f"{path_parts[2]}/{path_parts[3]}", '%Y/%m/%d')
                        if file_date < cutoff_date:
                            s3_client.delete_object(Bucket=os.environ['S3_BUCKET'], Key=obj['Key'])
                            logger.info(f"Deleted old file: {obj['Key']}")
                    except ValueError:
                        continue
    except Exception as e:
        logger.error(f"Error cleaning up old files: {e}")
        raise

def process_client(email_cred: Dict, client_config: Dict) -> None:
    """Process a single client's emails"""
    try:
        # Create configurations
        email_config = EmailConfig(
            email=email_cred['email'],
            password=email_cred['password'],
            save_dir='/tmp',  # Lambda's writable directory
            client_id=client_config['client_id']
        )
        
        client_config_obj = ClientConfig(
            base_user_id=client_config['base_user_id'],
            token=client_config['token'],
            user_name=client_config['user_name'],
            sender_email=client_config['sender_email']
        )
        
        # Initialize parser
        parser = FlipkartPOParser(email_config, client_config_obj)
        
        # Process emails
        parser.run()
        
        # Cleanup old files
        cleanup_old_files(client_config['client_id'])
        
    except Exception as e:
        logger.error(f"Error processing client {client_config['client_id']}: {e}")
        raise

def lambda_handler(event, context):
    """AWS Lambda handler function"""
    try:
        logger.info("Starting Lambda execution")
        
        # Get email credentials
        email_cred = get_email_credentials()
        logger.info("Retrieved email credentials")
        
        # Get Flipkart users
        client_configs = get_flipkart_users()
        logger.info(f"Found {len(client_configs)} Flipkart users to process")
        
        # Process each client
        for client_config in client_configs:
            try:
                logger.info(f"Processing client: {client_config['client_id']}")
                process_client(email_cred, client_config)
                logger.info(f"Successfully processed client: {client_config['client_id']}")
            except Exception as e:
                logger.error(f"Failed to process client {client_config['client_id']}: {e}")
                continue
        
        return {
            'statusCode': 200,
            'body': json.dumps('Successfully processed all clients')
        }
        
    except Exception as e:
        logger.error(f"Lambda execution failed: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        } 
