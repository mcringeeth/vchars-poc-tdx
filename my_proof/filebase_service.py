import boto3
import json
from datetime import datetime
import logging
import hashlib

class FilebaseService:
    def __init__(self, filebase_access_key_id, filebase_secret_access_key, bucket_name="vchars", remote_file_key="hash_list.json"):
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=filebase_access_key_id,
            aws_secret_access_key=filebase_secret_access_key,
        )

        self.bucket_name = bucket_name
        self.remote_file_key = remote_file_key

    def generate_hash(self, input):
        hash_object = hashlib.sha256(str(input).encode())
        return hash_object.hexdigest()
    
    def get_hash_list(self):
        try:
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=self.remote_file_key
            )
            data = json.loads(response['Body'].read().decode('utf-8'))
            return data.get('hash_list', [])
        except Exception as e:
            logging.error(f"Error fetching list of hashes: {str(e)}")
            raise e

    def update_hash_list(self, existing_hashes, new_hash):
        try:
            data = {
                'hash_list': existing_hashes + [new_hash]
            }
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=self.remote_file_key,
                Body=json.dumps(data, indent=2),
                ContentType='application/json'
            )
            return True
        except Exception as e:
            logging.error(f"Error updating hashes: {str(e)}")
            raise e
        