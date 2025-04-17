"""
DriveService module for Google Drive operations.
"""
import logging
import os
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.discovery_cache.base import Cache

class MemoryCache(Cache):
    """In-memory cache for the Google API client."""
    _CACHE = {}

    def get(self, url):
        return MemoryCache._CACHE.get(url)

    def set(self, url, content):
        MemoryCache._CACHE[url] = content

class DriveService:
    """Service for interacting with Google Drive."""
    
    SCOPES = ['https://www.googleapis.com/auth/drive']
    
    def __init__(self):
        """Initialize the Drive service."""
        self.service = self._initialize_service()
    
    def _initialize_service(self):
        """Initialize and return the Google Drive service."""
        try:
            credentials_dict = {
                "type": "service_account",
                "project_id": os.getenv('project_id'),
                "private_key_id": os.getenv('private_key_id'),
                "private_key": os.getenv('private_key', '').replace('\\n', '\n'),
                "client_email": os.getenv('client_email'),
                "client_id": os.getenv('client_id'),
                "auth_uri": os.getenv('auth_uri', 'https://accounts.google.com/o/oauth2/auth'),
                "token_uri": os.getenv('token_uri', 'https://oauth2.googleapis.com/token'),
                "auth_provider_x509_cert_url": os.getenv('auth_provider_x509_cert_url', 'https://www.googleapis.com/oauth2/v1/certs'),
                "client_x509_cert_url": os.getenv('client_x509_cert_url')
            }

            missing_vars = [k for k, v in credentials_dict.items() if not v and k != 'type']
            if missing_vars:
                raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

            credentials = service_account.Credentials.from_service_account_info(
                credentials_dict, scopes=self.SCOPES
            )

            return build('drive', 'v3', credentials=credentials, cache=MemoryCache())
        except Exception as e:
            logging.error(f"Error initializing Drive service: {str(e)}")
            raise

    def list_folders(self, parent_id):
        """List folders in a parent folder."""
        try:
            results = self.service.files().list(
                q=f"mimeType='application/vnd.google-apps.folder' and '{parent_id}' in parents",
                fields="files(id, name)"
            ).execute()
            
            return {folder['name']: folder['id'] for folder in results.get('files', [])}
        except Exception as e:
            logging.error(f"Error listing folders: {str(e)}")
            return {}

    def create_folder(self, folder_name, parent_id):
        """Create a new folder in Google Drive."""
        try:
            file_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [parent_id]
            }
            folder = self.service.files().create(
                body=file_metadata,
                fields='id'
            ).execute()
            return folder.get('id')
        except Exception as e:
            logging.error(f"Error creating folder: {str(e)}")
            return None

    def list_pdf_files(self, folder_id, created_after=None):
        """List PDF files in a folder with optional time filter."""
        try:
            query = f"mimeType='application/pdf' and '{folder_id}' in parents"
            if created_after:
                query += f" and createdTime > '{created_after}'"
                
            results = self.service.files().list(
                q=query,
                fields="files(id, name)",
                orderBy="createdTime"
            ).execute()
            
            return results.get('files', [])
        except Exception as e:
            logging.error(f"Error listing PDF files: {str(e)}")
            return []

    def setup_webhook(self, folder_id, webhook_url, channel_id=None):
        """Set up a webhook for a folder."""
        try:
            if not webhook_url:
                raise ValueError("Webhook URL is required")
                
            # Generate a channel ID if not provided
            if not channel_id:
                timestamp = int(datetime.now().timestamp())
                channel_id = f"invoice-processor-{timestamp}"
                
            # Set up watch (expires after 1 week - maximum allowed by Google)
            body = {
                'id': channel_id,
                'type': 'web_hook',
                'address': webhook_url,
                'expiration': int((datetime.now().timestamp() + 604800) * 1000)  # 1 week in milliseconds
            }
            
            response = self.service.files().watch(
                fileId=folder_id,
                body=body
            ).execute()
            
            return response, channel_id
        except Exception as e:
            logging.error(f"Error setting up webhook: {str(e)}")
            raise