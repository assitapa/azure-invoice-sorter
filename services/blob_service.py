"""
BlobService module for Azure Storage Blob operations.
"""
import logging
import json
import os
from azure.storage.blob import BlobServiceClient, BlobClient

class BlobService:
    """Service for interacting with Azure Blob Storage."""
    
    def __init__(self, connection_string=None, container_name=None):
        """Initialize the Blob service.
        
        Args:
            connection_string: The Azure Storage connection string.
                If None, uses the AzureWebJobsStorage environment variable.
            container_name: The blob container name.
                If None, uses 'invoice-data'.
        """
        self.connection_string = connection_string or os.getenv('AzureWebJobsStorage')
        if not self.connection_string:
            raise ValueError("AzureWebJobsStorage environment variable not set")
            
        self.container_name = container_name or 'invoice-data'
        self.client = self._get_client()
        self.ensure_container_exists()
    
    def _get_client(self):
        """Get the Azure Blob Service client."""
        try:
            return BlobServiceClient.from_connection_string(self.connection_string)
        except Exception as e:
            logging.error(f"Error creating blob service client: {str(e)}")
            raise
    
    def ensure_container_exists(self):
        """Ensure the blob container exists."""
        try:
            container_client = self.client.get_container_client(self.container_name)
            if not container_client.exists():
                self.client.create_container(self.container_name)
                logging.info(f"Created container {self.container_name}")
        except Exception as e:
            logging.error(f"Error ensuring container exists: {str(e)}")
    
    def load_data(self, blob_name):
        """Load data from a blob.
        
        Args:
            blob_name: The name of the blob to load.
            
        Returns:
            The content of the blob, or None if the blob doesn't exist.
        """
        try:
            blob_client = self.client.get_blob_client(
                container=self.container_name,
                blob=blob_name
            )
            
            if blob_client.exists():
                data = blob_client.download_blob().readall()
                return data
            return None
        except Exception as e:
            logging.warning(f"Error loading data from blob {blob_name}: {str(e)}")
            return None
    
    def save_data(self, blob_name, data, overwrite=True):
        """Save data to a blob.
        
        Args:
            blob_name: The name of the blob to save.
            data: The data to save.
            overwrite: Whether to overwrite an existing blob.
            
        Returns:
            True if saved successfully, False otherwise.
        """
        try:
            blob_client = self.client.get_blob_client(
                container=self.container_name,
                blob=blob_name
            )
            
            blob_client.upload_blob(data, overwrite=overwrite)
            return True
        except Exception as e:
            logging.error(f"Error saving data to blob {blob_name}: {str(e)}")
            return False
    
    def load_json(self, blob_name, default=None):
        """Load JSON data from a blob.
        
        Args:
            blob_name: The name of the blob to load.
            default: The default value to return if the blob doesn't exist.
            
        Returns:
            The parsed JSON data, or the default value if the blob doesn't exist.
        """
        try:
            data = self.load_data(blob_name)
            if data:
                return json.loads(data)
            return default if default is not None else {}
        except json.JSONDecodeError as e:
            logging.warning(f"Error parsing JSON from blob {blob_name}: {str(e)}")
            return default if default is not None else {}
    
    def save_json(self, blob_name, data, overwrite=True):
        """Save JSON data to a blob.
        
        Args:
            blob_name: The name of the blob to save.
            data: The data to save (must be JSON serializable).
            overwrite: Whether to overwrite an existing blob.
            
        Returns:
            True if saved successfully, False otherwise.
        """
        try:
            json_data = json.dumps(data)
            return self.save_data(blob_name, json_data, overwrite=overwrite)
        except Exception as e:
            logging.error(f"Error saving JSON to blob {blob_name}: {str(e)}")
            return False
    
    def load_vendors(self):
        """Load vendor data from the vendors blob.
        
        Returns:
            A dictionary of vendor names to folder IDs.
        """
        return self.load_json('known_vendors.json', {})
    
    def save_vendors(self, vendors):
        """Save vendor data to the vendors blob.
        
        Args:
            vendors: A dictionary of vendor names to folder IDs.
            
        Returns:
            True if saved successfully, False otherwise.
        """
        if not vendors:
            logging.warning("Attempting to save empty vendors list")
            return False
            
        result = self.save_json('known_vendors.json', vendors)
        if result:
            logging.info(f"Successfully saved {len(vendors)} vendors to blob storage")
        return result
    
    def get_channel_id(self):
        """Get the stored channel ID from blob storage.
        
        Returns:
            The channel ID, or None if not found.
        """
        try:
            data = self.load_data('channel_id.txt')
            return data.decode('utf-8').strip() if data else None
        except Exception:
            return None
    
    def save_channel_id(self, channel_id):
        """Save the channel ID to blob storage.
        
        Args:
            channel_id: The channel ID to save.
            
        Returns:
            True if saved successfully, False otherwise.
        """
        return self.save_data('channel_id.txt', channel_id)
    
    def get_last_check_time(self):
        """Get the timestamp of the last check from blob storage.
        
        Returns:
            The timestamp, or None if not found.
        """
        try:
            data = self.load_data('last_check_time.txt')
            return data.decode('utf-8').strip() if data else None
        except Exception:
            return None
    
    def save_last_check_time(self, timestamp):
        """Save the timestamp of the current check to blob storage.
        
        Args:
            timestamp: The timestamp to save.
            
        Returns:
            True if saved successfully, False otherwise.
        """
        return self.save_data('last_check_time.txt', timestamp)