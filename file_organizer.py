"""
File Organizer module for organizing PDF files in Google Drive.

This module provides functionality to rename files based on vendor and date information,
and move them to the appropriate vendor folders in Google Drive.
"""
import logging
from googleapiclient.http import MediaIoBaseDownload

class FileOrganizer:
    """Organize files in Google Drive based on invoice metadata.
    
    This class provides methods to rename PDF files according to a standardized format
    and move them to appropriate vendor folders in Google Drive.
    """
    
    def __init__(self, drive_service):
        """Initialize the file organizer.
        
        Args:
            drive_service: The Google Drive API service instance.
        """
        self.drive_service = drive_service
        
    def create_new_filename(self, vendor, date):
        """Create a standardized filename based on vendor and date.
        
        Args:
            vendor: The vendor name extracted from the invoice.
            date: The invoice date in MM-DD-YYYY format.
            
        Returns:
            str: The new standardized filename, or None if inputs are invalid.
        """
        if not vendor or not date:
            logging.warning("Missing vendor or date for filename creation")
            return None
            
        return f"{vendor} {date}.pdf"
        
    def move_and_rename_file(self, file_id, new_name, target_folder_id):
        """Move and rename a file in Google Drive.
        
        Args:
            file_id: The Google Drive file ID.
            new_name: The new filename for the file.
            target_folder_id: The ID of the target folder.
            
        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            # Verify inputs
            if not file_id or not new_name or not target_folder_id:
                logging.error("Missing required parameters for move_and_rename_file")
                return False
                
            # Get current parents
            file = self.drive_service.files().get(
                fileId=file_id,
                fields='parents'
            ).execute()
            
            # Move file to new folder
            previous_parents = ",".join(file.get('parents', []))
            
            file = self.drive_service.files().update(
                fileId=file_id,
                addParents=target_folder_id,
                removeParents=previous_parents,
                body={'name': new_name},
                fields='id, name, parents'
            ).execute()
            
            logging.info(f"Moved and renamed file to: {new_name} in folder: {target_folder_id}")
            return True
            
        except Exception as e:
            logging.error(f"Error moving/renaming file {file_id} to {new_name}: {str(e)}")
            return False