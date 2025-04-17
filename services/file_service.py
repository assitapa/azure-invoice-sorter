"""
FileService module for file organization operations.
"""
import logging
from file_organizer import FileOrganizer

class FileService:
    """Service for organizing files in Google Drive."""
    
    def __init__(self, drive_service):
        """Initialize the File service.
        
        Args:
            drive_service: The DriveService instance.
        """
        self.organizer = FileOrganizer(drive_service.service)
        self.drive_service = drive_service
    
    def create_vendor_folder(self, folder_name, parent_id):
        """Create a vendor folder if it doesn't exist.
        
        Args:
            folder_name: The name of the folder to create.
            parent_id: The ID of the parent folder.
            
        Returns:
            The folder ID if created successfully, or None if not.
        """
        return self.drive_service.create_folder(folder_name, parent_id)
    
    def move_to_vendor_folder(self, file_id, vendor, date, target_folder_id):
        """Move a file to a vendor folder and rename it.
        
        Args:
            file_id: The ID of the file to move.
            vendor: The vendor name.
            date: The invoice date.
            target_folder_id: The ID of the target folder.
            
        Returns:
            True if moved and renamed successfully, False otherwise.
        """
        try:
            # Create the new filename
            new_name = self.organizer.create_new_filename(vendor, date)
            if not new_name:
                logging.error(f"Could not create new filename for vendor={vendor}, date={date}")
                return False
            
            # Move and rename the file
            success = self.organizer.move_and_rename_file(file_id, new_name, target_folder_id)
            
            if success:
                logging.info(f"Successfully processed file: {new_name} to folder: {vendor}")
                return True
            else:
                logging.error(f"Failed to rename/move file {file_id}")
                return False
        except Exception as e:
            logging.error(f"Error moving file to vendor folder: {str(e)}")
            return False