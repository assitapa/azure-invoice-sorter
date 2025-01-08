import logging
from googleapiclient.http import MediaIoBaseDownload

class FileOrganizer:
    def __init__(self, drive_service):
        self.drive_service = drive_service
        
    def create_new_filename(self, vendor, date):
        if not vendor or not date:
            return None
        return f"{vendor} {date}.pdf"
        
    def move_and_rename_file(self, file_id, new_name, target_folder_id):
        try:
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
            
            logging.info(f"Moved and renamed file to: {new_name}")
            return True
            
        except Exception as e:
            logging.error(f"Error moving/renaming file: {str(e)}")
            return False