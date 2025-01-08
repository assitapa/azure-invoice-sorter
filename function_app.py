import azure.functions as func
import logging
import json
import os
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.discovery_cache.base import Cache
from datetime import datetime
from pdf_processor import PDFProcessor
from file_organizer import FileOrganizer
from azure.storage.blob import BlobServiceClient

# Constants
FOLDER_ID = '1cfpB8Cgb_H1NXZ_sMaVXxJH1hYTwSJQ4'
CONTAINER_NAME = 'invoice-data'
VENDORS_BLOB = 'known_vendors.json'
KNOWN_FILES_BLOB = 'known_files.json'


class MemoryCache(Cache):
    _CACHE = {}

    def get(self, url):
        return MemoryCache._CACHE.get(url)

    def set(self, url, content):
        MemoryCache._CACHE[url] = content


def get_blob_service_client():
    connection_string = os.environ['AzureWebJobsStorage']
    return BlobServiceClient.from_connection_string(connection_string)


def load_known_files():
    try:
        blob_service_client = get_blob_service_client()
        blob_client = blob_service_client.get_blob_client(
            container=CONTAINER_NAME,
            blob= KNOWN_FILES_BLOB
        )
        data = json.loads(blob_client.download_blob().readall())
        if isinstance(data, dict) and 'files' in data:
            return data
        return {'files': {}}
    except Exception as e:
        logging.warning(f"Could not load known files: {str(e)}")
        return {'files': {}}


def save_known_files(files):
    try:
        blob_service_client = get_blob_service_client()
        blob_client = blob_service_client.get_blob_client(
            container=CONTAINER_NAME,
            blob= KNOWN_FILES_BLOB
        )
        blob_client.upload_blob(json.dumps(files), overwrite=True)
    except Exception as e:
        logging.error(f"Error saving known files: {str(e)}")


def initialize_drive_service():
    try:
        credentials_dict = {
            "type": "service_account",
            "project_id": os.environ["GOOGLE_PROJECT_ID"],
            "private_key_id": os.environ["GOOGLE_PRIVATE_KEY_ID"],
            "private_key": os.environ["GOOGLE_PRIVATE_KEY"].replace('\\n', '\n'),
            "client_email": os.environ["GOOGLE_CLIENT_EMAIL"],
            "client_id": os.environ["GOOGLE_CLIENT_ID"],
            "auth_uri": os.environ["GOOGLE_AUTH_URI"],
            "token_uri": os.environ["GOOGLE_TOKEN_URI"],
            "auth_provider_x509_cert_url": os.environ["GOOGLE_AUTH_PROVIDER_X509_CERT_URL"],
            "client_x509_cert_url": os.environ["GOOGLE_CLIENT_X509_CERT_URL"]
        }
        
        credentials = Credentials.from_service_account_info(
            credentials_dict,
            scopes=['https://www.googleapis.com/auth/drive']
        )
        return build('drive', 'v3', credentials=credentials, cache=MemoryCache())
    except Exception as e:
        logging.error(f"Error initializing Drive service: {str(e)}")
        raise


def load_vendor_folders(drive_service):
    try:
        blob_service_client = get_blob_service_client()
        blob_client = blob_service_client.get_blob_client(
            container=CONTAINER_NAME,
            blob=VENDORS_BLOB
        )
        try:
            data = json.loads(blob_client.download_blob().readall())
            return data
        except Exception:
            # If file doesn't exist, create it from Drive
            results = drive_service.files().list(
                q=f"mimeType='application/vnd.google-apps.folder' and '{FOLDER_ID}' in parents",
                fields="files(id, name)"
            ).execute()
            
            vendors = {folder['name']: folder['id'] 
                      for folder in results.get('files', [])}
            
            # Save the new vendors data
            save_vendor_folders(vendors)
            return vendors
    except Exception as e:
        logging.error(f"Error loading vendor folders: {str(e)}")
        return {}


def save_vendor_folders(vendors):
    try:
        blob_service_client = get_blob_service_client()
        blob_client = blob_service_client.get_blob_client(
            container=CONTAINER_NAME,
            blob=VENDORS_BLOB
        )
        blob_client.upload_blob(json.dumps(vendors), overwrite=True)
    except Exception as e:
        logging.error(f"Error saving vendor folders: {str(e)}")


def create_vendor_folder(drive_service, vendor_name):
    try:
        file_metadata = {
            'name': vendor_name,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [FOLDER_ID]
        }
        folder = drive_service.files().create(
            body=file_metadata,
            fields='id'
        ).execute()
        return folder.get('id')
    except Exception as e:
        logging.error(f"Error creating vendor folder: {str(e)}")
        return None


def process_new_files(drive_service, new_file_ids):
    processor = PDFProcessor(drive_service)
    organizer = FileOrganizer(drive_service)

    # Load vendor folders
    vendors = load_vendor_folders(drive_service)

    for file_id in new_file_ids:
        try:
            logging.info(f"Processing file {file_id}")

            pdf_content = processor.download_file(file_id)
            text = processor.extract_text(pdf_content)

            # Pass vendors list to GPT
            vendor, date = processor.get_vendor_from_gpt(
                list(vendors.keys()), text)

            if vendor and date:
                # Create new vendor folder if needed
                if vendor not in vendors:
                    logging.info(f"Creating new vendor folder: {vendor}")
                    folder_id = create_vendor_folder(drive_service, vendor)
                    if folder_id:
                        vendors[vendor] = folder_id
                        with open(VENDORS_PATH, 'w') as f:
                            json.dump(vendors, f)

                # Move file to vendor folder
                new_name = organizer.create_new_filename(vendor, date)
                if new_name:
                    target_folder = vendors.get(vendor)
                    if (target_folder):
                        success = organizer.move_and_rename_file(
                            file_id, new_name, target_folder)
                        if success:
                            logging.info(
                                f"Successfully processed file: {new_name} to folder: {vendor}")
                        else:
                            logging.error(
                                f"Failed to rename/move file {file_id}")
                    else:
                        logging.error(f"Vendor folder not found for {vendor}")
            else:
                logging.warning(
                    f"Could not extract vendor/date from file {file_id}")

        except Exception as e:
            logging.error(f"Error processing file {file_id}: {str(e)}")


@func.timer_trigger(schedule="0 * * * * * *",  # Runs every minute
                   name="timer",
                   run_on_startup=True)
def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
    
    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Starting Azure Function file check at: %s', utc_timestamp)
    
    try:
        known_files = load_known_files()
        if not isinstance(known_files, dict) or 'files' not in known_files:
            known_files = {'files': {}}
            
        known_file_ids = set(known_files['files'].keys())
        logging.info(f"Loaded {len(known_file_ids)} known files")
        
        drive_service = initialize_drive_service()
        results = drive_service.files().list(
            q=f"mimeType='application/pdf' and '{FOLDER_ID}' in parents",
            fields="files(id, name)",
            orderBy="createdTime desc"
        ).execute()
        
        current_files = {}
        files = results.get('files', [])
        
        if files:
            logging.info(f"Found {len(files)} files in Drive")
            for file in files:
                current_files[file['id']] = file['name']
                
            current_file_ids = set(current_files.keys())
            new_files = current_file_ids - known_file_ids
            removed_files = known_file_ids - current_file_ids
            
            if removed_files:
                logging.info(f"Detected {len(removed_files)} removed files")
                for file_id in removed_files:
                    logging.info(f"File removed: {known_files['files'].get(file_id, 'Unknown name')}")
            
            if new_files:
                logging.info(f"Processing {len(new_files)} new files")
                process_new_files(drive_service, list(new_files))
            else:
                logging.info("No new files found")
                
            # Update known files with current state
            known_files['files'] = current_files
            save_known_files(known_files)
            logging.info("Updated known files list")
        else:
            logging.info("No files found in Drive")
            known_files['files'] = {}
            save_known_files(known_files)
            
    except Exception as e:
        logging.error(f"Error in Azure Function: {str(e)}")

    logging.info('Python timer trigger function completed at %s', utc_timestamp)
