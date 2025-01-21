import azure.functions as func
import logging
import json
import os
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.discovery_cache.base import Cache
from datetime import datetime, timezone
from pdf_processor import PDFProcessor
from file_organizer import FileOrganizer
from azure.storage.blob import BlobServiceClient

# Constants
FOLDER_ID = '1cfpB8Cgb_H1NXZ_sMaVXxJH1hYTwSJQ4'
CONTAINER_NAME = 'invoice-data'
VENDORS_BLOB = 'known_vendors.json'
SCOPES = ['https://www.googleapis.com/auth/drive']

app = func.FunctionApp()


class MemoryCache(Cache):
    _CACHE = {}

    def get(self, url):
        return MemoryCache._CACHE.get(url)

    def set(self, url, content):
        MemoryCache._CACHE[url] = content


def get_blob_service_client():
    connection_string = os.getenv('AzureWebJobsStorage')
    if not connection_string:
        raise ValueError("AzureWebJobsStorage connection string not found")
    return BlobServiceClient.from_connection_string(connection_string)


def initialize_drive_service():
    try:
        credentials_dict = {
            "type": "service_account",
            "project_id": os.getenv('project_id'),
            "private_key_id": os.getenv('private_key_id'),
            "private_key": os.getenv('private_key').replace('\\n', '\n'),
            "client_email": os.getenv('client_email'),
            "client_id": os.getenv('client_id'),
            "auth_uri": os.getenv('auth_uri'),
            "token_uri": os.getenv('token_uri'),
            "auth_provider_x509_cert_url": os.getenv('auth_provider_x509_cert_url'),
            "client_x509_cert_url": os.getenv('client_x509_cert_url')
        }

        credentials = service_account.Credentials.from_service_account_info(
            credentials_dict, scopes=SCOPES
        )

        return build('drive', 'v3', credentials=credentials)
    except Exception as e:
        logging.error(f"Error initializing Drive service: {str(e)}")
        raise


def load_vendor_folders(drive_service):
    try:
        # First get all current folders from Drive
        results = drive_service.files().list(
            q=f"mimeType='application/vnd.google-apps.folder' and '{FOLDER_ID}' in parents",
            fields="files(id, name)"
        ).execute()

        current_vendors = {folder['name']: folder['id']
                           for folder in results.get('files', [])}

        # Try to load existing vendors from blob
        blob_service_client = get_blob_service_client()
        blob_client = blob_service_client.get_blob_client(
            container=CONTAINER_NAME,
            blob=VENDORS_BLOB
        )

        try:
            # Load existing vendors
            existing_vendors = json.loads(
                blob_client.download_blob().readall())

            # Update with any new vendors
            if existing_vendors != current_vendors:
                logging.info("Updating vendors list with new folders")
                save_vendor_folders(current_vendors)

            return current_vendors

        except Exception as e:
            logging.info(f"No existing vendors file found or error: {str(e)}")
            # Save the newly discovered vendors
            save_vendor_folders(current_vendors)
            return current_vendors

    except Exception as e:
        logging.error(f"Error loading vendor folders: {str(e)}")
        return {}


def save_vendor_folders(vendors):
    try:
        if not vendors:
            logging.warning("Attempting to save empty vendors list")
            return

        blob_service_client = get_blob_service_client()
        blob_client = blob_service_client.get_blob_client(
            container=CONTAINER_NAME,
            blob=VENDORS_BLOB
        )
        blob_client.upload_blob(json.dumps(vendors), overwrite=True)
        logging.info(
            f"Successfully saved {len(vendors)} vendors to blob storage")
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


def process_new_files(drive_service, file_ids):
    processor = PDFProcessor(drive_service)
    organizer = FileOrganizer(drive_service)
    vendors = load_vendor_folders(drive_service)

    for file_id in file_ids:
        try:
            logging.info(f"Processing file {file_id}")
            pdf_content = processor.download_file(file_id)
            text = processor.extract_text(pdf_content)
            vendor, date = processor.get_vendor_from_gpt(
                list(vendors.keys()), text)

            if vendor and date:
                if vendor not in vendors:
                    logging.info(f"Creating new vendor folder: {vendor}")
                    folder_id = create_vendor_folder(drive_service, vendor)
                    if folder_id:
                        vendors[vendor] = folder_id
                        save_vendor_folders(vendors)  # Save to blob storage

                new_name = organizer.create_new_filename(vendor, date)
                if new_name:
                    target_folder = vendors.get(vendor)
                    if target_folder:
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


@app.function_name("InvoiceProcessorTimer")
@app.schedule(schedule="0 */1 * * * *", arg_name="mytimer", run_on_startup=True,
              connection="AzureWebJobsStorage")
def timer_trigger(mytimer: func.TimerRequest) -> None:
    try:
        utc_timestamp = datetime.now(timezone.utc).isoformat()
        logging.info('Starting Azure Function file check at: %s', utc_timestamp)

        drive_service = initialize_drive_service()
        
        # First, sync vendor folders
        logging.info("Syncing vendor folders...")
        vendors = load_vendor_folders(drive_service)
        logging.info(f"Found {len(vendors)} vendor folders")

        # Then process PDF files
        results = drive_service.files().list(
            q=f"mimeType='application/pdf' and '{FOLDER_ID}' in parents",
            fields="files(id, name)",
            orderBy="createdTime desc"
        ).execute()

        files = results.get('files', [])
        if files:
            logging.info(f"Found {len(files)} PDF files in Drive")
            file_ids = [file['id'] for file in files]
            process_new_files(drive_service, file_ids)
        else:
            logging.info("No PDF files found in Drive")

    except Exception as e:
        logging.error(f"Timer trigger failed: {str(e)}")
        raise

    logging.info('Python timer trigger function completed at %s', utc_timestamp)