"""
Azure Functions for invoice sorting.
"""
import azure.functions as func
import logging
import os
from datetime import datetime, timezone
from services.drive_service import DriveService
from services.blob_service import BlobService
from services.pdf_service import PdfService
from services.file_service import FileService

# Constants
FOLDER_ID = '1cfpB8Cgb_H1NXZ_sMaVXxJH1hYTwSJQ4'

app = func.FunctionApp()

def process_invoice_file(file_id, drive_service, blob_service, pdf_service, file_service):
    """Process a single invoice file.
    
    Args:
        file_id: The Google Drive file ID of the PDF to process.
        drive_service: The DriveService instance.
        blob_service: The BlobService instance.
        pdf_service: The PdfService instance.
        file_service: The FileService instance.
    
    Returns:
        True if processed successfully, False otherwise.
    """
    try:
        # Get vendor folders
        vendors = drive_service.list_folders(FOLDER_ID)
        
        # Save vendor folders to blob storage
        blob_service.save_vendors(vendors)
        
        # Process PDF file
        vendor, date = pdf_service.process_file(file_id, list(vendors.keys()))
        
        if vendor and date:
            # Create vendor folder if needed
            if vendor not in vendors:
                logging.info(f"Creating new vendor folder: {vendor}")
                folder_id = file_service.create_vendor_folder(vendor, FOLDER_ID)
                
                if folder_id:
                    vendors[vendor] = folder_id
                    blob_service.save_vendors(vendors)
                else:
                    logging.error(f"Failed to create vendor folder for {vendor}")
                    return False
            
            # Move file to vendor folder
            target_folder_id = vendors.get(vendor)
            if target_folder_id:
                return file_service.move_to_vendor_folder(file_id, vendor, date, target_folder_id)
            else:
                logging.error(f"Vendor folder ID not found for {vendor}")
                return False
        else:
            logging.warning(f"Could not extract vendor/date from file {file_id}")
            return False
    except Exception as e:
        logging.error(f"Error processing invoice file {file_id}: {str(e)}")
        return False

def process_new_files(drive_service, blob_service, pdf_service, file_service, last_check_time=None):
    """Process new PDF files in Google Drive.
    
    Args:
        drive_service: The DriveService instance.
        blob_service: The BlobService instance.
        pdf_service: The PdfService instance.
        file_service: The FileService instance.
        last_check_time: Optional time to filter files created after.
        
    Returns:
        The number of files processed successfully.
    """
    try:
        # Get files created after last_check_time
        files = drive_service.list_pdf_files(FOLDER_ID, last_check_time)
        
        if not files:
            logging.info("No new PDF files found in Drive")
            return 0
            
        logging.info(f"Found {len(files)} PDF files to process")
        
        # Process each file
        successes = 0
        for file in files:
            file_id = file['id']
            if process_invoice_file(file_id, drive_service, blob_service, pdf_service, file_service):
                successes += 1
                
        logging.info(f"Successfully processed {successes} of {len(files)} files")
        return successes
    except Exception as e:
        logging.error(f"Error processing new files: {str(e)}")
        return 0

# Webhook handler
@app.function_name("InvoiceProcessorWebhook")
@app.route(route="gdrive-webhook", auth_level=func.AuthLevel.FUNCTION)
def webhook_trigger(req: func.HttpRequest) -> func.HttpResponse:
    """Handle Google Drive webhook notifications."""
    try:
        logging.info('Google Drive webhook received')
        
        # Get resource state from headers
        state = req.headers.get('X-Goog-Resource-State')
        logging.info(f"Resource state: {state}")
        
        # Handle subscription verification
        if state == 'sync':
            logging.info('Received subscription verification request')
            return func.HttpResponse("Webhook verified", status_code=200)
        
        # Handle file changes
        if state in ('change', 'update'):
            logging.info(f"Processing {state} notification")
            
            # Create services
            drive_service = DriveService()
            blob_service = BlobService()
            pdf_service = PdfService(drive_service)
            file_service = FileService(drive_service)
            
            # Verify channel ID
            channel_id = req.headers.get('X-Goog-Channel-ID')
            stored_channel_id = blob_service.get_channel_id()
            
            if stored_channel_id and channel_id != stored_channel_id:
                logging.warning(f"Received notification for unknown channel: {channel_id}")
                return func.HttpResponse("Unknown channel", status_code=400)
            
            # Get current time
            current_time = datetime.now(timezone.utc).isoformat()
            
            # Get last check time
            last_check_time = blob_service.get_last_check_time()
            
            # Process new files
            processed = process_new_files(
                drive_service, blob_service, pdf_service, file_service, last_check_time
            )
            
            # Save current time as last check time
            blob_service.save_last_check_time(current_time)
            
            return func.HttpResponse(f"Processed {processed} files", status_code=200)
        
        # Handle other resource states
        return func.HttpResponse(f"Ignored state: {state}", status_code=200)
        
    except Exception as e:
        error_msg = f"Error processing webhook: {str(e)}"
        logging.error(error_msg)
        return func.HttpResponse(error_msg, status_code=500)

# Timer trigger for setting up webhook
@app.function_name("DriveWatchSetup")
@app.timer_trigger(schedule="0 0 */6 * * *", arg_name="myTimer", run_on_startup=True)
async def setup_watch(myTimer: func.TimerRequest) -> None:
    """Set up Google Drive webhook notification."""
    try:
        logging.info("Setting up Google Drive watch notification")
        
        # Create services
        drive_service = DriveService()
        blob_service = BlobService()
        
        # Get webhook URL from environment
        webhook_url = os.getenv('FUNCTION_WEBHOOK_URL')
        if not webhook_url:
            logging.error("FUNCTION_WEBHOOK_URL environment variable not set")
            return
        
        # Set up webhook using same approach as manual function
        try:
            response, channel_id = drive_service.setup_webhook(FOLDER_ID, webhook_url)
            
            # Save channel ID
            blob_service.save_channel_id(channel_id)
            
            logging.info(f"Google Drive watch notification set up successfully: {response}")
            logging.info(f"Google Drive webhook setup successful. Channel ID: {channel_id}")
        except Exception as setup_error:
            logging.error(f"Failed to set up webhook: {str(setup_error)}")
            raise setup_error
        
    except Exception as e:
        logging.error(f"Error setting up Google Drive watch: {str(e)}")
        logging.exception("Full exception details:")

# Manual webhook setup
@app.function_name("ManualDriveSetup")
@app.route(route="setup-webhook", auth_level=func.AuthLevel.FUNCTION)
def manual_setup(req: func.HttpRequest) -> func.HttpResponse:
    """Manually trigger Google Drive webhook setup."""
    try:
        logging.info("Manually setting up Google Drive watch notification")
        
        # Create services
        drive_service = DriveService()
        blob_service = BlobService()
        
        # Get webhook URL from environment
        webhook_url = os.getenv('FUNCTION_WEBHOOK_URL')
        if not webhook_url:
            return func.HttpResponse(
                "FUNCTION_WEBHOOK_URL environment variable not set",
                status_code=400
            )
        
        # Set up webhook
        response, channel_id = drive_service.setup_webhook(FOLDER_ID, webhook_url)
        
        # Save channel ID
        blob_service.save_channel_id(channel_id)
        
        logging.info(f"Google Drive watch notification set up successfully: {response}")
        
        return func.HttpResponse(
            f"Google Drive webhook setup successful. Channel ID: {channel_id}",
            status_code=200
        )
        
    except Exception as e:
        error_msg = f"Error setting up Google Drive watch: {str(e)}"
        logging.error(error_msg)
        return func.HttpResponse(
            error_msg,
            status_code=500
        )