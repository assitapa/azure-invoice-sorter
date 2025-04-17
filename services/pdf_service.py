"""
PdfService module for PDF processing operations.
"""
import logging
from pdf_processor import PDFProcessor

class PdfService:
    """Service for processing PDF documents."""
    
    def __init__(self, drive_service):
        """Initialize the PDF service.
        
        Args:
            drive_service: The DriveService instance.
        """
        self.processor = PDFProcessor(drive_service.service)
    
    def process_file(self, file_id, known_vendors):
        """Process a PDF file from Google Drive.
        
        Args:
            file_id: The Google Drive file ID of the PDF to process.
            known_vendors: A list of known vendor names.
            
        Returns:
            A tuple of (vendor, date) if successful, or (None, None) if not.
        """
        try:
            logging.info(f"Processing file {file_id}")
            pdf_content = self.processor.download_file(file_id)
            text = self.processor.extract_text(pdf_content)
            
            if text:
                vendor, date = self.processor.get_vendor_from_gpt(known_vendors, text)
                
                if vendor and date:
                    logging.info(f"Extracted vendor={vendor}, date={date} from file {file_id}")
                    return vendor, date
                else:
                    logging.warning(f"Could not extract vendor/date from file {file_id}")
            else:
                logging.warning(f"Could not extract text from file {file_id}")
                
            return None, None
            
        except Exception as e:
            logging.error(f"Error processing PDF file {file_id}: {str(e)}")
            return None, None