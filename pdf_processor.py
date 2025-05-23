"""
PDF Processor module for extracting and analyzing invoice content.

This module provides functionality to download, extract text from, and
analyze PDF files using OpenAI to identify vendors and dates.
"""
import fitz
import re
from datetime import datetime
import os
import logging
from googleapiclient.http import MediaIoBaseDownload
import io
from openai import OpenAI
from dotenv import load_dotenv


class PDFProcessor:
    """Process PDF documents from Google Drive.
    
    This class provides methods to download PDFs from Google Drive,
    extract their text content, and analyze them using OpenAI
    to identify vendor information and invoice dates.
    """

    def __init__(self, drive_service):
        """Initialize the PDF processor.
        
        Args:
            drive_service: The Google Drive API service instance.
        """
        self.drive_service = drive_service
        load_dotenv()
        self._initialize_openai_client()
        
    def _initialize_openai_client(self):
        """Initialize the OpenAI client with API key.
        
        Raises:
            ValueError: If the OpenAI API key is not set.
        """
        api_key = os.getenv("openai_api_key")
        if not api_key:
            logging.warning("OpenAI API key not found in environment variables")
        self.openai_client = OpenAI(api_key=api_key)

    def download_file(self, file_id):
        """Download a PDF file from Google Drive.
        
        Args:
            file_id: The Google Drive file ID.
            
        Returns:
            bytes: The PDF file content.
            
        Raises:
            Exception: If there's an error downloading the file.
        """
        try:
            request = self.drive_service.files().get_media(fileId=file_id)
            file = io.BytesIO()
            downloader = MediaIoBaseDownload(file, request)
            done = False
            while done is False:
                _, done = downloader.next_chunk()
            return file.getvalue()
        except Exception as e:
            logging.error(f"Error downloading file {file_id}: {str(e)}")
            raise

    def extract_text(self, pdf_content):
        """Extract text from PDF content.
        
        Args:
            pdf_content: The PDF content as bytes.
            
        Returns:
            str: The extracted text, or None if extraction fails.
        """
        try:
            pdf_document = fitz.open(stream=pdf_content, filetype="pdf")
            text = ""
            for page in pdf_document:
                text += page.get_text()
            return text
        except Exception as e:
            logging.error(f"Error extracting text: {str(e)}")
            return None

    def get_vendor_from_gpt(self, known_vendors, text: str) -> tuple[str | None, str | None]:
        """Extract vendor and date from invoice text using OpenAI.
        
        Args:
            known_vendors: List of known vendor names.
            text: The text content of the invoice.
            
        Returns:
            tuple: (vendor_name, date) or (None, None) if extraction fails.
        """
        if not text:
            logging.warning("Empty text provided to get_vendor_from_gpt")
            return None, None

        try:
            response = self.openai_client.chat.completions.create(
                messages=[{
                    "role": "system",
                    "content": "You are an AI trained to extract vendor names and dates from invoices. Return only the vendor name and date."
                }, {
                    "role": "user",
                    "content": self._create_openai_prompt(known_vendors, text)
                }],
                model='o4-mini'
            )
            
            return self._parse_openai_response(response)
            
        except Exception as e:
            logging.error(f"Error calling OpenAI API: {str(e)}")
            return None, None
            
    def _create_openai_prompt(self, known_vendors, text):
        """Create the prompt for the OpenAI API.
        
        Args:
            known_vendors: List of known vendor names.
            text: The text content of the invoice.
            
        Returns:
            str: The formatted prompt.
        """
        return f"""Extract the vendor name and date from this invoice.
                Known vendors: {', '.join(known_vendors)}
                Sangam Supermarket is not a vendor, Sangam Supermarket is the customer in nearly all cases.
                Usually when new vendors are found, they have completely different names than known vendors so if you think a vendor name is similar to a known vendor, it is likely the known vendor.
                Many times invoices cana have chopped off vendor names, so instead of creating a new one try to match it up to a know vendor first.
                If you are not sure, use the known vendor list to help you.
                Make sure to also use other context clues to determine the vendor name. For example a mistake you made when I ran you in the past was thinking Raja Foods was Ra'a Foods. 
                If you had used context clues such as their email or website you would have gotten it right. 
                If vendor not in list, identify the most likely vendor name.
                Format response exactly as: 
                
                Vendor Name MM-DD-YYYY
                
                Input text: {text}"""
    
    def _parse_openai_response(self, response):
        """Parse the response from OpenAI API.
        
        Args:
            response: The response from the OpenAI API.
            
        Returns:
            tuple: (vendor_name, date) or (None, None) if parsing fails.
        """
        response_content = response.choices[0].message.content.strip()
        logging.debug(f"OpenAI response: {response_content}")

        # More flexible date patterns
        date_patterns = [
            r'\d{1,2}[-/]\d{1,2}[-/]\d{4}',  # MM-DD-YYYY or MM/DD/YYYY
            r'\d{4}[-/]\d{1,2}[-/]\d{1,2}',  # YYYY-MM-DD or YYYY/MM/DD
        ]
        
        for pattern in date_patterns:
            # Allow optional quotes, allow extra spaces
            vendor_date_pattern = rf'^\s*"?(.*?)"?\s+({pattern})\s*$'
            match = re.search(vendor_date_pattern, response_content.strip())

            if match:
                found_vendor = match.group(1).strip('"').strip()
                date_str = match.group(2).strip()
                
                # Normalize date format
                try:
                    if '/' in date_str:
                        date_obj = datetime.strptime(date_str, '%m/%d/%Y')
                    else:
                        date_obj = datetime.strptime(date_str, '%m-%d-%Y')
                    formatted_date = date_obj.strftime('%m-%d-%Y')
                    
                    logging.info(f"Successfully extracted vendor: {found_vendor} and date: {formatted_date}")
                    return found_vendor, formatted_date
                except ValueError as e:
                    logging.debug(f"Date parsing failed: {e}")
                    continue
        
        logging.warning(f"Could not parse vendor/date from response: {response_content}")
        return None, None
