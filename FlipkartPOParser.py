import imaplib
import email
import os
from email import message_from_bytes
from email.policy import default
from datetime import datetime
import requests
import json
import re
import pandas as pd
import time
import smtplib
import textwrap
import email.message
import mimetypes
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import traceback
from typing import Dict, List, Optional, Any, Tuple
import logging
from dataclasses import dataclass
from pathlib import Path
import boto3

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize S3 client
s3_client = boto3.client('s3')

@dataclass
class EmailConfig:
    """Configuration for email settings"""
    imap_server: str = "imap.gmail.com"
    smtp_server: str = "smtp.gmail.com"
    smtp_port: int = 587
    email: str = "po@xxxx.com"
    password: str = "xxxx"
    inbox_label: str = "Inbox"
    main_label: str = "FLIPKART_GROCERY_PO"
    save_dir: str = "/tmp"  # Changed to Lambda's writable directory
    client_id: str = ""  # Added client_id field

@dataclass
class ClientConfig:
    """Configuration for client details"""
    base_user_id: str = "xx"
    token: str = "xx-9000229-xx"
    user_name: str = "xx"
    sender_email: str = "xx.xxx@gmail.com"

class FlipkartPOParser:
    """Main class for parsing Flipkart Purchase Orders"""
    
    def __init__(self, email_config: EmailConfig, client_config: ClientConfig):
        self.email_config = email_config
        self.client_config = client_config
        self.checklist = {
            "Download File": False,
            "Fetch Order Status Id": False,
            "Fetch Order Source Id": False,
            "Check And Fetch Custom Field": False,
            "Parse file": False,
            "Check if a order already exist": False,
            "Create Order Json": False,
            "Create order": False,
        }
        self.extracted_data = {}
        self.sku_map = {}
        self.calculated_total = 0
        self.file_path = ""
        self.email_data = {}
        self.extra_field_to_po_field_mapping = {
            'PO Number': "po#",
            'PO Expiry': "po_expiry"
        }
        
        # Create save directory if it doesn't exist
        os.makedirs(self.email_config.save_dir, exist_ok=True)

    def connect_mail(self) -> imaplib.IMAP4_SSL:
        """Establish connection to email server"""
        try:
            mail = imaplib.IMAP4_SSL(self.email_config.imap_server)
            mail.login(self.email_config.email, self.email_config.password)
            return mail
        except Exception as e:
            logger.error(f"Failed to connect to email server: {e}")
            raise

    def move_email(self, mail: imaplib.IMAP4_SSL, email_id: bytes, current_label: str, new_label: str) -> None:
        """Move email from current label to new label"""
        try:
            logger.info(f"Moving email {email_id} from {current_label} to {new_label}")
            
            if current_label != self.email_config.inbox_label:
                mail.store(email_id, "-X-GM-LABELS", f'"{current_label}"')
            
            mail.store(email_id, "+X-GM-LABELS", f'"{new_label}"')
            mail.store(email_id, "+FLAGS", "\\Deleted")
            mail.expunge()
            
            logger.info(f"Successfully moved email {email_id} to {new_label}")
        except Exception as e:
            logger.error(f"Failed to move email {email_id}: {e}")
            raise

    def fetch_emails(self, mail: imaplib.IMAP4_SSL, folder: str = "INBOX") -> List[bytes]:
        """Fetch emails from specified folder"""
        try:
            mail.select(folder)
            status, messages = mail.search(None, 
                f'(SUBJECT "FLIPKART GROCERY PO" FROM "{self.client_config.sender_email}")')
            
            if status != "OK" or not messages[0]:
                logger.info(f"No emails found in {folder}")
                return []
            
            return messages[0].split()
        except Exception as e:
            logger.error(f"Failed to fetch emails: {e}")
            raise

    def upload_to_s3(self, file_path: str) -> str:
        """Upload file to S3 and return the S3 key"""
        try:
            file_name = os.path.basename(file_path)
            s3_key = f"po_files/{self.email_config.client_id}/{datetime.now().strftime('%Y/%m/%d')}/{file_name}"
            
            s3_client.upload_file(file_path, os.environ['S3_BUCKET'], s3_key)
            logger.info(f"File uploaded to S3: {s3_key}")
            return s3_key
        except Exception as e:
            logger.error(f"Error uploading to S3: {e}")
            raise

    def process_email(self, mail: imaplib.IMAP4_SSL, email_id: bytes) -> Optional[Dict]:
        """Process email and extract attachments"""
        try:
            status, msg_data = mail.fetch(email_id, "(RFC822)")
            
            if status != "OK":
                logger.error(f"Failed to fetch email {email_id}")
                return None

            for response_part in msg_data:
                if isinstance(response_part, tuple):
                    msg = message_from_bytes(response_part[1], policy=default)
                    
                    email_data = {
                        "FROM": msg.get("From"),
                        "TO": msg.get("To"),
                        "CC": msg.get("Cc"),
                        "SUBJECT": msg.get("Subject"),
                        "BODY": "",
                        "MESSAGE_ID": msg.get("Message-ID"),
                        "attachments": []
                    }

                    for part in msg.walk():
                        if part.get_content_maintype() == "multipart":
                            continue
                        
                        filename = part.get_filename()
                        if not filename:
                            continue

                        name, ext = os.path.splitext(filename)
                        ext = ext.lower()

                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        email_from = email_data["FROM"].split('<')[0].strip()
                        new_filename = f"{email_from}_{name}_{timestamp}{ext}"
                        new_filename = self.normalize_field_name(new_filename)

                        if ext in ['.pdf', '.xls', '.xlsx']:
                            filepath = os.path.join(self.email_config.save_dir, new_filename)
                            
                            with open(filepath, "wb") as f:
                                f.write(part.get_payload(decode=True))
                            
                            # Upload to S3
                            s3_key = self.upload_to_s3(filepath)
                            
                            attachment_info = {
                                "original_filename": new_filename,
                                "new_filename": new_filename,
                                "filepath": filepath,
                                "s3_key": s3_key,
                                "extension": ext
                            }
                            email_data["attachments"].append(attachment_info)
                            logger.info(f"Downloaded and uploaded to S3: {new_filename}")

                            required_files = [
                                attachment for attachment in email_data["attachments"] 
                                if attachment["extension"].lower() == ".xls"
                            ]
                            
                            if required_files:
                                email_data["required_files"] = required_files
                                logger.info(f"Found {len(required_files)} files with .xls extension")
                            else:
                                logger.warning("No files found with .xls extension")

                    return email_data
            return None
        except Exception as e:
            logger.error(f"Error processing email: {e}")
            raise

    def normalize_field_name(self, field_name: str) -> str:
        """Normalize field names by removing special characters and standardizing format"""
        try:
            if not field_name:
                return ""
            
            normalized = str(field_name).strip().lower()
            normalized = re.sub(r'\s+', ' ', normalized)
            normalized = normalized.replace(' ', '_')
            normalized = re.sub(r'_+', '_', normalized)
            normalized = normalized.strip('_')
            
            return normalized
        except Exception as e:
            logger.error(f"Error normalizing field name '{field_name}': {e}")
            raise

    def call_api(self, method: str, parameters: Optional[Dict] = None) -> Dict:
        """Make API call to Base system"""
        try:
            if parameters is None:
                parameters = []
                
            api_params = {
                'method': method,
                'parameters': json.dumps(parameters)
            }

            headers = {'X-BLToken': self.client_config.token}

            response = requests.post(
                "https://api.baselinker.com/connector.php",
                headers=headers,
                data=api_params
            )
            response.raise_for_status()
            response_data = response.json()
            
            if response_data.get("status") != "SUCCESS":
                error_message = f"API Response Error: {json.dumps(response_data, indent=2)}"
                logger.error(error_message)
                self.send_error_email(error_message)
                return False

            return response_data

        except requests.exceptions.RequestException as e:
            error_message = f"API Request Error: {str(e)}"
            logger.error(error_message)
            self.send_error_email(error_message)
            raise

    def send_success_email(self, order_id: str, po_number: str, data: Dict) -> None:
        """Send success email notification"""
        try:
            subject = f"FLIPKART GROCERY PO Order Created Successfully - Order ID: {order_id}"
            
            email_body = f"""
            Hi,

            🎉 **Your order has been successfully created!** 🎉

            **Order Details:**  
            - **Base Order ID:** {order_id}  
            - **PO Number:** {po_number}  

            Thank you for using our service. If you have any questions, feel free to contact support.

            *This is an auto-generated email. Please do not reply.*

            Regards,  
            Support Team
            """

            msg = email.message.EmailMessage()
            msg["Subject"] = subject
            msg["From"] = self.email_config.email
            msg["To"] = self.client_config.sender_email
            msg.set_content(email_body)

            if data:
                msg["In-Reply-To"] = data.get("MESSAGE_ID")
                msg["References"] = data.get("MESSAGE_ID")
                msg.replace_header("Subject", f"Re: {data.get('SUBJECT', 'No Subject')}")   

            with smtplib.SMTP(self.email_config.smtp_server, self.email_config.smtp_port) as server:
                server.starttls()
                server.login(self.email_config.email, self.email_config.password)
                server.send_message(msg)
            
            logger.info("Success email sent successfully!")
            
        except Exception as e:
            logger.error(f"Failed to send success email: {e}")
            raise

    def send_error_email(self, error_message: str, cc_email: Optional[str] = None) -> None:
        """Send error email notification"""
        try:
            html_content = f"""
            <html>
            <body style="font-family: Arial, sans-serif; color: #333; padding: 20px;">
                <h2 style="color: #d9534f;">🚨 Oops, Purchase Order Creation Failed!</h2>
                <p style="font-size: 16px;">Hi,</p>
                <p>We encountered an issue while processing the purchase order. Please find the details below:</p>
                <div style="background-color: #f8d7da; color: #721c24; padding: 10px; border-radius: 5px;">
                    <strong>Reason:</strong> {error_message}
                </div>
                <hr style="border-top: 1px solid #ccc;">
                <p style="font-size: 12px; color: #777;">
                    📌 This is an auto-generated email. Please do not reply to this message.
                </p>
                <p style="font-size: 14px;"><strong>Best Regards,<br>Base Team India</strong></p>
            </body>
            </html>
            """

            msg = MIMEMultipart()
            msg["Subject"] = f"🔴 [FLIPKART_GROCERY_PO/FAILED] Urgent: Purchase Order Creation Failed"
            msg["From"] = self.email_config.email
            msg["To"] = self.client_config.sender_email
            if cc_email:
                msg["Cc"] = cc_email

            msg.attach(MIMEText(html_content, "html"))
            
            if self.email_data:
                msg["In-Reply-To"] = self.email_data.get("MESSAGE_ID")
                msg["References"] = self.email_data.get("MESSAGE_ID")
                msg.replace_header("Subject", f"Re: {self.email_data.get('SUBJECT', 'No Subject')}") 

            if self.file_path:
                try:
                    with open(self.file_path, "rb") as file:
                        mime_type, _ = mimetypes.guess_type(self.file_path)
                        mime_type = mime_type or "application/octet-stream"
                        part = MIMEApplication(file.read(), _subtype=mime_type.split("/")[-1])
                        part.add_header("Content-Disposition", f"attachment; filename={os.path.basename(self.file_path)}")
                        msg.attach(part)
                except Exception as e:
                    logger.error(f"Failed to attach file: {e}")

            with smtplib.SMTP(self.email_config.smtp_server, self.email_config.smtp_port) as server:
                server.starttls()
                server.login(self.email_config.email, self.email_config.password)
                server.send_message(msg)
            
            logger.info("Error email sent successfully!")
            
            # Send support email
            self._send_support_email(error_message)
            
            raise ValueError(error_message)
            
        except Exception as e:
            logger.error(f"Failed to send error email: {e}")
            raise

    def _send_support_email(self, error_message: str) -> None:
        """Send error notification to support team"""
        try:
            checklist_text = "\n".join([f"✅ {step}" if status else f"❌ {step}" 
                                      for step, status in self.checklist.items()])

            email_body = textwrap.dedent(f"""\
            Hi,

            **Oops! Purchase Order Creation Failed.** 🚨

            **Error Details:**  
            {error_message}

            **Checklist Status:**  
            {checklist_text}

            **Client Information:**  
            UserName: {self.client_config.user_name}
            Base User Id: {self.client_config.base_user_id}

            📌 *This is an auto-generated email. Please do not reply.*
            """)

            msg = MIMEMultipart()
            msg.attach(email.message.EmailMessage())
            msg["Subject"] = f"🔴 [FLIPKART_GROCERY_PO/FAILED] Urgent: Order Processing Failed"
            msg["From"] = self.email_config.email
            msg["To"] = self.email_config.email
            msg.attach(MIMEText(email_body, "plain"))

            if self.file_path:
                try:
                    with open(self.file_path, "rb") as file:
                        mime_type, _ = mimetypes.guess_type(self.file_path)
                        mime_type = mime_type or "application/octet-stream"
                        part = MIMEApplication(file.read(), _subtype=mime_type.split("/")[-1])
                        part.add_header("Content-Disposition", f"attachment; filename={os.path.basename(self.file_path)}")
                        msg.attach(part)
                except Exception as e:
                    logger.error(f"Failed to attach file to support email: {e}")

            with smtplib.SMTP(self.email_config.smtp_server, self.email_config.smtp_port) as server:
                server.starttls()
                server.login(self.email_config.email, self.email_config.password)
                server.send_message(msg)
            
            logger.info("Support email sent successfully!")
            
        except Exception as e:
            logger.error(f"Failed to send support email: {e}")
            raise

    def process_po_file(self, file_path: str) -> None:
        """Process the PO file and extract data"""
        try:
            logger.info(f"Reading Excel file: {file_path}")
            df = pd.read_excel(file_path, sheet_name=0, engine="xlrd")
            logger.info(f"Excel file loaded successfully with {len(df)} rows")
            
            # Extract order details
            logger.info("Extracting order details from Excel file")
            self._extract_order_details(df)
            logger.info("Order details extracted successfully")
            
            # Process line items
            logger.info("Processing line items from Excel file")
            self._process_line_items(df)
            logger.info(f"Processed {len(self.extracted_data.get('Line_Items', []))} line items")
            
            # Process addresses
            logger.info("Processing addresses")
            self._process_addresses()
            logger.info("Addresses processed successfully")
            
            logger.info("Successfully processed PO file")
            self.checklist["Parse file"] = True
            
        except Exception as e:
            logger.error(f"Error processing PO file: {e}")
            raise

    def _extract_order_details(self, df: pd.DataFrame) -> None:
        """Extract order details from the Excel file"""
        try:
            logger.info("Starting order details extraction")
            found_order_details_flag = False
            order_details_index = 0
            index = 0

            while not found_order_details_flag and index < len(df):
                row = df.iloc[index]
                if isinstance(row.iloc[0], str):
                    for col_index, cell in enumerate(row):
                        if isinstance(cell, str):
                            self._process_cell(cell, row, col_index)
                        
                        if "ORDER DETAILS" in str(cell):
                            order_details_index = index
                            found_order_details_flag = True
                            logger.info(f"Found ORDER DETAILS section at row {index}")
                            break
                index += 1

            if not found_order_details_flag:
                raise ValueError("Could not find ORDER DETAILS section in the file")

            self.order_details_index = order_details_index
            logger.info("Order details extraction completed successfully")

        except Exception as e:
            logger.error(f"Error extracting order details: {e}")
            raise

    def _process_cell(self, cell: str, row: pd.Series, col_index: int) -> None:
        """Process individual cell in the Excel file"""
        try:
            fields_to_extract = [
                "PO#", "Nature Of Supply", "Nature of Transaction", "PO Expiry", 
                "CATEGORY", "ORDER DATE", "SUPPLIER NAME", "CATEGORY",
                "SUPPLIER ADDRESS", "SUPPLIER CONTACT", "Billed by", "Shipped From", 
                "BILLED TO ADDRESS", "SHIPPED TO ADDRESS", "MODE OF PAYMENT",
                "CONTRACT REF ID", "CONTRACT VERSION", "CREDIT TERM"
            ]

            if cell in fields_to_extract:
                self.extracted_data[self.normalize_field_name(cell)] = self._get_next_non_empty_value(row, col_index)
            
            # Process special fields
            if "EMAIL" in cell:
                self.extracted_data[self.normalize_field_name("Supplier EMAIL")] = self._get_next_non_empty_value(row, col_index)
            
            if "GSTIN" in cell:
                self.extracted_data[self.normalize_field_name("Billed By GSTIN")] = self._get_next_non_empty_value(row, col_index)
            
            if "State Code" in cell:
                self.extracted_data[self.normalize_field_name("Billed By State Code")] = self._get_next_non_empty_value(row, col_index)

        except Exception as e:
            logger.error(f"Error processing cell: {e}")
            raise

    def _get_next_non_empty_value(self, row: pd.Series, start_col_index: int) -> Any:
        """Get the next non-empty value in a row"""
        for next_col in range(start_col_index + 1, len(row)):
            if pd.notna(row.iloc[next_col]):
                return row.iloc[next_col]
        return None

    def _process_line_items(self, df: pd.DataFrame) -> None:
        """Process line items from the Excel file"""
        try:
            logger.info("Starting line items processing")
            start_row = self.order_details_index + 1
            table_df = df.iloc[start_row:].reset_index(drop=True)
            
            # Find important notification row
            important_notification_row_index = None
            for index, row in table_df.iterrows():
                if "Important Notification" in row.to_string():
                    important_notification_row_index = index
                    logger.info(f"Found Important Notification at row {index}")
                    break

            if important_notification_row_index is not None:
                table_df = table_df.iloc[:important_notification_row_index]
                logger.info("Truncated table at Important Notification")

            table_df.columns = table_df.iloc[0]
            table_df = table_df[1:].reset_index(drop=True)
            self.extracted_data["Line_Items"] = table_df.to_dict(orient="records")
            logger.info(f"Processed {len(self.extracted_data['Line_Items'])} line items")

            # Process totals
            row = df.iloc[start_row + important_notification_row_index]
            for col_index, cell in enumerate(row):
                if isinstance(cell, str):
                    if "Total Quantity=" in cell:
                        total_qty = self._get_next_non_empty_value(row, col_index)
                        self.extracted_data[self.normalize_field_name("Total Quantity=")] = total_qty
                        logger.info(f"Total Quantity: {total_qty}")
                    if "Total=" in cell:
                        total = self._get_next_non_empty_value(row, col_index)
                        self.extracted_data[self.normalize_field_name("Total=")] = total
                        logger.info(f"Total Amount: {total}")

            logger.info("Line items processing completed successfully")

        except Exception as e:
            logger.error(f"Error processing line items: {e}")
            raise

    def _process_addresses(self) -> None:
        """Process and normalize addresses"""
        try:
            self.extracted_data["billed_to_address"] = self._extract_details_from_address(
                self.extracted_data.get("billed_to_address"))
            self.extracted_data["shipped_to_address"] = self._extract_details_from_address(
                self.extracted_data.get("shipped_to_address"))
            self.extracted_data["supplier_address"] = self._extract_details_from_address(
                self.extracted_data.get("supplier_address"))
        except Exception as e:
            logger.error(f"Error processing addresses: {e}")
            raise

    def _extract_details_from_address(self, address: str) -> Dict:
        """Extract details from address string"""
        try:
            if not address:
                return {"address": "", "pincode": None, "state": None}

            pincode_pattern = r"\b\d{6}\b"
            pincode_found = None
            state_found = None

            # Extract pincode
            pincode_matches = re.findall(pincode_pattern, address)
            if pincode_matches:
                pincode_found = pincode_matches[0]

            # Extract state
            indian_states = [
                "Andhra Pradesh", "Arunachal Pradesh", "Assam", "Bihar", "Chhattisgarh", "Goa",
                "Gujarat", "Haryana", "Himachal Pradesh", "Jharkhand", "Karnataka", "Kerala",
                "Madhya Pradesh", "Maharashtra", "Manipur", "Meghalaya", "Mizoram", "Nagaland",
                "Odisha", "Punjab", "Rajasthan", "Sikkim", "Tamil Nadu", "Telangana",
                "Tripura", "Uttar Pradesh", "Uttarakhand", "West Bengal", "Delhi", "Puducherry",
                "Jammu and Kashmir", "Ladakh", "Andaman and Nicobar Islands", "Chandigarh",
                "Dadra and Nagar Haveli and Daman and Diu", "Lakshadweep"
            ]

            for state in indian_states:
                if re.search(r'\b' + re.escape(state) + r'\b', address, re.IGNORECASE):
                    state_found = state
                    break

            # Clean address
            cleaned_address = address
            if pincode_found:
                cleaned_address = re.sub(
                    r'(\b' + re.escape(pincode_found) + r'\b)(.*)',
                    lambda m: m.group(1) + m.group(2).replace(pincode_found, ''),
                    cleaned_address,
                    count=1
                )

            if state_found:
                cleaned_address = re.sub(
                    r'(\b' + re.escape(state_found) + r'\b)(.*)',
                    lambda m: m.group(1) + m.group(2).replace(state_found, ''),
                    cleaned_address,
                    flags=re.IGNORECASE,
                    count=1
                )

            cleaned_address = re.sub(r'\s{2,}', ' ', cleaned_address).strip()
            cleaned_address = re.sub(r',\s*,', ',', cleaned_address).strip()
            cleaned_address = re.sub(r',\s*$', '', cleaned_address).strip()
            cleaned_address = re.sub(r'(?i)\bGSTIN NO:\s*', '', cleaned_address).strip()

            return {
                "address": cleaned_address,
                "pincode": pincode_found,
                "state": state_found
            }

        except Exception as e:
            logger.error(f"Error extracting details from address: {e}")
            raise

    def create_order(self) -> None:
        """Create order in Base system"""
        try:
            logger.info("Starting order creation process")
            
            # Get order status ID
            logger.info("Fetching order status list")
            status_response = self.call_api('getOrderStatusList')
            status_id = self._get_status_id(status_response, "New orders")
            if not status_id:
                raise ValueError("Status 'New orders' not found")
            logger.info(f"Found status ID: {status_id}")
            self.checklist["Fetch Order Status Id"] = True

            # Get order source ID
            logger.info("Fetching order sources")
            source_response = self.call_api('getOrderSources')
            source_id = self._get_source_id(source_response, "FLIPKART GROCERY")
            if not source_id:
                raise ValueError("Order source 'FLIPKART GROCERY' not found")
            logger.info(f"Found source ID: {source_id}")
            self.checklist["Fetch Order Source Id"] = True

            # Get extra fields
            logger.info("Fetching extra fields")
            extra_fields_response = self.call_api('getOrderExtraFields')
            extra_field_mapping = self._process_extra_fields(
                extra_fields_response,
                ["PO Number", "PO Expiry", "Sender Mail"]
            )
            logger.info(f"Found {len(extra_field_mapping)} extra fields")
            self.checklist["Check And Fetch Custom Field"] = True

            # Check if order exists
            logger.info("Checking if order already exists")
            self._check_order_exists(extra_field_mapping)
            logger.info("Order existence check completed")
            self.checklist["Check if a order already exist"] = True

            # Create order JSON
            logger.info("Creating order JSON")
            order_json = self._create_order_json(status_id, source_id, extra_field_mapping)
            logger.info("Order JSON created successfully")
            self.checklist["Create Order Json"] = True

            # Add order
            logger.info("Adding order to Base system")
            response = self.call_api('addOrder', order_json)
            if response:
                self.checklist["Create order"] = True
                order_id = response.get('order_id')
                logger.info(f"Order created successfully with ID: {order_id}")
                self.send_success_email(order_id, self.extracted_data.get("po"), self.email_data)
            else:
                raise ValueError("Failed to create order")

        except Exception as e:
            logger.error(f"Error creating order: {e}")
            raise

    def _get_status_id(self, data: Dict, target_name: str) -> Optional[int]:
        """Get status ID from response data"""
        for status in data.get("statuses", []):
            if status.get("name").lower() == target_name.lower():
                return status.get("id")
        return None

    def _get_source_id(self, data: Dict, target_name: str) -> Optional[str]:
        """Get source ID from response data"""
        for key, value in data.get("sources", {}).get("personal", {}).items():
            if value == target_name:
                return key
        return None

    def _process_extra_fields(self, data: Dict, extra_fields: List[str]) -> Dict:
        """Process extra fields from response data"""
        field_mapping = {}
        missing_fields = []
        
        extra_fields_response = data.get("extra_fields", [])

        for field in extra_fields:
            field_entry = next(
                (item for item in extra_fields_response if item["name"] == field),
                None
            )
            if field_entry:
                field_mapping[field] = field_entry["extra_field_id"]
            else:
                missing_fields.append(field)

        if missing_fields:
            error_message = f"Missing fields in response: {', '.join(missing_fields)}"
            logger.error(error_message)
            self.send_error_email(error_message)

        return field_mapping

    def _check_order_exists(self, extra_field_mapping: Dict) -> None:
        """Check if order already exists"""
        parameters = {
            "include_custom_extra_fields": True,
            "filter_order_source": "personal",
            "filter_order_source_id": self._get_source_id(
                self.call_api('getOrderSources'),
                "FLIPKART GROCERY"
            )
        }
        
        response = self.call_api("getOrders", parameters)
        target_po_number = self.extracted_data.get('po')
        
        for order in response.get("orders", []):
            if order.get("custom_extra_fields", {}).get(
                str(extra_field_mapping.get('PO Number'))
            ) == str(target_po_number):
                error_message = f"Order exists with ID: {order['order_id']}"
                logger.error(error_message)
                self.send_error_email(error_message)

    def _create_order_json(self, status_id: int, source_id: str, extra_field_mapping: Dict) -> Dict:
        """Create order JSON for API"""
        try:
            logger.info("Starting order JSON creation")
            order_json = {
                "order_status_id": status_id,
                "custom_source_id": source_id,
                "date_add": self._convert_to_unix(self.extracted_data.get('order_date')),
                "currency": "INR",
                "payment_method_cod": "1",
                "paid": "0",
                "email": "",
                "phone": "",
                
                "delivery_fullname": "",
                "delivery_company": "",
                "delivery_address": self.extracted_data.get("shipped_to_address").get("address"),
                "delivery_state": self.extracted_data.get("shipped_to_address").get("state"),
                "delivery_postcode": self.extracted_data.get("shipped_to_address").get("pincode"),
                "delivery_country_code": "IN",
                
                "delivery_point_id": "",
                "delivery_point_name": self.extracted_data.get("supplier_name"),
                "delivery_point_address": self.extracted_data.get("supplier_address").get("address"),
                "delivery_point_postcode": self.extracted_data.get("supplier_address").get("pincode"),

                "invoice_nip": self.extracted_data.get("billed_by_gstin"),
                "invoice_company": "",
                "invoice_address": self.extracted_data.get("billed_to_address").get("address"),
                "invoice_state": self.extracted_data.get("billed_to_address").get("state"),
                "invoice_postcode": self.extracted_data.get("billed_to_address").get("pincode"),
                "invoice_country_code": "IN",
                "products": [],
                "custom_extra_fields": {
                    value: self.extracted_data.get(self.extra_field_to_po_field_mapping.get(key))
                    for key, value in extra_field_mapping.items()
                }
            }

            # Process products
            logger.info("Processing products")
            self.calculated_total = 0
            for item in self.extracted_data.get("Line_Items")[:-1]:
                product = {
                    "name": item.get("Title"),
                    "ean": item.get("FSN/ISBN13").replace(" ", ""),
                    "price_brutto": round(
                        float(item.get("Total Amount", "0").replace(',', '')) /
                        float(item.get("Quantity")),
                        2
                    ),
                    "tax_rate": sum(
                        float(item.get(rate, "0").replace("%", ""))
                        for rate in ["SGST/UTGST Rate", "CGST Rate", "IGST Rate"]
                    ),
                    "quantity": int(item.get("Quantity")),
                }
                order_json["products"].append(product)
                logger.info(f"Added product: {product['name']} (Quantity: {product['quantity']})")

                order_json["custom_extra_fields"][extra_field_mapping.get("Sender Mail")] = self.client_config.sender_email
                
                self.calculated_total += round(
                    product["price_brutto"] * float(item.get("Quantity")),
                    2
                )

                self.sku_map[product["ean"]] = product["price_brutto"]

            # Validate total
            grand_total = float(
                self.extracted_data.get("Line_Items")[-1]
                .get("Total Amount")
                .replace("INR", "")
                .replace(" ", "")
            )
            
            difference = abs(self.calculated_total - grand_total)
            logger.info(f"Calculated total: {self.calculated_total}, Grand total: {grand_total}, Difference: {difference}")
            
            if difference > 5:
                raise ValueError("Calculated total price and PDF total price do not match")

            logger.info("Order JSON created successfully")
            return order_json

        except Exception as e:
            logger.error(f"Error creating order JSON: {e}")
            raise

    def _convert_to_unix(self, date_str: str) -> int:
        """Convert date string to Unix timestamp"""
        try:
            date_str = date_str.strip()
            return int(time.mktime(datetime.strptime(date_str, "%d-%m-%y").timetuple()))
        except Exception as e:
            logger.error(f"Error converting date to Unix timestamp: {e}")
            raise

    def run(self) -> None:
        """Main execution method"""
        try:
            logger.info("Starting Flipkart PO Parser")
            mail = self.connect_mail()
            
            # Process new emails
            logger.info("Checking for new emails in inbox")
            emails = self.fetch_emails(mail)
            logger.info(f"Found {len(emails)} new emails to process")
            
            for email_id in emails:
                logger.info(f"Moving email {email_id} to NEW folder")
                self.move_email(mail, email_id, self.email_config.inbox_label, f"{self.email_config.main_label}/NEW")

            # Process NEW folder
            logger.info("Processing emails in NEW folder")
            new_emails = self.fetch_emails(mail, f"{self.email_config.main_label}/NEW")
            logger.info(f"Found {len(new_emails)} emails in NEW folder to process")
            
            for email_id in new_emails:
                try:
                    logger.info(f"Processing email {email_id}")
                    self.email_data = self.process_email(mail, email_id)
                    if not self.email_data or not self.email_data.get("required_files"):
                        logger.warning(f"No required files found in email {email_id}, skipping")
                        continue

                    self.file_path = self.email_data["required_files"][0]["filepath"]
                    logger.info(f"Processing PO file: {self.file_path}")
                    
                    # Process the PO file
                    self.process_po_file(self.file_path)
                    logger.info("PO file processed successfully")
                    
                    # Create order
                    logger.info("Creating order in Base system")
                    self.create_order()
                    logger.info("Order created successfully")
                    
                    # Move to processed folder
                    logger.info(f"Moving email {email_id} to PROCESSED folder")
                    self.move_email(mail, email_id, f"{self.email_config.main_label}/NEW", f"{self.email_config.main_label}/PROCESSED")

                except Exception as e:
                    logger.error(f"Error processing email {email_id}: {e}")
                    logger.info(f"Moving failed email {email_id} to FAILED folder")
                    self.move_email(mail, email_id, f"{self.email_config.main_label}/NEW", f"{self.email_config.main_label}/FAILED")
                    continue

            logger.info("Logging out from email server")
            mail.logout()
            logger.info("Flipkart PO Parser completed successfully")

        except Exception as e:
            logger.error(f"Error in main execution: {e}")
            raise

def main():
    """Main entry point"""
    try:
        logger.info("Initializing Flipkart PO Parser")
        email_config = EmailConfig()
        client_config = ClientConfig()
        
        parser = FlipkartPOParser(email_config, client_config)
        logger.info("Starting parser execution")
        parser.run()
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise

if __name__ == "__main__":
    main() 
