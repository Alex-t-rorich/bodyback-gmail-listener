#!/usr/bin/env python
import os
import json
import base64
import pickle
from google.cloud import pubsub_v1
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from dotenv import load_dotenv
import time
from datetime import datetime
import re
import logging
from database import BodyBackDB

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Create loggers
body_logger = logging.getLogger('email_body')
body_handler = logging.FileHandler('logs/email_bodies.log')
body_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
body_logger.addHandler(body_handler)
body_logger.setLevel(logging.INFO)

error_logger = logging.getLogger('errors')
error_handler = logging.FileHandler('logs/email_errors.log')
error_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
error_logger.addHandler(error_handler)
error_logger.setLevel(logging.ERROR)

# Configuration
project_id = os.getenv('PROJECT_ID')
subscription_name = os.getenv('SUBSCRIPTION_NAME')
service_account_file = os.getenv('GMAIL_CREDENTIALS_FILE')
oauth_file = os.getenv('GMAIL_OAUTH_FILE')

# Extract just the subscription ID from the full path
subscription_id = subscription_name.split('/')[-1]

print(f"Starting Gmail Pub/Sub listener for project {project_id}")
print(f"Using subscription: {subscription_id}")

# Define email types to watch for
WATCHED_SUBJECTS = {
    'new_lead': 'NEW M LEAD',
    'home_page': 'SA Home page message from "BodyBack"',
    'packages_page': 'SA Packages page message from "BodyBack"'
}

# Initialize database
db = BodyBackDB()

# Test database connection on startup
if not db.test_connection():
    print("❌ Database connection failed - exiting")
    exit(1)

# Setup Pub/Sub credentials
pubsub_credentials = service_account.Credentials.from_service_account_file(
    service_account_file,
    scopes=["https://www.googleapis.com/auth/cloud-platform"]
)

# Initialize the Pub/Sub subscriber client
subscriber = pubsub_v1.SubscriberClient(credentials=pubsub_credentials)
subscription_path = subscriber.subscription_path(project_id, subscription_id)

print(f"🔗 Pub/Sub subscription path: {subscription_path}")

# Setup Gmail API
def get_gmail_service():
    """Get Gmail API service using OAuth credentials."""
    creds = None
    
    # Token file stores the user's access and refresh tokens
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    
    # If there are no (valid) credentials available, let the user log in
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            error_logger.error("No valid Gmail API credentials found")
            return None
            
    return build('gmail', 'v1', credentials=creds)

# Initialize Gmail service
gmail_service = get_gmail_service()

def identify_email_type(subject):
    """Identify the type of email based on subject line."""
    if not subject:
        return None
    
    # Handle forwarded emails - remove "Fwd: " prefix
    clean_subject = re.sub(r'^Fwd:\s*', '', subject, flags=re.IGNORECASE)
    
    # Check against watched subjects
    for email_type, watched_subject in WATCHED_SUBJECTS.items():
        if watched_subject.lower() in clean_subject.lower():
            return email_type
    
    return None

def get_email_content(message_id):
    """Fetch email content from Gmail API."""
    try:
        # Get the email message
        message = gmail_service.users().messages().get(
            userId='me',
            id=message_id,
            format='full'
        ).execute()
        
        # Extract email details
        headers = message['payload'].get('headers', [])
        email_data = {
            'id': message_id,
            'from': None,
            'to': None,
            'subject': None,
            'date': None,
            'body': None,
            'email_type': None,
            'is_forward': False
        }
        
        # Parse headers
        for header in headers:
            name = header.get('name', '').lower()
            value = header.get('value', '')
            
            if name == 'from':
                email_data['from'] = value
            elif name == 'to':
                email_data['to'] = value
            elif name == 'subject':
                email_data['subject'] = value
                email_data['is_forward'] = value.lower().startswith('fwd:')
                email_data['email_type'] = identify_email_type(value)
            elif name == 'date':
                email_data['date'] = value
        
        # Extract body
        body = extract_body(message['payload'])
        email_data['body'] = body
        
        return email_data
        
    except Exception as e:
        error_logger.error(f"Error fetching email content: {e}")
        return None

def extract_body(payload):
    """Extract body from email payload."""
    body = ""
    
    if 'parts' in payload:
        for part in payload['parts']:
            if part['mimeType'] == 'text/plain':
                data = part['body']['data']
                body += base64.urlsafe_b64decode(data).decode('utf-8', errors='ignore')
            elif part['mimeType'] == 'multipart/alternative':
                body += extract_body(part)
    elif payload['body'].get('data'):
        body = base64.urlsafe_b64decode(payload['body']['data']).decode('utf-8', errors='ignore')
    
    return body

def process_watched_email(email_content):
    """Process emails that match our watched subjects."""
    email_type = email_content['email_type']
    
    # Log email body data
    body_logger.info(f"EMAIL_TYPE: {email_type.upper()}")
    body_logger.info(f"SUBJECT: {email_content['subject']}")
    body_logger.info(f"FROM: {email_content['from']}")
    body_logger.info(f"DATE: {email_content['date']}")
    body_logger.info(f"IS_FORWARD: {email_content['is_forward']}")
    body_logger.info(f"BODY_START")
    body_logger.info(email_content['body'])
    body_logger.info(f"BODY_END")
    body_logger.info("="*80)
    
    # Check if already processed (double-check)
    if not db.email_already_processed(email_content['id']):
        user_id = db.save_lead(email_content, email_type)
        if user_id:
            print(f"✅ SUCCESS {email_type.upper()} - Saved lead from {email_content['from']} (User ID: {user_id})")
        else:
            print(f"⚠️  SKIPPED {email_type.upper()} - Invalid data from {email_content['from']}")
            # Changed from ERROR to INFO - this is expected behavior, not an error
            body_logger.info(f"SKIPPED {email_type} - Invalid data format: {email_content['subject']}")
    else:
        print(f"🔄 DUPLICATE {email_type.upper()} - Already processed, skipping")
        body_logger.info(f"DUPLICATE {email_type} - Already processed: {email_content['subject']}")
             
def process_message(message):
    """Process incoming Pub/Sub message from Gmail."""
    print(f"📨 PUB/SUB MESSAGE RECEIVED! Raw data: {message.data}")
    
    try:
        # Decode the message data
        message_data = json.loads(message.data.decode('utf-8'))
        print(f"📧 Decoded message: {message_data}")
        
        # Extract email address and history ID
        email_address = message_data.get('emailAddress')
        history_id = message_data.get('historyId')
        
        print(f"📬 Email: {email_address}")
        print(f"📊 History ID: {history_id}")
        
        if gmail_service and email_address:
            print("🔍 Checking recent emails...")
            
            # Get recent messages (last 5)
            recent_messages = gmail_service.users().messages().list(
                userId='me',
                maxResults=5,
                labelIds=['INBOX']
            ).execute()
            
            if 'messages' in recent_messages:
                print(f"📬 Found {len(recent_messages['messages'])} recent emails")
                
                # Check each recent email
                for msg in recent_messages['messages']:
                    print(f"🔍 Checking email ID: {msg['id']}")
                    
                    # Skip if already processed
                    if db.email_already_processed(msg['id']):
                        print(f"⏭️  Email {msg['id']} already processed - skipping")
                        continue
                    
                    email_content = get_email_content(msg['id'])
                    
                    if email_content:
                        print(f"📋 Subject: {email_content['subject']}")
                        print(f"📋 From: {email_content['from']}")
                        print(f"📋 Email type: {email_content['email_type']}")
                        
                        if email_content['email_type']:
                            print(f"🎯 MATCHED WATCHED EMAIL TYPE: {email_content['email_type']}")
                            process_watched_email(email_content)
                            break  # Process only the first matching email
                        else:
                            print("⏭️  Email type not watched - checking next email")
                    else:
                        print(f"❌ Failed to get content for email {msg['id']}")
            else:
                print("📭 No recent messages found")
        else:
            print("❌ Gmail service not available or no email address in message")
            
    except Exception as e:
        print(f"❌ Error in process_message: {e}")
        error_logger.error(f"Error in process_message: {e}")
        import traceback
        error_logger.error(traceback.format_exc())
    
    try:
        # Acknowledge the message
        message.ack()
        print("✅ Message acknowledged")
    except Exception as e:
        print(f"❌ Error acknowledging message: {e}")
        error_logger.error(f"Error acknowledging message: {e}")

# Set up the subscription
print(f"🔄 Setting up Pub/Sub subscription...")
streaming_pull_future = subscriber.subscribe(
    subscription_path, callback=process_message
)
print(f"🎯 Pub/Sub subscription active!")

print(f"✅ Database connected")
print(f"📧 Watching for: NEW M LEAD, Home page, Packages page")
print(f"📝 Logging email bodies to: email_bodies.log")
print(f"❌ Logging errors to: errors.log")
print(f"🎧 Listening for new emails...")
print("Press Ctrl+C to stop\n")

# Keep the main thread alive
try:
    while True:
        time.sleep(60)
        print(f"⏰ Still listening... {datetime.now().strftime('%H:%M:%S')}")
except KeyboardInterrupt:
    streaming_pull_future.cancel()
    print("\n🛑 Stopped")