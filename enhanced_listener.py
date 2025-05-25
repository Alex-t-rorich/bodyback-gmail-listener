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

# Load environment variables
load_dotenv()

# Configuration
project_id = os.getenv('PROJECT_ID')
subscription_name = os.getenv('SUBSCRIPTION_NAME')
service_account_file = os.getenv('GMAIL_CREDENTIALS_FILE')  # For Pub/Sub
oauth_file = os.getenv('GMAIL_OAUTH_FILE')  # For Gmail API

# Extract just the subscription ID from the full path
subscription_id = subscription_name.split('/')[-1]

print(f"Starting Gmail Pub/Sub listener for project {project_id}")
print(f"Using subscription: {subscription_id}")
print(f"Using service account: {service_account_file}")
print(f"Using OAuth credentials: {oauth_file}")

# Setup Pub/Sub credentials
pubsub_credentials = service_account.Credentials.from_service_account_file(
    service_account_file,
    scopes=["https://www.googleapis.com/auth/cloud-platform"]
)

# Initialize the Pub/Sub subscriber client
subscriber = pubsub_v1.SubscriberClient(credentials=pubsub_credentials)
subscription_path = subscriber.subscription_path(project_id, subscription_id)

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
            print("Error: No valid Gmail API credentials found. Please run the watch setup script first.")
            return None
            
    return build('gmail', 'v1', credentials=creds)

# Initialize Gmail service
gmail_service = get_gmail_service()

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
            'threadId': message.get('threadId'),
            'snippet': message.get('snippet'),
            'from': None,
            'to': None,
            'subject': None,
            'date': None,
            'body': None
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
            elif name == 'date':
                email_data['date'] = value
        
        # Extract body
        body = extract_body(message['payload'])
        email_data['body'] = body
        
        return email_data
        
    except Exception as e:
        print(f"Error fetching email content: {e}")
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

def get_history_changes(history_id):
    """Get email changes since the given history ID."""
    try:
        # Get history of changes
        history = gmail_service.users().history().list(
            userId='me',
            startHistoryId=history_id,
            historyTypes=['messageAdded']
        ).execute()
        
        messages = []
        if 'history' in history:
            for history_record in history['history']:
                if 'messagesAdded' in history_record:
                    for added_message in history_record['messagesAdded']:
                        message_id = added_message['message']['id']
                        messages.append(message_id)
        
        return messages
        
    except Exception as e:
        print(f"Error getting history: {e}")
        return []

def process_message(message):
    """Process incoming Pub/Sub message from Gmail."""
    print(f"\n{'='*60}")
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] New Pub/Sub Message")
    print(f"Message ID: {message.message_id}")
    
    # Decode the message data
    try:
        payload = message.data.decode('utf-8')
        data = json.loads(payload)
        
        print(f"Email Address: {data.get('emailAddress', 'Unknown')}")
        print(f"History ID: {data.get('historyId', 'Unknown')}")
        
        # Get new messages from history
        history_id = data.get('historyId')
        if history_id and gmail_service:
            message_ids = get_history_changes(history_id)
            
            if message_ids:
                print(f"\nFound {len(message_ids)} new message(s)")
                
                for msg_id in message_ids:
                    email_content = get_email_content(msg_id)
                    
                    if email_content:
                        print(f"\n--- Email Details ---")
                        print(f"From: {email_content['from']}")
                        print(f"To: {email_content['to']}")
                        print(f"Subject: {email_content['subject']}")
                        print(f"Date: {email_content['date']}")
                        print(f"Snippet: {email_content['snippet']}")
                        print(f"\n--- Email Body ---")
                        print(email_content['body'][:500])  # First 500 chars
                        if len(email_content['body']) > 500:
                            print("... (truncated)")
                        print("-" * 40)
            else:
                print("No new messages found in history")
        else:
            print("No Gmail service available or no history ID")
            
    except json.JSONDecodeError:
        print("Could not parse message as JSON")
    except Exception as e:
        print(f"Error processing message: {e}")
    
    # Acknowledge the message
    message.ack()
    print(f"Message acknowledged")
    print("="*60)

# Set up the subscription
streaming_pull_future = subscriber.subscribe(
    subscription_path, callback=process_message
)

print(f"\nListening for messages on {subscription_path}")
print("Press Ctrl+C to stop\n")

# Keep the main thread alive
try:
    while True:
        time.sleep(60)
except KeyboardInterrupt:
    streaming_pull_future.cancel()
    print("\nSubscription cancelled")