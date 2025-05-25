#!/usr/bin/env python
import os
import pickle
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
project_id = os.getenv('PROJECT_ID')
topic_name = os.getenv('TOPIC_NAME')

# Get OAuth credentials file from environment
oauth_credentials_file = os.getenv('GMAIL_OAUTH_FILE', 'oauth-credentials.json')

# Gmail API scopes
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

def setup_gmail_watch():
    """Set up Gmail API push notifications to Pub/Sub."""
    
    print("Gmail Watch Setup")
    print("=================")
    print(f"Project ID: {project_id}")
    print(f"Topic: {topic_name}")
    print(f"OAuth credentials file: {oauth_credentials_file}")
    print("")
    
    # Get credentials
    creds = None
    
    # Check if token file exists
    if os.path.exists('token.pickle'):
        print("Found existing authentication token...")
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    
    # If no credentials or they're invalid, get new ones
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            print("Refreshing expired credentials...")
            creds.refresh(Request())
        else:
            print("No valid credentials found. Starting OAuth flow...")
            print("A browser window will open for authentication.")
            flow = InstalledAppFlow.from_client_secrets_file(oauth_credentials_file, SCOPES)
            creds = flow.run_local_server(port=0)
            
        # Save credentials for future use
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
            print("Saved authentication token for future use.")
    
    # Build Gmail API service
    print("\nConnecting to Gmail API...")
    service = build('gmail', 'v1', credentials=creds)
    
    # Get the full topic name
    if topic_name.startswith('projects/'):
        full_topic_name = topic_name
    else:
        full_topic_name = f'projects/{project_id}/topics/{topic_name.split("/")[-1]}'
    
    print(f"Setting up watch for topic: {full_topic_name}")
    
    try:
        # Set up the watch
        response = service.users().watch(
            userId='me',
            body={
                'topicName': full_topic_name,
                'labelIds': ['INBOX']  # Watch inbox only (you can add more labels if needed)
            }
        ).execute()
        
        print("\n✅ SUCCESS! Gmail watch has been set up!")
        print(f"History ID: {response.get('historyId')}")
        print(f"Expiration: {response.get('expiration')} (milliseconds since epoch)")
        
        # Convert expiration to readable format
        import datetime
        expiration_ms = int(response.get('expiration', 0))
        expiration_date = datetime.datetime.fromtimestamp(expiration_ms / 1000)
        print(f"Watch expires at: {expiration_date}")
        print(f"\nNote: You'll need to renew the watch before it expires (usually 7 days).")
        
        return response
        
    except Exception as e:
        print(f"\n❌ ERROR setting up Gmail watch: {e}")
        print("\nPossible issues:")
        print("1. Make sure the Gmail API is enabled in your project")
        print("2. Verify that gmail-api-push@system.gserviceaccount.com has Publisher permission on your topic")
        print("3. Check that your OAuth credentials are correct")
        print("4. Ensure you're authenticating with the correct Gmail account")
        return None

if __name__ == '__main__':
    setup_gmail_watch()