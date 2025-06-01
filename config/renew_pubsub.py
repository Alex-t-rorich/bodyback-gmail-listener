#!/usr/bin/env python
"""
Script to renew Gmail push notification watch
Run this when Pub/Sub stops receiving messages (every 7 days max)
"""

import os
import pickle
from googleapiclient.discovery import build
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

# Add PROJECT_ID to your .env file
PROJECT_ID = 'gmail-pubsub-test-459416'

def create_pubsub_topic_if_needed():
    """Create Pub/Sub topic if it doesn't exist."""
    from google.cloud import pubsub_v1
    from google.oauth2 import service_account
    
    try:
        project_id = PROJECT_ID or os.getenv('PROJECT_ID')
        topic_name = f'projects/{project_id}/topics/gmail-push-notification'
        
        # Create publisher client
        credentials = service_account.Credentials.from_service_account_file(
            os.getenv('GMAIL_CREDENTIALS_FILE'),
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        publisher = pubsub_v1.PublisherClient(credentials=credentials)
        
        # Try to get the topic (this will fail if it doesn't exist)
        try:
            publisher.get_topic(request={"topic": topic_name})
            print(f"✅ Pub/Sub topic already exists: gmail-push-notification")
        except Exception:
            # Topic doesn't exist, create it
            print(f"🔧 Creating Pub/Sub topic: gmail-push-notification")
            publisher.create_topic(request={"name": topic_name})
            print(f"✅ Pub/Sub topic created successfully!")
            
        return True
    except Exception as e:
        print(f"❌ Error with Pub/Sub topic: {e}")
        return False

def renew_gmail_watch():
    """Renew Gmail push notification watch."""
    
    project_id = os.getenv('PROJECT_ID')
    
    # First, ensure the Pub/Sub topic exists
    if not create_pubsub_topic_if_needed():
        return False
    
    topic_name = f'projects/{project_id}/topics/gmail-push-notification'
    
    print(f"🔄 Renewing Gmail watch for project: {project_id}")
    print(f"📡 Topic: {topic_name}")
    
    try:
        # Load Gmail credentials
        if not os.path.exists('token.pickle'):
            print("❌ token.pickle file not found!")
            print("Make sure you're running this from the project directory.")
            return False
            
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
        
        # Build Gmail service
        service = build('gmail', 'v1', credentials=creds)
        
        # Get current user info
        profile = service.users().getProfile(userId='me').execute()
        email = profile.get('emailAddress', 'Unknown')
        print(f"📧 Gmail account: {email}")
        
        # Set up watch request
        watch_request = {
            'topicName': topic_name,
            'labelIds': ['INBOX']  # Watch INBOX for new emails
        }
        
        print("🔍 Sending watch request...")
        
        # Renew the watch
        result = service.users().watch(userId='me', body=watch_request).execute()
        
        # Parse expiration time (it's in milliseconds)
        expiration_ms = int(result.get('expiration', 0))
        expiration_date = datetime.fromtimestamp(expiration_ms / 1000)
        
        print("✅ Gmail watch renewed successfully!")
        print(f"⏰ Expires at: {expiration_date}")
        print(f"📊 History ID: {result.get('historyId', 'Unknown')}")
        print(f"🎯 Topic: {result.get('topicName', 'Unknown')}")
        
        # Calculate days until expiration
        days_until_expiration = (expiration_date - datetime.now()).days
        print(f"📅 Days until expiration: {days_until_expiration}")
        
        if days_until_expiration <= 1:
            print("⚠️  WARNING: Watch expires soon! Consider setting up auto-renewal.")
        
        return True
        
    except Exception as e:
        print(f"❌ Error renewing Gmail watch: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("=" * 60)
    print("🔄 Gmail Push Notification Watch Renewal")
    print("=" * 60)
    
    success = renew_gmail_watch()
    
    if success:
        print("\n🎉 Watch renewal completed!")
        print("📨 Your Gmail listener should now receive Pub/Sub messages again.")
        print("💡 Remember to renew the watch every 6-7 days to avoid interruptions.")
    else:
        print("\n❌ Watch renewal failed!")
        print("🔧 Check your credentials and try again.")
    
    print("\n" + "=" * 60)