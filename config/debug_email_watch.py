#!/usr/bin/env python
"""
Debug Gmail watch setup - check everything step by step
"""

import os
import pickle
import json
from googleapiclient.discovery import build
from google.cloud import pubsub_v1
from google.oauth2 import service_account
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

def debug_gmail_watch():
    """Debug the entire Gmail watch → Pub/Sub chain."""
    
    project_id = os.getenv('PROJECT_ID')
    service_account_file = os.getenv('GMAIL_CREDENTIALS_FILE')
    
    print("🔍 DEBUGGING GMAIL → PUB/SUB CONNECTION")
    print("=" * 60)
    
    # Step 1: Check Gmail API
    print("📧 STEP 1: Gmail API Connection")
    try:
        if not os.path.exists('token.pickle'):
            print("❌ token.pickle file not found!")
            return False
            
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
        
        service = build('gmail', 'v1', credentials=creds)
        profile = service.users().getProfile(userId='me').execute()
        email = profile.get('emailAddress', 'Unknown')
        
        print(f"✅ Gmail API working")
        print(f"📧 Email: {email}")
        print(f"📊 Current history ID: {profile.get('historyId', 'Unknown')}")
        
    except Exception as e:
        print(f"❌ Gmail API error: {e}")
        return False
    
    # Step 2: Check Pub/Sub setup
    print(f"\n🔧 STEP 2: Pub/Sub Setup")
    try:
        credentials = service_account.Credentials.from_service_account_file(
            service_account_file,
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        
        publisher = pubsub_v1.PublisherClient(credentials=credentials)
        subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
        
        topic_path = publisher.topic_path(project_id, 'gmail-push-notification')
        subscription_path = subscriber.subscription_path(project_id, 'gmail-push-notification-sub')
        
        # Check topic exists
        try:
            topic = publisher.get_topic(request={"topic": topic_path})
            print(f"✅ Topic exists: {topic.name}")
        except Exception as e:
            print(f"❌ Topic error: {e}")
            return False
        
        # Check subscription exists
        try:
            subscription = subscriber.get_subscription(request={"subscription": subscription_path})
            print(f"✅ Subscription exists: {subscription.name}")
        except Exception as e:
            print(f"❌ Subscription error: {e}")
            return False
            
        # Check permissions
        try:
            policy = publisher.get_iam_policy(request={"resource": topic_path})
            gmail_has_permission = False
            
            for binding in policy.bindings:
                if binding.role == "roles/pubsub.publisher":
                    for member in binding.members:
                        if member == "serviceAccount:gmail-api-push@system.gserviceaccount.com":
                            gmail_has_permission = True
                            break
            
            if gmail_has_permission:
                print(f"✅ Gmail has publish permissions")
            else:
                print(f"❌ Gmail missing publish permissions")
                print(f"🔧 Adding permissions...")
                
                # Add permission
                policy.bindings.add(
                    role="roles/pubsub.publisher",
                    members=["serviceAccount:gmail-api-push@system.gserviceaccount.com"]
                )
                publisher.set_iam_policy(request={"resource": topic_path, "policy": policy})
                print(f"✅ Permissions added!")
                
        except Exception as e:
            print(f"❌ Permission check error: {e}")
    
    except Exception as e:
        print(f"❌ Pub/Sub setup error: {e}")
        return False
    
    # Step 3: Check current watch status and renew
    print(f"\n🔄 STEP 3: Gmail Watch Setup")
    try:
        topic_name = f'projects/{project_id}/topics/gmail-push-notification'
        
        print(f"🎯 Topic name: {topic_name}")
        
        # Try to set up watch
        watch_request = {
            'topicName': topic_name,
            'labelIds': ['INBOX']
        }
        
        print(f"📤 Setting up Gmail watch...")
        result = service.users().watch(userId='me', body=watch_request).execute()
        
        expiration_ms = int(result.get('expiration', 0))
        expiration_date = datetime.fromtimestamp(expiration_ms / 1000)
        
        print(f"✅ Gmail watch set up successfully!")
        print(f"⏰ Expires: {expiration_date}")
        print(f"📊 History ID: {result.get('historyId', 'Unknown')}")
        print(f"🎯 Topic in response: {result.get('topicName', 'Unknown')}")
        
        days_until_expiration = (expiration_date - datetime.now()).days
        print(f"📅 Days until expiration: {days_until_expiration}")
        
    except Exception as e:
        print(f"❌ Gmail watch error: {e}")
        print(f"🔧 This might be the problem!")
        return False
    
    # Step 4: Test with a manual message
    print(f"\n🧪 STEP 4: Manual Pub/Sub Test")
    try:
        test_data = {
            "emailAddress": email,
            "historyId": "test-12345"
        }
        
        print(f"📤 Publishing test message...")
        message_data = json.dumps(test_data).encode('utf-8')
        future = publisher.publish(topic_path, message_data)
        message_id = future.result()
        
        print(f"✅ Test message published: {message_id}")
        print(f"💡 Check if your test_pubsub.py receives this message!")
        
    except Exception as e:
        print(f"❌ Manual test error: {e}")
    
    print(f"\n🎯 SUMMARY:")
    print(f"   If all steps above show ✅, the setup should work")
    print(f"   If you still don't get messages, try:")
    print(f"   1. Wait 1-2 minutes for Gmail to start sending")
    print(f"   2. Send another test email")
    print(f"   3. Check if the manual test message appears")
    
    return True

if __name__ == "__main__":
    debug_gmail_watch()