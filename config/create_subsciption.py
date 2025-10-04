#!/usr/bin/env python
"""
EMERGENCY: Recreate the deleted subscription
"""

import os
from google.cloud import pubsub_v1
from google.oauth2 import service_account
from dotenv import load_dotenv

load_dotenv()

def recreate_subscription():
    """Recreate the deleted subscription."""
    
    project_id = os.getenv('PROJECT_ID')
    service_account_file = os.getenv('GMAIL_CREDENTIALS_FILE')
    
    print("🚨 EMERGENCY: RECREATING DELETED SUBSCRIPTION")
    print("=" * 50)
    
    try:
        credentials = service_account.Credentials.from_service_account_file(
            service_account_file,
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        
        subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
        topic_path = f"projects/{project_id}/topics/gmail-push-notification"
        subscription_path = f"projects/{project_id}/subscriptions/gmail-push-notification-sub"
        
        print(f"🎯 Topic: {topic_path}")
        print(f"🎯 Subscription: {subscription_path}")
        
        # Create the subscription
        print(f"🔧 Creating subscription...")
        
        subscription = subscriber.create_subscription(
            request={
                "name": subscription_path,
                "topic": topic_path,
                "ack_deadline_seconds": 60
            }
        )
        
        print(f"✅ SUCCESS! Subscription recreated!")
        print(f"📊 Name: {subscription.name}")
        print(f"🎯 Topic: {subscription.topic}")
        print(f"⏰ Ack deadline: {subscription.ack_deadline_seconds}s")
        
        # Test the subscription
        print(f"\n🧪 Testing subscription...")
        test_subscription = subscriber.get_subscription(request={"subscription": subscription_path})
        print(f"✅ Subscription verified: {test_subscription.name}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error recreating subscription: {e}")
        print(f"\n🔧 Manual creation needed:")
        print(f"   1. Go to Google Cloud Console")
        print(f"   2. Navigate to Pub/Sub → Subscriptions")
        print(f"   3. Click 'Create Subscription'")
        print(f"   4. Subscription ID: gmail-push-notification-sub")
        print(f"   5. Topic: gmail-push-notification")
        print(f"   6. Delivery Type: Pull")
        print(f"   7. Click 'Create'")
        return False

if __name__ == "__main__":
    success = recreate_subscription()
    
    if success:
        print(f"\n🎉 FIXED! Your subscription is back!")
        print(f"📧 Now run your test_pubsub.py again")
        print(f"✉️  Send a test email to see if it works")
    else:
        print(f"\n❌ Manual fix needed - see instructions above")
    
    print(f"\n" + "=" * 50)