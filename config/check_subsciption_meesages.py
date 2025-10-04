#!/usr/bin/env python
"""
Check if messages are stuck in the subscription
"""

import os
from google.cloud import pubsub_v1
from google.oauth2 import service_account
from dotenv import load_dotenv

load_dotenv()

def check_subscription_status():
    """Check subscription metrics and try different pull methods."""
    
    project_id = os.getenv('PROJECT_ID')
    service_account_file = os.getenv('GMAIL_CREDENTIALS_FILE')
    subscription_id = 'gmail-push-notification-sub'
    
    print("🔍 CHECKING SUBSCRIPTION STATUS")
    print("=" * 50)
    
    try:
        credentials = service_account.Credentials.from_service_account_file(
            service_account_file,
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        
        subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
        subscription_path = subscriber.subscription_path(project_id, subscription_id)
        
        print(f"🎯 Subscription: {subscription_path}")
        
        # Get subscription details
        try:
            subscription = subscriber.get_subscription(request={"subscription": subscription_path})
            print(f"✅ Subscription exists")
            print(f"📊 Topic: {subscription.topic}")
            print(f"⏰ Message retention: {subscription.message_retention_duration}")
            print(f"🔄 Ack deadline: {subscription.ack_deadline_seconds}s")
        except Exception as e:
            print(f"❌ Error getting subscription details: {e}")
            return False
        
        # Try synchronous pull to see if there are any messages waiting
        print(f"\n🔍 Checking for pending messages...")
        try:
            request = pubsub_v1.PullRequest(
                subscription=subscription_path,
                max_messages=10,
            )
            
            response = subscriber.pull(request=request, timeout=5.0)
            
            if response.received_messages:
                print(f"📨 Found {len(response.received_messages)} pending messages!")
                
                for i, received_message in enumerate(response.received_messages):
                    print(f"\n📧 Message {i+1}:")
                    print(f"   Data: {received_message.message.data}")
                    print(f"   Attributes: {received_message.message.attributes}")
                    print(f"   Publish time: {received_message.message.publish_time}")
                    
                    # Acknowledge the message
                    subscriber.acknowledge(
                        subscription=subscription_path,
                        ack_ids=[received_message.ack_id]
                    )
                    print(f"   ✅ Message acknowledged")
                
            else:
                print(f"📭 No pending messages found")
                
        except Exception as e:
            print(f"❌ Error pulling messages: {e}")
        
        # Try to recreate the subscription with different settings
        print(f"\n🔧 Testing subscription recreation...")
        try:
            # Delete existing subscription
            print(f"🗑️  Deleting existing subscription...")
            subscriber.delete_subscription(request={"subscription": subscription_path})
            print(f"✅ Subscription deleted")
            
            # Recreate with optimal settings
            topic_path = subscriber.topic_path(project_id, 'gmail-push-notification')
            
            print(f"🔧 Creating new subscription...")
            request = pubsub_v1.Subscription(
                name=subscription_path,
                topic=topic_path,
                ack_deadline_seconds=60,  # Longer ack deadline
                enable_message_ordering=False
            )
            
            subscription = subscriber.create_subscription(request={
                "name": subscription_path,
                "topic": topic_path,
                "ack_deadline_seconds": 60
            })
            
            print(f"✅ New subscription created!")
            print(f"📊 Name: {subscription.name}")
            print(f"🎯 Topic: {subscription.topic}")
            
        except Exception as e:
            print(f"❌ Error recreating subscription: {e}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error checking subscription: {e}")
        return False

if __name__ == "__main__":
    check_subscription_status()
    
    print(f"\n💡 Next steps:")
    print(f"   1. Try running your test_pubsub.py again")
    print(f"   2. Send a test email while it's running")
    print(f"   3. The subscription has been recreated with better settings")