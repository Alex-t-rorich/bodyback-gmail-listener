#!/usr/bin/env python
"""
Script to check existing Pub/Sub setup and find the correct topic name
"""

import os
from google.cloud import pubsub_v1
from google.oauth2 import service_account
from dotenv import load_dotenv

load_dotenv()

def check_pubsub_setup():
    """Check what Pub/Sub topics and subscriptions exist."""
    
    try:
        # Use service account credentials
        credentials = service_account.Credentials.from_service_account_file(
            'service-credentials.json',
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        
        project_id = 'gmail-pubsub-test-459416'
        
        print(f"🔍 Checking Pub/Sub setup for project: {project_id}")
        print("=" * 60)
        
        # Create publisher and subscriber clients
        publisher = pubsub_v1.PublisherClient(credentials=credentials)
        subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
        
        # List all topics
        print("📡 EXISTING TOPICS:")
        project_path = f"projects/{project_id}"
        topics = publisher.list_topics(request={"project": project_path})
        
        topic_found = False
        for topic in topics:
            topic_name = topic.name.split('/')[-1]
            print(f"  ✅ {topic_name}")
            if 'gmail' in topic_name.lower():
                topic_found = True
        
        if not topic_found:
            print("  ❌ No Gmail-related topics found")
        
        print("\n📮 EXISTING SUBSCRIPTIONS:")
        subscriptions = subscriber.list_subscriptions(request={"project": project_path})
        
        subscription_found = False
        for subscription in subscriptions:
            sub_name = subscription.name.split('/')[-1]
            topic_name = subscription.topic.split('/')[-1] if subscription.topic else "Unknown"
            print(f"  ✅ {sub_name} → {topic_name}")
            if 'gmail' in sub_name.lower():
                subscription_found = True
        
        if not subscription_found:
            print("  ❌ No Gmail-related subscriptions found")
        
        # Check what's in your .env
        print("\n📋 YOUR .ENV SETTINGS:")
        print(f"  PROJECT_ID: {os.getenv('PROJECT_ID', 'NOT SET')}")
        print(f"  SUBSCRIPTION_NAME: {os.getenv('SUBSCRIPTION_NAME', 'NOT SET')}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error checking Pub/Sub: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    check_pubsub_setup()