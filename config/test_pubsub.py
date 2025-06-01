#!/usr/bin/env python
"""
Continuous test script to check if Pub/Sub subscription is receiving ANY messages
Run until stopped with Ctrl+C
"""

import os
from google.cloud import pubsub_v1
from google.oauth2 import service_account
from dotenv import load_dotenv
import time
from datetime import datetime

load_dotenv()

project_id = os.getenv('PROJECT_ID')
subscription_name = os.getenv('SUBSCRIPTION_NAME')
service_account_file = os.getenv('GMAIL_CREDENTIALS_FILE')

# Extract subscription ID
subscription_id = subscription_name.split('/')[-1]

print(f"🔍 Testing Pub/Sub subscription: {subscription_id}")
print(f"📡 Project: {project_id}")

# Setup credentials
credentials = service_account.Credentials.from_service_account_file(
    service_account_file,
    scopes=["https://www.googleapis.com/auth/cloud-platform"]
)

# Initialize subscriber
subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
subscription_path = subscriber.subscription_path(project_id, subscription_id)

print(f"🔗 Subscription path: {subscription_path}")

message_count = 0

def test_callback(message):
    global message_count
    message_count += 1
    
    print(f"\n📨 MESSAGE #{message_count} RECEIVED!")
    print(f"🕐 Time: {datetime.now().strftime('%H:%M:%S')}")
    print(f"📊 Data: {message.data}")
    print(f"🏷️  Attributes: {message.attributes}")
    print(f"⏰ Publish time: {message.publish_time}")
    print("-" * 50)
    message.ack()

print("🎧 Listening for Pub/Sub messages continuously...")
print("📧 Send yourself test emails to see messages come through")
print("🛑 Press Ctrl+C to stop")
print("⏰ Started at:", datetime.now().strftime('%H:%M:%S'))
print("=" * 60)

# Pull messages continuously
streaming_pull_future = subscriber.subscribe(subscription_path, callback=test_callback)

try:
    # Keep listening indefinitely until Ctrl+C
    while True:
        time.sleep(60)  # Check every minute
        print(f"⏰ Still listening... {datetime.now().strftime('%H:%M:%S')} (Messages received: {message_count})")
        
except KeyboardInterrupt:
    print(f"\n🛑 Stopping listener...")
    streaming_pull_future.cancel()
    print(f"📊 Total messages received: {message_count}")
    print(f"🕐 Stopped at: {datetime.now().strftime('%H:%M:%S')}")
    
except Exception as e:
    print(f"❌ Error: {e}")
    streaming_pull_future.cancel()

print("\n📊 Test completed!")
print("💡 Summary:")
print(f"   📨 Total messages received: {message_count}")
if message_count > 0:
    print("   ✅ Pub/Sub is working correctly!")
    print("   🔧 Issue is likely in your main listener parsing logic")
else:
    print("   ❌ No messages received")
    print("   🔧 Issue is between Gmail → Pub/Sub (permissions, watch, etc.)")