#!/usr/bin/env python
import os
import json
import base64
from google.cloud import pubsub_v1
from google.oauth2 import service_account
from dotenv import load_dotenv
import time

# Load environment variables
load_dotenv()

# Configuration
project_id = os.getenv('PROJECT_ID')
subscription_name = os.getenv('SUBSCRIPTION_NAME')
credentials_file = os.getenv('GMAIL_CREDENTIALS_FILE')

# Extract just the subscription ID from the full path
subscription_id = subscription_name.split('/')[-1]

print(f"Starting Gmail Pub/Sub listener for project {project_id}")
print(f"Using subscription: {subscription_id}")
print(f"Using credentials file: {credentials_file}")

# Setup credentials
credentials = service_account.Credentials.from_service_account_file(
    credentials_file,
    scopes=["https://www.googleapis.com/auth/cloud-platform"]
)

# Initialize the Pub/Sub subscriber client
subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
subscription_path = subscriber.subscription_path(project_id, subscription_id)

def process_message(message):
    """Process incoming Pub/Sub message from Gmail."""
    print(f"Received message ID: {message.message_id}")
    # ... rest of the message processing code

# Set up the subscription
streaming_pull_future = subscriber.subscribe(
    subscription_path, callback=process_message
)

print(f"Listening for messages on {subscription_path}")
print("Press Ctrl+C to stop")

# Keep the main thread alive
try:
    while True:
        time.sleep(60)
except KeyboardInterrupt:
    streaming_pull_future.cancel()
    print("\nSubscription cancelled")