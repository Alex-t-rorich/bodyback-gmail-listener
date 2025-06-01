#!/usr/bin/env python
"""
Fix Pub/Sub permissions for Gmail API
This script grants the Gmail service account permission to publish to your topic

DO NOT USE THIS
"""

import os
from google.cloud import pubsub_v1
from google.oauth2 import service_account
from dotenv import load_dotenv

load_dotenv()

def fix_pubsub_permissions():
    """Add Gmail publish permissions to the Pub/Sub topic."""
    
    project_id = os.getenv('PROJECT_ID')
    service_account_file = os.getenv('GMAIL_CREDENTIALS_FILE')
    
    print("🔧 FIXING PUB/SUB PERMISSIONS FOR GMAIL")
    print("=" * 50)
    
    try:
        # Setup credentials with broader scope
        credentials = service_account.Credentials.from_service_account_file(
            service_account_file,
            scopes=[
                "https://www.googleapis.com/auth/cloud-platform",
                "https://www.googleapis.com/auth/pubsub"
            ]
        )
        
        publisher = pubsub_v1.PublisherClient(credentials=credentials)
        topic_path = publisher.topic_path(project_id, 'gmail-push-notification')
        
        print(f"🎯 Topic: {topic_path}")
        print(f"🔧 Adding Gmail service account permissions...")
        
        # Get current policy
        try:
            policy = publisher.get_iam_policy(request={"resource": topic_path})
            print(f"✅ Retrieved current IAM policy")
        except Exception as e:
            print(f"❌ Error getting policy: {e}")
            print(f"💡 Your service account might need 'Pub/Sub Admin' role")
            return False
        
        # Check if Gmail already has permission
        gmail_service_account = "serviceAccount:gmail-api-push@system.gserviceaccount.com"
        has_permission = False
        
        for binding in policy.bindings:
            if binding.role == "roles/pubsub.publisher":
                if gmail_service_account in binding.members:
                    has_permission = True
                    break
        
        if has_permission:
            print(f"✅ Gmail already has publisher permissions!")
        else:
            print(f"🔧 Adding Gmail publisher permissions...")
            
            # Find existing publisher binding or create new one
            publisher_binding = None
            for binding in policy.bindings:
                if binding.role == "roles/pubsub.publisher":
                    publisher_binding = binding
                    break
            
            if publisher_binding:
                # Add to existing binding
                if gmail_service_account not in publisher_binding.members:
                    publisher_binding.members.append(gmail_service_account)
            else:
                # Create new binding
                policy.bindings.add(
                    role="roles/pubsub.publisher",
                    members=[gmail_service_account]
                )
            
            # Update the policy
            try:
                publisher.set_iam_policy(request={"resource": topic_path, "policy": policy})
                print(f"✅ Gmail permissions added successfully!")
            except Exception as e:
                print(f"❌ Error setting policy: {e}")
                return False
        
        # Verify the permission was added
        print(f"🔍 Verifying permissions...")
        updated_policy = publisher.get_iam_policy(request={"resource": topic_path})
        
        gmail_verified = False
        for binding in updated_policy.bindings:
            if binding.role == "roles/pubsub.publisher":
                for member in binding.members:
                    if member == gmail_service_account:
                        gmail_verified = True
                        print(f"✅ Gmail permissions verified!")
                        break
        
        if not gmail_verified:
            print(f"❌ Gmail permissions not found after update")
            return False
            
        print(f"\n🎉 SUCCESS! Gmail can now publish to your Pub/Sub topic")
        print(f"📧 Try sending a test email to see if notifications work")
        
        return True
        
    except Exception as e:
        print(f"❌ Error fixing permissions: {e}")
        print(f"\n💡 Manual fix needed:")
        print(f"   1. Go to Google Cloud Console")
        print(f"   2. Navigate to Pub/Sub → Topics")
        print(f"   3. Click on 'gmail-push-notification'")
        print(f"   4. Go to 'Permissions' tab")
        print(f"   5. Click 'Add Principal'")
        print(f"   6. Principal: gmail-api-push@system.gserviceaccount.com")
        print(f"   7. Role: Pub/Sub Publisher")
        print(f"   8. Click 'Save'")
        return False

if __name__ == "__main__":
    fix_pubsub_permissions()