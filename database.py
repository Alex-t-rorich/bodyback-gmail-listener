#!/usr/bin/env python
"""
Database module for BodyBack Gmail listener
Handles all database operations for leads and customer inquiries
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from datetime import datetime
import re
import json
import logging

# Load environment variables
load_dotenv()

# Set up database logger
db_logger = logging.getLogger('database')
db_handler = logging.FileHandler('logs/database.log')
db_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
db_logger.addHandler(db_handler)
db_logger.setLevel(logging.INFO)

error_logger = logging.getLogger('errors')

class BodyBackDB:
    def __init__(self):
        self.connection_params = {
            'host': os.getenv('DB_HOST'),
            'port': os.getenv('DB_PORT'),
            'database': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
        }
        db_logger.info(f"Database configured: {self.connection_params['host']}:{self.connection_params['port']}/{self.connection_params['database']}")
    
    def get_connection(self):
        """Get database connection."""
        return psycopg2.connect(
            **self.connection_params,
            cursor_factory=RealDictCursor
        )
    
    def email_already_processed(self, email_id):
        """Check if email has already been processed."""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT 1 FROM customers WHERE profile_data->>'gmail_id' = %s
                        LIMIT 1
                    """, (email_id,))
                    
                    result = cur.fetchone() is not None
                    if result:
                        db_logger.info(f"Email {email_id} already processed - skipping")
                    return result
        except Exception as e:
            error_logger.error(f"Error checking if email processed: {e}")
            return False
    
    def parse_new_lead(self, email_body):
        """Parse NEW M LEAD email body - first 4 lines are structured."""
        lines = [line.strip() for line in email_body.strip().split('\n') if line.strip()]
        
        data = {}
        
        if len(lines) >= 4:
            # Line 1: Name
            data['name'] = lines[0]
            
            # Line 2: Phone
            data['phone'] = lines[1]
            
            # Line 3: Email  
            data['email'] = lines[2]
            
            # Line 4: Location
            data['location'] = lines[3]
            
            # Everything else goes into customer_data
            remaining_lines = lines[4:]
            data['customer_data'] = '\n'.join(remaining_lines)
        else:
            # Fallback if format is different
            data['name'] = lines[0] if lines else ''
            data['phone'] = ''
            data['email'] = ''
            data['location'] = ''
            data['customer_data'] = email_body
        
        return data
    
    def parse_contact_form(self, email_body):
        """Parse SA Home/Packages page contact form."""
        data = {}
        
        # Extract name
        name_match = re.search(r'\*Name and Surname\*\*?\s*([^\n\r\*]+)', email_body, re.IGNORECASE)
        if name_match:
            data['name'] = name_match.group(1).strip()
        
        # Extract phone
        phone_match = re.search(r'\*Number \(10 digits\)\*\*?\s*([0-9]+)', email_body, re.IGNORECASE)
        if phone_match:
            data['phone'] = phone_match.group(1).strip()
        
        # Extract location
        location_match = re.search(r'\*Location\*\s*([^\n\r\*]+)', email_body, re.IGNORECASE)
        if location_match:
            data['location'] = location_match.group(1).strip()
        
        # Extract goals/details
        goals_match = re.search(r'\*Goals, injuries & other details\*\s*([^\n\r\*]+)', email_body, re.IGNORECASE)
        if goals_match:
            data['customer_data'] = goals_match.group(1).strip()
        else:
            data['customer_data'] = ''
        
        return data
    
    def split_name(self, full_name):
        """Split full name into first and last name."""
        if not full_name:
            return '', ''
        
        name_parts = full_name.strip().split(' ', 1)
        first_name = name_parts[0] if name_parts else ''
        last_name = name_parts[1] if len(name_parts) > 1 else ''
        
        return first_name, last_name
    
    def save_lead(self, email_content, email_type):
        """Save any type of lead to database."""
        try:
            # Parse based on email type
            if email_type == 'new_lead':
                parsed_data = self.parse_new_lead(email_content['body'])
            else:  # home_page or packages_page
                parsed_data = self.parse_contact_form(email_content['body'])
            
            # Split name
            first_name, last_name = self.split_name(parsed_data.get('name', ''))
            
            db_logger.info(f"Saving {email_type} lead: {first_name} {last_name} - {parsed_data.get('email', 'no email')}")
            
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Insert into users table first
                    cur.execute("""
                        INSERT INTO users (email, password_hash, first_name, last_name, phone_number, location, roles, active)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING id
                    """, (
                        parsed_data.get('email', ''),
                        'lead_no_password',  # Placeholder for leads
                        first_name,
                        last_name,
                        parsed_data.get('phone', ''),
                        parsed_data.get('location', ''),
                        ['lead'],  # Role as lead
                        True  # Active by default
                    ))
                    
                    user_id = cur.fetchone()['id']
                    db_logger.info(f"Created user with ID: {user_id}")
                    
                    # Prepare profile data for customers table
                    profile_data = {
                        'gmail_id': email_content['id'],
                        'source': email_type,
                        'customer_data': parsed_data.get('customer_data', ''),
                        'original_email_subject': email_content['subject'],
                        'original_email_from': email_content['from'],
                        'original_email_date': email_content['date'],
                        'is_forward': email_content['is_forward'],
                        'processed_at': datetime.now().isoformat()
                    }
                    
                    # Insert into customers table
                    cur.execute("""
                        INSERT INTO customers (user_id, profile_data)
                        VALUES (%s, %s)
                        RETURNING user_id
                    """, (
                        user_id,
                        json.dumps(profile_data)
                    ))
                    
                    customer_user_id = cur.fetchone()['user_id']
                    db_logger.info(f"Created customer record for user ID: {customer_user_id}")
                    
                    conn.commit()
                    db_logger.info(f"✅ {email_type} lead saved successfully! User ID: {user_id}")
                    return user_id
                    
        except psycopg2.IntegrityError as e:
            error_logger.error(f"Database integrity error (possibly duplicate email): {e}")
            return None
        except Exception as e:
            error_logger.error(f"Error saving lead to database: {e}")
            import traceback
            error_logger.error(traceback.format_exc())
            return None
    
    def test_connection(self):
        """Test database connection."""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) as count FROM users")
                    result = cur.fetchone()
                    db_logger.info(f"Database connection successful! Users count: {result['count']}")
                    return True
        except Exception as e:
            error_logger.error(f"Database connection failed: {e}")
            return False