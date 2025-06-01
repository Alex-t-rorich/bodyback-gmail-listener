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
    
    def clean_text(self, text):
        """Clean extracted text by removing extra whitespace and formatting."""
        if not text:
            return ''
        # Remove extra whitespace and newlines
        cleaned = re.sub(r'\s+', ' ', text.strip())
        return cleaned
    
    def parse_contact_form(self, email_body):
        """Parse SA Home/Packages page contact form with improved regex patterns."""
        data = {}
        
        # Clean the email body first - remove excessive whitespace but keep structure
        cleaned_body = re.sub(r'\n\s*\n', '\n', email_body)
        
        # Extract name - more flexible pattern that handles HTML formatting
        name_patterns = [
            r'Name and Surname\*?\s*\n.*?\n.*?\n.*?\n.*?\n.*?\n\s*([A-Z][A-Z\s]+)',  # Pattern for your example
            r'\*?Name and Surname\*?\s*\n+\s*([^\n]+)',  # Alternative pattern
            r'Name and Surname\*?\s*([^\n\r]+)',  # Simple pattern
            r'Name.*?\n+\s*([A-Z][A-Z\s]+)'  # Even more flexible
        ]
        
        for pattern in name_patterns:
            name_match = re.search(pattern, email_body, re.IGNORECASE | re.DOTALL)
            if name_match:
                data['name'] = self.clean_text(name_match.group(1))
                break
        
        if 'name' not in data:
            data['name'] = ''
        
        # Extract phone - look for 10 digit numbers
        phone_patterns = [
            r'Number \(10 digits\)\*?\s*\n.*?\n.*?\n.*?\n.*?\n.*?\n\s*(\d{10})',  # Pattern for your example
            r'\*?Number.*?\*?\s*\n+\s*([0-9]+)',  # Alternative pattern
            r'Number.*?(\d{10})',  # Simple 10-digit pattern
            r'(\d{10})'  # Just find any 10-digit number
        ]
        
        for pattern in phone_patterns:
            phone_match = re.search(pattern, email_body, re.IGNORECASE | re.DOTALL)
            if phone_match:
                data['phone'] = self.clean_text(phone_match.group(1))
                break
        
        if 'phone' not in data:
            data['phone'] = ''
        
        # Extract location
        location_patterns = [
            r'Location\s*\n.*?\n.*?\n.*?\n.*?\n.*?\n\s*([^\n]+)',  # Pattern for your example
            r'\*?Location\*?\s*\n+\s*([^\n]+)',  # Alternative pattern  
            r'Location\*?\s*([^\n\r]+)',  # Simple pattern
            r'Location.*?\n+\s*([A-Za-z\s]+)'  # Flexible pattern
        ]
        
        for pattern in location_patterns:
            location_match = re.search(pattern, email_body, re.IGNORECASE | re.DOTALL)
            if location_match:
                data['location'] = self.clean_text(location_match.group(1))
                break
        
        if 'location' not in data:
            data['location'] = ''
        
        # Extract goals/details
        goals_patterns = [
            r'Goals, injuries & other details\s*\n.*?\n.*?\n.*?\n.*?\n.*?\n\s*([^\n]+(?:\n[^\n]+)*?)(?=\n\s*Date:|$)',  # Pattern for your example
            r'\*?Goals, injuries & other details\*?\s*\n+\s*([^\n\r]+)',  # Alternative pattern
            r'Goals.*?details.*?\s*([^\n\r]+)',  # Simple pattern
            r'Goals.*?\n+\s*([^Date:]+)'  # Everything until Date:
        ]
        
        for pattern in goals_patterns:
            goals_match = re.search(pattern, email_body, re.IGNORECASE | re.DOTALL)
            if goals_match:
                data['customer_data'] = self.clean_text(goals_match.group(1))
                break
        
        if 'customer_data' not in data:
            data['customer_data'] = ''
        
        # Log what we extracted for debugging
        db_logger.info(f"Extracted data: Name='{data.get('name')}', Phone='{data.get('phone')}', Location='{data.get('location')}', Data='{data.get('customer_data')}'")
        
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