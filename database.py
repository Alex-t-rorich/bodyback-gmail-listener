import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from datetime import datetime
import re
import json
import logging

load_dotenv()

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
        return psycopg2.connect(
            **self.connection_params,
            cursor_factory=RealDictCursor
        )
    
    def email_already_processed(self, email_id):
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
        lines = [line.strip() for line in email_body.strip().split('\n') if line.strip()]
        
        data = {}
        
        if len(lines) >= 4:
            data['name'] = lines[0]
            
            data['phone'] = lines[1]
            
            data['email'] = lines[2]
            
            data['location'] = lines[3]
            
            remaining_lines = lines[4:]
            data['customer_data'] = '\n'.join(remaining_lines)
        else:
            data['name'] = lines[0] if lines else ''
            data['phone'] = ''
            data['email'] = ''
            data['location'] = ''
            data['customer_data'] = email_body
        
        return data
    
    def clean_text(self, text):
        if not text:
            return ''

        cleaned = re.sub(r'\s+', ' ', text.strip())
        return cleaned
    
    def parse_contact_form(self, email_body):
        data = {}
        
        cleaned_body = re.sub(r'\n\s*\n', '\n', email_body)
        
        name_patterns = [
            r'Name and Surname\*?\s*\n.*?\n.*?\n.*?\n.*?\n.*?\n\s*([A-Z][A-Z\s]+)',
            r'\*?Name and Surname\*?\s*\n+\s*([^\n]+)',
            r'Name and Surname\*?\s*([^\n\r]+)',
            r'Name.*?\n+\s*([A-Z][A-Z\s]+)'
        ]
        
        for pattern in name_patterns:
            name_match = re.search(pattern, email_body, re.IGNORECASE | re.DOTALL)
            if name_match:
                data['name'] = self.clean_text(name_match.group(1))
                break
        
        if 'name' not in data:
            data['name'] = ''
        
        phone_patterns = [
            r'Number \(10 digits\)\*?\s*\n.*?\n.*?\n.*?\n.*?\n.*?\n\s*(\d{10})',
            r'\*?Number.*?\*?\s*\n+\s*([0-9]+)',
            r'Number.*?(\d{10})',
            r'(\d{10})'
        ]
        
        for pattern in phone_patterns:
            phone_match = re.search(pattern, email_body, re.IGNORECASE | re.DOTALL)
            if phone_match:
                data['phone'] = self.clean_text(phone_match.group(1))
                break
        
        if 'phone' not in data:
            data['phone'] = ''
        
        location_patterns = [
            r'Location\s*\n.*?\n.*?\n.*?\n.*?\n.*?\n\s*([^\n]+)',
            r'\*?Location\*?\s*\n+\s*([^\n]+)',
            r'Location\*?\s*([^\n\r]+)',
            r'Location.*?\n+\s*([A-Za-z\s]+)'
        ]
        
        for pattern in location_patterns:
            location_match = re.search(pattern, email_body, re.IGNORECASE | re.DOTALL)
            if location_match:
                data['location'] = self.clean_text(location_match.group(1))
                break
        
        if 'location' not in data:
            data['location'] = ''
        
        goals_patterns = [
            r'Goals, injuries & other details\s*\n.*?\n.*?\n.*?\n.*?\n.*?\n\s*([^\n]+(?:\n[^\n]+)*?)(?=\n\s*Date:|$)',
            r'\*?Goals, injuries & other details\*?\s*\n+\s*([^\n\r]+)',
            r'Goals.*?details.*?\s*([^\n\r]+)',
            r'Goals.*?\n+\s*([^Date:]+)'
        ]
        
        for pattern in goals_patterns:
            goals_match = re.search(pattern, email_body, re.IGNORECASE | re.DOTALL)
            if goals_match:
                data['customer_data'] = self.clean_text(goals_match.group(1))
                break
        
        if 'customer_data' not in data:
            data['customer_data'] = ''
        
        db_logger.info(f"Extracted data: Name='{data.get('name')}', Phone='{data.get('phone')}', Location='{data.get('location')}', Data='{data.get('customer_data')}'")
        
        return data
    
    def split_name(self, full_name):
        if not full_name:
            return '', ''
        
        name_parts = full_name.strip().split(' ', 1)
        first_name = name_parts[0] if name_parts else ''
        last_name = name_parts[1] if len(name_parts) > 1 else ''
        
        return first_name, last_name
    
    def save_lead(self, email_content, email_type):
        try:
            if email_type == 'new_lead':
                parsed_data = self.parse_new_lead(email_content['body'])
            else:
                parsed_data = self.parse_contact_form(email_content['body'])

            first_name, last_name = self.split_name(parsed_data.get('name', ''))

            db_logger.info(f"Saving {email_type} lead: {first_name} {last_name} - {parsed_data.get('email', 'no email')}")

            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # First, get the Customer role ID
                    cur.execute("SELECT id FROM roles WHERE name = 'Customer'")
                    role_result = cur.fetchone()
                    if not role_result:
                        error_logger.error("Customer role not found in database")
                        return None
                    customer_role_id = role_result['id']

                    # Generate a unique email if none provided (to avoid constraint violations)
                    email = parsed_data.get('email', '')
                    if not email:
                        # Generate a placeholder email based on gmail message ID
                        email = f"lead_{email_content['id']}@placeholder.com"

                    # Insert into users table with new schema (UUID id, role_id instead of roles array)
                    cur.execute("""
                        INSERT INTO users (email, password_hash, first_name, last_name, phone_number, location, role_id, active)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING id
                    """, (
                        email,
                        'lead_no_password',
                        first_name,
                        last_name,
                        parsed_data.get('phone', ''),
                        parsed_data.get('location', ''),
                        customer_role_id,
                        False
                    ))

                    user_id = cur.fetchone()['id']
                    db_logger.info(f"Created user with ID: {user_id}")

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

                    # Insert into customers table (user_id is the primary key)
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