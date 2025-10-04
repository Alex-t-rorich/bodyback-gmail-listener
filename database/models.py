import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from datetime import datetime
import json
import logging

load_dotenv()

os.makedirs('logs', exist_ok=True)

db_logger = logging.getLogger('database')
if not db_logger.handlers:
    db_handler = logging.FileHandler('logs/database.log', encoding='utf-8')
    db_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    db_logger.addHandler(db_handler)
    db_logger.setLevel(logging.INFO)

error_logger = logging.getLogger('errors')


class DatabaseConnection:
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


class EmailProcessor:
    def __init__(self, db_connection):
        self.db = db_connection
    
    def email_already_processed(self, email_id):
        try:
            with self.db.get_connection() as conn:
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
    
    def lead_already_exists(self, parsed_data, email_type):
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    if email_type in ['home_page', 'packages_page']:
                        phone = parsed_data.get('phone', '').strip()
                        if phone:
                            cur.execute("""
                                SELECT 1 FROM users WHERE phone_number = %s
                                LIMIT 1
                            """, (phone,))
                            
                            result = cur.fetchone() is not None
                            if result:
                                db_logger.info(f"Contact form lead with phone {phone} already exists - skipping")
                            return result
                    
                    elif email_type == 'new_lead':
                        email = parsed_data.get('email', '').strip()
                        if email:
                            cur.execute("""
                                SELECT 1 FROM users WHERE email = %s
                                LIMIT 1
                            """, (email,))
                            
                            result = cur.fetchone() is not None
                            if result:
                                db_logger.info(f"NEW M LEAD with email {email} already exists - skipping")
                            return result
                    
                    return False
        except Exception as e:
            error_logger.error(f"Error checking if lead exists: {e}")
            return False


class LeadSaver:
    def __init__(self, db_connection):
        self.db = db_connection
    
    def save_lead(self, email_content, email_type, parsed_data):
        try:
            from .validators import split_name, is_valid_email
            import uuid
            
            if not parsed_data:
                db_logger.info(f"SKIPPED {email_type} - invalid data format, not saving to database")
                return None
            
            first_name, last_name = split_name(parsed_data.get('name', ''))
            
            email_address = parsed_data.get('email', '').strip()
            
            if email_type in ['home_page', 'packages_page']:
                unique_id = str(uuid.uuid4())[:8]
                email_address = f"contact_{unique_id}@bodyback-lead.local"
                db_logger.info(f"Contact form lead - using placeholder email: {email_address}")
                
            elif email_type == 'new_lead':
                if not email_address or not is_valid_email(email_address):
                    unique_id = str(uuid.uuid4())[:8]
                    email_address = f"lead_{unique_id}@bodyback-lead.local"
                    db_logger.info(f"NEW M LEAD missing email - using placeholder: {email_address}")
            
            db_logger.info(f"Saving {email_type} lead: {first_name} {last_name} - {email_address}")
            
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO users (email, password_hash, first_name, last_name, phone_number, location, roles, active)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING id
                    """, (
                        email_address,
                        'lead_no_password',
                        first_name,
                        last_name,
                        parsed_data.get('phone', ''),
                        parsed_data.get('location', ''),
                        ['lead'],
                        True
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
                        'processed_at': datetime.now().isoformat(),
                        'has_real_email': bool(parsed_data.get('email', '').strip() and is_valid_email(parsed_data.get('email', ''))),
                        'primary_contact_method': 'phone' if email_type in ['home_page', 'packages_page'] else 'email'
                    }
                    
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
                    db_logger.info(f"SUCCESS: {email_type} lead saved successfully! User ID: {user_id}")
                    return user_id
                    
        except psycopg2.IntegrityError as e:
            db_logger.info(f"SKIPPED {email_type} - duplicate detected: {e}")
            return None
        except Exception as e:
            error_logger.error(f"SYSTEM ERROR saving lead to database: {e}")
            import traceback
            error_logger.error(traceback.format_exc())
            return None

class BodyBackDB:
    def __init__(self):
        self.connection = DatabaseConnection()
        self.email_processor = EmailProcessor(self.connection)
        self.lead_saver = LeadSaver(self.connection)
    
    def get_connection(self):
        return self.connection.get_connection()
    
    def test_connection(self):
        return self.connection.test_connection()
    
    def email_already_processed(self, email_id):
        return self.email_processor.email_already_processed(email_id)
    
    def save_lead(self, email_content, email_type):
        try:
            from .parsers import parse_new_lead, parse_contact_form
            
            if email_type == 'new_lead':
                parsed_data = parse_new_lead(email_content['body'])
            else:
                parsed_data = parse_contact_form(email_content['body'])
            
            if not parsed_data:
                db_logger.info(f"SKIPPED {email_type} - invalid data format, not saving to database")
                return None
            
            if self.email_processor.lead_already_exists(parsed_data, email_type):
                db_logger.info(f"SKIPPED {email_type} - duplicate lead detected")
                return None
            
            result = self.lead_saver.save_lead(email_content, email_type, parsed_data)
            
            if result:
                db_logger.info(f"SUCCESS: {email_type} lead saved successfully! User ID: {result}")
            
            return result
            
        except Exception as e:
            error_logger.error(f"SYSTEM ERROR in save_lead: {e}")
            import traceback
            error_logger.error(traceback.format_exc())
            return None