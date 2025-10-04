import re
import logging
from .validators import is_valid_email, is_valid_phone, is_valid_name, clean_text

parser_logger = logging.getLogger('parsers')


def parse_new_lead(email_body):
    lines = [line.strip() for line in email_body.strip().split('\n') if line.strip()]
    
    if len(lines) < 4:
        parser_logger.info(f"NEW M LEAD email doesn't have expected format. Lines: {len(lines)}")
        return None
    
    data = {
        'name': lines[0],
        'phone': lines[1],
        'email': lines[2],
        'location': lines[3],
        'customer_data': '\n'.join(lines[4:]) if len(lines) > 4 else ''
    }
    
    if not is_valid_name(data['name']):
        parser_logger.info(f"Invalid name in NEW M LEAD: '{data['name']}' - skipping")
        return None
        
    if not is_valid_phone(data['phone']):
        parser_logger.info(f"Invalid phone in NEW M LEAD: '{data['phone']}' - skipping")
        return None
        
    if not is_valid_email(data['email']):
        parser_logger.info(f"Invalid email in NEW M LEAD: '{data['email']}' - skipping")
        return None
    
    parser_logger.info(f"SUCCESS: Valid NEW M LEAD data extracted: {data['name']} - {data['email']}")
    return data


def parse_contact_form(email_body):
    data = {}
    
    
    cleaned_body = re.sub(r'\n\s*\n', '\n', email_body)
    
    name_patterns = [
        r'\*Name and Surname\*\*\s*\n\s*([A-Za-z][A-Za-z\s]+?)(?=\s*\*Number|\s*$)',
        r'\*?Name and Surname\*?\s*\n?\s*([A-Za-z][A-Za-z\s]+?)(?=\s*\*?Number|\s*$)',
        r'Name and Surname\*?\s*\n.*?\n.*?\n.*?\n.*?\n.*?\n\s*([A-Z][A-Z\s]+)',
        r'\*?Name and Surname\*?\s*\n+\s*([^\n]+)',
        r'Name and Surname\*?\s*([^\n\r]+)',
        r'Name.*?\n+\s*([A-Z][A-Z\s]+)'
    ]
    
    for i, pattern in enumerate(name_patterns):
        name_match = re.search(pattern, email_body, re.IGNORECASE | re.DOTALL)
        if name_match:
            potential_name = clean_text(name_match.group(1))
            potential_name = re.sub(r'\*+.*$', '', potential_name).strip()
            if len(potential_name) >= 2 and re.search(r'[a-zA-Z]', potential_name):
                data['name'] = potential_name
                break
    
    if 'name' not in data:
        data['name'] = ''
    
    phone_patterns = [
        r'\*Number \(10 digits\)\*\*\s*\n\s*([0-9]{9,})',
        r'\*?Number.*?\*?\s*\n?\s*([0-9]{9,})',
        r'Number \(10 digits\)\*?\s*\n.*?\n.*?\n.*?\n.*?\n.*?\n\s*(\d{9,})',
        r'\*?Number.*?\*?\s*\n+\s*([0-9]+)',
        r'Number.*?(\d{9,})',
        r'(\d{9,})'
    ]
    
    for i, pattern in enumerate(phone_patterns):
        phone_match = re.search(pattern, email_body, re.IGNORECASE | re.DOTALL)
        if phone_match:
            data['phone'] = clean_text(phone_match.group(1))
            break
    
    if 'phone' not in data:
        data['phone'] = ''
    
    location_patterns = [
        r'\*Location\*\s*\n\s*([^\n*]+?)(?=\s*\*?Goals|\s*$)',
        r'\*?Location\*?\s*\n?\s*([^\n*]+?)(?=\s*\*?Goals|\s*$)',
        r'Location\s*\n.*?\n.*?\n.*?\n.*?\n.*?\n\s*([^\n]+)',
        r'\*?Location\*?\s*\n+\s*([^\n]+)',
        r'Location\*?\s*([^\n\r]+)',
        r'Location.*?\n+\s*([A-Za-z0-9\s,]+)'
    ]
    
    for i, pattern in enumerate(location_patterns):
        location_match = re.search(pattern, email_body, re.IGNORECASE | re.DOTALL)
        if location_match:
            potential_location = clean_text(location_match.group(1))
            potential_location = re.sub(r'\*+.*$', '', potential_location).strip()
            if len(potential_location) >= 2:
                data['location'] = potential_location
                break
    
    if 'location' not in data:
        data['location'] = ''
    
    goals_patterns = [
        r'\*Goals, injuries & other details\*\s*\n\s*([^\n*]+?)(?=\s*Date:|\s*$)',
        r'\*?Goals, injuries & other details\*?\s*\n?\s*([^\n*]+?)(?=\s*Date:|\s*$)',
        r'Goals, injuries & other details\s*\n.*?\n.*?\n.*?\n.*?\n.*?\n\s*([^\n]+(?:\n[^\n]+)*?)(?=\n\s*Date:|$)',
        r'\*?Goals, injuries & other details\*?\s*\n+\s*([^\n\r]+)',
        r'Goals.*?details.*?\s*([^\n\r]+)',
        r'Goals.*?\n+\s*([^Date:]+)'
    ]
    
    for i, pattern in enumerate(goals_patterns):
        goals_match = re.search(pattern, email_body, re.IGNORECASE | re.DOTALL)
        if goals_match:
            potential_goals = clean_text(goals_match.group(1))
            potential_goals = re.sub(r'\*+.*$', '', potential_goals).strip()
            if len(potential_goals) >= 2:
                data['customer_data'] = potential_goals
                break
    
    if 'customer_data' not in data:
        data['customer_data'] = ''
    
    
    if not is_valid_name(data['name']):
        parser_logger.info(f"Contact form missing valid name - skipping")
        return None
        
    if not is_valid_phone(data['phone']) and not data['location']:
        parser_logger.info(f"Contact form missing both valid phone and location - skipping")
        return None
    
    parser_logger.info(f"SUCCESS: Contact form data extracted: Name='{data.get('name')}', Phone='{data.get('phone')}', Location='{data.get('location')}', Data='{data.get('customer_data')}'")
    
    return data