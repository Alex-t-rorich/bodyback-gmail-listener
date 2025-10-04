import re

def is_valid_name(name):
    if not name or len(name.strip()) < 2:
        return False
    return bool(re.search(r'[a-zA-Z]', name))

def split_name(full_name):
    if not full_name:
        return '', ''
    
    name_parts = full_name.strip().split()
    
    if len(name_parts) == 0:
        return '', ''
    elif len(name_parts) == 1:
        return name_parts[0], ''
    elif len(name_parts) == 2:
        return name_parts[0], name_parts[1]
    else:
        first_name = ' '.join(name_parts[:-1])
        last_name = name_parts[-1]
        return first_name, last_name

def is_valid_email(email):
    if not email or len(email) < 5:
        return False
    return '@' in email and '.' in email.split('@')[-1]

def is_valid_phone(phone):
    if not phone:
        return False
    digits_only = re.sub(r'\D', '', phone)
    return len(digits_only) >= 9

def clean_text(text):
    if not text:
        return ''
    cleaned = re.sub(r'\s+', ' ', text.strip())
    return cleaned