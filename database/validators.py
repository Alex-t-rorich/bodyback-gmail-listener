import re

def is_valid_name(name):
    """
    Validate name - accepts both single names and full names.
    Examples: "John", "John Smith", "Mary Jane Watson"
    """
    if not name or len(name.strip()) < 2:
        return False
    # Must contain at least one letter
    return bool(re.search(r'[a-zA-Z]', name))

def split_name(full_name):
    """
    Split name into first and last name.
    If only one name provided, put it in first_name, leave last_name empty.
    
    Examples:
    - "John" → first="John", last=""
    - "John Smith" → first="John", last="Smith"  
    - "Mary Jane Watson" → first="Mary Jane", last="Watson"
    """
    if not full_name:
        return '', ''
    
    # Clean and split the name
    name_parts = full_name.strip().split()
    
    if len(name_parts) == 0:
        return '', ''
    elif len(name_parts) == 1:
        # Single name goes to first_name
        return name_parts[0], ''
    elif len(name_parts) == 2:
        # Two names: first, last
        return name_parts[0], name_parts[1]
    else:
        # Multiple names: everything except last goes to first_name
        first_name = ' '.join(name_parts[:-1])
        last_name = name_parts[-1]
        return first_name, last_name

def is_valid_email(email):
    """Validate email address."""
    if not email or len(email) < 5:
        return False
    return '@' in email and '.' in email.split('@')[-1]

def is_valid_phone(phone):
    """
    Validate phone number - accept 9 or 10+ digits.
    SA mobile numbers can be 9 digits (without leading 0) or 10+ digits.
    """
    if not phone:
        return False
    digits_only = re.sub(r'\D', '', phone)
    # Accept 9 digits (e.g. 829626790) or 10+ digits (e.g. 0827579541)
    return len(digits_only) >= 9

def clean_text(text):
    """Clean and normalize text."""
    if not text:
        return ''
    cleaned = re.sub(r'\s+', ' ', text.strip())
    return cleaned