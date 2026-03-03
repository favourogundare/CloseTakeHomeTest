#!/usr/bin/env python3
"""
Close CRM Import Script
-----------------------
This script imports companies and contacts from a CSV file into Close CRM,
queries leads by founding date range, and generates a state-segmented report.

Author: Favour Ogundare
"""

import csv
import re
import sys
import argparse
import statistics
from datetime import datetime
from collections import defaultdict
from typing import Optional, Dict, List, Any, Tuple

import requests
from requests.auth import HTTPBasicAuth


# =============================================================================
# CONFIGURATION
# =============================================================================

API_BASE_URL = "https://api.close.com/api/v1"


def load_api_key() -> str:
    """
    Load the API key from .env file.
    
    The .env file should contain:
    CLOSE_API_KEY=your_api_key_here
    
    Returns:
        The API key string
        
    Raises:
        ValueError: If CLOSE_API_KEY is not set
    """
    from dotenv import load_dotenv
    import os
    
    load_dotenv()
    
    api_key = os.getenv('CLOSE_API_KEY')
    
    if not api_key:
        raise ValueError(
            "CLOSE_API_KEY not found.\n"
            "Please create a '.env' file with:\n"
            "CLOSE_API_KEY=your_api_key_here\n"
            "See README.md for instructions."
        )
    
    return api_key


# =============================================================================
# DATA VALIDATION FUNCTIONS
# =============================================================================

def is_valid_email(email: str) -> bool:
    """
    Validate an email address using a regex pattern.
    
    Checks for:
    - Contains @ symbol
    - Has valid characters before and after @
    - Has a valid domain extension
    - No strange characters or formatting issues
    """
    if not email or not isinstance(email, str):
        return False
    
    email = email.strip()
    
    # Basic email regex pattern
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    
    return bool(re.match(pattern, email))


def clean_phone_number(phone: str) -> Optional[str]:
    """
    Clean and validate a phone number.
    
    Removes:
    - Emoji characters
    - Invalid symbols (like ??)
    - Extra whitespace
    
    Returns None if the phone number is invalid or "unknown"
    """
    if not phone or not isinstance(phone, str):
        return None
    
    phone = phone.strip()
    
    # Skip if it's explicitly "unknown"
    if phone.lower() == "unknown":
        return None
    
    # Remove emoji and other non-phone characters
    # Keep only: digits, +, -, (, ), spaces
    cleaned = re.sub(r'[^\d+\-() ]', '', phone)
    
    # Remove any remaining non-standard characters like ??
    cleaned = re.sub(r'\?+', '', cleaned)
    
    # Check if we have at least some digits
    if not re.search(r'\d{4,}', cleaned):
        return None
    
    return cleaned.strip() if cleaned.strip() else None


def parse_date(date_str: str) -> Optional[str]:
    """
    Parse a date string in various formats and return ISO format (YYYY-MM-DD).
    
    Handles formats like:
    - DD.MM.YYYY (e.g., 17.05.1987)
    - D.M.YYYY (e.g., 8.6.1987)
    """
    if not date_str or not isinstance(date_str, str):
        return None
    
    date_str = date_str.strip()
    
    # Try different date formats
    formats = [
        '%d.%m.%Y',  # 17.05.1987
        '%d.%m.%y',  # 17.05.87
    ]
    
    for fmt in formats:
        try:
            parsed = datetime.strptime(date_str, fmt)
            return parsed.strftime('%Y-%m-%d')
        except ValueError:
            continue
    
    return None


def parse_revenue(revenue_str: str) -> Optional[float]:
    """
    Parse a revenue string and return a float value.
    
    Handles formats like:
    - $1231970.94
    - $2,777,611.57
    """
    if not revenue_str or not isinstance(revenue_str, str):
        return None
    
    revenue_str = revenue_str.strip()
    
    # Remove $ and commas, then convert to float
    try:
        cleaned = revenue_str.replace('$', '').replace(',', '').strip()
        if cleaned:
            return float(cleaned)
    except ValueError:
        pass
    
    return None


def parse_emails(email_str: str) -> List[str]:
    """
    Parse an email field that may contain multiple emails separated by
    commas, semicolons, or newlines.
    
    Returns a list of valid email addresses.
    """
    if not email_str or not isinstance(email_str, str):
        return []
    
    # Split by various delimiters
    emails = re.split(r'[,;\n]+', email_str)
    
    valid_emails = []
    for email in emails:
        email = email.strip()
        if is_valid_email(email):
            valid_emails.append(email)
    
    return valid_emails


def parse_phones(phone_str: str) -> List[str]:
    """
    Parse a phone field that may contain multiple phone numbers separated
    by newlines.
    
    Returns a list of valid phone numbers.
    """
    if not phone_str or not isinstance(phone_str, str):
        return []
    
    # Split by newlines
    phones = phone_str.split('\n')
    
    valid_phones = []
    for phone in phones:
        cleaned = clean_phone_number(phone)
        if cleaned:
            valid_phones.append(cleaned)
    
    return valid_phones


def is_valid_us_state(state: str) -> bool:
    """
    Check if a string is a valid US state name.
    """
    valid_states = {
        'Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado',
        'Connecticut', 'Delaware', 'Florida', 'Georgia', 'Hawaii', 'Idaho',
        'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana',
        'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota',
        'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada',
        'New Hampshire', 'New Jersey', 'New Mexico', 'New York',
        'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon',
        'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota',
        'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington',
        'West Virginia', 'Wisconsin', 'Wyoming', 'District of Columbia'
    }
    
    if not state or not isinstance(state, str):
        return False
    
    return state.strip() in valid_states


def normalize_contact_name(name: str) -> Optional[str]:
    """
    Normalize a contact name - handle various capitalizations and formats.
    
    Handles cases like:
    - "BARYRAM ABRAMOVICI" -> "Baryram Abramovici"
    - "PaTrIzIo pEdDeRsEn" -> "Patrizio Peddersen"
    - "lUcKy uTtRiDgE" -> "Lucky Uttridge"
    """
    if not name or not isinstance(name, str):
        return None
    
    name = name.strip()
    
    if not name:
        return None
    
    # Handle titles like "Dr.", "Mr.", "Ms.", etc.
    # Title case the name properly
    words = name.split()
    normalized_words = []
    
    for word in words:
        # Check for common titles
        if word.upper() in ['DR.', 'MR.', 'MS.', 'MRS.', 'IV', 'III', 'II', 'JR.', 'SR.']:
            normalized_words.append(word.upper() if len(word) <= 3 else word.capitalize())
        else:
            normalized_words.append(word.capitalize())
    
    return ' '.join(normalized_words)


# =============================================================================
# CSV PARSING
# =============================================================================

def read_csv_file(filepath: str) -> List[Dict[str, str]]:
    """
    Read a CSV file and return a list of row dictionaries.
    
    Handles multi-line cells properly.
    """
    rows = []
    
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    
    return rows


def process_csv_data(rows: List[Dict[str, str]]) -> Dict[str, Dict[str, Any]]:
    """
    Process CSV rows and group contacts by company.
    
    Returns a dictionary where keys are company names and values contain
    company info and a list of contacts.
    
    Invalid data is discarded based on the following rules:
    1. Company name must be present
    2. Invalid email formats are removed (but contact may still be valid)
    3. Invalid phone numbers are removed (but contact may still be valid)
    4. Contacts without both name and valid contact info are discarded
    """
    companies = defaultdict(lambda: {
        'contacts': [],
        'founded_date': None,
        'revenue': None,
        'state': None,
        'name': None
    })
    
    invalid_rows = []
    
    for row_num, row in enumerate(rows, start=2):  # Start at 2 to account for header
        company_name = row.get('Company', '').strip()
        
        # Rule 1: Company name must be present
        if not company_name:
            invalid_rows.append({
                'row': row_num,
                'reason': 'Missing company name',
                'data': row
            })
            continue
        
        # Parse company-level data (same for all contacts in a company)
        if companies[company_name]['name'] is None:
            companies[company_name]['name'] = company_name
            companies[company_name]['founded_date'] = parse_date(
                row.get('custom.Company Founded', '')
            )
            companies[company_name]['revenue'] = parse_revenue(
                row.get('custom.Company Revenue', '')
            )
            state = row.get('Company US State', '').strip()
            companies[company_name]['state'] = state if is_valid_us_state(state) else None
        
        # Parse contact data
        contact_name = normalize_contact_name(row.get('Contact Name', ''))
        emails = parse_emails(row.get('Contact Emails', ''))
        phones = parse_phones(row.get('Contact Phones', ''))
        
        # A contact is valid if it has a name OR at least one valid email/phone
        has_valid_contact_info = contact_name or emails or phones
        
        if has_valid_contact_info:
            contact = {
                'name': contact_name,
                'emails': emails,
                'phones': phones
            }
            companies[company_name]['contacts'].append(contact)
        else:
            invalid_rows.append({
                'row': row_num,
                'reason': 'Contact has no valid name, email, or phone',
                'data': row
            })
    
    # Print summary of invalid data
    if invalid_rows:
        print(f"\n⚠️  Discarded {len(invalid_rows)} invalid rows:")
        for item in invalid_rows[:10]:  # Show first 10
            print(f"   Row {item['row']}: {item['reason']}")
        if len(invalid_rows) > 10:
            print(f"   ... and {len(invalid_rows) - 10} more")
    
    return dict(companies)


# =============================================================================
# CLOSE API FUNCTIONS
# =============================================================================

class CloseAPI:
    """
    Wrapper class for Close API operations.
    """
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.auth = HTTPBasicAuth(api_key, '')
        self.base_url = API_BASE_URL
        self.custom_field_ids = {}
    
    def _request(self, method: str, endpoint: str, data: Optional[Dict] = None) -> Dict:
        """Make an API request to Close."""
        url = f"{self.base_url}/{endpoint}"
        
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        
        try:
            if method == 'GET':
                response = requests.get(url, auth=self.auth, headers=headers, params=data)
            elif method == 'POST':
                response = requests.post(url, auth=self.auth, headers=headers, json=data)
            elif method == 'PUT':
                response = requests.put(url, auth=self.auth, headers=headers, json=data)
            elif method == 'DELETE':
                response = requests.delete(url, auth=self.auth, headers=headers)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            response.raise_for_status()
            
            if response.text:
                return response.json()
            return {}
            
        except requests.exceptions.HTTPError as e:
            print(f"HTTP Error: {e}")
            print(f"Response: {e.response.text if e.response else 'No response'}")
            raise
        except requests.exceptions.RequestException as e:
            print(f"Request Error: {e}")
            raise
    
    def get_or_create_custom_fields(self) -> Dict[str, str]:
        """
        Get or create the required custom fields for leads.
        
        Returns a dictionary mapping field names to their IDs.
        """
        required_fields = {
            'Company Founded': 'date',
            'Company Revenue': 'number',
            'Company US State': 'text'
        }
        
        # Get existing custom fields
        response = self._request('GET', 'custom_field/lead/')
        existing_fields = {f['name']: f['id'] for f in response.get('data', [])}
        
        field_ids = {}
        
        for field_name, field_type in required_fields.items():
            if field_name in existing_fields:
                field_ids[field_name] = existing_fields[field_name]
                print(f"   Found existing custom field: {field_name}")
            else:
                # Create the custom field
                new_field = self._request('POST', 'custom_field/lead/', {
                    'name': field_name,
                    'type': field_type
                })
                field_ids[field_name] = new_field['id']
                print(f"   Created custom field: {field_name}")
        
        self.custom_field_ids = field_ids
        return field_ids
    
    def create_lead(self, company_data: Dict[str, Any]) -> Dict:
        """
        Create a lead with its contacts in Close.
        
        Args:
            company_data: Dictionary containing company info and contacts
            
        Returns:
            The created lead response from Close API
        """
        # Build contacts list for the lead
        contacts = []
        for contact in company_data['contacts']:
            contact_obj = {}
            
            if contact['name']:
                contact_obj['name'] = contact['name']
            
            if contact['emails']:
                contact_obj['emails'] = [
                    {'email': email, 'type': 'office'}
                    for email in contact['emails']
                ]
            
            if contact['phones']:
                contact_obj['phones'] = [
                    {'phone': phone, 'type': 'office'}
                    for phone in contact['phones']
                ]
            
            if contact_obj:  # Only add if contact has some data
                contacts.append(contact_obj)
        
        # Build the lead payload
        lead_data = {
            'name': company_data['name'],
            'contacts': contacts
        }
        
        # Add custom fields
        if company_data['founded_date'] and 'Company Founded' in self.custom_field_ids:
            lead_data[f"custom.{self.custom_field_ids['Company Founded']}"] = company_data['founded_date']
        
        if company_data['revenue'] is not None and 'Company Revenue' in self.custom_field_ids:
            lead_data[f"custom.{self.custom_field_ids['Company Revenue']}"] = company_data['revenue']
        
        if company_data['state'] and 'Company US State' in self.custom_field_ids:
            lead_data[f"custom.{self.custom_field_ids['Company US State']}"] = company_data['state']
        
        return self._request('POST', 'lead/', lead_data)
    
    def get_all_leads(self) -> List[Dict]:
        """
        Get all leads from Close, handling pagination.
        """
        all_leads = []
        skip = 0
        limit = 100
        
        while True:
            response = self._request('GET', 'lead/', {
                '_skip': skip,
                '_limit': limit,
                '_fields': 'id,name,contacts,custom'
            })
            
            leads = response.get('data', [])
            all_leads.extend(leads)
            
            if not response.get('has_more', False):
                break
            
            skip += limit
        
        return all_leads
    
    def get_leads_by_date_range(self, start_date: str, end_date: str) -> List[Dict]:
        """
        Get leads that were founded within a date range.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            List of leads matching the criteria
        """
        all_leads = self.get_all_leads()
        
        if 'Company Founded' not in self.custom_field_ids:
            # Try to get the custom field IDs
            self.get_or_create_custom_fields()
        
        founded_field_id = self.custom_field_ids.get('Company Founded')
        revenue_field_id = self.custom_field_ids.get('Company Revenue')
        state_field_id = self.custom_field_ids.get('Company US State')
        
        matching_leads = []
        
        for lead in all_leads:
            custom = lead.get('custom', {})
            
            # Get the founded date from custom fields
            founded_date = None
            if founded_field_id:
                founded_date = custom.get(f'custom.{founded_field_id}') or custom.get(founded_field_id)
            
            # Also check by field name (older API format)
            if not founded_date:
                founded_date = custom.get('Company Founded')
            
            if founded_date:
                try:
                    # Parse the date for comparison
                    if isinstance(founded_date, str):
                        lead_date = datetime.strptime(founded_date, '%Y-%m-%d')
                        start = datetime.strptime(start_date, '%Y-%m-%d')
                        end = datetime.strptime(end_date, '%Y-%m-%d')
                        
                        if start <= lead_date <= end:
                            # Add revenue and state info to the lead
                            lead['_founded_date'] = founded_date
                            
                            if revenue_field_id:
                                lead['_revenue'] = custom.get(f'custom.{revenue_field_id}') or custom.get(revenue_field_id) or custom.get('Company Revenue')
                            
                            if state_field_id:
                                lead['_state'] = custom.get(f'custom.{state_field_id}') or custom.get(state_field_id) or custom.get('Company US State')
                            
                            matching_leads.append(lead)
                except (ValueError, TypeError):
                    continue
        
        return matching_leads
    
    def test_connection(self) -> bool:
        """Test the API connection."""
        try:
            response = self._request('GET', 'me/')
            print(f"✓ Connected to Close as: {response.get('first_name', '')} {response.get('last_name', '')}")
            return True
        except Exception as e:
            print(f"✗ Failed to connect to Close: {e}")
            return False


# =============================================================================
# REPORT GENERATION
# =============================================================================

def segment_leads_by_state(leads: List[Dict]) -> Dict[str, List[Dict]]:
    """
    Segment leads by US State.
    
    Returns a dictionary where keys are state names and values are lists of leads.
    """
    state_leads = defaultdict(list)
    
    for lead in leads:
        state = lead.get('_state')
        if state and is_valid_us_state(state):
            state_leads[state].append(lead)
    
    return dict(state_leads)


def generate_state_report(state_leads: Dict[str, List[Dict]], output_file: str):
    """
    Generate a CSV report segmented by state.
    
    Output columns:
    - US State
    - Total number of leads
    - The lead with most revenue
    - Total revenue
    - Median revenue
    """
    report_data = []
    
    for state, leads in sorted(state_leads.items()):
        # Calculate metrics
        revenues = []
        max_revenue = 0
        max_revenue_lead = None
        
        for lead in leads:
            revenue = lead.get('_revenue')
            if revenue is not None:
                try:
                    revenue_val = float(revenue)
                    revenues.append(revenue_val)
                    
                    if revenue_val > max_revenue:
                        max_revenue = revenue_val
                        max_revenue_lead = lead['name']
                except (ValueError, TypeError):
                    continue
        
        total_leads = len(leads)
        total_revenue = sum(revenues) if revenues else 0
        median_revenue = statistics.median(revenues) if revenues else 0
        
        report_data.append({
            'US State': state,
            'Total number of leads': total_leads,
            'The lead with most revenue': max_revenue_lead or 'N/A',
            'Total revenue': f"${total_revenue:,.2f}",
            'Median revenue': f"${median_revenue:,.2f}"
        })
    
    # Write to CSV
    fieldnames = [
        'US State',
        'Total number of leads',
        'The lead with most revenue',
        'Total revenue',
        'Median revenue'
    ]
    
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(report_data)
    
    print(f"\n✓ Report saved to: {output_file}")
    
    # Also print to console
    print("\n" + "=" * 80)
    print("STATE SEGMENTATION REPORT")
    print("=" * 80)
    print(f"{'State':<20} {'Leads':>8} {'Top Revenue Lead':<25} {'Total Revenue':>18} {'Median':>18}")
    print("-" * 80)
    
    for row in report_data:
        print(f"{row['US State']:<20} {row['Total number of leads']:>8} {row['The lead with most revenue']:<25} {row['Total revenue']:>18} {row['Median revenue']:>18}")


# =============================================================================
# MAIN WORKFLOW
# =============================================================================

def import_leads_from_csv(api: CloseAPI, csv_file: str) -> int:
    """
    Import leads and contacts from CSV to Close.
    
    Returns the number of leads successfully imported.
    """
    print(f"\n📂 Reading CSV file: {csv_file}")
    rows = read_csv_file(csv_file)
    print(f"   Found {len(rows)} rows")
    
    print("\n🔍 Processing and validating data...")
    companies = process_csv_data(rows)
    print(f"   Found {len(companies)} unique companies")
    
    print("\n⚙️  Setting up custom fields in Close...")
    api.get_or_create_custom_fields()
    
    print("\n📤 Importing leads to Close...")
    imported_count = 0
    failed_count = 0
    
    for company_name, company_data in companies.items():
        try:
            lead = api.create_lead(company_data)
            imported_count += 1
            contact_count = len(company_data['contacts'])
            print(f"   ✓ Created lead: {company_name} ({contact_count} contacts)")
        except Exception as e:
            failed_count += 1
            print(f"   ✗ Failed to create lead: {company_name} - {e}")
    
    print(f"\n✅ Import complete: {imported_count} leads created, {failed_count} failed")
    
    return imported_count


def query_and_report(api: CloseAPI, start_date: str, end_date: str, output_file: str):
    """
    Query leads by founding date range and generate state report.
    """
    print(f"\n🔎 Searching for leads founded between {start_date} and {end_date}...")
    
    leads = api.get_leads_by_date_range(start_date, end_date)
    print(f"   Found {len(leads)} leads in date range")
    
    print("\n📊 Segmenting leads by US State...")
    state_leads = segment_leads_by_state(leads)
    
    if state_leads:
        generate_state_report(state_leads, output_file)
    else:
        print("   No leads with valid US state found in the date range")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Import leads to Close CRM and generate state-segmented reports',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Import leads from CSV
  python close_import.py import --csv input.csv
  
  # Query leads and generate report
  python close_import.py report --start-date 1960-01-01 --end-date 2000-12-31 --output report.csv
  
  # Do both import and report
  python close_import.py all --csv input.csv --start-date 1960-01-01 --end-date 2000-12-31 --output report.csv
        """
    )
    
    parser.add_argument(
        'action',
        choices=['import', 'report', 'all'],
        help='Action to perform: import (CSV to Close), report (query and generate CSV), or all (both)'
    )
    
    parser.add_argument(
        '--csv',
        help='Path to the input CSV file (required for import and all)'
    )
    
    parser.add_argument(
        '--start-date',
        help='Start date for founding date range (YYYY-MM-DD format, required for report and all)'
    )
    
    parser.add_argument(
        '--end-date',
        help='End date for founding date range (YYYY-MM-DD format, required for report and all)'
    )
    
    parser.add_argument(
        '--output',
        default='state_report.csv',
        help='Output CSV file path (default: state_report.csv)'
    )
    
    args = parser.parse_args()
    
    # Validate arguments based on action
    if args.action in ['import', 'all'] and not args.csv:
        parser.error("--csv is required for import and all actions")
    
    if args.action in ['report', 'all'] and (not args.start_date or not args.end_date):
        parser.error("--start-date and --end-date are required for report and all actions")
    
    # Initialize API
    print("\n" + "=" * 60)
    print("CLOSE CRM IMPORT SCRIPT")
    print("=" * 60)
    
    # Load API key from .env file
    print("\n🔑 Loading API key from .env file...")
    try:
        api_key = load_api_key()
        print("   ✓ API key loaded successfully")
    except ValueError as e:
        print(f"   ✗ Error: {e}")
        sys.exit(1)
    
    api = CloseAPI(api_key)
    
    print("\n🔗 Testing Close API connection...")
    if not api.test_connection():
        sys.exit(1)
    
    # Execute action
    if args.action == 'import':
        import_leads_from_csv(api, args.csv)
    
    elif args.action == 'report':
        query_and_report(api, args.start_date, args.end_date, args.output)
    
    elif args.action == 'all':
        import_leads_from_csv(api, args.csv)
        query_and_report(api, args.start_date, args.end_date, args.output)
    
    print("\n✨ Done!")


if __name__ == '__main__':
    main()