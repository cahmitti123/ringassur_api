import requests
from urllib3.exceptions import InsecureRequestWarning
from datetime import datetime
import re
import pandas as pd
import json
from io import BytesIO

# Disable SSL warning
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

class CRMClient:
    def __init__(self):
        self.base_url = "https://ringassur.comunikcrm.info"
        self.session = requests.Session()
        # Set up default headers
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'Accept': '*/*',
            'Accept-Language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7,ar;q=0.6',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'X-Requested-With': 'XMLHttpRequest',
            'Origin': 'https://ringassur.comunikcrm.info',
            'Referer': 'https://ringassur.comunikcrm.info/vvci/login'
        })

    def login(self, username, password, account="ringassur"):
        """Login to the CRM system"""
        login_url = f"{self.base_url}/vvci/login/login_check"
        
        payload = {
            'username': username,
            'account': account,
            'password': password,
            'poste': '',
            'code': '',
            'checkForTwoFactor': '0',
            'language': 'fr'
        }

        try:
            response = self.session.post(
                login_url,
                data=payload,
                verify=False
            )
            
            if response.status_code == 200:
                print("Successfully logged into CRM")
                return True
            else:
                print(f"Failed to login. Status code: {response.status_code}")
                print(f"Response: {response.text}")
                return False
                
        except Exception as e:
            print(f"Error during login: {str(e)}")
            return False

    def get_campaigns(self, start_date=None, end_date=None):
        """Get available campaigns for the given date range"""
        # Set fixed test date
        start_date = "2024-12-20 00:00:00"
        end_date = "2024-12-20 23:59:59"

        url = f"{self.base_url}/vvci/gestioncontacts/gestioncontacts/prodFilterDate"
        
        payload = {
            'start': start_date,
            'end': end_date
        }

        try:
            response = self.session.post(url, data=payload, verify=False)
            
            if response.status_code == 200:
                campaigns = self._parse_campaign_response(response.text)
                return campaigns
            else:
                print(f"Failed to get campaigns. Status code: {response.status_code}")
                return None

        except Exception as e:
            print(f"Error getting campaigns: {str(e)}")
            return None

    def _parse_campaign_response(self, html_content):
        """Parse the HTML response to extract campaign information"""
        campaigns = {}
        
        # Use regex to find optgroup and option elements
        optgroup_pattern = r'<optgroup label="([^"]+)">(.*?)</optgroup>'
        option_pattern = r'<option value="(\d+)" data-numcampagne="(\d+)">([^<]+)</option>'
        
        # Find all optgroups
        for group_match in re.finditer(optgroup_pattern, html_content, re.DOTALL):
            group_name = group_match.group(1)
            group_content = group_match.group(2)
            
            campaigns[group_name] = []
            
            # Find all options within the optgroup
            for option_match in re.finditer(option_pattern, group_content):
                campaign = {
                    'value': option_match.group(1),
                    'num_campagne': option_match.group(2),
                    'name': option_match.group(3).strip()
                }
                campaigns[group_name].append(campaign)
        
        return campaigns

    def get_campaign_data(self, campaign_values, start_date=None, end_date=None):
        """Get data for selected campaigns"""
        if not start_date:
            start_date = datetime.now().strftime("%Y-%m-%d 00:00:00")
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d 23:59:59")

        url = f"{self.base_url}/vvci/gestioncontacts/gestioncontacts/search"
        
        # Prepare the payload
        payload = {
            'CMK_FORM_ACTION': 'display',
            'CMK_DWNLOAD_TOKEN': '',
            'CMK_FORM_MODEL': '-1',
            'CMK_FORM_CONTACTS': '',
            'selectGroups[]': campaign_values,  # List of campaign values
            'selectGroup': 'on',
            'dateprod[start]': start_date,
            'dateprod[end]': end_date,
            'dateType': '1',
            'dateTraitement': f'Du {datetime.now().strftime("%d %B %Y")} Au {datetime.now().strftime("%d %B %Y")}',
            'datetrait[start]': start_date,
            'datetrait[end]': end_date,
            'selectChamps[]': '',
            'selectQualifs[7][]': ['76', '523']
        }

        # Add selectItem entries for each campaign value
        for value in campaign_values:
            payload[f'selectItem'] = value

        try:
            response = self.session.post(url, data=payload, verify=False)
            
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Failed to get campaign data. Status code: {response.status_code}")
                return None

        except Exception as e:
            print(f"Error getting campaign data: {str(e)}")
            return None

    def export_campaign_data(self, campaign_values, start_date=None, end_date=None):
        """Export campaign data as CSV"""
        # Set fixed test date
        start_date = "2024-12-20 00:00:00"
        end_date = "2024-12-20 23:59:59"

        url = f"{self.base_url}/vvci/gestioncontacts/gestioncontacts/search"
        
        # Generate unique download token
        download_token = f"cmk_export_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        # Prepare the payload for CSV export
        payload = {
            'CMK_FORM_ACTION': 'csv',
            'CMK_DWNLOAD_TOKEN': download_token,
            'CMK_FORM_MODEL': '54',
            'CMK_FORM_CONTACTS': '-1',
            'selectQualifs[7][]': ['76', '523'],
            'selectGroups[]': campaign_values,
            'selectGroup': 'on',
            'dateprod[start]': start_date,
            'dateprod[end]': end_date,
            'dateType': '1',
            'dateTraitement': f'Du 20 December 2024 Au 20 December 2024',
            'datetrait[start]': start_date,
            'datetrait[end]': end_date,
            'selectChamps[]': '',
            'selectInputs[]': '-1'
        }

        # Add selectItem entries for each campaign value
        for value in campaign_values:
            payload['selectItem'] = value

        try:
            response = self.session.post(url, data=payload, verify=False)
            
            if response.status_code == 200:
                # Save the CSV content to a file
                filename = f"export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                with open(filename, 'wb') as f:
                    f.write(response.content)
                print(f"Data exported successfully to {filename}")
                return filename
            else:
                print(f"Failed to export data. Status code: {response.status_code}")
                return None

        except Exception as e:
            print(f"Error exporting data: {str(e)}")
            return None

    def get_data_as_json(self, start_date=None, end_date=None):
        """Get CRM data as JSON"""
        try:
            # Use today's date as default
            if not start_date:
                start_date = datetime.now().strftime("%Y-%m-%d 00:00:00")
            if not end_date:
                end_date = datetime.now().strftime("%Y-%m-%d 23:59:59")
            
            # Get campaigns first
            campaigns = self.get_campaigns(start_date, end_date)
            if not campaigns:
                return {"error": "Failed to get campaigns"}
            
            # Get campaign values for Prevoyance
            campaign_values = [c['value'] for c in campaigns.get('Prevoyance', [])]
            
            # Export the data using the same payload as flashProdScript
            url = f"{self.base_url}/vvci/gestioncontacts/gestioncontacts/search"
            
            # Generate unique download token
            download_token = f"cmk_export_{datetime.now().strftime('%Y%m%d%H%M%S')}"
            
            # Use the exact same payload as in flashProdScript
            payload = {
                'CMK_FORM_ACTION': 'csv',
                'CMK_DWNLOAD_TOKEN': download_token,
                'CMK_FORM_MODEL': '54',
                'CMK_FORM_CONTACTS': '-1',
                'selectQualifs[7][]': ['76', '523'],
                'selectGroups[]': campaign_values,
                'selectGroup': 'on',
                'dateprod[start]': start_date,
                'dateprod[end]': end_date,
                'dateType': '1',
                'dateTraitement': f'Du {datetime.now().strftime("%d %B %Y")} Au {datetime.now().strftime("%d %B %Y")}',
                'datetrait[start]': start_date,
                'datetrait[end]': end_date,
                'selectChamps[]': '',
                'selectInputs[]': '-1'
            }

            # Add selectItem entries for each campaign value
            for value in campaign_values:
                payload['selectItem'] = value

            response = self.session.post(url, data=payload, verify=False)
            
            if response.status_code == 200:
                # Read CSV data
                df = pd.read_csv(BytesIO(response.content), sep=';')
                # Convert to JSON
                json_data = json.loads(df.to_json(orient='records', date_format='iso'))
                return {"success": True, "data": json_data}
            else:
                return {"error": f"Failed to get data. Status code: {response.status_code}"}

        except Exception as e:
            return {"error": f"Error getting data: {str(e)}"}

class ERPClient:
    def __init__(self):
        self.base_url = "https://erp.ringassur.fr"
        self.session = requests.Session()
        self.last_fetch_time = None
        # Set up default headers
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7,ar;q=0.6'
        })
        # Initialize stored data
        self.stored_data = pd.DataFrame()

    def login(self, email, password):
        """Login to the ERP system"""
        try:
            # First get the login page to get the initial session
            print("\nGetting login page...")
            response = self.session.get(
                f"{self.base_url}/",  # Changed back to root URL
                verify=False,
                allow_redirects=True
            )
            print(f"Initial page status: {response.status_code}")
            print(f"Initial cookies: {self.session.cookies.get_dict()}")

            # Get CSRF token from cookies
            csrf_token = self.session.cookies.get('XSRF-TOKEN')
            if csrf_token:
                from urllib.parse import unquote
                csrf_token = unquote(csrf_token)
                print(f"Decoded CSRF token: {csrf_token}")

            # Update headers for login request
            self.session.headers.update({
                'X-XSRF-TOKEN': csrf_token,
                'Content-Type': 'application/x-www-form-urlencoded',
                'Origin': self.base_url,
                'Referer': f"{self.base_url}/login",
                'X-Requested-With': 'XMLHttpRequest',
                'Accept': 'application/json'  # Added to expect JSON response
            })

            # Prepare login payload
            payload = {
                'email': email,
                'password': password,
                'remember': 'true'  # Added remember me option
            }

            print("\nAttempting login...")
            login_response = self.session.post(
                f"{self.base_url}/login",
                data=payload,
                verify=False,
                allow_redirects=False  # Don't follow redirects for initial login
            )
            print(f"Login response status: {login_response.status_code}")
            print(f"Login response headers: {dict(login_response.headers)}")

            # If we get a redirect, follow it manually
            if login_response.status_code in (301, 302):
                redirect_url = login_response.headers.get('Location')
                if redirect_url:
                    if not redirect_url.startswith('http'):
                        redirect_url = f"{self.base_url}{redirect_url}"
                    print(f"Following redirect to: {redirect_url}")
                    redirect_response = self.session.get(
                        redirect_url,
                        verify=False,
                        allow_redirects=True
                    )
                    print(f"Redirect response status: {redirect_response.status_code}")

            # Check if we're logged in
            check_response = self.session.get(
                f"{self.base_url}/dashboard",
                verify=False,
                headers={'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'}
            )
            if check_response.status_code == 200 and 'login' not in check_response.url:
                print("Successfully logged into ERP")
                return True
            else:
                print("Login failed - cannot access dashboard")
                print(f"Response URL: {check_response.url}")
                print(f"Response content: {check_response.text[:500]}...")
                return False

        except Exception as e:
            print(f"Error during ERP login: {str(e)}")
            print(f"Full error: {str(e.__class__.__name__)}: {str(e)}")
            import traceback
            traceback.print_exc()
            return False

    def get_contracts_as_json(self, force_full_refresh=False):
        """Get ERP contracts data as JSON with incremental loading"""
        try:
            # Get CSRF token for the request
            csrf_token = self.session.cookies.get('XSRF-TOKEN')
            if csrf_token:
                from urllib.parse import unquote
                csrf_token = unquote(csrf_token)
                self.session.headers.update({'X-XSRF-TOKEN': csrf_token})

            # If it's first time or force refresh, get all data
            if self.last_fetch_time is None or force_full_refresh:
                print("Fetching full data...")
                url = f"{self.base_url}/contracts/export"
                response = self.session.get(url, verify=False)
                
                if response.status_code == 200:
                    # Read Excel data
                    self.stored_data = pd.read_excel(BytesIO(response.content))
                    self.last_fetch_time = datetime.now()
                    
                    # Convert to JSON
                    json_data = json.loads(self.stored_data.to_json(orient='records', date_format='iso'))
                    return {"success": True, "data": json_data, "type": "full_refresh"}
                else:
                    return {"error": f"Failed to get contracts. Status code: {response.status_code}"}
            
            else:
                # Get only new data since last fetch
                print(f"Fetching incremental data since {self.last_fetch_time}...")
                url = f"{self.base_url}/contracts"
                params = {
                    'start_date': self.last_fetch_time.strftime("%Y-%m-%d %H:%M:%S")
                }
                response = self.session.get(url, params=params, verify=False)
                
                if response.status_code == 200:
                    try:
                        # Parse new data
                        new_data = pd.DataFrame(response.json())
                        
                        if not new_data.empty:
                            # Append new data to stored data
                            self.stored_data = pd.concat([self.stored_data, new_data], ignore_index=True)
                            # Remove duplicates if any
                            self.stored_data = self.stored_data.drop_duplicates(subset=['id'], keep='last')
                            
                        self.last_fetch_time = datetime.now()
                        
                        # Convert to JSON
                        json_data = json.loads(self.stored_data.to_json(orient='records', date_format='iso'))
                        return {
                            "success": True, 
                            "data": json_data,
                            "type": "incremental",
                            "new_records": len(new_data) if not new_data.empty else 0
                        }
                    except Exception as e:
                        print(f"Error processing incremental data: {e}")
                        # If there's an error with incremental update, fall back to full refresh
                        return self.get_contracts_as_json(force_full_refresh=True)
                else:
                    return {"error": f"Failed to get incremental data. Status code: {response.status_code}"}

        except Exception as e:
            return {"error": f"Error getting contracts: {str(e)}"}
