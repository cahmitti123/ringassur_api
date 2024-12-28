import requests
from urllib3.exceptions import InsecureRequestWarning
from datetime import datetime
import re
import pandas as pd
import json
from io import BytesIO
from bs4 import BeautifulSoup
from typing import Optional
import random
import time

# Disable SSL warning
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)




class CRMClientFormaExpert: 
    def __init__(self):
        self.base_url = "https://formaexpert.comunikcrm.info"
        self.session = requests.session()
        
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'Accept': '*/*',
            'Accept-Language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7,ar;q=0.6',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'X-Requested-With': 'XMLHttpRequest',
            'Origin': 'https://formaexpert.comunikcrm.info',
            'Referer': 'https://formaexpert.comunikcrm.info/vvci/login'
        })
        
    def login(self, username='root', password='P@ssW0rd@2024', account='formaexpert'):
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
            print(f"\nAttempting login to FormaExpert CRM...")
            print(f"Login URL: {login_url}")
            print(f"Payload: {payload}")
            
            response = self.session.post(
                login_url,
                data=payload,
                verify=False
            )
            
            print(f"Login response status: {response.status_code}")
            print(f"Login response headers: {dict(response.headers)}")
            print(f"Login cookies: {self.session.cookies.get_dict()}")
            
            if response.status_code == 200:
                # Verify we're actually logged in by checking dashboard access
                check_url = f"{self.base_url}/vvci/dashboard"
                check_response = self.session.get(check_url, verify=False)
                print(f"Dashboard check status: {check_response.status_code}")
                print(f"Dashboard URL: {check_response.url}")
                
                if check_response.status_code == 200 and 'login' not in check_response.url:
                    print("Successfully logged into FormaExpert CRM")
                    return True
                else:
                    print("Login succeeded but dashboard check failed")
                    return False
            else:
                print(f"Failed to login. Response content: {response.text[:500]}...")
                return False
                
        except Exception as e:
            print(f"Error during login: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
        
    def get_campaigns(self, start_date=None, end_date=None):
        """Get available campaigns for the given date range"""
        try:
            # Remove hardcoded dates and use parameters
            if not start_date:
                start_date = datetime.now().strftime("%Y-%m-%d 00:00:00")
            if not end_date:
                end_date = datetime.now().strftime("%Y-%m-%d 23:59:59")

            url = f"{self.base_url}/vvci/gestioncontacts/gestioncontacts/prodFilterDate"
            
            payload = {
                'start': start_date,
                'end': end_date
            }

            print(f"Fetching campaigns from URL: {url}")
            print(f"With payload: {payload}")
            print(f"Session cookies: {self.session.cookies.get_dict()}")
            print(f"Session headers: {self.session.headers}")

            response = self.session.post(url, data=payload, verify=False)
            
            print(f"Campaign response status: {response.status_code}")
            print(f"Campaign response content: {response.text[:500]}...")  # First 500 chars
            
            if response.status_code == 200:
                campaigns = self._parse_campaign_response(response.text)
                print(f"Parsed campaigns: {campaigns}")
                return campaigns
            else:
                print(f"Failed to get campaigns. Status code: {response.status_code}")
                print(f"Response content: {response.text}")
                return None

        except Exception as e:
            print(f"Error getting campaigns: {str(e)}")
            import traceback
            traceback.print_exc()
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
            
            # Get campaign values
            campaign_values = [c['value'] for c in campaigns.get('Energie_Rabat', [])]
            
            # Export the data using the same payload as flashProdScript
            url = f"{self.base_url}/vvci/gestioncontacts/gestioncontacts/search"
            
            # Generate unique download token
            download_token = f"cmk_export_{datetime.now().strftime('%Y%m%d%H%M%S')}"
            
            # Use the exact same payload as in flashProdScript
            payload = {
                'CMK_FORM_ACTION': 'csv',
                'CMK_DWNLOAD_TOKEN': download_token,
                'CMK_FORM_CONTACTS': '-1',
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
                try:
                    # Try different encodings
                    encodings = ['utf-8', 'latin1', 'iso-8859-1', 'cp1252']
                    df = None
                    
                    for encoding in encodings:
                        try:
                            df = pd.read_csv(BytesIO(response.content), 
                                           sep=';', 
                                           encoding=encoding)
                            break
                        except UnicodeDecodeError:
                            continue
                    
                    if df is None:
                        return {"error": "Failed to decode CSV data with any known encoding"}
                    
                    # Convert to JSON
                    json_data = json.loads(df.to_json(orient='records', date_format='iso'))
                    return {"success": True, "data": json_data}
                except Exception as e:
                    print(f"Error processing CSV: {str(e)}")
                    return {"error": f"Error processing CSV: {str(e)}"}
            else:
                return {"error": f"Failed to get data. Status code: {response.status_code}"}

        except Exception as e:
            print("Error", e)
            return {"error": f"Error getting data: {str(e)}"}

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

    def close(self):
        """Close the client's session"""
        try:
            if self.session:
                self.session.close()
                self.session = None
        except Exception as e:
            print(f"Error closing session: {str(e)}")

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
        # Remove hardcoded dates and use parameters
        if not start_date:
            start_date = datetime.now().strftime("%Y-%m-%d 00:00:00")
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d 23:59:59")

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
            print("Error" , e)
            return {"error": f"Error getting data: {str(e)}"}

    def close(self):
        """Close the client's session"""
        try:
            if self.session:
                self.session.close()
                self.session = None
        except Exception as e:
            print(f"Error closing session: {str(e)}")

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

    def close(self):
        """Close the client's session"""
        try:
            if self.session:
                self.session.close()
                self.session = None
        except Exception as e:
            print(f"Error closing session: {str(e)}")

class JobsClient:
    def __init__(self):
        self.base_url = "https://www.moncallcenter.ma"
        self.session = requests.Session()
        self.last_request_time = 0
        self.min_request_interval = 1  # Minimum seconds between requests
        
    def _wait_for_rate_limit(self):
        """Ensure minimum time between requests"""
        now = time.time()
        time_since_last = now - self.last_request_time
        if time_since_last < self.min_request_interval:
            time.sleep(self.min_request_interval - time_since_last)
        self.last_request_time = time.time()
        
    def login(self, username: str, password: str) -> bool:
        """Login to moncallcenter.ma"""
        try:
            # Clear any existing session
            self.session = requests.Session()
            
            login_url = f"{self.base_url}/components/centre/loger_centre.php"
            login_data = {
                "LOGIN_C": username,
                "PASSWORD_C": password
            }
            
            response = self.session.post(login_url, data=login_data)
            if response.status_code == 200:
                # Verify login by checking if we can access a protected page
                check_response = self.session.get(f"{self.base_url}/recruteurs/")
                if check_response.status_code == 200 and 'login' not in check_response.url:
                    print(f"Successfully logged into moncallcenter.ma as {username}")
                    return True
            
            print(f"Failed to login to moncallcenter.ma as {username}. Status: {response.status_code}")
            return False
            
        except Exception as e:
            print(f"Error during login: {str(e)}")
            return False

    def get_job_details(self, job_url: str):
        """Get detailed information about a specific job"""
        try:
            print(f"\nFetching details for job: {job_url}")
            response = self.session.get(job_url)
            if response.status_code != 200:
                print(f"Failed to fetch job details. Status: {response.status_code}")
                raise Exception("Failed to fetch job details")

            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find duplicate button with exact classes and attributes
            duplicate_button = soup.find("a", {"class": "duplioffre", "href": "javascript:void(0)"})
            
            if duplicate_button:
                print(f"Found duplicate button with data-id: {duplicate_button.get('data-id')}")
                print(f"Button HTML: {duplicate_button}")
                
            can_duplicate = bool(duplicate_button)
            print(f"Can duplicate: {can_duplicate}")

            # Get main job info
            title = soup.find("h1").text.strip()
            company = soup.find("h2").find("a").text.strip()
            metadata = soup.find("span", text=re.compile(r"\d{2}-\d{2}-\d{4}")).text.strip()
            date_str = re.search(r"(\d{2}-\d{2}-\d{4})", metadata).group(1)
            date = datetime.strptime(date_str, "%d-%m-%Y").strftime("%Y-%m-%d")
            location = metadata.split(" - ")[-1]
            
            # Get stats
            stats_badge = soup.find("i", class_="badge")
            applications = re.search(r"Nbr candidatures :\s*(\d+)", stats_badge.text).group(1) if stats_badge else "0"
            
            # Get job sections
            sections = {}
            for section in soup.find_all("h3"):
                section_title = section.text.strip()
                section_content = section.find_next("p").text.strip()
                sections[section_title] = section_content

            # Get languages
            languages = []
            lang_span = soup.find("span", text=re.compile("Langue\(s\)"))
            if lang_span:
                languages = [a.text.strip("# ") for a in lang_span.find_all("a")]

            result = {
                "title": title,
                "company": company,
                "url": job_url,
                "date": date,
                "location": location,
                "languages": languages,
                "stats": {
                    "applications": int(applications)
                },
                "sections": sections,
                "can_duplicate": can_duplicate
            }
            print(f"Job details parsed successfully: {result}")
            return result

        except Exception as e:
            print(f"Error getting job details: {str(e)}")
            import traceback
            traceback.print_exc()
            raise

    def get_jobs(self, company: Optional[str] = None):
        """Get job listings from moncallcenter.ma"""
        try:
            # Get jobs page
            jobs_url = f"{self.base_url}/{company.lower()}/jobsoffres-emploi" if company else f"{self.base_url}/offres-emploi/"
            jobs_response = self.session.get(jobs_url)
            if jobs_response.status_code != 200:
                raise Exception("Failed to fetch jobs page")

            # Parse HTML
            soup = BeautifulSoup(jobs_response.text, 'html.parser')
            jobs_divs = soup.find_all("div", class_="offres")

            jobs = []
            for job_div in jobs_divs:
                try:
                    # Extract job details
                    title_elem = job_div.find("h2").find("a")
                    title = title_elem.text.strip()
                    url = f"{self.base_url}{title_elem['href']}"
                    
                    # Get full job details
                    job_details = self.get_job_details(url)
                    jobs.append(job_details)

                except Exception as e:
                    print(f"Error parsing job: {str(e)}")
                    continue

            return {
                "total": len(jobs),
                "jobs": jobs
            }

        except Exception as e:
            print(f"Error getting jobs: {str(e)}")
            import traceback
            traceback.print_exc()
            raise

    def duplicate_job(self, job_id: str) -> bool:
        """Duplicate a specific job offer"""
        try:
            # The duplication endpoint
            url = f"{self.base_url}/components/offre/duplioffre.php"
            
            # Make the duplication request with the data-id
            payload = {"id": job_id}
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
                'X-Requested-With': 'XMLHttpRequest',
                'Referer': f"{self.base_url}/offre-emploi/-{job_id}"
            }
            
            print(f"Making duplicate request to {url}")
            print(f"With payload: {payload}")
            print(f"With headers: {headers}")
            
            response = self.session.post(url, data=payload, headers=headers)
            print(f"Duplicate response status: {response.status_code}")
            print(f"Duplicate response content: {response.text[:1000]}")
            
            return response.status_code == 200
            
        except Exception as e:
            print(f"Error duplicating job {job_id}: {str(e)}")
            return False

    def check_login(self) -> bool:
        """Check if we're currently logged in"""
        try:
            response = self.session.get(f"{self.base_url}/recruteurs/")
            return response.status_code == 200 and 'login' not in response.url
        except Exception as e:
            print(f"Error checking login status: {e}")
            return False

    def _wait_for_rate_limit(self):
        """Ensure minimum time between requests"""
        now = time.time()
        time_since_last = now - self.last_request_time
        if time_since_last < self.min_request_interval:
            time.sleep(self.min_request_interval - time_since_last)
        self.last_request_time = time.time()
        
    def get_duplicatable_jobs(self, company: str) -> list:
        """Get all jobs and filter those that can be duplicated"""
        try:
            self._wait_for_rate_limit()  # Add rate limiting
            print(f"\nFetching duplicatable jobs for {company}...")
            jobs_url = f"{self.base_url}/{company}/offres-emploi"
            response = self.session.get(jobs_url)
            
            if response.status_code != 200:
                print(f"Failed to fetch jobs page. Status: {response.status_code}")
                return []

            soup = BeautifulSoup(response.text, 'html.parser')
            job_divs = soup.find_all("div", class_="offres")
            print(f"Found {len(job_divs)} total job listings")

            duplicatable_jobs = []
            for job_div in job_divs:
                try:
                    # Extract basic job info
                    title_elem = job_div.find("h2")
                    if not title_elem:
                        continue
                        
                    link_elem = title_elem.find("a")
                    if not link_elem:
                        continue
                        
                    title = link_elem.text.strip()
                    relative_url = link_elem.get('href')
                    if not relative_url:
                        continue
                        
                    full_url = f"{self.base_url}{relative_url}"
                    job_id = relative_url.split("-")[-1]

                    # Check if job can be duplicated
                    job_response = self.session.get(full_url)
                    if job_response.status_code != 200:
                        print(f"Failed to fetch job details for {job_id}")
                        continue

                    job_soup = BeautifulSoup(job_response.text, 'html.parser')
                    duplicate_button = job_soup.find("a", {
                        "class": "duplioffre", 
                        "href": "javascript:void(0)"
                    })

                    if duplicate_button:
                        print(f"Found duplicatable job: {title} (ID: {job_id})")
                        duplicatable_jobs.append({
                            "id": job_id,
                            "title": title,
                            "url": full_url
                        })

                except Exception as e:
                    print(f"Error processing job div: {str(e)}")
                    continue

            print(f"Found {len(duplicatable_jobs)} duplicatable jobs")
            return duplicatable_jobs

        except Exception as e:
            print(f"Error in get_duplicatable_jobs: {str(e)}")
            import traceback
            traceback.print_exc()
            return []

    def duplicate_random_job(self, company: str) -> dict:
        """Get duplicatable jobs and duplicate a random one for specific company"""
        try:
            print(f"\nStarting duplicate_random_job for {company}...")
            
            # Verify login status
            if not self.check_login():
                return {
                    "success": False,
                    "error": "Not logged in"
                }

            # Get duplicatable jobs for this company
            duplicatable_jobs = self.get_duplicatable_jobs(company)
            
            if not duplicatable_jobs:
                return {
                    "success": False,
                    "error": f"No duplicatable jobs found for {company}"
                }

            # Select random job
            selected_job = random.choice(duplicatable_jobs)
            print(f"\nSelected job for duplication:")
            print(f"Title: {selected_job['title']}")
            print(f"ID: {selected_job['id']}")

            # Attempt duplication
            duplicate_url = f"{self.base_url}/components/offre/duplioffre.php"
            payload = {"id": selected_job['id']}
            headers = {
                "Content-Type": "application/x-www-form-urlencoded",
                "X-Requested-With": "XMLHttpRequest",
                "Referer": selected_job['url']
            }

            print(f"Sending duplication request...")
            response = self.session.post(duplicate_url, data=payload, headers=headers)
            
            if response.status_code == 200:
                return {
                    "success": True,
                    "message": "Job duplicated successfully",
                    "job": selected_job
                }
            else:
                return {
                    "success": False,
                    "error": f"Duplication failed with status {response.status_code}",
                    "job": selected_job
                }

        except Exception as e:
            print(f"Error in duplicate_random_job: {str(e)}")
            import traceback
            traceback.print_exc()
            return {
                "success": False,
                "error": str(e)
            }

    def close(self):
        """Close the client's session"""
        try:
            if self.session:
                self.session.close()
                self.session = None
        except Exception as e:
            print(f"Error closing session: {str(e)}")
