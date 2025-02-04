import requests
from urllib3.exceptions import InsecureRequestWarning
from datetime import datetime, timedelta
import re
import pandas as pd
import json
from io import BytesIO
from bs4 import BeautifulSoup
from typing import Optional, List
import random
import time
import os
from urllib.parse import urlparse
import xlsxwriter


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
    
    def prod_filter_date(self, start_date: None, end_date: None):
        """Get campaigns and leads lists for given date range"""
        try:
            # Use today's date as default
            if not start_date:
                start_date = datetime.now().strftime("%Y-%m-%d 00:00:00")
            if not end_date:
                end_date = datetime.now().strftime("%Y-%m-%d 23:59:59")
            
            url = f"{self.base_url}/vvci/gestioncontacts/gestioncontacts/prodFilterDate"
            
            payload = {
                'start': start_date,
                'end': end_date
            }
            
            response = self.session.post(url, data=payload, verify=False)
            
            if response.status_code != 200:
                return {
                    "success": False,
                    "error": f"Request failed with status code {response.status_code}"
                }

            # Parse HTML response using BeautifulSoup
            soup = BeautifulSoup(response.text, 'html.parser')
            
            campaigns = {}
            
            # Find all optgroup elements
            for optgroup in soup.find_all('optgroup'):
                campaign_name = optgroup.get('label')
                leads_lists = []
                
                # Get all options within this campaign group
                for option in optgroup.find_all('option'):
                    leads_lists.append({
                        'id': option.get('value'),
                        'name': option.text,
                        'campaign_id': option.get('data-numcampagne')
                    })
                
                # Group by campaign ID
                campaign_id = leads_lists[0]['campaign_id'] if leads_lists else None
                
                campaigns[campaign_name] = {
                    'campaign_id': campaign_id,
                    'leads_lists': leads_lists
                }
            
            return {
                "success": True,
                "campaigns": campaigns
            }

        except Exception as e:
            return {
                "success": False,
                "error": str(e)
            }
    
    # def get_qualifs(self, campaigns_ids):
        
    
    # def get_contacts_data_display(self, start_date:None, end_date:None):
    #     """Get CRM data as JSON ONLY DISPLAY"""
    #     try:
    #         # Use today's date as default
    #         if not start_date:
    #             start_date = datetime.now().strftime("%Y-%m-%d 00:00:00")
    #         if not end_date:
    #             end_date = datetime.now().strftime("%Y-%m-%d 23:59:59")
            
    #         # Get campaigns first
    #         campaigns = self.get_campaigns(start_date, end_date)
    #         if not campaigns:
    #             return {"error": "Failed to get campaigns"}
            
    #         # Get campaign values for Prevoyance
    #         campaign_values = [c['value'] for c in campaigns.get('Prevoyance', [])]
            
    #         # Export the data using the same payload as flashProdScript
    #         url = f"{self.base_url}/vvci/gestioncontacts/gestioncontacts/search"
            
    #         # Generate unique download token
    #         download_token = f"cmk_export_{datetime.now().strftime('%Y%m%d%H%M%S')}"
            
            
            
            
    #         # Use the exact same payload as in flashProdScript
    #         payload = {
    #             'CMK_FORM_ACTION': 'display',
    #             'CMK_DWNLOAD_TOKEN': download_token,
    #             'CMK_FORM_MODEL': '54',
    #             'CMK_FORM_CONTACTS': '-1',
    #             'selectQualifs[7][]': ['76', '523'],
    #             'selectGroups[]': campaign_values,
    #             'selectGroup': 'on',
    #             'dateprod[start]': start_date,
    #             'dateprod[end]': end_date,
    #             'dateType': '1',
    #             'dateTraitement': f'Du {datetime.now().strftime("%d %B %Y")} Au {datetime.now().strftime("%d %B %Y")}',
    #             'datetrait[start]': start_date,
    #             'datetrait[end]': end_date,
    #             'selectChamps[]': '',
    #             'selectInputs[]': '-1'
    #         }

    #         # Add selectItem entries for each campaign value
    #         for value in campaign_values:
    #             payload['selectItem'] = value

    #         response = self.session.post(url, data=payload, verify=False)
            
    #         if response.status_code == 200:
    #             # Read CSV data
    #             df = pd.read_csv(BytesIO(response.content), sep=';')
    #             # Convert to JSON
    #             json_data = json.loads(df.to_json(orient='records', date_format='iso'))
    #             return {"success": True, "data": json_data}
    #         else:
    #             return {"error": f"Failed to get data. Status code: {response.status_code}"}

    #     except Exception as e:
    #         print("Error" , e)
    #         return {"error": f"Error getting data: {str(e)}"}

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
                'CMK_FORM_MODEL': '-1',
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
                # Try different encodings
                encodings = ['utf-8', 'latin1', 'iso-8859-1', 'cp1252']
                df = None
                
                for encoding in encodings:
                    try:
                        print(f"Trying encoding {encoding}")
                        df = pd.read_csv(
                            BytesIO(response.content),
                            sep=';',
                            encoding=encoding,
                            on_bad_lines='skip'
                        )
                        if df is not None and not df.empty:
                            print(f"Successfully read CSV with {encoding} encoding")
                            break
                    except Exception as e:
                        print(f"Failed with encoding {encoding}: {str(e)}")
                        continue
                        
                if df is None or df.empty:
                    return {"error": "Failed to decode CSV data with any known encoding"}
                
                try:
                    # Clean and convert the data
                    df = df.fillna('')
                    records = []
                    for _, row in df.iterrows():
                        clean_row = {}
                        for col in df.columns:
                            try:
                                val = row[col]
                                if pd.isna(val) or val == '':
                                    clean_row[col] = None
                                else:
                                    clean_row[col] = str(val).encode('utf-8', errors='ignore').decode('utf-8')
                            except Exception as e:
                                print(f"Error processing column {col}: {str(e)}")
                                clean_row[col] = None
                        records.append(clean_row)
                    
                    return {"success": True, "data": records}
                    
                except Exception as e:
                    print(f"Error during data conversion: {str(e)}")
                    return {"error": f"Error converting data: {str(e)}"}
            else:
                return {"error": f"Failed to get data. Status code: {response.status_code}"}

        except Exception as e:
            print("Error", e)
            import traceback
            traceback.print_exc()
            return {"error": f"Error getting data: {str(e)}"}

    def get_data_as_json_full(self, start_date=None, end_date=None):
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
                'CMK_FORM_MODEL': '-1',
                'CMK_FORM_CONTACTS': '-1',
                'selectQualifs[7][]': [],
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
                # Try different encodings
                encodings = ['utf-8', 'latin1', 'iso-8859-1', 'cp1252']
                df = None
                
                for encoding in encodings:
                    try:
                        print(f"Trying encoding {encoding}")
                        df = pd.read_csv(
                            BytesIO(response.content),
                            sep=';',
                            encoding=encoding,
                            on_bad_lines='skip'
                        )
                        if df is not None and not df.empty:
                            print(f"Successfully read CSV with {encoding} encoding")
                            break
                    except Exception as e:
                        print(f"Failed with encoding {encoding}: {str(e)}")
                        continue
                        
                if df is None or df.empty:
                    return {"error": "Failed to decode CSV data with any known encoding"}
                
                try:
                    # Clean and convert the data
                    df = df.fillna('')
                    records = []
                    for _, row in df.iterrows():
                        clean_row = {}
                        for col in df.columns:
                            try:
                                val = row[col]
                                if pd.isna(val) or val == '':
                                    clean_row[col] = None
                                else:
                                    clean_row[col] = str(val).encode('utf-8', errors='ignore').decode('utf-8')
                            except Exception as e:
                                print(f"Error processing column {col}: {str(e)}")
                                clean_row[col] = None
                        records.append(clean_row)
                    
                    return {"success": True, "data": records}
                    
                except Exception as e:
                    print(f"Error during data conversion: {str(e)}")
                    return {"error": f"Error converting data: {str(e)}"}
            else:
                return {"error": f"Failed to get data. Status code: {response.status_code}"}

        except Exception as e:
            print("Error", e)
            import traceback
            traceback.print_exc()
            return {"error": f"Error getting data: {str(e)}"}

    def get_data_as_json_full(self, start_date=None, end_date=None, page=1, page_size=1000):
        """Get CRM data as JSON with pagination"""
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
            
            url = f"{self.base_url}/vvci/gestioncontacts/gestioncontacts/search"
            download_token = f"cmk_export_{datetime.now().strftime('%Y%m%d%H%M%S')}"
            
            payload = {
                'CMK_FORM_ACTION': 'csv',
                'CMK_DWNLOAD_TOKEN': download_token,
                'CMK_FORM_MODEL': '-1',
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

            for value in campaign_values:
                payload['selectItem'] = value

            response = self.session.post(url, data=payload, verify=False)
            
            if response.status_code == 200:
                encodings = ['utf-8', 'latin1', 'iso-8859-1', 'cp1252']
                df = None
                
                for encoding in encodings:
                    try:
                        print(f"Trying encoding {encoding}")
                        df = pd.read_csv(
                            BytesIO(response.content),
                            sep=';',
                            encoding=encoding,
                            on_bad_lines='skip'
                        )
                        if df is not None and not df.empty:
                            print(f"Successfully read CSV with {encoding} encoding")
                            break
                    except Exception as e:
                        print(f"Failed with encoding {encoding}: {str(e)}")
                        continue
                        
                if df is None or df.empty:
                    return {"error": "Failed to decode CSV data with any known encoding"}
                
                try:
                    # Calculate total records and pages
                    total_records = len(df)
                    total_pages = (total_records + page_size - 1) // page_size
                    
                    # Calculate start and end indices for the requested page
                    start_idx = (page - 1) * page_size
                    end_idx = min(start_idx + page_size, total_records)
                    
                    # Get the subset of data for this page
                    df_page = df.iloc[start_idx:end_idx]
                    
                    # Clean and convert the page data
                    df_page = df_page.fillna('')
                    records = []
                    for _, row in df_page.iterrows():
                        clean_row = {}
                        for col in df_page.columns:
                            try:
                                val = row[col]
                                if pd.isna(val) or val == '':
                                    clean_row[col] = None
                                else:
                                    clean_row[col] = str(val).encode('utf-8', errors='ignore').decode('utf-8')
                            except Exception as e:
                                print(f"Error processing column {col}: {str(e)}")
                                clean_row[col] = None
                        records.append(clean_row)
                    
                    return {
                        "success": True,
                        "data": records,
                        "pagination": {
                            "page": page,
                            "page_size": page_size,
                            "total_records": total_records,
                            "total_pages": total_pages
                        }
                    }
                    
                except Exception as e:
                    print(f"Error during data conversion: {str(e)}")
                    return {"error": f"Error converting data: {str(e)}"}
            
            else: 
                return {"error": f"Failed to get data. Status code: {response.status_code}"}

        except Exception as e:
            print("Error", e)
            import traceback
            traceback.print_exc()
            return {"error": f"Error getting data: {str(e)}"}

    def close(self):
        """Close the client's session"""
        try:
            if self.session:
                self.session.close()
                self.session = None
        except Exception as e:
            print(f"Error closing session: {str(e)}")

    def get_campaign_qualifs(self, campaign_ids: List[str]):
        """Get and parse qualifications for given campaign IDs"""
        try:
            url = f"{self.base_url}/vvci/gestioncontacts/gestioncontacts/getQualifCampagnes"
            
            # Prepare payload
            payload = {f'campagnes[]': campaign_ids} if isinstance(campaign_ids, str) else {
                f'campagnes[]': id for id in campaign_ids
            }
            
            response = self.session.post(url, data=payload, verify=False)
            
            if response.status_code != 200:
                return {"error": f"Failed to fetch qualifications. Status code: {response.status_code}"}
            
            data = response.json()
            
            # Define positive qualification names
            positive_names = {
                "Vente", "Transfert", "Vente Reprise", "Transfert Reprise"
            }
            
            # Define callback qualification names
            callback_names = {
                "Rappel", "Rappel General", "NRP", "Répondeur"
            }
            
            # Parse and organize qualifications by campaign
            parsed_qualifs = {}
            
            for campaign in data:
                campaign_id = campaign['li_attr']['num_campagne']
                campaign_name = campaign['text']
                
                qualifs = {
                    "campaign_id": campaign_id,
                    "campaign_name": campaign_name,
                    "sales_qualifs": [],      # Ventes et transferts
                    "callback_qualifs": [],   # Rappels et NRP
                    "rejection_qualifs": [],  # Refus et autres négatifs
                    "other_qualifs": []      # Autres qualifications
                }
                
                # Process children (qualifications)
                for qualif in campaign['children']:
                    qualif_data = {
                        "id": qualif['li_attr']['num_qualif'],
                        "name": qualif['text'],
                        "type": qualif['li_attr']['type'],
                        "argumente": qualif['li_attr']['argumente'],
                        "type_qualif": qualif['li_attr']['type_qualif'],
                        "man_auto": qualif['li_attr']['man_auto']
                    }
                    
                    # Categorize based on name and business logic
                    name = qualif['text']
                    if name in positive_names:
                        qualifs["sales_qualifs"].append(qualif_data)
                    elif name in callback_names:
                        qualifs["callback_qualifs"].append(qualif_data)
                    elif name.startswith("Refus") or name in {
                        "Bloctel", "Pas interssé", "Ne pas Appeler", "Faux numero",
                        "Hors cible", "CMU", "LIVRET A / Pas de Compte"
                    }:
                        qualifs["rejection_qualifs"].append(qualif_data)
                    else:
                        qualifs["other_qualifs"].append(qualif_data)
                
                parsed_qualifs[campaign_id] = qualifs
                
            return {
                "success": True,
                "data": parsed_qualifs
            }
            
        except Exception as e:
            print(f"Error getting campaign qualifications: {str(e)}")
            import traceback
            traceback.print_exc()
            return {"error": str(e)}

    def search_data(self, campaign_ids=None, qualif_types=None, qualif_ids=None, 
                    start_date=None, end_date=None):
        """Dynamic search for CRM data"""
        try:
            # Set default dates to today if not provided
            today = datetime.now().strftime("%Y-%m-%d")
            if not start_date:
                start_date = f"{today} 00:00:00"
            if not end_date:
                end_date = f"{today} 23:59:59"

            print(f"\n=== Search Data Debug ===")
            print(f"Date range: {start_date} to {end_date}")
            print(f"Campaign IDs: {campaign_ids}")
            print(f"Qualif types: {qualif_types}")
            print(f"Initial qualif IDs: {qualif_ids}")

            # Get campaigns if not provided
            if not campaign_ids:
                campaigns_result = self.get_campaigns(start_date, end_date)
                if campaigns_result and 'Prevoyance' in campaigns_result:
                    campaign_ids = [c['value'] for c in campaigns_result['Prevoyance']]
                    print(f"Auto-selected campaigns: {campaign_ids}")

            # Get qualifications if qualif_types provided but no specific IDs
            if qualif_types and not qualif_ids:
                qualifs_result = self.get_campaign_qualifs(campaign_ids[0])
                if "success" in qualifs_result and qualifs_result["success"]:
                    qualif_ids = []
                    campaign_data = list(qualifs_result["data"].values())[0]
                    for qualif_type in qualif_types:
                        qualif_ids.extend([q["id"] for q in campaign_data[qualif_type]])
                    print(f"Selected qualif IDs based on types {qualif_types}: {qualif_ids}")

            # Generate unique download token
            download_token = f"cmk_export_{datetime.now().strftime('%Y%m%d%H%M%S')}"

            # Base payload with single values
            base_payload = {
                'CMK_FORM_ACTION': 'display',
                'CMK_DWNLOAD_TOKEN': '',
                'CMK_FORM_MODEL': '-1',
                'CMK_FORM_CONTACTS': '-1',
                'selectGroup': 'on',
                'dateprod[start]': start_date,
                'dateprod[end]': end_date,
                'dateType': '1',
                'qualifType': '1',  # Changed back to '1'
                'dateTraitement': f'Du {datetime.strptime(start_date.split()[0], "%Y-%m-%d").strftime("%d %B %Y")} Au {datetime.strptime(end_date.split()[0], "%Y-%m-%d").strftime("%d %B %Y")}',
                'datetrait[start]': start_date,
                'datetrait[end]': end_date,
                'selectChamps[]': '',
                'selectInputs[]': '-1'
            }

            # Create a list for items that can have multiple values
            multi_value_items = []

            # Add campaign IDs
            if campaign_ids:
                for campaign_id in campaign_ids:
                    multi_value_items.extend([
                        ('selectGroups[]', campaign_id),
                        ('selectItem', campaign_id)
                    ])

            # Add qualification IDs
            if qualif_ids:
                for qualif_id in qualif_ids:
                    multi_value_items.append(('selectQualifs[7][]', qualif_id))

            # Add default system qualifs (as seen in the example)
            for i in range(10, 31):
                multi_value_items.append(('selectQualifs[-1][]', f'-{i}'))

            print("\n=== Request Details ===")
            print(f"URL: {self.base_url}/vvci/gestioncontacts/gestioncontacts/search")
            print(f"Headers: {dict(self.session.headers)}")

            # Convert base_payload to list of tuples and combine with multi_value_items
            final_payload = [(k, v) for k, v in base_payload.items()] + multi_value_items
            print(f"Final Payload: {final_payload}")

            # Make the search request
            url = f"{self.base_url}/vvci/gestioncontacts/gestioncontacts/search"
            response = self.session.post(url, data=final_payload, verify=False)

            print("\n=== Response Details ===")
            print(f"Status Code: {response.status_code}")
            print(f"Response Headers: {dict(response.headers)}")
            print(f"Response Content (first 1000 chars): {response.text[:1000]}")

            if response.status_code != 200:
                return {"error": f"Search request failed with status {response.status_code}"}

            # Parse the response
            try:
                data = response.json()
                print("\n=== Parsed Response ===")
                print(f"Response Keys: {data.keys()}")
                print(f"Data Length: {len(data.get('data', []))}")
                print(f"Count Result: {data.get('countresult', 0)}")
                
                return {
                    "success": True,
                    "data": data.get("data", []),
                    "total": data.get("countresult", 0),
                    "date_range": {
                        "start": start_date,
                        "end": end_date
                    }
                }
            except json.JSONDecodeError as e:
                print(f"\nJSON Decode Error: {str(e)}")
                print(f"Raw Response: {response.text}")
                return {"error": "Failed to parse response JSON"}

        except Exception as e:
            print(f"\nError in search_data: {str(e)}")
            import traceback
            traceback.print_exc()
            return {"error": str(e)}

class CRMIncrementalClient(CRMClient):
    def __init__(self):
        super().__init__()
        self.last_fetch_file = "last_fetch.json"
        self.load_last_fetch()
        
    def load_last_fetch(self):
        """Load the last fetch time from file"""
        try:
            if os.path.exists(self.last_fetch_file):
                with open(self.last_fetch_file, 'r') as f:
                    data = json.load(f)
                    self.last_fetch_time = datetime.fromisoformat(data.get('last_fetch', '2024-01-01 00:00:00'))
            else:
                # Default to start of year if no file exists
                self.last_fetch_time = datetime(2024, 1, 1)
        except Exception as e:
            print(f"Error loading last fetch time: {e}")
            self.last_fetch_time = datetime(2024, 1, 1)

    def save_last_fetch(self, fetch_time):
        """Save the last fetch time to file"""
        try:
            with open(self.last_fetch_file, 'w') as f:
                json.dump({
                    'last_fetch': fetch_time.isoformat()
                }, f)
        except Exception as e:
            print(f"Error saving last fetch time: {e}")

    def get_incremental_data(self, current_time=None):
        """Get data since last fetch in 15-minute chunks"""
        try:
            if current_time is None:
                current_time = datetime.now()

            # If we've never fetched or it's been more than a day, limit to last 24 hours
            if (current_time - self.last_fetch_time).days >= 1:
                self.last_fetch_time = current_time - timedelta(days=1)

            # Calculate time ranges for all 15-minute intervals we need to fetch
            time_ranges = []
            interval_start = self.last_fetch_time
            while interval_start < current_time:
                interval_end = min(
                    interval_start + timedelta(minutes=15),
                    current_time
                )
                time_ranges.append((interval_start, interval_end))
                interval_start = interval_end

            all_records = []
            total_records = 0

            # Fetch data for each time range
            for start_time, end_time in time_ranges:
                try:
                    print(f"Fetching data from {start_time} to {end_time}")
                    result = self.get_data_as_json_full(
                        start_date=start_time.strftime("%Y-%m-%d %H:%M:%S"),
                        end_date=end_time.strftime("%Y-%m-%d %H:%M:%S")
                    )

                    if result.get("success") and "data" in result:
                        # Filter out duplicates based on unique identifier
                        new_records = []
                        seen_ids = set()
                        for record in result["data"]:
                            record_id = record.get("CMK_S_FIELD_ID_UNIQUE")
                            if record_id and record_id not in seen_ids:
                                seen_ids.add(record_id)
                                new_records.append(record)

                        all_records.extend(new_records)
                        total_records += len(new_records)
                        print(f"Found {len(new_records)} new records in this interval")

                except Exception as e:
                    print(f"Error fetching interval {start_time} to {end_time}: {e}")
                    continue

            # Update last fetch time only if we successfully got data
            if all_records:
                self.save_last_fetch(current_time)

            return {
                "success": True,
                "data": all_records,
                "metadata": {
                    "total_records": total_records,
                    "intervals_processed": len(time_ranges),
                    "time_range": {
                        "start": self.last_fetch_time.isoformat(),
                        "end": current_time.isoformat()
                    }
                }
            }

        except Exception as e:
            print(f"Error in incremental fetch: {e}")
            import traceback
            traceback.print_exc()
            return {"error": f"Error getting incremental data: {str(e)}"}

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
        """Get ERP contracts data as JSON with incremental loading, including daily and weekly stats"""
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
                    try:
                        # Save response content to a temporary file
                        temp_file = BytesIO(response.content)
                        
                        # Try to detect file type from content
                        content_type = response.headers.get('Content-Type', '').lower()
                        print(f"Content-Type: {content_type}")
                        
                        # Try reading with different methods
                        read_methods = [
                            # Try openpyxl first for xlsx
                            lambda: pd.read_excel(
                                temp_file,
                                engine='openpyxl'
                            ),
                            # Try odf for ods files
                            lambda: pd.read_excel(
                                temp_file,
                                engine='odf'
                            ),
                            # Try CSV with different encodings
                            lambda: pd.read_csv(
                                temp_file,
                                encoding='utf-8'
                            ),
                            lambda: pd.read_csv(
                                temp_file,
                                encoding='latin1'
                            ),
                            lambda: pd.read_csv(
                                temp_file,
                                encoding='iso-8859-1'
                            )
                        ]
                        
                        last_error = None
                        for read_method in read_methods:
                            try:
                                temp_file.seek(0)  # Reset file pointer
                                self.stored_data = read_method()
                                if not self.stored_data.empty:
                                    print("Successfully read data")
                                    break
                            except Exception as e:
                                print(f"Read attempt failed: {str(e)}")
                                last_error = e
                                continue
                        
                        if self.stored_data is None or self.stored_data.empty:
                            raise Exception(f"Failed to read data with any method. Last error: {str(last_error)}")

                        # Clean the data
                        self.stored_data = self.stored_data.replace({pd.NA: None})
                        self.stored_data = self.stored_data.fillna('')
                        
                        # Convert dates to standard format
                        for col in self.stored_data.columns:
                            try:
                                if self.stored_data[col].dtype == 'object':
                                    # Try to convert to datetime
                                    self.stored_data[col] = pd.to_datetime(
                                        self.stored_data[col], 
                                        errors='ignore',
                                        format='mixed'
                                    )
                            except Exception as e:
                                print(f"Error converting column {col}: {str(e)}")
                                continue
                        
                        # Format datetime columns
                        date_columns = self.stored_data.select_dtypes(include=['datetime64']).columns
                        for col in date_columns:
                            self.stored_data[col] = self.stored_data[col].dt.strftime('%Y-%m-%d %H:%M:%S')

                        self.last_fetch_time = datetime.now()

                        # Convert to JSON
                        json_data = self.stored_data.to_dict(orient='records')

                        # Get daily and weekly stats
                        daily_stats = self.get_daily_stats()
                        weekly_stats = self.get_weekly_stats()

                        return {
                            "success": True,
                            "data": json_data,
                            "type": "full_refresh",
                            "daily_stats": daily_stats,
                            "weekly_stats": weekly_stats
                        }

                    except Exception as e:
                        print(f"Error processing data: {str(e)}")
                        import traceback
                        traceback.print_exc()
                        return {"error": f"Error processing data: {str(e)}"}
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

                        # Get daily and weekly stats
                        daily_stats = self.get_daily_stats()
                        weekly_stats = self.get_weekly_stats()

                        return {
                            "success": True,
                            "data": json_data,
                            "type": "incremental",
                            "new_records": len(new_data) if not new_data.empty else 0,
                            "daily_stats": daily_stats,
                            "weekly_stats": weekly_stats
                        }
                    except Exception as e:
                        print(f"Error processing incremental data: {e}")
                        # If there's an error with incremental update, fall back to full refresh
                        return self.get_contracts_as_json(force_full_refresh=True)
                else:
                    return {"error": f"Failed to get incremental data. Status code: {response.status_code}"}

        except Exception as e:
            print(f"Error getting contracts: {str(e)}")
            import traceback
            traceback.print_exc()
            return {"error": f"Error getting contracts: {str(e)}"}

    def get_daily_stats(self):
        """Get daily stats of the sales"""
        try:
            if self.stored_data.empty:
                return {"error": "No data available to calculate daily stats."}

            df = self.stored_data.copy()

            # Ensure 'Créer le' is in datetime format
            df["Créer le"] = pd.to_datetime(df["Créer le"], errors='coerce')

            # Group by commercial and day, then count sales
            daily_stats = df.groupby([df['Commercial'], df['Créer le'].dt.date]).size().reset_index(name='Daily Sales')

            # Convert 'Daily Sales' to int to ensure no floats
            daily_stats['Daily Sales'] = daily_stats['Daily Sales'].astype(int)

            # Convert the dataframe to a list of dictionaries (JSON serializable format)
            daily_stats = daily_stats.to_dict(orient='records')

            return daily_stats

        except Exception as e:
            return {"error": f"Error getting daily stats: {str(e)}"}

    def get_weekly_stats(self):
        """Get weekly stats of the sales"""
        try:
            if self.stored_data.empty:
                return {"error": "No data available to calculate weekly stats."}

            df = self.stored_data.copy()

            # Ensure 'Créer le' is in datetime format
            df["Créer le"] = pd.to_datetime(df["Créer le"], errors='coerce')

            # Extract month and week number for each sale relative to the month
            df['Month'] = df['Créer le'].dt.to_period('M')  # Extracts month in YYYY-MM format
            df['Day of Month'] = df['Créer le'].dt.day
            df['Relative Week Number'] = ((df['Day of Month'] - 1) // 7) + 1

            # Group by commercial, month, and relative week number
            weekly_stats = df.groupby([df['Commercial'], df['Month'], df['Relative Week Number']]).size().reset_index(name='Weekly Sales')

            # Convert 'Weekly Sales' to int to ensure no floats
            weekly_stats['Weekly Sales'] = weekly_stats['Weekly Sales'].astype(int)

            # Convert the dataframe to a list of dictionaries (JSON serializable format)
            weekly_stats = weekly_stats.to_dict(orient='records')

            return weekly_stats

        except Exception as e:
            return {"error": f"Error getting weekly stats: {str(e)}"}


        
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
        self.session = requests.Session()
        self.base_url = "https://www.moncallcenter.ma"
        self.login_url = f"{self.base_url}/components/centre/loger_centre.php"
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })

    def login(self, username, password, timeout=30):
        """Login to the portal with retries and improved error handling"""
        max_retries = 3
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                response = self.session.post(
                    self.login_url,
                    data={
                        'username': username,
                        'password': password
                    },
                    timeout=timeout,
                    verify=False
                )
                
                if response.status_code == 200:
                    return True
                    
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                if attempt < max_retries - 1:
                    print(f"Login attempt {attempt + 1} failed: {str(e)}")
                    time.sleep(retry_delay)
                    continue
                else:
                    print(f"Error during login: {str(e)}")
                    return False
                    
            except Exception as e:
                print(f"Unexpected error during login: {str(e)}")
                return False
                
        return False

    def close(self):
        """Close the session"""
        try:
            self.session.close()
        except:
            pass

class NeoClient:
    def __init__(self):
        self.base_url = "https://extranet.neoliane.fr"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7',
            'Origin': 'https://extranet.neoliane.fr',
            'Referer': 'https://extranet.neoliane.fr/'
        })
        self.csrf_token = None
        self.mfa_code_callback = None
        self.username = None  # Store credentials
        self.password = None  # Store credentials

    def set_mfa_callback(self, callback):
        """Set callback function for getting MFA code"""
        self.mfa_code_callback = callback

    async def login(self, username, password):
        """Login to Neoliane extranet"""
        try:
            # Store credentials at the start of login
            self.username = username
            self.password = password
            
            print("\n=== Starting Neoliane Login Process ===")
            
            # Clear any existing session
            self.session = requests.Session()
            self.session.headers.update({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7'
            })
            
            # Get initial CSRF token
            print(f"Getting CSRF token from {self.base_url}/connection")
            if not self.get_csrf_token():
                print("Failed to get CSRF token")
                return False
            
            print(f"Got CSRF token: {self.csrf_token}")

            login_url = f"{self.base_url}/connection"
            payload = {
                'redirect': 'dashboard',
                'csrf_extranet_token_name': self.csrf_token,
                'lostpage': '',
                'username': username,
                'password': password,
                'g-recaptcha-response': ''
            }

            print("\nSending login request...")
            print(f"URL: {login_url}")
            print(f"Payload: {payload}")
            print(f"Current cookies: {dict(self.session.cookies)}")

            # Make login request without following redirects
            response = self.session.post(
                login_url,
                data=payload,
                verify=False,
                allow_redirects=False
            )

            print(f"\nLogin Response:")
            print(f"Status Code: {response.status_code}")
            print(f"Location header: {response.headers.get('Location', 'No redirect')}")

            # Handle redirect manually
            if response.status_code in (301, 302, 303):
                redirect_url = response.headers.get('Location')
                if redirect_url:
                    if not redirect_url.startswith('http'):
                        redirect_url = f"{self.base_url}{redirect_url}"
                    print(f"Following redirect to: {redirect_url}")
                    response = self.session.get(redirect_url, verify=False)

            # Check if we need MFA
            if 'mfa' in response.url:
                print("\nMFA required - handling MFA process")
                mfa_success = await self.handle_mfa(response.url)
                if not mfa_success:
                    print("MFA verification failed")
                    return False

            # Verify login success by checking dashboard access
            dashboard_response = self.session.get(
                f"{self.base_url}/dashboard",
                verify=False
            )
            
            login_success = dashboard_response.status_code == 200 and 'dashboard' in dashboard_response.url
            if login_success:
                print("Successfully logged into Neoliane")
                return True
            else:
                print(f"Login failed - redirected to {dashboard_response.url}")
                return False

        except Exception as e:
            print(f"\nError during login: {e}")
            import traceback
            traceback.print_exc()
            return False

    def check_login(self):
        """Check if current session is valid"""
        try:
            response = self.session.get(f"{self.base_url}/dashboard", verify=False)
            return response.status_code == 200 and 'connection' not in response.url
        except Exception as e:
            print(f"Error checking login status: {e}")
            return False

    def close(self):
        """Close the session"""
        try:
            if self.session:
                self.session.close()
                self.session = None
        except Exception as e:
            print(f"Error closing session: {e}")

    def get_csrf_token(self):
        """Get CSRF token from the login page"""
        try:
            response = self.session.get(f"{self.base_url}/connection", verify=False)
            if response.status_code == 200:
                cookies = self.session.cookies.get_dict()
                self.csrf_token = cookies.get('csrf_extranet_cookie_name')
                return self.csrf_token
            return None
        except Exception as e:
            print(f"Error getting CSRF token: {e}")
            return None

    async def handle_mfa(self, mfa_url):
        """Handle MFA verification with manual code input"""
        try:
            print(f"\n=== Starting MFA Process ===")
            print(f"MFA URL: {mfa_url}")
            
            # Parse email from MFA URL
            email = re.search(r'email=([^&]+)', mfa_url).group(1)
            who_is_email = re.search(r'whoIsEmail=([^&]+)', mfa_url).group(1)
            
            print(f"MFA required for {email} as {who_is_email}")
            print(f"Current CSRF token: {self.csrf_token}")
            print(f"Current cookies: {dict(self.session.cookies)}")
            
            # First, get the MFA page to ensure we have the correct tokens
            mfa_page_response = self.session.get(mfa_url, verify=False)
            print(f"\nMFA page response status: {mfa_page_response.status_code}")
            print(f"MFA page cookies: {dict(self.session.cookies)}")
            
            # Trigger email sending
            confirm_payload = {
                'whoIsEmail': who_is_email,
                'email': email,
                'csrf_extranet_token_name': self.csrf_token
            }
            
            print("\nSending MFA trigger request...")
            print(f"Payload: {confirm_payload}")
            
            confirm_response = self.session.post(
                f"{self.base_url}/connection/mfa/send",
                data=confirm_payload,
                verify=False
            )
            
            print(f"\nMFA trigger response:")
            print(f"Status: {confirm_response.status_code}")
            print(f"Headers: {dict(confirm_response.headers)}")
            
            if confirm_response.status_code != 200:
                print("Failed to trigger MFA email")
                return False
                
            print("MFA email triggered successfully")
            
            # Get MFA code using callback if set, otherwise use input
            if self.mfa_code_callback:
                print("Using callback for MFA code")
                mfa_code = await self.mfa_code_callback()
            else:
                print("Using manual input for MFA code")
                mfa_code = input("Please enter the MFA code from email: ")
            
            if not mfa_code:
                print("No MFA code provided")
                return False
                
            # Submit the MFA code with the correct payload format
            verify_payload = {
                'whoIsEmail': who_is_email,
                'email': email,
                'csrf_extranet_token_name': self.csrf_token,
                'code[]': mfa_code
            }
            
            print("\nSubmitting MFA verification...")
            print(f"URL: {mfa_url}")
            print(f"Payload: {verify_payload}")
            print(f"Current cookies: {dict(self.session.cookies)}")
            
            verify_response = self.session.post(
                mfa_url,
                data=verify_payload,
                verify=False,
                allow_redirects=True
            )
            
            print(f"\nMFA verification response:")
            print(f"Status: {verify_response.status_code}")
            print(f"Final URL: {verify_response.url}")
            print(f"Headers: {dict(verify_response.headers)}")
            print(f"Cookies: {dict(self.session.cookies)}")
            
            if verify_response.status_code in [200, 302, 303]:
                if 'dashboard' in verify_response.url:
                    print("MFA verification successful - reached dashboard")
                    return True
                elif 'tokenMfa' in verify_response.cookies:
                    print("MFA token received - verifying access...")
                    # Double-check dashboard access
                    dashboard_check = self.session.get(f"{self.base_url}/dashboard", verify=False)
                    if dashboard_check.status_code == 200 and 'dashboard' in dashboard_check.url:
                        print("Dashboard access confirmed")
                        return True
            
            print("MFA verification failed")
            print(f"Response content preview: {verify_response.text[:500]}")
            return False

        except Exception as e:
            print(f"\nError during MFA handling: {e}")
            print("Full error details:")
            import traceback
            traceback.print_exc()
            return False

    async def _make_request(self, url, params=None, method='GET', data=None, headers=None):
        """Helper method to make HTTP requests with full debugging"""
        try:
            # Prepare request details
            request_headers = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'Accept-Language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7',
                'Cache-Control': 'no-cache',
                'Pragma': 'no-cache',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'same-origin',
                'Sec-Fetch-User': '?1',
                'Upgrade-Insecure-Requests': '1',
                'Referer': f"{self.base_url}/dashboard"
            }
            
            if headers:
                request_headers.update(headers)
            
            self.session.headers.update(request_headers)

            # Debug request details
            print(f"\n=== Making {method} Request ===")
            print(f"URL: {url}")
            print(f"Params: {params}")
            print(f"Data: {data}")
            print(f"Headers: {dict(self.session.headers)}")
            print(f"Cookies: {dict(self.session.cookies)}")

            # Make the request
            if method.upper() == 'GET':
                response = self.session.get(url, params=params, verify=False, allow_redirects=True)
            else:
                response = self.session.post(url, params=params, data=data, verify=False, allow_redirects=True)

            # Debug response details
            print(f"\n=== Response Details ===")
            print(f"Status Code: {response.status_code}")
            print(f"Final URL: {response.url}")
            print(f"Response Headers: {dict(response.headers)}")
            print(f"New Cookies: {dict(response.cookies)}")
            
            # Save response content for debugging
            debug_filename = f"debug_response_{method.lower()}_{int(time.time())}.html"
            with open(debug_filename, 'w', encoding='utf-8') as f:
                f.write(f"<!-- Request URL: {response.url} -->\n")
                f.write(f"<!-- Status Code: {response.status_code} -->\n")
                f.write(f"<!-- Headers: {dict(response.headers)} -->\n")
                f.write(f"<!-- Cookies: {dict(response.cookies)} -->\n\n")
                f.write(response.text)
            print(f"\nResponse content saved to {debug_filename}")

            return response

        except Exception as e:
            print(f"\nError making request: {str(e)}")
            print("Full error details:")
            traceback.print_exc()
            raise

    async def get_contracts(self, start_date=None, end_date=None, page=1, limit=20):
        """Get contracts data from Neoliane extranet"""
        try:
            print("\n=== Getting Contracts Data ===")
            
            # Step 1: First check dashboard access
            dashboard_response = await self._make_request(f"{self.base_url}/dashboard")
            if '/connection' in str(dashboard_response.url):
                print("\nNot logged in, attempting login...")
                if not await self.login(self.username, self.password):
                    return {
                        "success": False,
                        "error": "Failed to login"
                    }

            # Step 2: Make the search request
            url = f"{self.base_url}/search"
            params = {
                'page': page,
                'limit': limit,
                'csrf_extranet_token_name': self.csrf_token
            }

            # Add date parameters if provided
            if start_date:
                params.update({
                    'dateinsertstart': start_date,
                    'datesignstart': start_date,
                    'dateeffectstart': start_date
                })
            if end_date:
                params.update({
                    'dateinsertend': end_date,
                    'datesignend': end_date,
                    'dateeffectend': end_date
                })

            response = await self._make_request(url, params=params)
            
            # Step 3: Check if we got redirected
            if '/connection' in str(response.url):
                print("\nGot redirected to login page after search request!")
                return {
                    "success": False,
                    "error": "Session expired during search request"
                }

            # Step 4: Parse the response
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Debug page structure
            print("\n=== Page Structure Analysis ===")
            title = soup.find('title')
            print(f"Title: {title.text if title else 'No title'}")
            print(f"Forms found: {len(soup.find_all('form'))}")
            print(f"Tables found: {len(soup.find_all('table'))}")
            
            # Find main content elements
            main_content = soup.find('div', class_='page-content')
            if not main_content:
                print("WARNING: Could not find main content div")
                
            # Find the contracts table
            table = soup.find('table', {'id': 'jsResultSearch'})
            if not table:
                print("\nERROR: Could not find contracts table!")
                print("Available tables:", [
                    {'id': t.get('id', 'no-id'), 
                     'class': t.get('class', []),
                     'rows': len(t.find_all('tr'))} 
                    for t in soup.find_all('table')
                ])
                return {
                    "success": False,
                    "error": "Could not find contracts table"
                }

            # Step 5: Extract contracts data
            contracts = []
            contract_rows = table.find_all('tr', attrs={'data-contract-id': True})
            print(f"\nFound {len(contract_rows)} contract rows")
            
            for row in contract_rows:
                try:
                    contract = self._parse_contract_row(row)
                    if contract:
                        contracts.append(contract)
                except Exception as row_error:
                    print(f"Error parsing row: {str(row_error)}")
                    continue

            # Step 6: Get pagination info
            pagination_info = self._extract_pagination_info(soup, len(contracts), limit)

            return {
                "success": True,
                "data": contracts,
                "pagination": pagination_info
            }

        except Exception as e:
            print(f"\nError getting contracts: {str(e)}")
            print("Full error details:")
            traceback.print_exc()
            return {
                "success": False,
                "error": f"Error getting contracts: {str(e)}"
            }

    def _parse_contract_row(self, row):
        """Helper method to parse a contract row"""
        try:
            contract_id = row.get('data-contract-id')
            
            # Debug row parsing
            print(f"\nParsing contract {contract_id}:")
            
            # Extract all cells with debug info
            cells = {
                'product': row.find('td', class_='x-column--product-name'),
                'status': row.find('td', class_='x-column--product-status'),
                'price': row.find('td', class_='x-column--product-price'),
                'effect_date': row.find('td', class_='x-column--effect-date'),
                'subscriber': row.find('td', class_='x-column--subscriber'),
                'phone': row.find('td', class_='x-column--contact')
            }
            
            # Debug found cells
            for name, cell in cells.items():
                print(f"- {name}: {'Found' if cell else 'Not found'}")
            
            return {
                'id': contract_id,
                'formula': cells['product'].text.strip() if cells['product'] else 'N/A',
                'status': cells['status'].find('span', class_='x-text--first-letter').text.strip() 
                         if cells['status'] else 'N/A',
                'price': cells['price'].text.strip() if cells['price'] else 'N/A',
                'effect_date': cells['effect_date'].text.strip() if cells['effect_date'] else 'N/A',
                'subscriber': cells['subscriber'].text.strip() if cells['subscriber'] else 'N/A',
                'phone': cells['phone'].text.strip() if cells['phone'] else 'N/A'
            }
            
        except Exception as e:
            print(f"Error parsing contract row {contract_id}: {str(e)}")
            return None

    def _extract_pagination_info(self, soup, contracts_count, limit):
        """Helper method to extract pagination information"""
        try:
            # Try multiple patterns to find total results
            patterns = [
                r'(\d+)\s*résultats',
                r'(\d+)\s*results',
                r'Page\s*\d+\s*/\s*(\d+)'
            ]
            
            total_count = None
            for pattern in patterns:
                results_text = soup.find(text=re.compile(pattern, re.I))
                if results_text:
                    match = re.search(pattern, results_text, re.I)
                    if match:
                        total_count = int(match.group(1))
                        break
            
            if not total_count:
                print("WARNING: Could not find total results count")
                total_count = contracts_count
            
            return {
                "current_page": 1,  # You might want to extract this from the response
                "limit": limit,
                "total": total_count,
                "total_pages": (total_count + limit - 1) // limit
            }
            
        except Exception as e:
            print(f"Error extracting pagination info: {str(e)}")
            return {
                "current_page": 1,
                "limit": limit,
                "total": contracts_count,
                "total_pages": 1
            }
