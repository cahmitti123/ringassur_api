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
import csv
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Union, Any
from bs4 import BeautifulSoup
from io import BytesIO
import base64
from urllib.parse import urljoin, urlparse
import pickle
import google.auth
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


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
        
class BaseProxyClient:
    # List of proxy servers
    PROXY_LIST = [
        "http://qapurtqr:5l41wybi63dm@198.23.239.134:6540",
        "http://qapurtqr:5l41wybi63dm@207.244.217.165:6712",
        "http://qapurtqr:5l41wybi63dm@107.172.163.27:6543",
        "http://qapurtqr:5l41wybi63dm@64.137.42.112:5157",
        "http://qapurtqr:5l41wybi63dm@173.211.0.148:6641",
        "http://qapurtqr:5l41wybi63dm@161.123.152.115:6360",
        "http://qapurtqr:5l41wybi63dm@23.94.138.75:6349",
        "http://qapurtqr:5l41wybi63dm@154.36.110.199:6853",
        "http://qapurtqr:5l41wybi63dm@173.0.9.70:5653",
        "http://qapurtqr:5l41wybi63dm@173.0.9.209:5792"
    ]

    def __init__(self):
        self.session = requests.Session()
        self.current_proxy = None
        
        # Configure session with retries
        self.session.mount('https://', requests.adapters.HTTPAdapter(
            max_retries=3,
            pool_connections=100,
            pool_maxsize=100
        ))

    def get_random_proxy(self):
        """Get a random proxy from the list"""
        self.current_proxy = random.choice(self.PROXY_LIST)
        return {'https': self.current_proxy}

    def make_request(self, method, url, retry_count=3, **kwargs):
        """Make a request with proxy rotation and retry logic"""
        last_error = None
        
        for attempt in range(retry_count):
            try:
                # Only use proxy if we're having connection issues
                if attempt > 0:
                    kwargs['proxies'] = self.get_random_proxy()
                    print(f"Attempt {attempt + 1} using proxy: {self.current_proxy.split('@')[1]}")
                
                response = self.session.request(method, url, **kwargs)
                return response
                
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                last_error = e
                if attempt < retry_count - 1:
                    print(f"Request attempt {attempt + 1} failed: {str(e)}")
                    print("Waiting before retry with new proxy...")
                    time.sleep(5)
                    continue
                
        raise last_error

class ERPClient(BaseProxyClient):
    def __init__(self):
        super().__init__()
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

class JobsClient(BaseProxyClient):
    def __init__(self):
        super().__init__()
        self.base_url = "https://www.moncallcenter.ma"
        self.mcdesk_url = "https://mcdesk.moncallcenter.ma"
        self.mcdesk_client = McProxyClient()  # Use proxy-enabled client for mcdesk
        self.last_request_time = 0
        self.min_request_interval = 1  
        self.log_file: str = 'downloaded_cvs.txt'
        self.cvs_folder: str = 'cvs'
        
        # Add default timeouts
        self.timeout = (10, 30)  # (connect timeout, read timeout)
        
        # Set up default headers
        default_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7'
        }
        
        self.session.headers.update(default_headers)

        # Create the cvs directory if it doesn't exist
        if not os.path.exists(self.cvs_folder):
            os.makedirs(self.cvs_folder)

    def login(self, username: str, password: str, timeout=None) -> bool:
        """Login to moncallcenter.ma and mcdesk subdomain"""
        try:
            # Set timeout from parameter or use default
            request_timeout = timeout or self.timeout
            
            # Login to main site using proxy-enabled request method
            login_url = f"{self.base_url}/components/centre/loger_centre.php"
            login_data = {
                "LOGIN_C": username,
                "PASSWORD_C": password
            }
            
            try:
                # Login to main site using proxy-enabled request method
                response = self.make_request(
                    'POST',
                    login_url, 
                    data=login_data, 
                    timeout=request_timeout
                )
            except requests.exceptions.RequestException as e:
                print(f"Login failed after retries: {str(e)}")
                raise
            
            # Login to mcdesk using proxy-enabled client
            mcdesk_success = self.mcdesk_client.login(username, password)
            
            # Verify main site login with proxy support
            try:
                check_response = self.make_request(
                    'GET',
                    f"{self.base_url}/recruteurs/",
                    timeout=request_timeout
                )
                main_success = check_response.status_code == 200 and 'login' not in check_response.url
                if main_success:
                    print(f"Successfully logged into moncallcenter.ma as {username}")
            except requests.exceptions.RequestException as e:
                print(f"Login verification failed: {str(e)}")
                main_success = False
            
            # Return true only if both logins succeeded
            return main_success and mcdesk_success

        except Exception as e:
            print(f"Error during login: {str(e)}")
            import traceback
            traceback.print_exc()
            return False

    def _wait_for_rate_limit(self):
        """Ensure minimum time between requests"""
        now = time.time()
        time_since_last = now - self.last_request_time
        if time_since_last < self.min_request_interval:
            time.sleep(self.min_request_interval - time_since_last)
        self.last_request_time = time.time()

    def login_mcdesk(self, username: str, password: str) -> bool:
        """Login to mcdesk.moncallcenter.ma"""
        try:
            login_url = f"{self.mcdesk_url}/components/session/loger.php"
            login_data = {
                "LOGIN_APP": username,
                "PASSWORD_APP": password
            }

            response = self.mcdesk_client.session.post(login_url, data=login_data)
            if response.status_code == 200:
                print(f"Successfully logged into mcdesk as {username}")
                return True

            print(f"Failed to login to mcdesk. Status: {response.status_code}")
            return False

        except Exception as e:
            print(f"Error during mcdesk login: {str(e)}")
            return False

    def check_mcdesk_session(self) -> bool:
        """Check if mcdesk session is active"""
        try:
            response = self.mcdesk_client.session.get(self.mcdesk_url)
            return response.status_code == 200 and "login" not in response.url
        except Exception as e:
            print(f"Error checking mcdesk session: {str(e)}")
            return False

    def get_mcdesk_data(self, endpoint: str, params: dict = None) -> dict:
        """Fetch data from mcdesk.moncallcenter.ma"""
        try:
            url = f"{self.mcdesk_url}/{endpoint}"
            response = self.mcdesk_client.session.get(url, params=params)

            if response.status_code == 200:
                print(f"Data fetched successfully from {url}")
                return response.json()

            print(f"Failed to fetch data from mcdesk. Status: {response.status_code}")
            return {}

        except Exception as e:
            print(f"Error fetching data from mcdesk: {str(e)}")
            return {}

    def close(self):
        """Close all sessions"""
        try:
            if self.session:
                self.session.close()
                self.session = None
            if self.mcdesk_client.session:
                self.mcdesk_client.session.close()
                self.mcdesk_client.session = None
        except Exception as e:
            print(f"Error closing sessions: {str(e)}")
            
    def get_job_details(self, job_url: str):
        """Get detailed information about a specific job"""
        try:
            print(f"\nFetching details for job: {job_url}")
            response = self.make_request('GET', job_url)
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
    
    
    def parse_pagination(self, html_content: str) -> dict:
        """
        Parse pagination information from mcdesk.moncallcenter.ma HTML content.
        
        Args:
            html_content (str): Raw HTML content from the page
            
        Returns:
            dict: Dictionary containing pagination information including:
                - current_page: Current page number
                - last_page: Last page number
                - total_entries: Total number of entries if available
        """
        try:
            print("Starting pagination parsing...")
            
            # Parse HTML content
            soup = BeautifulSoup(html_content, 'html.parser')
            print("HTML content parsed successfully.")
            
            # Find all pagination links
            pagination_links = soup.find_all('a', href=True)
            print(f"Found {len(pagination_links)} links on the page.")
            
            # Extract page numbers
            page_numbers = []
            for link in pagination_links:
                href = link.get('href')
                if href and 'page=' in href:
                    try:
                        page_num = int(re.search(r'page=(\d+)', href).group(1))
                        page_numbers.append(page_num)
                    except (ValueError, AttributeError):
                        continue

            # Determine last page number
            if page_numbers:
                last_page = max(page_numbers)
                print(f"Last page number determined: {last_page}")
            else:
                # If no pagination is found, assume we're on the only page
                last_page = 1
                print("No pagination found - assuming single page (last_page=1).")
            
            # Get total entries if available
            total_entries = None
            entries_text = soup.find('h3')
            if entries_text:
                print(f"Found entries text: {entries_text.text}")
                match = re.search(r'(\d+)\s+candidatures', entries_text.text)
                if match:
                    total_entries = int(match.group(1))
                    print(f"Total entries found: {total_entries}")
                else:
                    print("No total entries match found in text.")
            else:
                print("No entries text (h3) found.")
            
            print("Pagination parsing completed successfully.")
            return {
                "current_page": 1,  # Default to 1 since we're on the first page
                "last_page": last_page,
                "total_entries": total_entries
            }
        
        except Exception as e:
            print(f"Error parsing pagination: {str(e)}")
            import traceback
            traceback.print_exc()
            # Return default values instead of None to avoid errors
            return {
                "current_page": 1,
                "last_page": 1,
                "total_entries": None
            }
    
    def download_cv(self, cv_url: str):
        """Download the CV file and store it in the cvs folder with original filename."""
        try:
            response = self.mcdesk_client.make_request('GET', cv_url)
            response.raise_for_status()
            
            # Get filename from Content-Disposition header, fallback to last URL segment
            if 'Content-Disposition' in response.headers:
                content_disposition = response.headers['Content-Disposition']
                filename = re.findall("filename=(.+)", content_disposition)[0].strip('"')
            else:
                filename = cv_url.split('/')[-1]
                
            file_path = os.path.join(self.cvs_folder, filename)
            
            with open(file_path, 'wb') as file:
                file.write(response.content)
            print(f"Downloaded: {cv_url} to {file_path}")
            
            with open(self.log_file, 'a') as log:
                log.write(f"{cv_url}\n")
                
        except requests.exceptions.RequestException as e:
            print(f"Error downloading {cv_url}: {e}")

    def get_candidatures(self, company: Optional[str] = None) -> List[dict]:
        """Get candidatures listings and details from mcdesk."""
        try:
            cands_url = f"{self.mcdesk_url}/candidatures/?"
            print(f"Fetching candidatures from: {cands_url}")
            cands_response = self.mcdesk_client.make_request('GET', cands_url)
            
            if cands_response.status_code != 200:
                raise Exception(f"Failed to fetch candidatures: {cands_response.status_code}")
            
            # Save the HTML for debugging
            with open('debug_candidates_page.html', 'w', encoding='utf-8') as f:
                f.write(cands_response.text)
            print("Saved candidates HTML to debug_candidates_page.html for inspection")
            
            # Parse the main page
            soup = BeautifulSoup(cands_response.text, 'html.parser')
            
            # Find the candidature count (e.g., "1941 candidatures")
            candidature_text = soup.find(text=re.compile(r'\d+\s+candidatures'))
            total_candidatures = 0
            if candidature_text:
                match = re.search(r'(\d+)\s+candidatures', candidature_text)
                if match:
                    total_candidatures = int(match.group(1))
                    print(f"Found {total_candidatures} total candidatures")
            
            # Get pagination information
            pagination = soup.select('ul.pagination li a')
            page_numbers = []
            for link in pagination:
                if link.text.isdigit():
                    page_numbers.append(int(link.text))
            
            if page_numbers:
                last_page = max(page_numbers)
                print(f"Found pagination with {last_page} pages")
            else:
                # Look for the last page indicator
                last_page_elem = soup.select_one('a[href*="page"][href$="-86"]')
                if last_page_elem:
                    try:
                        last_page_text = last_page_elem.text.strip()
                        if last_page_text.isdigit():
                            last_page = int(last_page_text)
                            print(f"Found last page through href: {last_page}")
                        else:
                            last_page = 1
                    except:
                        last_page = 1
                else:
                    last_page = 1
                    print("No pagination found, assuming single page")
            
            # Initialize storage
            candidates_details = []
            
            # Process each page
            for page in range(1, last_page + 1):
                page_url = f"{cands_url}page={page}"
                print(f"\nProcessing page {page} of {last_page}...")
                
                if page > 1:  # We already have page 1 content
                    page_response = self.mcdesk_client.make_request('GET', page_url)
                    if page_response.status_code != 200:
                        print(f"Failed to fetch page {page}. Skipping...")
                        continue
                    page_soup = BeautifulSoup(page_response.text, 'html.parser')
                else:
                    page_soup = soup  # Use the already parsed soup
                
                # Find the main candidates table - based on the screenshot it appears to be the only table
                candidate_table = page_soup.find('table', class_='table-bordered')
                if not candidate_table:
                    # Try without the class if not found
                    candidate_table = page_soup.find('table')
                
                if not candidate_table:
                    print(f"No candidate table found on page {page}")
                    continue
                
                # Get all rows from the table
                rows = candidate_table.find_all('tr')
                header_row = rows[0] if rows else None
                
                if not header_row:
                    print("No header row found in the table")
                    continue
                
                # Extract table headers to identify columns
                headers = [th.text.strip() for th in header_row.find_all(['th', 'td'])]
                print(f"Found table headers: {headers}")
                
                # Determine column indices
                date_idx = next((i for i, h in enumerate(headers) if 'date' in h.lower()), 0)
                name_idx = next((i for i, h in enumerate(headers) if 'nom' in h.lower() or 'name' in h.lower()), 1)
                cv_idx = next((i for i, h in enumerate(headers) if 'cv' in h.lower()), 2)
                offer_idx = next((i for i, h in enumerate(headers) if 'offre' in h.lower() or 'offer' in h.lower()), 3)
                
                # Process each row
                for row in rows[1:]:  # Skip header
                    try:
                        # Extract cells
                        cells = row.find_all(['td', 'th'])
                        if len(cells) <= max(date_idx, name_idx, cv_idx, offer_idx):
                            continue
                        
                        # Extract date and time
                        date_cell = cells[date_idx]
                        date_text = date_cell.text.strip()
                        try:
                            date_parts = date_text.split(' ')
                            date = date_parts[0] if date_parts else "Unknown"
                            time = date_parts[1] if len(date_parts) > 1 else "00:00"
                        except:
                            date = date_text
                            time = "00:00"
                        
                        # Extract candidate name
                        name_cell = cells[name_idx]
                        name = name_cell.text.strip()
                        
                        # Look for candidate details link
                        candidate_url = None
                        candidate_id = None
                        detail_link = name_cell.find('a')
                        if detail_link and 'href' in detail_link.attrs:
                            candidate_url = detail_link['href']
                            id_match = re.search(r'id-(\d+)', candidate_url)
                            candidate_id = id_match.group(1) if id_match else None
                        
                        # Extract CV link
                        cv_cell = cells[cv_idx]
                        cv_link = cv_cell.find('a')
                        cv_url = cv_link['href'] if cv_link and 'href' in cv_link.attrs else None
                        
                        # Get offer details
                        offer_cell = cells[offer_idx]
                        offer = offer_cell.text.strip()
                        
                        # Create candidate record
                        candidate = {
                            'id': candidate_id or f"unknown-{len(candidates_details)}",
                            'name': name,
                            'date': date,
                            'time': time,
                            'offer': offer,
                            'url': f"{self.mcdesk_url}{candidate_url}" if candidate_url else None,
                            'cv_url': cv_url
                        }
                        
                        print(f"Found candidate: {name} ({date})")
                        candidates_details.append(candidate)
                        
                    except Exception as e:
                        print(f"Error processing candidate row: {str(e)}")
                        continue
            
            print(f"\nTotal candidates processed: {len(candidates_details)}")
            return candidates_details
        
        except Exception as e:
            print(f"Error getting candidatures: {str(e)}")
            import traceback
            traceback.print_exc()
            return []

    def get_jobs(self, company: Optional[str] = None):
        """Get job listings from moncallcenter.ma"""
        try:
            # Get jobs page
            jobs_url = f"{self.base_url}/{company.lower()}/jobsoffres-emploi" if company else f"{self.base_url}/offres-emploi/"
            jobs_response = self.make_request('GET', jobs_url)
            
            print(
                'url ', jobs_url
            )
            print(
                'jobs_response' , jobs_response.status_code 
            )
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
            
            response = self.make_request('POST', url, data=payload, headers=headers)
            print(f"Duplicate response status: {response.status_code}")
            print(f"Duplicate response content: {response.text[:1000]}")
            
            return response.status_code == 200
            
        except Exception as e:
            print(f"Error duplicating job {job_id}: {str(e)}")
            return False

    def check_login(self) -> bool:
        """Check if we're currently logged in"""
        try:
            response = self.make_request('GET', f"{self.base_url}/recruteurs/")
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
            response = self.make_request('GET', jobs_url)
            
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
                    job_response = self.make_request('GET', full_url)
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

    def export_candidatures_to_csv(self, company: Optional[str] = None, output_file: str = 'candidatures.csv') -> str:
        """
        Export candidatures to a CSV file without downloading CV files
        
        Args:
            company: Optional company name to filter candidatures
            output_file: Name of the output CSV file
            
        Returns:
            Message with export result
        """
        try:
            # Get candidatures data (without downloading CVs)
            candidates = self.get_candidatures(company)
            
            if not candidates:
                print("No candidatures found to export")
                return f"No candidatures found to export"
            
            # Create CSV file
            import csv
            
            # Ensure the directory exists
            output_dir = os.path.dirname(output_file)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir)
            
            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                # Write header
                writer.writerow(['ID', 'Name', 'Date', 'Time', 'Offer', 'CV URL'])
                
                # Write data
                for candidate in candidates:
                    writer.writerow([
                        candidate.get('id', 'N/A'),
                        candidate.get('name', 'N/A'),
                        candidate.get('date', 'N/A'),
                        candidate.get('time', 'N/A'),
                        candidate.get('offer', 'N/A'),
                        candidate.get('cv_url', 'N/A')
                    ])
            
            print(f"Successfully exported {len(candidates)} candidatures to {output_file}")
            return f"Successfully exported {len(candidates)} candidatures to {output_file}"
            
        except Exception as e:
            error_msg = f"Error exporting candidatures to CSV: {str(e)}"
            print(error_msg)
            import traceback
            traceback.print_exc()
            return error_msg

    def export_candidatures_to_google_sheet(self, sheet_id: str, company: Optional[str] = None, sheet_name: Optional[str] = None) -> str:
        """
        Export candidatures to a Google Sheet and only add new candidates.
        Stops processing when it encounters an ID that already exists in the sheet.
        
        Args:
            sheet_id: The ID of the Google Sheet to update
            company: Optional company name to filter candidatures
            sheet_name: Optional name of the specific sheet/tab to update (e.g., "Xpercia" or "Perextel")
            
        Returns:
            Message with export result
        """
        try:
            # Load Google credentials from service account file
            # This assumes you have a credentials file named "google_credentials.json" in the project directory
            creds_file = "google_credentials.json"
            if not os.path.exists(creds_file):
                return f"Error: Google credentials file '{creds_file}' not found"
            
            # Authenticate with Google Sheets API
            try:
                credentials = Credentials.from_service_account_file(
                    creds_file,
                    scopes=['https://www.googleapis.com/auth/spreadsheets']
                )
                sheets_service = build('sheets', 'v4', credentials=credentials)
                print("Successfully authenticated with Google Sheets API")
            except Exception as e:
                error_msg = f"Error authenticating with Google Sheets API: {str(e)}"
                print(error_msg)
                return error_msg
            
            # Get candidatures data (without downloading CVs)
            candidates = self.get_candidatures(company)
            
            if not candidates:
                print("No candidatures found to export")
                return "No candidatures found to export"
            
            # Prepare sheet range with sheet name if provided
            sheet_range_prefix = f"'{sheet_name}'!" if sheet_name else ""
            
            # Read existing data from the sheet to get IDs
            try:
                result = sheets_service.spreadsheets().values().get(
                    spreadsheetId=sheet_id,
                    range=f"{sheet_range_prefix}A2:A"  # Assuming ID is in column A, starting from row 2 (after header)
                ).execute()
                
                existing_ids = []
                if 'values' in result:
                    existing_ids = [row[0] for row in result.get('values', []) if row]
                
                print(f"Found {len(existing_ids)} existing candidates in the sheet")
            except Exception as e:
                error_msg = f"Error reading existing data from Google Sheet: {str(e)}"
                print(error_msg)
                return error_msg
            
            # Filter candidates to only include new ones
            new_candidates = []
            for candidate in candidates:
                candidate_id = candidate.get('id', 'N/A')
                
                # If we encounter an existing ID, stop the extraction process
                if candidate_id in existing_ids:
                    print(f"Found existing candidate ID: {candidate_id}. Stopping extraction.")
                    break
                
                new_candidates.append([
                    candidate.get('id', 'N/A'),
                    candidate.get('name', 'N/A'),
                    candidate.get('date', 'N/A'),
                    candidate.get('time', 'N/A'),
                    candidate.get('offer', 'N/A'),
                    candidate.get('cv_url', 'N/A')
                ])
            
            if not new_candidates:
                print("No new candidates to add")
                return "No new candidates found to add to the sheet"
            
            # Check if the sheet has headers already
            result = sheets_service.spreadsheets().values().get(
                spreadsheetId=sheet_id,
                range=f"{sheet_range_prefix}A1:F1"  # Check the header row
            ).execute()
            
            header_exists = 'values' in result and len(result.get('values', [])) > 0
            
            # If no header exists, add it
            if not header_exists:
                header = [['ID', 'Name', 'Date', 'Time', 'Offer', 'CV URL']]
                sheets_service.spreadsheets().values().update(
                    spreadsheetId=sheet_id,
                    range=f"{sheet_range_prefix}A1:F1",
                    valueInputOption='RAW',
                    body={'values': header}
                ).execute()
                print("Added header row to sheet")
            
            # Append new candidates to the sheet
            append_range = f"{sheet_range_prefix}A2" if not existing_ids else f"{sheet_range_prefix}A{len(existing_ids) + 2}"
            sheets_service.spreadsheets().values().append(
                spreadsheetId=sheet_id,
                range=append_range,
                valueInputOption='RAW',
                insertDataOption='INSERT_ROWS',
                body={'values': new_candidates}
            ).execute()
            
            sheet_info = f" (in '{sheet_name}' tab)" if sheet_name else ""
            print(f"Successfully added {len(new_candidates)} new candidates to the Google Sheet{sheet_info}")
            return f"Successfully added {len(new_candidates)} new candidates to the Google Sheet{sheet_info}"
            
        except Exception as e:
            error_msg = f"Error exporting candidatures to Google Sheet: {str(e)}"
            print(error_msg)
            import traceback
            traceback.print_exc()
            return error_msg

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

    def close(self):
        """Close the client's sessions"""
        try:
            # Close the main session through parent class
            super().close()
            
            # Close the mcdesk session separately
            if self.mcdesk_client.session:
                self.mcdesk_client.session.close()
                self.mcdesk_client.session = None
                
            print("All sessions closed successfully")
        except Exception as e:
            print(f"Error closing sessions: {str(e)}")

class McProxyClient(BaseProxyClient):
    """Client specifically for mcdesk.moncallcenter.ma with proxy support"""
    def __init__(self):
        super().__init__()
        self.base_url = "https://mcdesk.moncallcenter.ma"
        self.timeout = (10, 30)  # (connect timeout, read timeout)
        
        # Set up default headers
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7'
        })

    def login(self, username: str, password: str, timeout=None) -> bool:
        """Login to mcdesk.moncallcenter.ma with proxy support"""
        try:
            # Use provided timeout or default
            request_timeout = timeout or self.timeout
            
            login_url = f"{self.base_url}/components/session/loger.php"
            login_data = {
                "LOGIN_APP": username,
                "PASSWORD_APP": password
            }
            
            # Login using proxy-enabled request method
            response = self.make_request(
                'POST',
                login_url, 
                data=login_data, 
                timeout=request_timeout
            )
            
            # Verify login success
            check_response = self.make_request(
                'GET',
                f"{self.base_url}/candidatures/?",
                timeout=request_timeout
            )
            
            success = check_response.status_code == 200 and 'login' not in check_response.url
            if success:
                print(f"Successfully logged into mcdesk.moncallcenter.ma as {username}")
            else:
                print(f"Failed to log into mcdesk.moncallcenter.ma")
                
            return success
            
        except Exception as e:
            print(f"Error logging into mcdesk: {str(e)}")
            return False

# ============================================================================
# PROXY IMPLEMENTATION SUMMARY
# ============================================================================
# 
# Current Implementation:
# - BaseProxyClient provides a proxy management base class with proxy rotation
# - ERPClient inherits from BaseProxyClient and uses proxy functionality
# - JobsClient now inherits from BaseProxyClient for main site requests
# - McProxyClient added for mcdesk site with proxy functionality
# 
# Remaining Work:
# - Some methods in JobsClient still need to be updated to use proxy functionality:
#   - login_mcdesk
#   - check_mcdesk_session
#   - get_mcdesk_data
#   - Any other methods using mcdesk_client.session directly
# 
# - Consider implementing a full proxy management system for both domains
#   to reduce the risk of IP bans and improve reliability
# ============================================================================
