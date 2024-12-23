import requests
from urllib3.exceptions import InsecureRequestWarning
from datetime import datetime
import re
import pandas as pd
import customtkinter as ctk
from CTkTable import CTkTable
import threading
import traceback

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

class ERPClient:
    def __init__(self):
        self.base_url = "https://erp.ringassur.fr"
        self.session = requests.Session()
        # Set up default headers
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7,ar;q=0.6'
        })

    def login(self, email, password):
        """Login to the ERP system"""
        try:
            # First get the login page to get the initial session
            print("\nGetting login page...")
            response = self.session.get(
                f"{self.base_url}/",
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

    def export_contracts(self):
        """Export contracts from ERP as Excel file"""
        try:
            url = f"{self.base_url}/contracts/export"
            response = self.session.get(url, verify=False)
            
            if response.status_code == 200:
                # Save the Excel content to a file
                filename = f"erp_contracts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                with open(filename, 'wb') as f:
                    f.write(response.content)
                print(f"Contracts exported successfully to {filename}")
                return filename
            else:
                print(f"Failed to export contracts. Status code: {response.status_code}")
                return None

        except Exception as e:
            print(f"Error exporting contracts: {str(e)}")
            return None

class ModernTheme:
    """Enhanced modern theme colors and styles"""
    # Colors
    PRIMARY = "#1976D2"  # Darker blue for better contrast
    PRIMARY_DARK = "#0D47A1"
    PRIMARY_LIGHT = "#BBDEFB"
    
    SECONDARY = "#43A047"  # Adjusted green
    WARNING = "#FB8C00"    # Warmer orange
    DANGER = "#E53935"     # Brighter red
    
    BG_LIGHT = "#FFFFFF"
    BG_DARK = "#1E1E1E"    # Darker background
    BG_GRAY = "#F5F5F5"
    
    TEXT_DARK = "#212121"
    TEXT_LIGHT = "#FFFFFF"
    TEXT_GRAY = "#757575"
    
    # Status Colors
    STATUS_EXCELLENT = "#43A047"
    STATUS_GOOD = "#FB8C00"
    STATUS_AVERAGE = "#FDD835"
    STATUS_POOR = "#E53935"
    
    # Gradients
    HEADER_GRADIENT = ["#1976D2", "#0D47A1"]
    SUCCESS_GRADIENT = ["#43A047", "#2E7D32"]
    WARNING_GRADIENT = ["#FB8C00", "#EF6C00"]
    DANGER_GRADIENT = ["#E53935", "#C62828"]
    
    # Fonts
    FONT_FAMILY = "Segoe UI"
    FONT_SIZES = {
        "title": 24,
        "subtitle": 18,
        "header": 14,
        "body": 12,
        "small": 10
    }
    
    # Spacing
    PADDING = {
        "small": 5,
        "medium": 10,
        "large": 20
    }
    
    # Borders
    BORDER_RADIUS = 10
    BORDER_WIDTH = 1

class LoadingOverlay:
    """Loading overlay with animation"""
    def __init__(self, master):
        # Create overlay with dark theme color
        self.overlay = ctk.CTkFrame(
            master,
            fg_color=("#000000", "#000000"),  # Black in both light/dark mode
            corner_radius=0,
            border_width=0
        )
        
        # Center container for spinner and message
        self.container = ctk.CTkFrame(
            self.overlay,
            fg_color="transparent"
        )
        
        # Create spinner with larger font
        self.spinner_label = ctk.CTkLabel(
            self.container,
            text="⟳",  # Unicode loading symbol
            font=(ModernTheme.FONT_FAMILY, 48),
            text_color=ModernTheme.PRIMARY_LIGHT
        )
        self.spinner_label.pack(pady=10)
        
        # Create message label
        self.message_label = ctk.CTkLabel(
            self.container,
            text="Loading...",
            font=(ModernTheme.FONT_FAMILY, ModernTheme.FONT_SIZES["body"]),
            text_color=ModernTheme.TEXT_LIGHT
        )
        self.message_label.pack(pady=5)
        
        # Configure transparency
        self._configure_transparency()
        
    def _configure_transparency(self):
        """Configure transparency for the overlay"""
        try:
            # Try to set window transparency (works on most systems)
            self.overlay.configure(fg_color=("#000000", "#000000"))
            
            # Create semi-transparent effect using multiple layers
            for i in range(3):
                layer = ctk.CTkFrame(
                    self.overlay,
                    fg_color=("#000000", "#000000"),
                    corner_radius=0
                )
                layer.place(relx=0, rely=0, relwidth=1, relheight=1)
        except Exception as e:
            print(f"Warning: Could not configure transparency: {e}")
        
    def show(self, message="Loading..."):
        """Show the loading overlay"""
        # Ensure overlay is on top
        self.overlay.lift()
        self.overlay.place(relx=0, rely=0, relwidth=1, relheight=1)
        
        # Center the container
        self.container.place(relx=0.5, rely=0.5, anchor="center")
        
        # Update message
        self.message_label.configure(text=message)
        
        # Start animation
        self._animate()
        
    def hide(self):
        """Hide the loading overlay"""
        self.overlay.place_forget()
        self.container.place_forget()
        
    def _animate(self):
        """Animate the spinner"""
        if self.overlay.winfo_ismapped():
            self.spinner_label.configure(text="⟳")
            self.overlay.after(100, lambda: self._rotate_frame(1))
    
    def _rotate_frame(self, frame):
        """Rotate the spinner frame by frame"""
        if self.overlay.winfo_ismapped():
            frames = ["⟳", "⟲", "⟱", "⟰"]
            self.spinner_label.configure(text=frames[frame % len(frames)])
            self.overlay.after(100, lambda: self._rotate_frame(frame + 1))

class ReportGUI:
    def __init__(self):
        self.theme = ModernTheme()
        
        # Set theme
        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("blue")
        
        # Create window
        self.root = ctk.CTk()
        self.root.title("Flash Prod Report")
        self.root.geometry("1400x900")
        
        # Create loading overlay
        self.loading = LoadingOverlay(self.root)
        
        # Create main container
        self.container = ctk.CTkFrame(
            self.root,
            fg_color=self.theme.BG_DARK,
            corner_radius=self.theme.BORDER_RADIUS
        )
        self.container.pack(fill="both", expand=True, padx=10, pady=10)
        
        # Setup UI components
        self._setup_header()
        self._setup_tabs()
        self._setup_tables()
        self._setup_footer()
        
        # Initialize data
        self.refresh_data()

    def _setup_header(self):
        """Setup header with gradient and controls"""
        self.header = ctk.CTkFrame(
            self.container,
            fg_color=self.theme.PRIMARY_DARK,
            corner_radius=10,
            height=60
        )
        self.header.pack(fill="x", padx=10, pady=(10, 5))
        
        # Title with icon
        title_frame = ctk.CTkFrame(self.header, fg_color="transparent")
        title_frame.pack(side="left", padx=20)
        
        title_icon = ctk.CTkLabel(
            title_frame,
            text="📊",
            font=(self.theme.FONT_FAMILY, self.theme.FONT_SIZES["title"])
        )
        title_icon.pack(side="left", padx=(0, 10))
        
        title = ctk.CTkLabel(
            title_frame,
            text="Flash Prod Dashboard",
            font=(self.theme.FONT_FAMILY, self.theme.FONT_SIZES["title"], "bold"),
            text_color=self.theme.TEXT_LIGHT
        )
        title.pack(side="left")
        
        # Controls frame
        controls = ctk.CTkFrame(self.header, fg_color="transparent")
        controls.pack(side="right", padx=20)
        
        # Date picker
        self.date_picker = ctk.CTkEntry(
            controls,
            placeholder_text="Select Date",
            width=120
        )
        self.date_picker.pack(side="left", padx=10)
        self.date_picker.insert(0, "2024-12-20")
        
        # Refresh button
        self.refresh_btn = ctk.CTkButton(
            controls,
            text="↻ Refresh",
            command=self.refresh_data,
            width=100,
            height=32,
            corner_radius=16,
            fg_color=self.theme.SECONDARY,
            hover_color=self.theme.PRIMARY_DARK
        )
        self.refresh_btn.pack(side="left", padx=10)
        
        # Status label
        self.status = ctk.CTkLabel(
            controls,
            text="",
            font=(self.theme.FONT_FAMILY, self.theme.FONT_SIZES["body"]),
            text_color=self.theme.TEXT_LIGHT
        )
        self.status.pack(side="left", padx=10)

    def _setup_tabs(self):
        """Setup tabview with modern styling"""
        self.tabview = ctk.CTkTabview(
            self.container,
            fg_color=self.theme.BG_GRAY,
            segmented_button_fg_color=self.theme.PRIMARY_DARK,
            segmented_button_selected_color=self.theme.PRIMARY,
            segmented_button_selected_hover_color=self.theme.PRIMARY_LIGHT,
            segmented_button_unselected_hover_color=self.theme.PRIMARY_DARK
        )
        self.tabview.pack(fill="both", expand=True, padx=10, pady=5)
        
        # Add tabs with icons
        self.hourly_tab = self.tabview.add("📊 Hourly Report")
        self.agents_tab = self.tabview.add("👥 Agent Performance")
        self.stats_tab = self.tabview.add("📈 Statistics")

    def _setup_footer(self):
        """Setup footer with additional info"""
        self.footer = ctk.CTkFrame(
            self.container,
            fg_color=self.theme.BG_DARK,
            height=30
        )
        self.footer.pack(fill="x", padx=10, pady=5)
        
        # Version info
        version = ctk.CTkLabel(
            self.footer,
            text="v1.0.0",
            font=(self.theme.FONT_FAMILY, self.theme.FONT_SIZES["small"]),
            text_color=self.theme.TEXT_GRAY
        )
        version.pack(side="right", padx=10)

    def setup_hourly_table(self):
        """Setup hourly report table with modern styling"""
        # Create scrollable container
        self.hourly_scroll = ctk.CTkScrollableFrame(
            self.hourly_tab,
            fg_color="transparent"
        )
        self.hourly_scroll.pack(fill="both", expand=True, padx=20, pady=20)
        
        # Headers with fixed widths
        headers = [
            ("👤 Agent", 200),
            ("🕙 10:00-11:00", 120),
            ("🕙 11:00-12:00", 120),
            ("🕙 12:00-13:00", 120),
            ("🕙 13:00-15:00", 120),
            ("🕙 15:00-16:00", 120),
            ("🕙 16:00-17:00", 120),
            ("🕙 17:00-18:00", 120),
            ("🕙 18:00-19:30", 120),
            ("📊 Total", 100)
        ]
        
        # Create header row
        header_frame = ctk.CTkFrame(
            self.hourly_scroll,
            fg_color=self.theme.PRIMARY,
            height=40
        )
        header_frame.pack(fill="x", pady=(0, 1))
        header_frame.pack_propagate(False)
        
        for header, width in headers:
            label = ctk.CTkLabel(
                header_frame,
                text=header,
                font=(self.theme.FONT_FAMILY, self.theme.FONT_SIZES["header"], "bold"),
                width=width,
                anchor="center"
            )
            label.pack(side="left", padx=1)
        
        # Initialize table cells with better spacing
        self.hourly_cells = []
        for i in range(12):  # Increased rows for better scrolling
            row_frame = ctk.CTkFrame(
                self.hourly_scroll,
                fg_color="transparent",
                height=35
            )
            row_frame.pack(fill="x", pady=1)
            row_frame.pack_propagate(False)
            
            row_cells = []
            for _, width in headers:
                cell = ctk.CTkLabel(
                    row_frame,
                    text="",
                    font=(self.theme.FONT_FAMILY, self.theme.FONT_SIZES["body"]),
                    fg_color=self.theme.BG_LIGHT,
                    width=width,
                    height=35,
                    anchor="center"
                )
                cell.pack(side="left", padx=1)
                row_cells.append(cell)
            self.hourly_cells.append(row_cells)

    def setup_agents_table(self):
        """Setup agent performance table with modern styling"""
        # Create main container
        self.agents_container = ctk.CTkFrame(
            self.agents_tab,
            fg_color=self.theme.BG_LIGHT,
            corner_radius=10
        )
        self.agents_container.pack(fill="both", expand=True, padx=20, pady=20)

        # Create table frame (left side, 70% width)
        table_frame = ctk.CTkFrame(
            self.agents_container,
            fg_color="transparent"
        )
        table_frame.pack(side="left", fill="both", expand=True, padx=(0, 10))

        # Headers with icons and fixed widths
        headers = [
            ("👤 Agent", 200),
            ("📊 Total", 80),
            ("✅ Contracts", 80),
            ("📈 Ratio", 80),
            ("⏱️ Avg/Hour", 100),
            ("⭐ Quality", 80),
            ("📋 Status", 120)
        ]

        # Create scrollable frame for table content
        self.agents_scroll = ctk.CTkScrollableFrame(
            table_frame,
            fg_color="transparent"
        )
        self.agents_scroll.pack(fill="both", expand=True)

        # Create header row
        header_frame = ctk.CTkFrame(
            self.agents_scroll,
            fg_color=self.theme.PRIMARY
        )
        header_frame.pack(fill="x", pady=(0, 1))

        for header, width in headers:
            label = ctk.CTkLabel(
                header_frame,
                text=header,
                font=(self.theme.FONT_FAMILY, self.theme.FONT_SIZES["header"], "bold"),
                text_color=self.theme.TEXT_LIGHT,
                width=width,
                height=35
            )
            label.pack(side="left", padx=1)

        # Initialize table cells
        self.agent_cells = []
        for i in range(15):
            row_frame = ctk.CTkFrame(
                self.agents_scroll,
                fg_color="transparent"
            )
            row_frame.pack(fill="x", pady=1)
            
            row_cells = []
            for _, width in headers:
                cell = ctk.CTkLabel(
                    row_frame,
                    text="",
                    font=(self.theme.FONT_FAMILY, self.theme.FONT_SIZES["body"]),
                    fg_color=self.theme.BG_LIGHT,
                    width=width,
                    height=35,
                    anchor="center"  # Center align text
                )
                cell.pack(side="left", padx=1)
                row_cells.append(cell)
            self.agent_cells.append(row_cells)

        # Create metrics panel (right side, 30% width)
        self.metrics_panel = ctk.CTkFrame(
            self.agents_container,
            fg_color=self.theme.BG_GRAY,
            width=350  # Fixed width
        )
        self.metrics_panel.pack(side="right", fill="y", padx=(10, 0))
        self.metrics_panel.pack_propagate(False)

        self._setup_metrics_panel()

    def setup_stats_table(self):
        """Setup statistics table"""
        headers = ["Metric", "Value"]
        
        # Initialize with empty rows
        empty_data = [["" for _ in range(len(headers))] for _ in range(10)]
        initial_data = [headers] + empty_data
        
        self.stats_table = CTkTable(
            master=self.stats_tab,
            values=initial_data,
            colors=["#E3E3E3", "#EEEEEE"],
            header_color="#2196F3",
            hover_color="#BBE3FF",
            font=("Segoe UI", 12),
            text_color="black",  # Added text color
            width=200,  # Wider columns for stats
            height=30
        )
        self.stats_table.pack(fill="both", expand=True, padx=20, pady=20)

    def update_table(self, table, data):
        """Update table with new data"""
        try:
            headers = data.columns.tolist()
            rows = data.values.tolist()
            
            # Format numbers as integers
            formatted_rows = []
            for row in rows:
                formatted_row = []
                for cell in row:
                    if isinstance(cell, (int, float)) and cell != int(cell):
                        formatted_row.append(str(int(cell)))
                    else:
                        formatted_row.append(str(cell))
                formatted_rows.append(formatted_row)
            
            values = [headers] + formatted_rows
            
            # Add empty rows if needed
            while len(values) < 10:
                values.append(["" for _ in range(len(headers))])
            
            # Color coding for performance
            if 'Total' in headers:
                total_col = headers.index('Total')
                max_total = max(float(row[total_col]) for row in formatted_rows[:-1] if row[total_col])
                
                for i, row in enumerate(values[1:], 1):
                    if row[total_col]:
                        total = float(row[total_col])
                        if total >= max_total * 0.8:  # Top performers (>= 80% of max)
                            row.append("#4CAF50")  # Green
                        elif total >= max_total * 0.5:  # Mid performers (>= 50% of max)
                            row.append("#FFC107")  # Yellow
                        else:  # Low performers
                            row.append("#F44336")  # Red
                    else:
                        row.append("")
            
            table.update_values(values)
            
        except Exception as e:
            print(f"Error updating table: {str(e)}")
            import traceback
            traceback.print_exc()
            self.update_status(f"Error updating table: {str(e)}")

    def update_tables(self, hourly_data, agent_data, stats_data):
        """Update all tables with new data"""
        try:
            print("Updating hourly table...")
            print(f"Hourly data columns: {hourly_data.columns}")
            self.update_hourly_table(hourly_data)
            
            print("Updating agent table...")
            print(f"Agent data columns: {agent_data.columns}")
            self.update_agent_table(agent_data, self.crm_df)
            
            print("Updating stats table...")
            print(f"Stats data columns: {stats_data.columns}")
            self.update_table(self.stats_table, stats_data)
            
        except Exception as e:
            print(f"Error updating tables: {str(e)}")
            import traceback
            traceback.print_exc()
            self.update_status(f"Error updating tables: {str(e)}")

    def refresh_data(self):
        """Refresh all data and update tables"""
        self.loading.show("Refreshing data...")
        self.refresh_btn.configure(state="disabled")
        
        # Start refresh in separate thread
        thread = threading.Thread(target=self._refresh_data_thread)
        thread.daemon = True  # Make thread daemon so it closes with main window
        thread.start()

    def _refresh_data_thread(self):
        """Handle data refresh in separate thread"""
        try:
            # Initialize clients
            self.root.after(0, lambda: self.loading.show("Connecting to CRM..."))
            crm = CRMClient()
            erp = ERPClient()
            
            # Login
            crm_login = crm.login("root", "p@SSw0RD@2025")
            erp_login = erp.login("chouaib@xpercia.fr", "Xpercia@24")
            
            if not (crm_login and erp_login):
                self.root.after(0, lambda: self.update_status("Failed to login"))
                return
            
            # Export data
            self.root.after(0, lambda: self.loading.show("Exporting data..."))
            erp_file = erp.export_contracts()
            
            # Get Prevoyance campaign data
            campaigns = crm.get_campaigns()
            if not campaigns or 'Prevoyance' not in campaigns:
                self.root.after(0, lambda: self.update_status("Failed to get campaigns"))
                return
            
            campaign_ids = [c['value'] for c in campaigns['Prevoyance']]
            crm_file = crm.export_campaign_data(campaign_ids)
            
            if not (erp_file and crm_file):
                self.root.after(0, lambda: self.update_status("Failed to export data"))
                return
            
            # Process data
            self.root.after(0, lambda: self.loading.show("Processing data..."))
            self.process_data(crm_file, erp_file)
            
            self.root.after(0, lambda: self.update_status("Data refreshed successfully"))
            
        except Exception as e:
            print(f"Error in refresh thread: {str(e)}")
            traceback.print_exc()
            self.root.after(0, lambda: self.update_status(f"Error: {str(e)}"))
        
        finally:
            self.root.after(0, lambda: self.loading.hide())
            self.root.after(0, lambda: self.refresh_btn.configure(state="normal"))

    def update_status(self, message):
        """Update status label from any thread"""
        self.root.after(0, lambda: self.status.configure(text=message))

    def process_data(self, crm_file, erp_file):
        """Process data and update all tables"""
        try:
            print(f"Reading files: CRM={crm_file}, ERP={erp_file}")
            
            # Read data
            self.crm_df = pd.read_csv(crm_file, sep=';', encoding='utf-8')
            erp_df = pd.read_excel(erp_file)
            
            # Debug print data info
            print("\nCRM Data Sample:")
            print(self.crm_df[['Agent', 'Date Heure']].head())
            print(f"\nDate Range:")
            print(f"Min date: {self.crm_df['Date Heure'].min()}")
            print(f"Max date: {self.crm_df['Date Heure'].max()}")
            
            # Convert timestamps
            self.crm_df['Date Heure'] = pd.to_datetime(self.crm_df['Date Heure'])
            erp_df['Créer le'] = pd.to_datetime(erp_df['Créer le'])
            
            # Filter ERP data for the same date
            erp_date = pd.Timestamp('2024-12-20').date()
            erp_df = erp_df[erp_df['Créer le'].dt.date == erp_date]
            
            print("\nCreating reports...")
            # Create reports
            hourly_data = self.create_hourly_report(self.crm_df)
            print("Hourly report created")
            
            agent_data = self.create_agent_report(self.crm_df, erp_df)
            print("Agent report created")
            
            stats_data = self.create_stats(self.crm_df, erp_df)
            print("Stats created")
            
            print("\nUpdating tables...")
            # Update tables
            self.root.after(0, lambda: self.update_tables(hourly_data, agent_data, stats_data))
            
        except Exception as e:
            print(f"Error processing data: {str(e)}")
            import traceback
            traceback.print_exc()
            self.update_status(f"Error processing data: {str(e)}")

    def create_hourly_report(self, crm_df):
        """Create detailed hourly report dataframe with transfers per agent"""
        time_ranges = [
            ('10:00', '11:00'), ('11:00', '12:00'), ('12:00', '13:00'),
            ('13:00', '15:00'), ('15:00', '16:00'), ('16:00', '17:00'),
            ('17:00', '18:00'), ('18:00', '19:30')
        ]
        
        agent_data = {}
        
        for agent in crm_df['Agent'].unique():
            agent_data[agent] = {'Agent': agent}
            total_transfers = 0
            
            for start, end in time_ranges:
                mask = (
                    (crm_df['Agent'] == agent) &
                    (crm_df['Date Heure'].dt.strftime('%H:%M') >= start) &
                    (crm_df['Date Heure'].dt.strftime('%H:%M') < end)
                )
                count = len(crm_df[mask])
                agent_data[agent][f'{start}-{end}'] = int(count)  # Convert to integer
                total_transfers += count
            
            agent_data[agent]['Total'] = int(total_transfers)  # Convert to integer
        
        df = pd.DataFrame(list(agent_data.values()))
        
        # Sort by total transfers
        df = df.sort_values('Total', ascending=False)
        
        # Add totals row
        totals = {'Agent': 'TOTAL'}
        for col in df.columns:
            if col != 'Agent':
                totals[col] = int(df[col].sum())  # Convert sums to integers
        df = pd.concat([df, pd.DataFrame([totals])], ignore_index=True)
        
        return df

    def calculate_working_hours(self, crm_df):
        """Calculate number of working hours until current time"""
        if len(crm_df) == 0:
            return 0
        
        # Use 19:30 as end time for test date
        test_end_time = pd.Timestamp('2024-12-20 19:30:00').time()
        first_transfer_time = crm_df['Date Heure'].min().time()
        
        # Convert times to hours (float)
        end_hours = test_end_time.hour + test_end_time.minute/60
        start_hours = first_transfer_time.hour + first_transfer_time.minute/60
        
        # Calculate working hours
        working_hours = max(0, end_hours - start_hours)
        return working_hours

    def create_agent_report(self, crm_df, erp_df):
        """Create agent performance report dataframe"""
        agent_stats = []
        
        for agent in crm_df['Agent'].unique():
            # Count transfers
            transfers = len(crm_df[crm_df['Agent'] == agent])
            
            # Count valid contracts
            contracts = len(erp_df[
                (erp_df['Transféreur'] == agent) & 
                (erp_df['Statut'].isin(['Valider', 'Nouveau Contrat']))
            ])
            
            # Calculate ratio
            ratio = (contracts/transfers * 100) if transfers > 0 else 0
            
            # Add to stats list
            agent_stats.append({
                'Agent': agent,
                'Total Transfers': transfers,  # Store as integer
                'Contracts': contracts,        # Store as integer
                'Ratio': f"{round(ratio)}%"    # Format as string with %
            })
        
        # Create DataFrame and sort
        df = pd.DataFrame(agent_stats)
        df = df.sort_values('Total Transfers', ascending=False)
        
        return df

    def create_stats(self, crm_df, erp_df):
        """Create general statistics dataframe"""
        try:
            # Count only valid contracts
            valid_contracts = len(erp_df[erp_df['Statut'].isin(['Valider', 'Nouveau Contrat'])])
            total_transfers = len(crm_df)
            
            # Calculate working hours
            working_hours = self.calculate_working_hours(crm_df)
            
            # Calculate transfer quality per agent
            transfer_quality = {}
            for agent in crm_df['Agent'].unique():
                transfers = len(crm_df[crm_df['Agent'] == agent])
                positive_transfers = len(erp_df[
                    (erp_df['Transféreur'] == agent) & 
                    (erp_df['Statut'].isin(['Valider', 'Nouveau Contrat']))
                ])
                if transfers > 0:
                    quality = (positive_transfers / transfers) * 100
                    transfer_quality[agent] = quality
            
            # Find best quality transferrer
            best_quality_agent = max(transfer_quality.items(), key=lambda x: x[1]) if transfer_quality else ("N/A", 0)
            
            # Calculate average transfers per hour
            num_agents = len(crm_df['Agent'].unique())
            avg_transfers_per_hour = total_transfers / (num_agents * working_hours) if working_hours > 0 and num_agents > 0 else 0
            
            stats_data = [
                ['Total Transfers', str(total_transfers)],
                ['Valid Contracts', str(valid_contracts)],
                ['Overall Quality', f"{round((valid_contracts/total_transfers * 100) if total_transfers > 0 else 0)}%"],
                ['Transfers/Hour/Agent', f"{round(avg_transfers_per_hour, 2)}"],
                ['Working Hours', f"{round(working_hours, 2)}h"],
                ['Best Quality Agent', f"{best_quality_agent[0]} ({round(best_quality_agent[1])}%)"],
                ['Date', "2024-12-20"]
            ]
            
            # Create DataFrame with the stats data
            df = pd.DataFrame(stats_data, columns=['Metric', 'Value'])
            return df
            
        except Exception as e:
            print(f"Error creating stats: {str(e)}")
            import traceback
            traceback.print_exc()
            # Return empty DataFrame with correct columns if there's an error
            return pd.DataFrame({'Metric': [], 'Value': []})

    def update_hourly_table(self, data):
        """Update hourly table with performance coloring"""
        try:
            # Get max values for each time slot for color scaling
            time_cols = [col for col in data.columns if '-' in col]  # Time range columns
            max_vals = data[time_cols].max()
            
            # Update cells with data and colors
            for i, row in data.iterrows():
                if i >= len(self.hourly_cells):  # Skip if beyond table size
                    break
                
                row_cells = self.hourly_cells[i]
                
                # Agent name
                row_cells[0].configure(
                    text=row['Agent'],
                    fg_color="#FFFFFF",
                    text_color="#000000"
                )
                
                # Time slots
                for j, col in enumerate(time_cols, 1):
                    val = int(row[col])
                    max_val = max_vals[col]
                    
                    # Color scale based on performance
                    if max_val > 0:
                        ratio = val / max_val
                        if ratio >= 0.8:
                            bg_color = "#E8F5E9"  # Light green
                            text_color = "#2E7D32"  # Dark green
                        elif ratio >= 0.5:
                            bg_color = "#FFF3E0"  # Light orange
                            text_color = "#E65100"  # Dark orange
                        elif ratio > 0:
                            bg_color = "#FFEBEE"  # Light red
                            text_color = "#C62828"  # Dark red
                        else:
                            bg_color = "#FFFFFF"  # White
                            text_color = "#9E9E9E"  # Gray
                    else:
                        bg_color = "#FFFFFF"
                        text_color = "#9E9E9E"
                    
                    row_cells[j].configure(
                        text=str(val),
                        fg_color=bg_color,
                        text_color=text_color
                    )
                
                # Total column
                total = int(row['Total'])
                row_cells[-1].configure(
                    text=str(total),
                    fg_color="#E3F2FD",  # Light blue
                    text_color="#1565C0"  # Dark blue
                )
            
            # Update totals row
            totals = data.iloc[-1]
            for j, col in enumerate(data.columns):
                val = totals[col]
                self.totals_cells[j].configure(
                    text=str(val) if isinstance(val, (int, float)) else val,
                    fg_color="#1E88E5",
                    text_color="white"
                )
            
            # Add insights panel
            self.add_hourly_insights(data)
            
        except Exception as e:
            print(f"Error updating hourly table: {str(e)}")
            traceback.print_exc()

    def add_hourly_insights(self, data):
        """Add insights panel for hourly report"""
        # Clear previous insights if any
        for widget in self.hourly_tab.winfo_children():
            if isinstance(widget, ctk.CTkFrame) and widget != self.hourly_frame:
                widget.destroy()
        
        insights_frame = ctk.CTkFrame(self.hourly_tab)
        insights_frame.pack(fill="x", padx=20, pady=(10, 20))
        
        # Calculate insights
        time_cols = [col for col in data.columns if '-' in col]
        total_transfers = int(data['Total'].iloc[:-1].sum())  # Exclude totals row
        peak_hour = max(time_cols, key=lambda col: data[col].iloc[:-1].sum())
        peak_hour_transfers = int(data[peak_hour].iloc[:-1].sum())
        
        # Find top performers
        top_agents = data.nlargest(3, 'Total')[['Agent', 'Total']].values.tolist()
        
        # Calculate hourly trends
        hourly_totals = [int(data[col].iloc[:-1].sum()) for col in time_cols]
        trend = "↑" if hourly_totals[-1] > hourly_totals[0] else "↓"
        
        insights = [
            f"📊 Total Transfers Today: {total_transfers}",
            f"⏰ Peak Hour: {peak_hour} ({peak_hour_transfers} transfers)",
            f"🏆 Top Performers:",
            *[f"   {i+1}. {agent} ({transfers} transfers)" for i, (agent, transfers) in enumerate(top_agents)],
            f"📈 Trend: {trend} ({hourly_totals[-1] - hourly_totals[0]:+d} change)",
            f"⚡ Highest Single Hour: {max(hourly_totals)} transfers"
        ]
        
        for insight in insights:
            label = ctk.CTkLabel(
                insights_frame,
                text=insight,
                font=("Segoe UI", 12),
                anchor="w",
                padx=10
            )
            label.pack(fill="x", pady=2)

    def _get_status_color(self, ratio, transfers):
        """Get status and colors based on performance"""
        if ratio >= 10 and transfers >= 50:
            return "⭐ Excellent", "#E8F5E9", "#2E7D32"
        elif ratio >= 8 or transfers >= 40:
            return "✅ Good", "#FFF3E0", "#E65100"
        elif ratio >= 5 or transfers >= 30:
            return "⚠️ Average", "#FFF9C4", "#F57F17"
        else:
            return "❗ Needs Improvement", "#FFEBEE", "#C62828"

    def update_agent_table(self, data, crm_df):
        """Update agent performance table with enhanced metrics"""
        try:
            max_transfers = data['Total Transfers'].astype(int).max()
            
            for i, row in data.iterrows():
                if i >= len(self.agent_cells):
                    break
                
                cells = self.agent_cells[i]
                
                # Get values
                agent = row['Agent']
                transfers = int(row['Total Transfers'])
                contracts = int(row['Contracts'])
                ratio = float(row['Ratio'].strip('%'))
                
                # Calculate metrics
                working_hours = self.calculate_working_hours(crm_df)
                avg_per_hour = transfers / working_hours if working_hours > 0 else 0
                quality = (ratio * 0.4 + (transfers/max_transfers * 100) * 0.6)
                
                # Get colors for each cell
                transfer_colors = self._get_color_scale(transfers/max_transfers)
                ratio_colors = self._get_color_scale(ratio/100)
                quality_colors = self._get_color_scale(quality/100)
                
                # Update cells with colors
                cells[0].configure(text=agent, fg_color="#FFFFFF", text_color="#000000")
                cells[1].configure(text=str(transfers), fg_color=transfer_colors[0], text_color=transfer_colors[1])
                cells[2].configure(text=str(contracts), fg_color="#E3F2FD", text_color="#1565C0")
                cells[3].configure(text=f"{ratio:.1f}%", fg_color=ratio_colors[0], text_color=ratio_colors[1])
                cells[4].configure(text=f"{avg_per_hour:.1f}", fg_color="#F3E5F5", text_color="#4A148C")
                cells[5].configure(text=f"{quality:.0f}", fg_color=quality_colors[0], text_color=quality_colors[1])
                
                # Add status indicator
                status = "⭐ Excellent" if quality >= 80 else "✅ Good" if quality >= 60 else "⚠️ Average" if quality >= 40 else "❗ Needs Improvement"
                cells[6].configure(text=status, fg_color=quality_colors[0], text_color=quality_colors[1])
                
            # Update metrics panel
            self._update_metrics_panel(data)
            
        except Exception as e:
            print(f"Error updating agent table: {str(e)}")
            traceback.print_exc()

    def _get_color_scale(self, ratio):
        """Get background and text colors based on ratio"""
        if ratio >= 0.8:
            return "#E8F5E9", "#2E7D32"  # Green
        elif ratio >= 0.6:
            return "#FFF3E0", "#E65100"  # Orange
        elif ratio >= 0.4:
            return "#FFF9C4", "#F57F17"  # Yellow
        else:
            return "#FFEBEE", "#C62828"  # Red

    def _setup_metrics_panel(self):
        """Enhanced metrics panel setup"""
        # Create main metrics container
        metrics_container = ctk.CTkFrame(
            self.metrics_panel,
            fg_color="transparent"
        )
        metrics_container.pack(fill="both", expand=True, padx=10, pady=10)
        
        # Top performers section with gradient background
        top_frame = ctk.CTkFrame(
            metrics_container,
            fg_color=self.theme.PRIMARY_LIGHT,
            corner_radius=self.theme.BORDER_RADIUS
        )
        top_frame.pack(fill="x", pady=(0, 10))
        
        # Gradient header
        header = ctk.CTkLabel(
            top_frame,
            text="🏆 Top Performers",
            font=(self.theme.FONT_FAMILY, self.theme.FONT_SIZES["subtitle"], "bold"),
            fg_color=self.theme.PRIMARY,
            corner_radius=self.theme.BORDER_RADIUS
        )
        header.pack(fill="x", pady=(0, 5))
        
        # Top performers list with badges
        self.top_performers = []
        for i in range(3):
            performer_frame = ctk.CTkFrame(
                top_frame,
                fg_color="transparent"
            )
            performer_frame.pack(fill="x", padx=10, pady=2)
            
            # Medal emoji based on position
            medal = ["🥇", "🥈", "🥉"][i]
            
            position = ctk.CTkLabel(
                performer_frame,
                text=medal,
                font=(self.theme.FONT_FAMILY, self.theme.FONT_SIZES["body"])
            )
            position.pack(side="left", padx=5)
            
            label = ctk.CTkLabel(
                performer_frame,
                text="",
                font=(self.theme.FONT_FAMILY, self.theme.FONT_SIZES["body"]),
                anchor="w"
            )
            label.pack(side="left", fill="x", expand=True)
            self.top_performers.append(label)

        # Team metrics section with cards
        metrics_frame = ctk.CTkFrame(
            metrics_container,
            fg_color=self.theme.BG_LIGHT,
            corner_radius=self.theme.BORDER_RADIUS
        )
        metrics_frame.pack(fill="both", expand=True)
        
        header = ctk.CTkLabel(
            metrics_frame,
            text="📊 Team Performance",
            font=(self.theme.FONT_FAMILY, self.theme.FONT_SIZES["subtitle"], "bold"),
            fg_color=self.theme.PRIMARY
        )
        header.pack(fill="x", pady=(0, 10))
        
        # Metric cards grid
        self.team_metrics = {}
        metrics = [
            ("Avg Transfers/Hour", "⏱️", self.theme.PRIMARY_LIGHT),
            ("Success Rate", "✅", self.theme.SECONDARY),
            ("Quality Score", "⭐", self.theme.WARNING),
            ("Active Agents", "👥", self.theme.PRIMARY)
        ]
        
        for i, (metric, icon, color) in enumerate(metrics):
            card = ctk.CTkFrame(
                metrics_frame,
                fg_color=color,
                corner_radius=self.theme.BORDER_RADIUS
            )
            card.pack(fill="x", pady=5, padx=10)
            
            # Metric label
            ctk.CTkLabel(
                card,
                text=f"{icon} {metric}",
                font=(self.theme.FONT_FAMILY, self.theme.FONT_SIZES["body"]),
                anchor="w"
            ).pack(side="left", padx=10, pady=5)
            
            # Value label with background
            value = ctk.CTkLabel(
                card,
                text="",
                font=(self.theme.FONT_FAMILY, self.theme.FONT_SIZES["body"], "bold"),
                fg_color=self.theme.BG_LIGHT,
                corner_radius=5
            )
            value.pack(side="right", padx=10, pady=5)
            self.team_metrics[metric] = value

    def _setup_tables(self):
        """Initialize all tables"""
        # Setup tables
        self.setup_hourly_table()
        self.setup_agents_table()
        self.setup_stats_table()

    def _update_metrics_panel(self, data):
        """Update metrics panel with latest stats"""
        try:
            # Convert Total Transfers to numeric for calculations
            data = data.copy()
            data['Total Transfers'] = pd.to_numeric(data['Total Transfers'])
            data['Contracts'] = pd.to_numeric(data['Contracts'])
            data['Ratio'] = data['Ratio'].str.rstrip('%').astype(float)
            
            # Update top performers
            top_agents = data.nlargest(3, 'Total Transfers')
            for i, (_, row) in enumerate(top_agents.iterrows()):
                text = f"{i+1}. {row['Agent']} ({int(row['Total Transfers'])} transfers)"
                self.top_performers[i].configure(
                    text=text,
                    fg_color=self._get_color_scale(row['Total Transfers']/data['Total Transfers'].max())[0]
                )
            
            # Calculate team metrics
            avg_transfers = data['Total Transfers'].mean()
            success_rate = (data['Contracts'].sum() / data['Total Transfers'].sum() * 100)
            
            # Calculate quality scores
            quality_scores = []
            for _, row in data.iterrows():
                transfer_score = min(100, (row['Total Transfers'] / data['Total Transfers'].max() * 100))
                ratio_score = row['Ratio']
                quality = (ratio_score * 0.4 + transfer_score * 0.6)
                quality_scores.append(quality)
            
            avg_quality = sum(quality_scores) / len(quality_scores)
            active_agents = len(data)
            
            # Update metrics with colors and formatting
            metrics = {
                "Avg Transfers/Hour": (
                    f"{avg_transfers/9.5:.1f}",  # 9.5 hours workday
                    self._get_color_scale(avg_transfers/(data['Total Transfers'].max()/9.5))
                ),
                "Success Rate": (
                    f"{success_rate:.1f}%",
                    self._get_color_scale(success_rate/100)
                ),
                "Quality Score": (
                    f"{avg_quality:.0f}",
                    self._get_color_scale(avg_quality/100)
                ),
                "Active Agents": (
                    f"{active_agents}",
                    ("#FFFFFF", "#000000")
                )
            }
            
            # Update metric labels with colors
            for metric, (value, colors) in metrics.items():
                self.team_metrics[metric].configure(
                    text=value,
                    fg_color=colors[0],
                    text_color=colors[1]
                )
            
        except Exception as e:
            print(f"Error updating metrics panel: {str(e)}")
            traceback.print_exc()

def main():
    app = ReportGUI()
    app.root.mainloop()

if __name__ == "__main__":
    main()
