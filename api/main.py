from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from datetime import datetime
import pandas as pd
import requests
from urllib3.exceptions import InsecureRequestWarning
import re
from typing import Dict, List, Optional
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Import the client classes
from controllers import CRMClient, ERPClient

# Disable SSL warning
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

app = FastAPI(title="Flash Prod API")

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global client instances
crm_client = CRMClient()
erp_client = ERPClient()

# Initialize clients on startup
@app.on_event("startup")
async def startup_event():
    try:
        # Get credentials from environment variables
        crm_username = os.getenv("CRM_USERNAME")
        crm_password = os.getenv("CRM_PASSWORD")
        erp_email = os.getenv("ERP_EMAIL")
        erp_password = os.getenv("ERP_PASSWORD")
        
        if not all([crm_username, crm_password, erp_email, erp_password]):
            print("Error: Missing environment variables. Please check your .env file.")
            return
        
        # Login to CRM
        print("Logging into CRM...")
        if not crm_client.login(crm_username, crm_password):
            print("Error: Failed to login to CRM")
            return
        print("Successfully logged into CRM")
        
        # Login to ERP
        print("Logging into ERP...")
        if not erp_client.login(erp_email, erp_password):
            print("Error: Failed to login to ERP")
            return
        print("Successfully logged into ERP")
        
    except Exception as e:
        print(f"Error during startup: {str(e)}")

class TimeRange(BaseModel):
    start_date: Optional[str] = None
    end_date: Optional[str] = None

    class Config:
        json_schema_extra = {
            "example": {
                "start_date": datetime.now().strftime("%Y-%m-%d 00:00:00"),
                "end_date": datetime.now().strftime("%Y-%m-%d 23:59:59")
            }
        }

@app.post("/api/crm/data")
async def get_crm_data(time_range: TimeRange = TimeRange()):
    try:
        # Get the data using the global client instance
        result = crm_client.get_data_as_json(time_range.start_date, time_range.end_date)
        
        if "error" in result:
            raise HTTPException(status_code=500, detail=result["error"])
            
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/erp/data")
async def get_erp_data(force_refresh: bool = False):
    try:
        # Check if we need to re-authenticate
        dashboard_response = erp_client.session.get(f"{erp_client.base_url}/dashboard", verify=False)
        if dashboard_response.status_code != 200 or 'login' in dashboard_response.url:
            # Re-authenticate
            erp_email = os.getenv("ERP_EMAIL")
            erp_password = os.getenv("ERP_PASSWORD")
            if not erp_client.login(erp_email, erp_password):
                print("Failed to authenticate with ERP")
                print(f"Using email: {erp_email}")
                print("Cookies:", erp_client.session.cookies.get_dict())
                raise HTTPException(status_code=401, detail="ERP authentication failed")

        # Get the data with optional force refresh
        result = erp_client.get_contracts_as_json(force_full_refresh=force_refresh)
        
        if "error" in result:
            raise HTTPException(status_code=500, detail=result["error"])
            
        return result
        
    except Exception as e:
        print(f"Error in get_erp_data: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 