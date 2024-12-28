from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from datetime import datetime
import requests
from urllib3.exceptions import InsecureRequestWarning
from typing import Optional
import os
from dotenv import load_dotenv
from contextlib import asynccontextmanager


# Load environment variables
load_dotenv()

# Import the client classes
from controllers import CRMClient, ERPClient, CRMClientFormaExpert, JobsClient

# Disable SSL warning
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)



# Global client instances
crm_client = CRMClient()
crm_client_formaexpert = CRMClientFormaExpert()
erp_client = ERPClient()
jobs_client = JobsClient()

# Add these environment variables at the top with the others
XPERCIA_LOGIN = os.getenv("XPERCIA_LOGIN")
XPERCIA_PASSWORD = os.getenv("XPERCIA_PASSWORD")
PEREXTEL_LOGIN = os.getenv("PEREXTEL_LOGIN")
PEREXTEL_PASSWORD = os.getenv("PEREXTEL_PASSWORD")

# Create separate client instances for each company
xpercia_client = JobsClient()
perextel_client = JobsClient()

from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        # Get credentials from environment variables
        crm_username = os.getenv("CRM_USERNAME")
        crm_password = os.getenv("CRM_PASSWORD")
        erp_email = os.getenv("ERP_EMAIL")
        erp_password = os.getenv("ERP_PASSWORD")
        
        # Check all required credentials
        required_vars = {
            "CRM_USERNAME": crm_username,
            "CRM_PASSWORD": crm_password,
            "ERP_EMAIL": erp_email,
            "ERP_PASSWORD": erp_password,
            "XPERCIA_LOGIN": XPERCIA_LOGIN,
            "XPERCIA_PASSWORD": XPERCIA_PASSWORD,
            "PEREXTEL_LOGIN": PEREXTEL_LOGIN,
            "PEREXTEL_PASSWORD": PEREXTEL_PASSWORD
        }
        
        missing_vars = [k for k, v in required_vars.items() if not v]
        if missing_vars:
            print(f"Error: Missing environment variables: {', '.join(missing_vars)}")
            yield
            return

        # Initialize all clients
        print("\nInitializing clients...")
        
        # Login to CRM systems
        print("Logging into CRM...")
        if not crm_client.login(crm_username, crm_password):
            print("Error: Failed to login to CRM")
        else:
            print("Successfully logged into CRM")
            
        print("Logging into FormaExpert CRM...")
        if not crm_client_formaexpert.login():
            print("Error: Failed to login to FormaExpert CRM")
        else:
            print("Successfully logged into FormaExpert CRM")
            
        # Login to ERP
        print("Logging into ERP...")
        if not erp_client.login(erp_email, erp_password):
            print("Error: Failed to login to ERP")
        else:
            print("Successfully logged into ERP")
            
        # Login to job portals
        print("Logging into Xpercia job portal...")
        if not xpercia_client.login(XPERCIA_LOGIN, XPERCIA_PASSWORD):
            print("Error: Failed to login to Xpercia job portal")
        else:
            print("Successfully logged into Xpercia job portal")
            
        print("Logging into Perextel job portal...")
        if not perextel_client.login(PEREXTEL_LOGIN, PEREXTEL_PASSWORD):
            print("Error: Failed to login to Perextel job portal")
        else:
            print("Successfully logged into Perextel job portal")
        
        yield
    except Exception as e:
        print(f"Error during startup: {str(e)}")
        import traceback
        traceback.print_exc()
        yield
    finally:
        # Cleanup code - close sessions
        print("\nClosing client sessions...")
        for client in [crm_client, crm_client_formaexpert, erp_client, 
                      xpercia_client, perextel_client]:
            try:
                client.session.close()
            except:
                pass


app = FastAPI(title="Flash Prod API", lifespan=lifespan)


# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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
    
@app.post("/api/crm/data/temara")
async def get_crm_data(time_range: TimeRange = TimeRange()):
    try:
        print("\nProcessing /api/crm/data/temara request...")
        print(f"Time range: {time_range}")
        
        # Check if we're logged in
        if not crm_client_formaexpert.session.cookies:
            print("No session cookies found - attempting to re-login")
            if not crm_client_formaexpert.login():
                raise HTTPException(status_code=401, detail="Failed to authenticate with FormaExpert CRM")
        
        # Get the data using the global client instance
        result = crm_client_formaexpert.get_data_as_json(time_range.start_date, time_range.end_date)
        
        if "error" in result:
            print(f"Error in result: {result['error']}")
            raise HTTPException(status_code=500, detail=result["error"])
            
        return result
        
    except Exception as e:
        print(f"Error in get_crm_data/temara: {str(e)}")
        import traceback
        traceback.print_exc()
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

@app.get("/api/jobs")
async def get_jobs(company: Optional[str] = None):
    """Get job listings from moncallcenter.ma"""
    try:
        # Login if needed
        if not jobs_client.session.cookies:
            username = os.getenv("MONCALLCENTER_LOGIN")
            password = os.getenv("MONCALLCENTER_PASSWORD")
            if not all([username, password]):
                raise HTTPException(status_code=500, detail="Missing moncallcenter.ma credentials")
                
            if not jobs_client.login(username, password):
                raise HTTPException(status_code=401, detail="Failed to authenticate with moncallcenter.ma")
        
        # Get and return jobs
        return jobs_client.get_jobs(company)
        
    except Exception as e:
        print(f"Error in get_jobs: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/jobs/duplicate-random")
async def duplicate_random_job():
    """Duplicate a random job offer"""
    try:
        # Check if logged in
        if not jobs_client.check_login():
            # Try to login
            username = os.getenv("MONCALLCENTER_LOGIN")
            password = os.getenv("MONCALLCENTER_PASSWORD")
            
            if not all([username, password]):
                raise HTTPException(
                    status_code=500, 
                    detail="Missing moncallcenter.ma credentials"
                )
                
            if not jobs_client.login(username, password):
                raise HTTPException(
                    status_code=401, 
                    detail="Failed to authenticate with moncallcenter.ma"
                )

        # Try to duplicate a random job
        result = jobs_client.duplicate_random_job()
        
        if not result["success"]:
            raise HTTPException(
                status_code=500,
                detail=result.get("error", "Unknown error occurred")
            )
            
        return result

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error in duplicate_random_job endpoint: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )

@app.post("/api/jobs/xpercia/duplicate-random")
async def duplicate_random_xpercia_job():
    """Duplicate a random Xpercia job offer"""
    try:
        # Check if logged in
        if not xpercia_client.check_login():
            # Try to login
            if not all([XPERCIA_LOGIN, XPERCIA_PASSWORD]):
                raise HTTPException(
                    status_code=500, 
                    detail="Missing Xpercia credentials"
                )
                
            if not xpercia_client.login(XPERCIA_LOGIN, XPERCIA_PASSWORD):
                raise HTTPException(
                    status_code=401, 
                    detail="Failed to authenticate Xpercia account"
                )

        # Try to duplicate a random job
        result = xpercia_client.duplicate_random_job("xpercia")
        
        if not result["success"]:
            raise HTTPException(
                status_code=500,
                detail=result.get("error", "Unknown error occurred")
            )
            
        return result

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error in Xpercia duplicate_random_job endpoint: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )

@app.post("/api/jobs/perextel/duplicate-random")
async def duplicate_random_perextel_job():
    """Duplicate a random Perextel job offer"""
    try:
        # Check if logged in
        if not perextel_client.check_login():
            # Try to login
            if not all([PEREXTEL_LOGIN, PEREXTEL_PASSWORD]):
                raise HTTPException(
                    status_code=500, 
                    detail="Missing Perextel credentials"
                )
                
            if not perextel_client.login(PEREXTEL_LOGIN, PEREXTEL_PASSWORD):
                raise HTTPException(
                    status_code=401, 
                    detail="Failed to authenticate Perextel account"
                )

        # Try to duplicate a random job
        result = perextel_client.duplicate_random_job("perextel")
        
        if not result["success"]:
            raise HTTPException(
                status_code=500,
                detail=result.get("error", "Unknown error occurred")
            )
            
        return result

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error in Perextel duplicate_random_job endpoint: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )

if __name__ == "__main__":
    import sys
    import os
    # Ajouter le répertoire parent au PYTHONPATH
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)