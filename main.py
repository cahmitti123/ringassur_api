from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from datetime import datetime, timedelta
import requests
from urllib3.exceptions import InsecureRequestWarning
from typing import Optional, Union, List
import os
from dotenv import load_dotenv
from contextlib import asynccontextmanager
from asyncio import Queue
import asyncio


# Load environment variables
load_dotenv()

# Import the client classes
from controllers import CRMClient, ERPClient, CRMClientFormaExpert, JobsClient, CRMIncrementalClient, NeoClient

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
NEO_LOGIN = os.getenv('NEO_LOGIN')
NEO_PASSWORD = os.getenv('NEO_PASSWORD')

# Create separate client instances for each company
xpercia_client = JobsClient()
perextel_client = JobsClient()

# Add the new client instance
crm_incremental_client = CRMIncrementalClient()

# Initialize neo_client as None
neo_client = None

# Add these new imports and globals
mfa_queue = Queue()
mfa_response_queue = Queue()

async def wait_for_mfa_code():
    """Wait for MFA code to be provided via API endpoint"""
    try:
        # Wait for MFA code with timeout
        code = await asyncio.wait_for(mfa_queue.get(), timeout=300)  # 5 minutes timeout
        return code
    except asyncio.TimeoutError:
        raise HTTPException(status_code=408, detail="MFA code input timeout")

async def initialize_neo_client():
    """Initialize the Neo client with MFA handling"""
    global neo_client
    if neo_client is None:
        neo_client = NeoClient()
        
        # Set up MFA callback
        async def mfa_callback():
            return await wait_for_mfa_code()
            
        neo_client.set_mfa_callback(mfa_callback)
    return neo_client

async def get_neo_client():
    """Get Neo client with MFA handling"""
    global neo_client
    if neo_client is None:
        neo_client = await initialize_neo_client()
    return neo_client

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
            "PEREXTEL_PASSWORD": PEREXTEL_PASSWORD,
            "NEO_LOGIN": NEO_LOGIN,
            "NEO_PASSWORD": NEO_PASSWORD
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
        
        # # Initialize Neo client
        # print("Logging into Neoliane extranet...")
        # neo_instance = await initialize_neo_client()
        # if not await neo_instance.login(NEO_LOGIN, NEO_PASSWORD):
        #     print("Error: Failed to login to Neoliane")
        # else:
        #     print("Successfully logged into Neoliane")
        
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
                client.close()
            except:
                pass
        
        # Close Neo client separately since it might be None
        if neo_client is not None:
            try:
                neo_client.close()
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
    page: Optional[int] = 1
    page_size: Optional[int] = 1000

    class Config:
        json_schema_extra = {
            "example": {
                "start_date": datetime.now().strftime("%Y-%m-%d 00:00:00"),
                "end_date": datetime.now().strftime("%Y-%m-%d 23:59:59"),
                "page": 1,
                "page_size": 1000
            }
        }

class CampaignRequest(BaseModel):
    campaign_ids: Union[List[str], str]

    class Config:
        json_schema_extra = {
            "example": {
                "campaign_ids": ["7"]  # Can be a single ID or list of IDs
            }
        }

class SearchRequest(BaseModel):
    campaign_ids: Optional[List[str]] = None
    qualif_types: Optional[List[str]] = None  # ['sales_qualifs', 'callback_qualifs', 'rejection_qualifs', 'other_qualifs']
    qualif_ids: Optional[List[str]] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None

    class Config:
        json_schema_extra = {
            "example": {
                "campaign_ids": ["7"],
                "qualif_types": ["sales_qualifs", "callback_qualifs"],
                "qualif_ids": None,
                "start_date": datetime.now().strftime("%Y-%m-%d 00:00:00"),
                "end_date": datetime.now().strftime("%Y-%m-%d 23:59:59")
            }
        }

# Add a new endpoint to receive MFA code
@app.post("/api/neo/mfa-code")
async def submit_mfa_code(code: str):
    """Submit MFA code for pending login"""
    await mfa_queue.put(code)
    try:
        # Wait for login result
        result = await asyncio.wait_for(mfa_response_queue.get(), timeout=30)
        if result.get("success"):
            return {"message": "Login successful"}
        else:
            raise HTTPException(status_code=401, detail=result.get("error", "MFA verification failed"))
    except asyncio.TimeoutError:
        raise HTTPException(status_code=408, detail="MFA verification timeout")


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
    
@app.post("/api/crm/data/full")
async def get_crm_data(time_range: TimeRange = TimeRange()):
    try:
        # Get the data using the global client instance
        result = crm_client.get_data_as_json_full(time_range.start_date, time_range.end_date)
        
        if "error" in result:
            raise HTTPException(status_code=500, detail=result["error"])
            
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/crm/data/assurance")
async def get_sales_data(time_range: TimeRange = TimeRange()):
    try:
        # Get the data using the global client instance with pagination
        result = crm_client.get_data_as_json_full(
            time_range.start_date, 
            time_range.end_date,
            time_range.page,
            time_range.page_size
        )
        
        if "error" in result:
            raise HTTPException(status_code=500, detail=result["error"])
            
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/crm/data/filter_groups")
async def get_crm_data(time_range: TimeRange = TimeRange()):
    try:
        print("\nProcessing /api/crm/data/filter_groups request...")
        print(f"Time range: {time_range}")
        
        # Check if we're logged in
        if not crm_client.session.cookies:
            print("No session cookies found - attempting to re-login")
            crm_username = os.getenv("CRM_USERNAME")
            crm_password = os.getenv("CRM_PASSWORD")
            if not crm_client.login(crm_username, crm_password):
                raise HTTPException(status_code=401, detail="Failed to authenticate with CRM")
        
        # Get the campaigns using the client instance
        campaigns = crm_client.get_campaigns(time_range.start_date, time_range.end_date)
        
        if campaigns is None:
            raise HTTPException(status_code=500, detail="Failed to fetch campaign groups")
            
        return {
            "success": True,
            "campaigns": campaigns
        }
        
    except Exception as e:
        print(f"Error in get_crm_data/filter_groups: {str(e)}")
        import traceback
        traceback.print_exc()
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
        
        # Ensure that both daily and weekly stats are included in the result
        daily_stats = result.get("daily_stats", [])
        weekly_stats = result.get("weekly_stats", [])
        
        # Prepare the final response
        return {
            "success": result.get("success", False),
            "data": result.get("data", []),
            "type": result.get("type", "incremental"),
            "new_records": result.get("new_records", 0),
            "daily_stats": daily_stats,
            "weekly_stats": weekly_stats
        }
        
    except Exception as e:
        print(f"Error in get_erp_data: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/jobs")
async def get_jobs(company: Optional[str] = None):
    """Get job listings from moncallcenter.ma"""
    try:
        # Determine login credentials dynamically
        if company:
            login_var = f"{company.upper()}_LOGIN"
            password_var = f"{company.upper()}_PASSWORD"
        else:
            # Default to MONCALLCENTER if no company is provided
            login_var = "MONCALLCENTER_LOGIN"
            password_var = "MONCALLCENTER_PASSWORD"
        
        username = os.getenv(login_var)
        password = os.getenv(password_var)

        if not all([username, password]):
            raise HTTPException(
                status_code=500,
                detail=f"Missing credentials for {company.upper() if company else 'MONCALLCENTER'}"
            )
        
        # Login if needed
        if not jobs_client.session.cookies:
            if not jobs_client.login(username, password):
                raise HTTPException(
                    status_code=401,
                    detail=f"Failed to authenticate with {company.upper() if company else 'MONCALLCENTER'}"
                )
        
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

@app.get("/api/perextel/cands")
async def get_cands(company: Optional[str] = None):
    """Get candidate listings from moncallcenter.ma"""
    try:
        if not perextel_client.check_login():
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
        
        result = perextel_client.get_candidatures()
        
        
        return result
        
    except Exception as e:
        print(f"Error in get_cands: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/xpercia/cands")
async def get_cands(company: Optional[str] = None):
    """Get candidate listings from moncallcenter.ma"""
    try:
        if not xpercia_client.check_login():
            if not all([XPERCIA_LOGIN, XPERCIA_PASSWORD]):
                raise HTTPException(
                    status_code=500, 
                    detail="Missing xpercia credentials"
                )
            if not xpercia_client.login(XPERCIA_LOGIN, XPERCIA_PASSWORD):
                raise HTTPException(
                    status_code=401, 
                    detail="Failed to authenticate xpercia account"
                )
        
        result = xpercia_client.get_candidatures()
        
        
        return result
        
    except Exception as e:
        print(f"Error in get_cands: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


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

@app.post("/api/crm/campaigns/qualifications")
async def get_campaign_qualifications(request: CampaignRequest):
    """Get qualifications for specified campaigns"""
    try:
        # Check if we're logged in
        if not crm_client.session.cookies:
            print("No session cookies found - attempting to re-login")
            crm_username = os.getenv("CRM_USERNAME")
            crm_password = os.getenv("CRM_PASSWORD")
            if not crm_client.login(crm_username, crm_password):
                raise HTTPException(status_code=401, detail="Failed to authenticate with CRM")
        
        # Get qualifications using the client instance
        result = crm_client.get_campaign_qualifs(request.campaign_ids)
        
        if "error" in result:
            raise HTTPException(status_code=500, detail=result["error"])
            
        return result
        
    except Exception as e:
        print(f"Error in get_campaign_qualifications: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/crm/search")
async def search_crm_data(request: SearchRequest):
    """Dynamic search endpoint for CRM data"""
    try:
        # Check if we're logged in
        if not crm_client.session.cookies:
            print("No session cookies found - attempting to re-login")
            crm_username = os.getenv("CRM_USERNAME")
            crm_password = os.getenv("CRM_PASSWORD")
            if not crm_client.login(crm_username, crm_password):
                raise HTTPException(status_code=401, detail="Failed to authenticate with CRM")

        # Get the data using the client instance
        result = crm_client.search_data(
            campaign_ids=request.campaign_ids,
            qualif_types=request.qualif_types,
            qualif_ids=request.qualif_ids,
            start_date=request.start_date,
            end_date=request.end_date
        )
        
        if "error" in result:
            raise HTTPException(status_code=500, detail=result["error"])
            
        return result
        
    except Exception as e:
        print(f"Error in search_crm_data: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/crm/data/incremental")
async def get_incremental_data():
    """
    Endpoint for getting incremental CRM data in 15-minute chunks.
    Designed to be called by a cron job every 15 minutes.
    """
    try:
        # Get current time
        current_time = datetime.now()
        
        # Get incremental data
        result = crm_incremental_client.get_incremental_data(current_time)
        
        if "error" in result:
            raise HTTPException(status_code=500, detail=result["error"])
            
        return result
        
    except Exception as e:
        print(f"Error in incremental endpoint: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/neo/contracts")
async def get_neo_contracts(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    page: int = 1,
    limit: int = 20,
    neo_client: NeoClient = Depends(get_neo_client)
):
    """Get contracts data from Neoliane extranet"""
    try:
        # Get the contracts data
        result = await neo_client.get_contracts(
            start_date=start_date,
            end_date=end_date,
            page=page,
            limit=limit
        )
        
        if not result.get("success"):
            raise HTTPException(
                status_code=500,
                detail=result.get("error", "Failed to fetch contracts from Neoliane")
            )
            
        return result
        
    except Exception as e:
        print(f"Error in get_neo_contracts: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import sys
    import os
    # Ajouter le répertoire parent au PYTHONPATH
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)