from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Optional
import logging
from datetime import datetime
import os
from RedditToBigQueryPipeline import RedditToBigQueryPipeline

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Reddit Ads to BigQuery API",
    description="API for syncing Reddit Ads data to BigQuery",
    version="1.0.0"
)

class SyncRequest(BaseModel):
    days_back: Optional[int] = 7
    skip_reference: Optional[bool] = True
    dataset_id: Optional[str] = "raw_ads"
    project_id: Optional[str] = "ivc-media-ads-warehouse"

class SyncResponse(BaseModel):
    status: str
    message: str
    records_synced: Optional[int] = None
    execution_time_seconds: Optional[float] = None
    timestamp: str

@app.get("/")
async def root():
    """Health check endpoint"""
    return {"message": "Reddit Ads to BigQuery API is running"}

@app.get("/health")
async def health_check():
    """Health check endpoint for Cloud Run"""
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}

@app.post("/sync-ads-insights", response_model=SyncResponse)
async def sync_ads_insights(request: Optional[SyncRequest] = None, background_tasks: BackgroundTasks = None):
    """
    Sync Reddit Ads performance data to BigQuery
    
    Args:
        request: Optional SyncRequest with configuration parameters (uses defaults if not provided)
        
    Returns:
        SyncResponse with sync results
    """
    start_time = datetime.utcnow()
    
    # Use defaults if no request body provided
    if request is None:
        request = SyncRequest()
    
    try:
        logger.info(f"Starting Reddit Ads sync with {request.days_back} days back")
        
        # Initialize pipeline
        pipeline = RedditToBigQueryPipeline(
            dataset_id=request.dataset_id,
            project_id=request.project_id
        )
        
        # Sync ads reference if requested
        if not request.skip_reference:
            logger.info("Syncing ads reference data...")
            ref_count = pipeline.sync_ads_reference()
            logger.info(f"Synced {ref_count} ads to reference table")
        
        # Sync ads performance data
        logger.info(f"Syncing ads performance data for last {request.days_back} days...")
        count = pipeline.sync_ads_performance(days_back=request.days_back)
        logger.info(f"Synced {count} performance records to temp table")
        
        # Merge performance with reference data
        if count > 0:
            logger.info("Merging performance data with reference...")
            pipeline.merge_ads_performance()
            logger.info("Successfully merged performance data")
        else:
            logger.info("No performance data to merge")
        
        end_time = datetime.utcnow()
        execution_time = (end_time - start_time).total_seconds()
        
        return SyncResponse(
            status="success",
            message=f"Successfully synced {count} records",
            records_synced=count,
            execution_time_seconds=execution_time,
            timestamp=end_time.isoformat()
        )
        
    except Exception as e:
        error_msg = f"Error syncing Reddit Ads data: {str(e)}"
        logger.error(error_msg)
        
        end_time = datetime.utcnow()
        execution_time = (end_time - start_time).total_seconds()
        
        raise HTTPException(
            status_code=500,
            detail={
                "status": "error",
                "message": error_msg,
                "execution_time_seconds": execution_time,
                "timestamp": end_time.isoformat()
            }
        )