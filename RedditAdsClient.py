"""
Reddit Ads Insights API Client
Pulls advertising insights and metrics from Reddit Ads API v3
"""

import requests
import json
import csv
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union
import time
import logging
import os
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class RedditAdsClient:
    """Client for interacting with Reddit Ads API v3"""
    
    def __init__(self, access_token: str, refresh_token: str, client_id: str, client_secret: str):
        """
        Initialize the Reddit Ads API client
        
        Args:
            access_token: OAuth access token
            refresh_token: OAuth refresh token for token renewal
            client_id: Reddit app client ID
            client_secret: Reddit app client secret
        """
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.client_id = client_id
        self.client_secret = client_secret
        self.base_url = "https://ads-api.reddit.com/api/v3"
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json',
            'User-Agent': os.getenv('REDDIT_APP_NAME', 'IVCAdsWarehouse/1.0')
        })
    
    def refresh_access_token(self) -> bool:
        """
        Refresh the access token using the refresh token
        
        Returns:
            bool: True if token refresh was successful
        """
        try:
            data = {
                'grant_type': 'refresh_token',
                'refresh_token': self.refresh_token
            }
            
            response = requests.post(
                'https://www.reddit.com/api/v1/access_token',
                data=data,
                auth=(self.client_id, self.client_secret),
                headers={'User-Agent': os.getenv('REDDIT_APP_NAME', 'IVCAdsWarehouse/1.0')}
            )
            
            if response.status_code == 200:
                token_data = response.json()
                self.access_token = token_data['access_token']
                self.session.headers['Authorization'] = f'Bearer {self.access_token}'
                logger.info("Access token refreshed successfully")
                return True
            else:
                logger.error(f"Token refresh failed: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Error refreshing token: {str(e)}")
            return False
    
    def make_get_request(self, endpoint: str, params: Dict = None) -> Dict:
        """
        Make a request to the Reddit Ads API with automatic token refresh
        
        Args:
            endpoint: API endpoint path
            params: Query parameters
            
        Returns:
            Dict: API response data
        """
        url = f"{self.base_url}/{endpoint}"
        
        try:
            response = self.session.get(url, params=params)
            
            # If unauthorized, try to refresh token and retry
            if response.status_code == 401:
                logger.info("Token expired, attempting to refresh...")
                if self.refresh_access_token():
                    response = self.session.get(url, params=params)
                else:
                    raise Exception("Unable to refresh access token or resource not found")
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {str(e)}")
            raise
    
    def make_post_request(self, endpoint: str, data: Dict = None) -> Dict:
        """
        Make a POST request to the Reddit Ads API with automatic token refresh
        
        Args:
            endpoint: API endpoint path
            data: JSON body data
            
        Returns:
            Dict: API response data
        """
        url = f"{self.base_url}/{endpoint}"
        
        try:
            response = self.session.post(url, json=data)
            
            # If unauthorized, try to refresh token and retry
            if response.status_code == 401:
                logger.info("Token expired, attempting to refresh...")
                if self.refresh_access_token():
                    response = self.session.post(url, json=data)
                else:
                    raise Exception("Unable to refresh access token")
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API POST request failed: {str(e)}")
            logger.error(f"Response: {response.text if 'response' in locals() else 'No response'}")
            raise
    
    def get_businesses(self) -> List[Dict]:
        """
        Get all businesses associated with the account
        
        Returns:
            List[Dict]: List of business objects
        """
        logger.info("Fetching businesses...")
        response = self.make_get_request("me/businesses")
        return response.get('data', [])
    
    def get_ad_accounts(self, business_id: str) -> List[Dict]:
        """
        Get all ad accounts for a business
        
        Args:
            business_id: Business ID
            
        Returns:
            List[Dict]: List of ad account objects
        """
        logger.info(f"Fetching ad accounts for business {business_id}...")
        response = self.make_get_request(f"businesses/{business_id}/ad_accounts")
        return response.get('data', [])
    
    def get_campaigns(self, account_id: str) -> List[Dict]:
        """
        Get all campaigns for an ad account
        
        Args:
            account_id: Ad account ID
            
        Returns:
            List[Dict]: List of campaign objects
        """
        logger.info(f"Fetching campaigns for account {account_id}...")
        response = self.make_get_request(f"ad_accounts/{account_id}/campaigns")
        return response.get('data', [])
    
    def get_ad_groups(self, account_id: str) -> List[Dict]:
        """
        Get all ad groups for an ad account
        
        Args:
            account_id: Ad account ID
            
        Returns:
            List[Dict]: List of ad group objects
        """
        logger.info(f"Fetching ad groups for account {account_id}...")
        response = self.make_get_request(f"ad_accounts/{account_id}/ad_groups")
        return response.get('data', [])
    
    def get_ads(self, account_id: str) -> List[Dict]:
        """
        Get all ads for an ad account
        
        Args:
            account_id: Ad account ID
            
        Returns:
            List[Dict]: List of ad objects
        """
        logger.info(f"Fetching ads for account {account_id}...")
        response = self.make_get_request(f"ad_accounts/{account_id}/ads")
        return response.get('data', [])
    
    def get_insights_report(self, account_id: str, starts_at: str, ends_at: str, 
                           breakdowns: List[str] = None, fields: List[str] = None,
                           filter: str = None, time_zone_id: str = "GMT") -> Dict:
        """
        Get insights report for an ad account
        
        Args:
            account_id: Ad account ID
            starts_at: Start date in ISO 8601 format (e.g., '2024-01-01T00:00:00Z')
            ends_at: End date in ISO 8601 format (e.g., '2024-01-31T00:00:00Z')
            breakdowns: List of dimensions to group by (e.g., ['date', 'campaign_id'])
            fields: List of metrics to include (e.g., ['impressions', 'clicks', 'spend'])
            filter: Optional filter string (e.g., 'campaign:effective_status==ACTIVE')
            time_zone_id: Time zone for the report (default: 'GMT')
            
        Returns:
            Dict: Insights report data
        """
        logger.info(f"Fetching insights report for account {account_id}...")
        
        # Construct the request body according to the API spec
        request_body = {
            "data": {
                "starts_at": starts_at,
                "ends_at": ends_at,
                "time_zone_id": time_zone_id
            }
        }
        
        # Add optional fields if provided
        if breakdowns:
            request_body["data"]["breakdowns"] = breakdowns
        
        if fields:
            request_body["data"]["fields"] = fields
            
        if filter:
            request_body["data"]["filter"] = filter
        
        logger.info(f"Request body: {request_body}")
        response = self.make_post_request(f"ad_accounts/{account_id}/reports", request_body)
        return response
    
    def get_account_summary(self, account_id: str) -> Dict:
        """
        Get account summary information
        
        Args:
            account_id: Ad account ID
            
        Returns:
            Dict: Account summary data
        """
        logger.info(f"Fetching account summary for {account_id}...")
        response = self.make_get_request(f"ad_accounts/{account_id}")
        return response.get('data', {})
    
    def create_report(self, ad_account_id: str, breakdowns: List[str], fields: List[str], 
                     starts_at: str, ends_at: str, filter: str = None, 
                     time_zone_id: str = "GMT") -> Dict:
        """
        Create a report using the Reddit Ads API v3 POST endpoint
        
        Args:
            ad_account_id: Ad account ID
            breakdowns: List of breakdowns (e.g., ['AD_ID', 'DATE'])
            fields: List of fields/metrics to include (e.g., ['CPC', 'impressions', 'clicks'])
            starts_at: Start date in ISO 8601 format (e.g., '2024-01-01T00:00:00Z')
            ends_at: End date in ISO 8601 format (e.g., '2024-01-31T00:00:00Z')
            filter: Optional filter string (e.g., 'campaign:effective_status==ACTIVE')
            time_zone_id: Time zone for the report (default: 'GMT')
            
        Returns:
            Dict: API response with report data
        """
        logger.info(f"Creating report for ad account {ad_account_id}...")
        
        # Construct the request body according to the API spec
        request_body = {
            "data": {
                "breakdowns": breakdowns,
                "fields": fields,
                "starts_at": starts_at,
                "ends_at": ends_at,
                "time_zone_id": time_zone_id
            }
        }
        
        # Add optional filter if provided
        if filter:
            request_body["data"]["filter"] = filter
        
        endpoint = f"ad_accounts/{ad_account_id}/reports"
        response = self.make_post_request(endpoint, request_body)
        
        logger.info(f"Report created successfully")
        return response