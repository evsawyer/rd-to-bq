#!/usr/bin/env python3
"""
Reddit Ads to BigQuery Pipeline
Fetches Reddit Ads data and loads it to BigQuery
"""

import os
import sys
import json
import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional
import logging
from dotenv import load_dotenv
import time
import argparse

# Import our custom clients
from RedditAdsClient import RedditAdsClient
from BigQueryClient import BigQueryClient
from google.cloud import bigquery

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


class RedditToBigQueryPipeline:
    """Pipeline for syncing Reddit Ads data to BigQuery"""
    
    def __init__(self, dataset_id: str = "raw_ads", project_id: str = "ivc-media-ads-warehouse"):
        """
        Initialize the pipeline with Reddit and BigQuery clients
        
        Args:
            dataset_id: BigQuery dataset ID
            project_id: GCP project ID
        """
        self.dataset_id = dataset_id
        self.project_id = project_id
        
        # Initialize Reddit client
        self.reddit_client = self._init_reddit_client()
        
        # Initialize BigQuery client
        self.bq_client = BigQueryClient()
        
        # Ensure dataset exists
        self._ensure_dataset_exists()
    
    def _init_reddit_client(self) -> RedditAdsClient:
        """Initialize Reddit Ads client with credentials from environment"""
        required_vars = [
            'REDDIT_ACCESS_TOKEN',
            'REDDIT_REFRESH_TOKEN', 
            'REDDIT_CLIENT_ID',
            'REDDIT_CLIENT_SECRET'
        ]
        
        # Check for required environment variables
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {missing_vars}")
        
        return RedditAdsClient(
            access_token=os.getenv('REDDIT_ACCESS_TOKEN'),
            refresh_token=os.getenv('REDDIT_REFRESH_TOKEN'),
            client_id=os.getenv('REDDIT_CLIENT_ID'),
            client_secret=os.getenv('REDDIT_CLIENT_SECRET')
        )
    
    def _ensure_dataset_exists(self):
        """Ensure the BigQuery dataset exists, create if not"""
        dataset_id = f"{self.project_id}.{self.dataset_id}"
        
        try:
            self.bq_client.client.get_dataset(dataset_id)
            logger.info(f"Dataset {dataset_id} already exists")
        except Exception:
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = "US"
            dataset = self.bq_client.client.create_dataset(dataset, exists_ok=True)
            logger.info(f"Created dataset {dataset_id}")
    
    def sync_ads_reference(self, table_name: str = "reddit_ads_reference") -> int:
        """
        Fetch all ads with their complete hierarchy information (ad, ad group, campaign, ad account names)
        and sync to BigQuery as a reference table
        
        Args:
            table_name: Name of the BigQuery table
            
        Returns:
            int: Number of records synced
        """
        logger.info("Starting ads reference sync...")
        
        try:
            # First get all businesses
            businesses = self.reddit_client.get_businesses()
            logger.info(f"Found {len(businesses)} businesses")
            
            # Collect all ads with complete hierarchy information
            all_ads_with_hierarchy = []
            
            for business in businesses:
                business_id = business['id']
                business_name = business.get('name', 'Unknown')
                logger.info(f"Processing business: {business_name} (ID: {business_id})")
                
                try:
                    # Get ad accounts for this business
                    ad_accounts = self.reddit_client.get_ad_accounts(business_id)
                    logger.info(f"  Found {len(ad_accounts)} ad accounts")
                    
                    for account in ad_accounts:
                        account_id = account['id']
                        account_name = account.get('name', 'Unknown')
                        logger.info(f"    Processing ad account: {account_name} (ID: {account_id})")
                        
                        try:
                            # Get campaigns for this ad account
                            campaigns = self.reddit_client.get_campaigns(account_id)
                            campaign_map = {c['id']: c.get('name', 'Unknown') for c in campaigns}
                            logger.info(f"      Found {len(campaigns)} campaigns")
                            
                            # Get ad groups for this ad account
                            ad_groups = self.reddit_client.get_ad_groups(account_id)
                            ad_group_map = {ag['id']: ag.get('name', 'Unknown') for ag in ad_groups}
                            logger.info(f"      Found {len(ad_groups)} ad groups")
                            
                            # Get ads for this ad account
                            ads = self.reddit_client.get_ads(account_id)
                            logger.info(f"      Found {len(ads)} ads")
                            
                            # Process each ad and enrich with hierarchy information
                            for ad in ads:
                                ad_reference = {
                                    # Ad information
                                    'ad_id': ad['id'],
                                    'ad_name': ad.get('name', 'Unknown'),
                                    'ad_type': ad.get('type'),
                                    'ad_status': ad.get('effective_status'),
                                    'ad_created_at': ad.get('created_at'),
                                    'ad_modified_at': ad.get('modified_at'),
                                    
                                    # Ad group information
                                    'ad_group_id': ad.get('ad_group_id'),
                                    'ad_group_name': ad_group_map.get(ad.get('ad_group_id'), 'Unknown'),
                                    
                                    # Campaign information
                                    'campaign_id': ad.get('campaign_id'),
                                    'campaign_name': campaign_map.get(ad.get('campaign_id'), 'Unknown'),
                                    
                                    # Ad account information
                                    'ad_account_id': account_id,
                                    'ad_account_name': account_name,
                                    'ad_account_currency': account.get('currency'),
                                    'ad_account_time_zone': account.get('time_zone_id'),
                                    
                                    # Business information
                                    'business_id': business_id,
                                    'business_name': business_name,
                                    
                                    # Additional ad metadata
                                    'click_url': ad.get('click_url'),
                                    'view_url': ad.get('view_url'),
                                    'third_party_url': ad.get('third_party_url'),
                                    'is_processing': ad.get('is_processing'),
                                    'rejection_reason': ad.get('rejection_reason')
                                }
                                
                                all_ads_with_hierarchy.append(ad_reference)
                            
                            # Add small delay to avoid rate limiting
                            time.sleep(1)
                            
                        except Exception as e:
                            logger.error(f"      Error processing ad account {account_id}: {str(e)}")
                            continue
                        
                except Exception as e:
                    logger.error(f"  Error fetching ad accounts for business {business_id}: {str(e)}")
                    continue
            
            logger.info(f"Total ads found: {len(all_ads_with_hierarchy)}")
            
            if not all_ads_with_hierarchy:
                logger.warning("No ads found")
                return 0
            
            # Convert to DataFrame and add metadata
            df = pd.DataFrame(all_ads_with_hierarchy)
            
            # Convert timestamp fields to datetime
            timestamp_fields = ['ad_created_at', 'ad_modified_at']
            for field in timestamp_fields:
                if field in df.columns:
                    df[field] = pd.to_datetime(df[field], errors='coerce')
            
            # Add metadata columns
            df['_loaded_at'] = pd.Timestamp.now(tz='UTC')
            df['_source'] = 'reddit_ads_api'
            
            # Configure load job with autodetect schema
            table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
            job_config = bigquery.LoadJobConfig(
                autodetect=True,  # Let BigQuery detect the schema
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Replace table
                time_partitioning=bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="_loaded_at"
                ),
                clustering_fields=["business_id", "ad_account_id", "campaign_id", "ad_group_id"]
            )
            
            # Load to BigQuery
            logger.info(f"Loading {len(df)} records to {table_id}")
            logger.info(f"Columns: {list(df.columns)}")
            
            job = self.bq_client.load_table_from_dataframe(
                df,
                table_id,
                job_config=job_config
            )
            
            # Wait for job to complete
            job.result()
            
            logger.info(f"Successfully loaded {len(df)} ads to BigQuery")
            
            # Log sample data
            if len(df) > 0:
                sample_cols = ['ad_id', 'ad_name', 'ad_group_name', 'campaign_name', 'ad_account_name', 'business_name']
                logger.info(f"Sample ad reference: {df[sample_cols].iloc[0].to_dict()}")
                
            # Get the created table schema
            table = self.bq_client.client.get_table(table_id)
            logger.info(f"Table schema: {[f'{field.name}:{field.field_type}' for field in table.schema]}")
            
            return len(df)
            
        except Exception as e:
            logger.error(f"Error syncing ads reference: {str(e)}")
            raise
    
    def sync_ads_performance(self, temp_table_name: str = "reddit_ads_temp", days_back: int = 30) -> int:
        """
        Fetch ads performance data for the last N days and sync to a temporary BigQuery table
        
        Args:
            temp_table_name: Name of the temporary BigQuery table
            days_back: Number of days to fetch data for (default: 30)
            
        Returns:
            int: Number of records synced
        """
        logger.info(f"Starting ads performance sync for last {days_back} days...")
        
        try:
            # Calculate date range
            end_date = datetime.now(timezone.utc)
            start_date = end_date - timedelta(days=days_back)
            
            # Format dates for Reddit API (ISO 8601 with hourly granularity)
            starts_at = start_date.strftime('%Y-%m-%dT00:00:00Z')
            ends_at = end_date.strftime('%Y-%m-%dT23:00:00Z')  # Changed from 23:59:59 to 23:00:00
            
            logger.info(f"Date range: {starts_at} to {ends_at}")
            
            # Get all businesses to iterate through accounts
            businesses = self.reddit_client.get_businesses()
            logger.info(f"Found {len(businesses)} businesses")
            
            # Collect all performance data
            all_performance_data = []
            
            for business in businesses:
                business_id = business['id']
                business_name = business.get('name', 'Unknown')
                logger.info(f"Processing business: {business_name} (ID: {business_id})")
                
                try:
                    # Get ad accounts for this business
                    ad_accounts = self.reddit_client.get_ad_accounts(business_id)
                    logger.info(f"  Found {len(ad_accounts)} ad accounts")
                    
                    for account in ad_accounts:
                        account_id = account['id']
                        account_name = account.get('name', 'Unknown')
                        logger.info(f"    Fetching performance for account: {account_name} (ID: {account_id})")
                        
                        try:
                            # Define breakdowns and fields
                            breakdowns = ["ad_id", "date"]
                            fields = [
                                "account_id", "campaign_id", "ad_group_id", "ad_id",
                                "impressions", "reach", "frequency", 
                                "spend", "clicks", "cpc", "ecpm", "ctr",
                                # Reddit conversion metrics
                                "reddit_leads", "app_install_view_content_count", "app_install_add_to_cart_count", 
                                "app_install_mmp_checkout_count", "app_install_skan_checkout_count",
                                "app_install_purchase_count",
                                # ROAS
                                "app_install_roas_double",
                                # Video engagement
                                "video_started", "video_watched_25_percent", "video_watched_50_percent", 
                                "video_watched_75_percent", "video_watched_100_percent",
                                "video_viewable_watched_15_seconds"
                            ]
                            
                            # Get insights report
                            report_response = self.reddit_client.get_insights_report(
                                account_id=account_id,
                                starts_at=starts_at,
                                ends_at=ends_at,
                                breakdowns=breakdowns,
                                fields=fields
                            )
                            
                            # Process the response
                            if 'data' in report_response:
                                # The API returns data in 'metrics' array
                                if 'metrics' in report_response['data']:
                                    rows = report_response['data']['metrics']
                                    logger.info(f"      Retrieved {len(rows)} rows of performance data")
                                    
                                    # Add each row to our collection
                                    for row in rows:
                                        # The row is already in the correct format
                                        # Just add it directly
                                        all_performance_data.append(row)
                                else:
                                    logger.warning(f"      No metrics found in response")
                            
                            # Add small delay to avoid rate limiting
                            time.sleep(2)  # Increased from 1 to 2 seconds
                            
                        except Exception as e:
                            logger.error(f"      Error fetching performance for account {account_id}: {str(e)}")
                            continue
                        
                except Exception as e:
                    logger.error(f"  Error fetching ad accounts for business {business_id}: {str(e)}")
                    continue
            
            logger.info(f"Total performance rows collected: {len(all_performance_data)}")
            
            if not all_performance_data:
                logger.warning("No performance data found")
                return 0
            
            # Convert to DataFrame
            df = pd.DataFrame(all_performance_data)
            
            # Convert date strings to datetime
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
            
            # Convert monetary values from micros (divide by 1,000,000)
            monetary_fields = ['spend', 'cpc', 'ecpm']
            for field in monetary_fields:
                if field in df.columns:
                    df[field] = df[field] / 1_000_000.0
            
            # Add metadata columns
            df['_loaded_at'] = pd.Timestamp.now(tz='UTC')
            df['_source'] = 'reddit_ads_api'
            
            # Configure load job - simple table, no partitioning
            table_id = f"{self.project_id}.{self.dataset_id}.{temp_table_name}"
            job_config = bigquery.LoadJobConfig(
                autodetect=True,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE  # Replace temp table
            )
            
            # Load to BigQuery
            logger.info(f"Loading {len(df)} records to {table_id}")
            logger.info(f"Columns: {list(df.columns)}")
            
            job = self.bq_client.load_table_from_dataframe(
                df,
                table_id,
                job_config=job_config
            )
            
            # Wait for job to complete
            job.result()
            
            logger.info(f"Successfully loaded {len(df)} performance records to BigQuery temp table")
            
            # Log sample data
            if len(df) > 0:
                sample_cols = ['date', 'ad_id', 'impressions', 'clicks', 'spend']
                available_cols = [col for col in sample_cols if col in df.columns]
                logger.info(f"Sample performance data: {df[available_cols].iloc[0].to_dict()}")
            
            return len(df)
            
        except Exception as e:
            logger.error(f"Error syncing ads performance: {str(e)}")
            raise
    
    def merge_ads_performance(self, temp_table_name: str = "reddit_ads_temp", 
                            final_table_name: str = "reddit_ads") -> None:
        """
        Merge performance data from temp table with reference data and update final table
        
        Args:
            temp_table_name: Name of the temporary table with performance data
            final_table_name: Name of the final table to update
        """
        logger.info("Starting merge of performance data with reference data...")
        
        try:
            # First check if the final table exists, if not create it
            final_table_id = f"{self.project_id}.{self.dataset_id}.{final_table_name}"
            
            try:
                self.bq_client.client.get_table(final_table_id)
                logger.info(f"Table {final_table_id} exists")
            except Exception:
                logger.info(f"Table {final_table_id} does not exist, creating it...")
                
                # Create table with schema from temp table + reference fields
                create_table_query = f"""
                CREATE TABLE IF NOT EXISTS `{self.project_id}.{self.dataset_id}.{final_table_name}` (
                    date DATE,
                    ad_id STRING,
                    ad_account_id STRING,
                    campaign_id STRING,
                    ad_group_id STRING,
                    impressions INT64,
                    reach INT64,
                    frequency FLOAT64,
                    spend FLOAT64,
                    clicks INT64,
                    cpc FLOAT64,
                    ecpm FLOAT64,
                    ctr FLOAT64,
                    reddit_leads INT64,
                    app_install_view_content_count INT64,
                    app_install_add_to_cart_count INT64,
                    app_install_mmp_checkout_count INT64,
                    app_install_skan_checkout_count INT64,
                    app_install_purchase_count INT64,
                    app_install_roas_double FLOAT64,
                    video_started INT64,
                    video_watched_25_percent INT64,
                    video_watched_50_percent INT64,
                    video_watched_75_percent INT64,
                    video_watched_100_percent INT64,
                    video_viewable_watched_15_seconds INT64,
                    ad_name STRING,
                    ad_type STRING,
                    ad_status STRING,
                    ad_group_name STRING,
                    campaign_name STRING,
                    ad_account_name STRING,
                    ad_account_currency STRING,
                    ad_account_time_zone STRING,
                    business_id STRING,
                    business_name STRING,
                    _loaded_at TIMESTAMP,
                    _updated_at TIMESTAMP,
                    _source STRING
                )
                """
                
                job = self.bq_client.client.query(create_table_query)
                job.result()
                logger.info(f"Created table {final_table_id}")
            
            # SQL query to merge temp data with reference data and update final table
            merge_query = f"""
            MERGE `{self.project_id}.{self.dataset_id}.{final_table_name}` AS target
            USING (
                SELECT 
                    CAST(t.date AS DATE) AS date,
                    t.ad_id,
                    t.app_install_add_to_cart_count,
                    t.app_install_mmp_checkout_count,
                    t.app_install_purchase_count,
                    t.app_install_roas_double,
                    t.app_install_skan_checkout_count,
                    t.app_install_view_content_count,
                    t.clicks,
                    t.cpc,
                    t.ctr,
                    t.ecpm,
                    t.frequency,
                    t.impressions,
                    t.reach,
                    t.reddit_leads,
                    t.spend,
                    t.video_started,
                    t.video_watched_25_percent,
                    t.video_watched_50_percent,
                    t.video_watched_75_percent,
                    t.video_watched_100_percent,
                    t.video_viewable_watched_15_seconds,
                    t._loaded_at,
                    t._source,
                    r.ad_name,
                    r.ad_type,
                    r.ad_status,
                    r.ad_group_id,
                    r.ad_group_name,
                    r.campaign_id,
                    r.campaign_name,
                    r.ad_account_id,
                    r.ad_account_name,
                    r.ad_account_currency,
                    r.ad_account_time_zone,
                    r.business_id,
                    r.business_name,
                    CURRENT_TIMESTAMP() AS _updated_at
                FROM `{self.project_id}.{self.dataset_id}.{temp_table_name}` t
                LEFT JOIN `{self.project_id}.{self.dataset_id}.reddit_ads_reference` r
                ON t.ad_id = r.ad_id
            ) AS source
            ON target.date = source.date AND target.ad_id = source.ad_id
            WHEN MATCHED THEN
                UPDATE SET 
                    target.ad_account_id = source.ad_account_id,
                    target.campaign_id = source.campaign_id,
                    target.ad_group_id = source.ad_group_id,
                    target.impressions = source.impressions,
                    target.reach = source.reach,
                    target.frequency = source.frequency,
                    target.spend = source.spend,
                    target.clicks = source.clicks,
                    target.cpc = source.cpc,
                    target.ecpm = source.ecpm,
                    target.ctr = source.ctr,
                    target.reddit_leads = source.reddit_leads,
                    target.app_install_view_content_count = source.app_install_view_content_count,
                    target.app_install_add_to_cart_count = source.app_install_add_to_cart_count,
                    target.app_install_mmp_checkout_count = source.app_install_mmp_checkout_count,
                    target.app_install_skan_checkout_count = source.app_install_skan_checkout_count,
                    target.app_install_purchase_count = source.app_install_purchase_count,
                    target.app_install_roas_double = source.app_install_roas_double,
                    target.video_started = source.video_started,
                    target.video_watched_25_percent = source.video_watched_25_percent,
                    target.video_watched_50_percent = source.video_watched_50_percent,
                    target.video_watched_75_percent = source.video_watched_75_percent,
                    target.video_watched_100_percent = source.video_watched_100_percent,
                    target.video_viewable_watched_15_seconds = source.video_viewable_watched_15_seconds,
                    target.ad_name = source.ad_name,
                    target.ad_type = source.ad_type,
                    target.ad_status = source.ad_status,
                    target.ad_group_name = source.ad_group_name,
                    target.campaign_name = source.campaign_name,
                    target.ad_account_name = source.ad_account_name,
                    target.ad_account_currency = source.ad_account_currency,
                    target.ad_account_time_zone = source.ad_account_time_zone,
                    target.business_id = source.business_id,
                    target.business_name = source.business_name,
                    target._updated_at = source._updated_at,
                    target._source = source._source
            WHEN NOT MATCHED THEN
                INSERT (
                    date, ad_id, ad_account_id, campaign_id, ad_group_id,
                    impressions, reach, frequency, spend, clicks, cpc, ecpm, ctr,
                    reddit_leads, app_install_view_content_count, app_install_add_to_cart_count,
                    app_install_mmp_checkout_count, app_install_skan_checkout_count,
                    app_install_purchase_count, app_install_roas_double,
                    video_started, video_watched_25_percent, video_watched_50_percent,
                    video_watched_75_percent, video_watched_100_percent, video_viewable_watched_15_seconds,
                    ad_name, ad_type, ad_status, ad_group_name, campaign_name,
                    ad_account_name, ad_account_currency, ad_account_time_zone,
                    business_id, business_name, _loaded_at, _updated_at, _source
                )
                VALUES (
                    source.date, source.ad_id, source.ad_account_id, source.campaign_id, source.ad_group_id,
                    source.impressions, source.reach, source.frequency, source.spend, source.clicks, 
                    source.cpc, source.ecpm, source.ctr,
                    source.reddit_leads, source.app_install_view_content_count, source.app_install_add_to_cart_count,
                    source.app_install_mmp_checkout_count, source.app_install_skan_checkout_count,
                    source.app_install_purchase_count, source.app_install_roas_double,
                    source.video_started, source.video_watched_25_percent, source.video_watched_50_percent,
                    source.video_watched_75_percent, source.video_watched_100_percent, source.video_viewable_watched_15_seconds,
                    source.ad_name, source.ad_type, source.ad_status, source.ad_group_name, source.campaign_name,
                    source.ad_account_name, source.ad_account_currency, source.ad_account_time_zone,
                    source.business_id, source.business_name, source._loaded_at, source._updated_at, source._source
                )
            """
            
            # Execute the merge query
            logger.info("Executing MERGE query...")
            job = self.bq_client.client.query(merge_query)
            job.result()  # Wait for completion
            
            # Get statistics
            logger.info(f"MERGE completed successfully")
            logger.info(f"Rows affected: {job.num_dml_affected_rows}")
            
        except Exception as e:
            logger.error(f"Error merging performance data: {str(e)}")
            raise
    
    def get_table_info(self, table_name: str) -> Dict:
        """
        Get information about a BigQuery table
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dict with table information
        """
        table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
        
        try:
            table = self.bq_client.client.get_table(table_id)
            
            return {
                "table_id": table_id,
                "created": table.created,
                "modified": table.modified,
                "num_rows": table.num_rows,
                "num_bytes": table.num_bytes,
                "schema": [{"name": field.name, "type": field.field_type} for field in table.schema]
            }
        except Exception as e:
            logger.error(f"Error getting table info: {str(e)}")
            return None
    
    def query_ads_reference(self, limit: int = 10) -> pd.DataFrame:
        """
        Query ads reference from BigQuery
        
        Args:
            limit: Number of records to return
            
        Returns:
            DataFrame with ads reference data
        """
        query = f"""
        SELECT *
        FROM `{self.project_id}.{self.dataset_id}.reddit_ads_reference`
        ORDER BY _loaded_at DESC
        LIMIT {limit}
        """
        
        return self.bq_client.client.query(query).to_dataframe()


def main():
    """Main execution function"""
    # Set up command line arguments
    parser = argparse.ArgumentParser(description='Sync Reddit Ads data to BigQuery')
    parser.add_argument('--days-back', type=int, default=60, 
                        help='Number of days of historical data to fetch (default: 60)')
    parser.add_argument('--skip-reference', action='store_true',
                        help='Skip syncing ads reference table')
    args = parser.parse_args()
    
    logger.info("Starting Reddit to BigQuery sync")
    logger.info(f"Fetching {args.days_back} days of historical data")
    
    try:
        # Initialize pipeline
        pipeline = RedditToBigQueryPipeline(
            dataset_id="raw_ads",
            project_id="ivc-media-ads-warehouse"
        )
        
        # Step 1: Sync ads reference (entity hierarchy)
        if not args.skip_reference:
            logger.info("\n" + "="*60)
            logger.info("STEP 1: Syncing ads reference data...")
            count = pipeline.sync_ads_reference()
            logger.info(f"Synced {count} ads to reference table")
        else:
            logger.info("\n" + "="*60)
            logger.info("STEP 1: Skipping ads reference sync (--skip-reference flag)")
        
        # Step 2: Sync ads performance data to temp table
        logger.info("\n" + "="*60)
        logger.info(f"STEP 2: Syncing ads performance data for last {args.days_back} days...")
        count = pipeline.sync_ads_performance(days_back=args.days_back)
        logger.info(f"Synced {count} performance records to temp table")
        
        # Step 3: Merge performance with reference data
        if count > 0:
            logger.info("\n" + "="*60)
            logger.info("STEP 3: Merging performance data with reference...")
            pipeline.merge_ads_performance()
            logger.info("Successfully merged performance data")
        else:
            logger.info("\n" + "="*60)
            logger.info("STEP 3: Skipping merge - no performance data to process")
        
        # Get final table info
        logger.info("\n" + "="*60)
        logger.info("Final table information:")
        table_info = pipeline.get_table_info("reddit_ads")
        if table_info:
            logger.info(f"Table info: {json.dumps(table_info, indent=2, default=str)}")
        
        # Query sample data from final table
        logger.info("\n" + "="*60)
        logger.info("Sample data from final reddit_ads table:")
        query = f"""
        SELECT 
            date,
            ad_name,
            campaign_name,
            impressions,
            clicks,
            spend,
            ctr
        FROM `{pipeline.project_id}.{pipeline.dataset_id}.reddit_ads`
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
        ORDER BY date DESC, spend DESC
        LIMIT 10
        """
        sample_data = pipeline.bq_client.client.query(query).to_dataframe()
        logger.info(f"\n{sample_data}")
        
        # Show date range of data
        logger.info("\n" + "="*60)
        logger.info("Date range of data in reddit_ads table:")
        date_range_query = f"""
        SELECT 
            MIN(date) as earliest_date,
            MAX(date) as latest_date,
            COUNT(DISTINCT date) as days_of_data,
            COUNT(*) as total_rows
        FROM `{pipeline.project_id}.{pipeline.dataset_id}.reddit_ads`
        """
        date_range = pipeline.bq_client.client.query(date_range_query).to_dataframe()
        logger.info(f"\n{date_range}")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise


if __name__ == "__main__":
    main() 