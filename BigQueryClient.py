from google.oauth2 import service_account
import os
from google.cloud import bigquery
import json
import io
import logging
from typing import Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


class BigQueryClient:
    """
    A wrapper class for Google BigQuery client with credential management.
    
    This class handles authentication from either environment variables (production)
    or local credentials file (development).
    """
    
    SCOPES = [
        'https://www.googleapis.com/auth/bigquery',
        'https://www.googleapis.com/auth/drive.readonly'
    ]
    
    def __init__(self, credentials_path: str = "credentials.json"):
        """
        Initialize BigQuery client with credentials from either file or environment variable.
        
        Args:
            credentials_path: Path to local credentials file (used if env var not found)
        """
        self.logger = logging.getLogger('bigquery')
        self.credentials_path = credentials_path
        self._client: Optional[bigquery.Client] = None
        self._credentials = None
        
        # Initialize the client on instantiation
        self._initialize_client()
    
    def _initialize_client(self) -> None:
        """Initialize the BigQuery client with appropriate credentials."""
        try:
            # First try to get credentials from environment variable (production)
            credentials_json = os.getenv('GOOGLE_CREDENTIALS')
            
            if credentials_json:
                # Production: Use credentials from environment variable
                self.logger.info("Using credentials from environment variable")
                credentials_info = json.loads(credentials_json)
                self._credentials = service_account.Credentials.from_service_account_info(
                    credentials_info, 
                    scopes=self.SCOPES
                )
            else:
                # Local: Try to use credentials file
                if not os.path.exists(self.credentials_path):
                    raise ValueError(
                        f"No credentials found in environment variable or local file: {self.credentials_path}"
                    )
                self.logger.info(f"Using credentials from file: {self.credentials_path}")
                self._credentials = service_account.Credentials.from_service_account_file(
                    self.credentials_path, 
                    scopes=self.SCOPES
                )
            
            # Create the BigQuery client
            self._client = bigquery.Client(
                project=self._credentials.project_id,
                credentials=self._credentials
            )
            self.logger.info(f"BigQuery client initialized for project: {self._credentials.project_id}")
            
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse credentials JSON: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Failed to initialize BigQuery client: {str(e)}")
            raise
    
    @property
    def client(self) -> bigquery.Client:
        """
        Get the BigQuery client instance.
        
        Returns:
            bigquery.Client: The initialized BigQuery client
        """
        if self._client is None:
            self._initialize_client()
        return self._client
    
    @property
    def project_id(self) -> str:
        """Get the current project ID."""
        return self._credentials.project_id if self._credentials else None
    
    def get_dataset(self, dataset_id: str) -> bigquery.Dataset:
        """
        Get a dataset reference.
        
        Args:
            dataset_id: The dataset ID
            
        Returns:
            bigquery.Dataset: Dataset reference
        """
        return self.client.dataset(dataset_id)
    
    def get_table(self, dataset_id: str, table_id: str) -> bigquery.Table:
        """
        Get a table reference.
        
        Args:
            dataset_id: The dataset ID
            table_id: The table ID
            
        Returns:
            bigquery.Table: Table reference
        """
        dataset = self.get_dataset(dataset_id)
        return dataset.table(table_id)
    
    def query(self, query: str, **kwargs) -> bigquery.QueryJob:
        """
        Execute a query.
        
        Args:
            query: SQL query string
            **kwargs: Additional arguments for query configuration
            
        Returns:
            bigquery.QueryJob: Query job instance
        """
        return self.client.query(query, **kwargs)
    
    def load_table_from_dataframe(self, dataframe, destination, **kwargs) -> bigquery.LoadJob:
        """
        Load a pandas DataFrame to a BigQuery table.
        
        Args:
            dataframe: pandas DataFrame to load
            destination: Table reference or table ID string
            **kwargs: Additional arguments for load job configuration
            
        Returns:
            bigquery.LoadJob: Load job instance
        """
        return self.client.load_table_from_dataframe(dataframe, destination, **kwargs)


# Create a default instance for backward compatibility
_default_client = None


def get_bigquery_client() -> BigQueryClient:
    """
    Get the default BigQuery client instance (singleton pattern).
    
    Returns:
        BigQueryClient: The default BigQuery client instance
    """
    global _default_client
    if _default_client is None:
        _default_client = BigQueryClient()
    return _default_client