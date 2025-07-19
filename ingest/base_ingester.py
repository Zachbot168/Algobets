import asyncio
import aiohttp
import time
import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
import json
import hashlib

from .database import db

logger = logging.getLogger(__name__)

class BaseIngester(ABC):
    """Base class for all data ingesters"""
    
    def __init__(self, rate_limit_per_minute: int = 60):
        self.rate_limit = rate_limit_per_minute
        self.last_request_time = 0
        self.request_count = 0
        self.session: Optional[aiohttp.ClientSession] = None
        
    async def __aenter__(self):
        """Async context manager entry"""
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
            headers=self.get_default_headers()
        )
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()
            
    @abstractmethod
    def get_default_headers(self) -> Dict[str, str]:
        """Get default headers for API requests"""
        pass
        
    @abstractmethod
    async def ingest_data(self, **kwargs) -> Dict[str, Any]:
        """Main ingestion method to be implemented by subclasses"""
        pass
        
    async def rate_limit_check(self):
        """Ensure we don't exceed rate limits"""
        current_time = time.time()
        
        # Reset counter every minute
        if current_time - self.last_request_time > 60:
            self.request_count = 0
            self.last_request_time = current_time
            
        # Wait if we've hit the rate limit
        if self.request_count >= self.rate_limit:
            wait_time = 60 - (current_time - self.last_request_time)
            if wait_time > 0:
                logger.info(f"Rate limit reached, waiting {wait_time:.2f} seconds")
                await asyncio.sleep(wait_time)
                self.request_count = 0
                self.last_request_time = time.time()
                
        self.request_count += 1
        
    async def make_request(self, url: str, params: Optional[Dict] = None, 
                          headers: Optional[Dict] = None) -> Optional[Dict]:
        """Make rate-limited HTTP request"""
        await self.rate_limit_check()
        
        try:
            request_headers = self.get_default_headers()
            if headers:
                request_headers.update(headers)
                
            async with self.session.get(url, params=params, headers=request_headers) as response:
                if response.status == 200:
                    return await response.json()
                elif response.status == 429:
                    # Rate limited - wait and retry
                    retry_after = int(response.headers.get('Retry-After', 60))
                    logger.warning(f"Rate limited, waiting {retry_after} seconds")
                    await asyncio.sleep(retry_after)
                    return await self.make_request(url, params, headers)
                else:
                    logger.error(f"Request failed with status {response.status}: {url}")
                    return None
                    
        except asyncio.TimeoutError:
            logger.error(f"Request timeout: {url}")
            return None
        except Exception as e:
            logger.error(f"Request error: {e}")
            return None
            
    def generate_id(self, *args) -> str:
        """Generate consistent ID from arguments"""
        combined = "_".join(str(arg) for arg in args)
        return hashlib.md5(combined.encode()).hexdigest()
        
    def get_current_timestamp(self) -> datetime:
        """Get current UTC timestamp"""
        return datetime.now(timezone.utc)
        
    def save_to_bronze(self, table_name: str, data: List[Dict[str, Any]]) -> int:
        """Save data to bronze layer table"""
        if not data:
            return 0
            
        try:
            # Get table columns (excluding auto-generated ones)
            sample_record = data[0]
            columns = list(sample_record.keys())
            
            # Create INSERT query
            placeholders = ", ".join([f"${i+1}" for i in range(len(columns))])
            column_names = ", ".join(columns)
            
            query = f"""
                INSERT OR REPLACE INTO {table_name} ({column_names}) 
                VALUES ({placeholders})
            """
            
            # Convert data to list of tuples
            values = [[record.get(col) for col in columns] for record in data]
            
            # Execute batch insert
            db.execute_many(query, values)
            
            logger.info(f"Successfully saved {len(data)} records to {table_name}")
            return len(data)
            
        except Exception as e:
            logger.error(f"Error saving to {table_name}: {e}")
            raise
            
    def log_ingestion_stats(self, source: str, records_count: int, 
                           start_time: datetime, errors: int = 0):
        """Log ingestion statistics"""
        duration = (self.get_current_timestamp() - start_time).total_seconds()
        
        logger.info(
            f"Ingestion complete - Source: {source}, "
            f"Records: {records_count}, Duration: {duration:.2f}s, "
            f"Errors: {errors}"
        )
        
    async def ingest_with_retry(self, max_retries: int = 3, **kwargs) -> Dict[str, Any]:
        """Execute ingestion with retry logic"""
        last_exception = None
        
        for attempt in range(max_retries):
            try:
                result = await self.ingest_data(**kwargs)
                return result
            except Exception as e:
                last_exception = e
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt  # Exponential backoff
                    logger.warning(f"Ingestion attempt {attempt + 1} failed, retrying in {wait_time}s: {e}")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"All ingestion attempts failed: {e}")
                    
        raise last_exception