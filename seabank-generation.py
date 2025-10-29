"""
SeaBank Generation Data Retrieval Script

This script retrieves half-hourly electricity generation data for Seabank power station
units (T_SEAB-1 and T_SEAB-2) for the entire year 2024 using the Elexon B1610 API.

The script handles:
- All 366 days of 2024 (leap year)
- Clock change days with 50 settlement periods
- Rate limiting to be server-friendly
- Error handling and recovery
- Output to CSV format using Polars
"""

import httpx
import polars as pl
import asyncio
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional, Tuple, Set
import json
import time
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('seabank_generation.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SeabankDataRetriever:
    """Main class for retrieving Seabank generation data from Elexon API."""
    
    def __init__(self):
        self.base_url = "https://data.elexon.co.uk/bmrs/api/v1/datasets/B1610"
        self.bm_units = ["T_SEAB-1", "T_SEAB-2"]
        self.year = 2024
        self.request_delay = 0.1  # 100ms between requests for concurrent execution
        self.max_concurrent_requests = 10  # Limit concurrent requests
        self.max_retries = 3
        self.checkpoint_file = "seabank_checkpoint.json"
        self.output_file = "seabank_generation_2024.csv"
        
        # UK DST dates for 2024
        self.dst_start = date(2024, 3, 31)  # Last Sunday in March
        self.dst_end = date(2024, 10, 27)   # Last Sunday in October
        
        # HTTP client will be set during execution
        self.client: Optional[httpx.AsyncClient] = None
        
    def generate_date_range(self) -> List[date]:
        """Generate all dates for 2024."""
        start_date = date(self.year, 1, 1)
        end_date = date(self.year, 12, 31)
        
        dates = []
        current_date = start_date
        while current_date <= end_date:
            dates.append(current_date)
            current_date += timedelta(days=1)
            
        logger.info(f"Generated {len(dates)} dates for {self.year}")
        return dates
    
    def get_settlement_periods(self, settlement_date: date) -> List[int]:
        """
        Get settlement periods for a given date.
        - Short days (DST start): 1-46 (clocks spring forward)
        - Normal days: 1-48
        - Long days (DST end): 1-50 (clocks fall back)
        """
        if settlement_date == self.dst_start:  # Clocks go forward - short day
            return list(range(1, 47))  # 1-46
        elif settlement_date == self.dst_end:  # Clocks go back - long day
            return list(range(1, 51))  # 1-50
        else:  # Normal day
            return list(range(1, 49))  # 1-48
    
    def build_request_url(self, settlement_date: date, settlement_period: int) -> str:
        """Build the API request URL with parameters."""
        date_str = settlement_date.strftime("%Y-%m-%d")
        
        # Build URL with multiple bmUnit parameters
        url_parts = [self.base_url + "?"]
        url_parts.append(f"settlementDate={date_str}")
        url_parts.append(f"&settlementPeriod={settlement_period}")
        
        for unit in self.bm_units:
            url_parts.append(f"&bmUnit={unit}")
        
        url_parts.append("&format=json")
        
        return "".join(url_parts)
    
    async def make_api_request(
        self, 
        settlement_date: date, 
        settlement_period: int
    ) -> Optional[List[Dict]]:
        """Make a single API request with error handling and retries."""
        if self.client is None:
            raise RuntimeError("HTTP client not initialized")
            
        url = self.build_request_url(settlement_date, settlement_period)
        
        for attempt in range(self.max_retries):
            try:
                response = await self.client.get(url, timeout=30.0)
                response.raise_for_status()
                
                data = response.json()
                if "data" in data and data["data"]:
                    return data["data"]
                else:
                    logger.warning(f"No data returned for {settlement_date} SP{settlement_period}")
                    return []
                    
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    logger.warning(f"No data available for {settlement_date} SP{settlement_period}")
                    return []
                else:
                    logger.error(f"HTTP error {e.response.status_code} for {settlement_date} SP{settlement_period}, attempt {attempt + 1}")
                    
            except httpx.RequestError as e:
                logger.error(f"Request error for {settlement_date} SP{settlement_period}, attempt {attempt + 1}: {e}")
                
            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error for {settlement_date} SP{settlement_period}, attempt {attempt + 1}: {e}")
            
            # Exponential backoff for retries
            if attempt < self.max_retries - 1:
                wait_time = (2 ** attempt) * self.request_delay
                await asyncio.sleep(wait_time)
        
        logger.error(f"Failed to retrieve data for {settlement_date} SP{settlement_period} after {self.max_retries} attempts")
        return None
    
    def load_checkpoint(self) -> Tuple[Optional[date], Optional[int], Set[Tuple[str, int]]]:
        """Load checkpoint data to resume interrupted downloads."""
        checkpoint_path = Path(self.checkpoint_file)
        if checkpoint_path.exists():
            try:
                with open(checkpoint_path, 'r') as f:
                    checkpoint = json.load(f)
                    last_date = datetime.strptime(checkpoint['last_date'], '%Y-%m-%d').date()
                    last_period = checkpoint['last_period']
                    failed_requests = set(tuple(req) for req in checkpoint.get('failed_requests', []))
                    logger.info(f"Resuming from checkpoint: {last_date} SP{last_period}")
                    if failed_requests:
                        logger.info(f"Found {len(failed_requests)} failed requests to retry")
                    return last_date, last_period, failed_requests
            except (json.JSONDecodeError, KeyError, ValueError) as e:
                logger.error(f"Error loading checkpoint: {e}")
        
        return None, None, set()
    
    def save_checkpoint(self, settlement_date: date, settlement_period: int, failed_requests: Set[Tuple[str, int]]):
        """Save checkpoint data for recovery."""
        checkpoint = {
            'last_date': settlement_date.strftime('%Y-%m-%d'),
            'last_period': settlement_period,
            'failed_requests': [list(req) for req in failed_requests]
        }
        try:
            with open(self.checkpoint_file, 'w') as f:
                json.dump(checkpoint, f)
        except IOError as e:
            logger.error(f"Error saving checkpoint: {e}")
    
    def should_skip_request(self, settlement_date: date, settlement_period: int, 
                          checkpoint_date: Optional[date], checkpoint_period: Optional[int]) -> bool:
        """Determine if a request should be skipped based on checkpoint."""
        if checkpoint_date is None or checkpoint_period is None:
            return False
        
        if settlement_date < checkpoint_date:
            return True
        elif settlement_date == checkpoint_date and settlement_period <= checkpoint_period:
            return True
        
        return False
    
    async def retrieve_all_data(self) -> Tuple[pl.DataFrame, bool]:
        """Retrieve all generation data for 2024 using concurrent requests."""
        dates = self.generate_date_range()
        all_data = []
        
        # Load checkpoint
        checkpoint_date, checkpoint_period, failed_requests = self.load_checkpoint()
        
        # Calculate total requests for progress tracking
        total_requests = sum(len(self.get_settlement_periods(d)) for d in dates)
        logger.info(f"Starting data retrieval for {len(dates)} days, {total_requests} API requests")
        
        # Track all requests and failures
        all_requests_succeeded = True
        completed_requests = 0
        current_failed_requests = set()
        
        # Concurrency control
        semaphore = asyncio.Semaphore(self.max_concurrent_requests)
        
        async def fetch_with_semaphore(settlement_date: date, settlement_period: int) -> Tuple[Optional[List[Dict]], str, int]:
            """Wrapper function to manage concurrency and rate limiting."""
            nonlocal completed_requests, all_requests_succeeded
            
            async with semaphore:
                # Skip if already processed based on checkpoint
                if self.should_skip_request(settlement_date, settlement_period, 
                                          checkpoint_date, checkpoint_period):
                    completed_requests += 1
                    return [], settlement_date.strftime('%Y-%m-%d'), settlement_period
                
                # Make API request
                data = await self.make_api_request(settlement_date, settlement_period)
                
                if data is None:
                    all_requests_succeeded = False
                    current_failed_requests.add((settlement_date.strftime('%Y-%m-%d'), settlement_period))
                    data = []
                
                completed_requests += 1
                
                # Rate limiting after request
                await asyncio.sleep(self.request_delay)
                
                return data, settlement_date.strftime('%Y-%m-%d'), settlement_period
        
        # Create all tasks
        tasks = []
        for settlement_date in dates:
            settlement_periods = self.get_settlement_periods(settlement_date)
            for settlement_period in settlement_periods:
                # Always include failed requests from previous runs
                date_str = settlement_date.strftime('%Y-%m-%d')
                if (date_str, settlement_period) in failed_requests:
                    logger.info(f"Retrying previously failed request: {date_str} SP{settlement_period}")
                
                tasks.append(fetch_with_semaphore(settlement_date, settlement_period))
        
        logger.info(f"Created {len(tasks)} concurrent tasks")
        
        # Execute tasks with progress tracking
        async with httpx.AsyncClient(timeout=30.0) as client:
            # Store client reference for make_api_request
            self.client = client
            
            for i, task in enumerate(asyncio.as_completed(tasks)):
                try:
                    data, date_str, period = await task
                    
                    if data:
                        all_data.extend(data)
                    
                    # Save checkpoint every 100 requests
                    if i % 100 == 0 and i > 0:
                        # Use the most recent date/period for checkpoint
                        latest_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                        self.save_checkpoint(latest_date, period, current_failed_requests)
                    
                    # Progress logging every 1000 requests
                    if i % 1000 == 0 or i == len(tasks) - 1:
                        progress = ((i + 1) / len(tasks)) * 100
                        logger.info(f"Progress: {i + 1}/{len(tasks)} ({progress:.1f}%)")
                        
                except Exception as e:
                    logger.error(f"Unexpected error in task execution: {e}")
                    all_requests_succeeded = False
        
        logger.info(f"Data retrieval complete. Retrieved {len(all_data)} records")
        
        if not all_requests_succeeded:
            logger.warning(f"Some requests failed. {len(current_failed_requests)} failed requests logged.")
        
        # Create Polars DataFrame
        if all_data:
            df = pl.DataFrame(all_data)
            
            # Ensure proper data types - let Polars infer datetime format
            df = df.with_columns([
                pl.col("settlementDate").str.to_date("%Y-%m-%d"),
                pl.col("settlementPeriod").cast(pl.Int32),
                pl.col("quantity").cast(pl.Float64),
                pl.col("halfHourEndTime").str.to_datetime()  # Let Polars infer format
            ])
            
            # Sort by date, period, and unit for consistent ordering
            df = df.sort(["settlementDate", "settlementPeriod", "bmUnit"])
            
            logger.info(f"Created DataFrame with {df.height} rows and {df.width} columns")
            return df, all_requests_succeeded
        else:
            logger.warning("No data retrieved - creating empty DataFrame")
            return pl.DataFrame(), all_requests_succeeded
    
    def save_to_csv(self, df: pl.DataFrame, all_succeeded: bool):
        """Save DataFrame to CSV file."""
        try:
            df.write_csv(self.output_file)
            logger.info(f"Data saved to {self.output_file}")
            
            # Only clean up checkpoint file on 100% success
            if all_succeeded:
                checkpoint_path = Path(self.checkpoint_file)
                if checkpoint_path.exists():
                    checkpoint_path.unlink()
                    logger.info("Checkpoint file removed after successful completion")
            else:
                logger.warning("Data saved, but some requests failed. Checkpoint file retained for retry.")
                
        except Exception as e:
            logger.error(f"Error saving to CSV: {e}")
            raise
    
    async def run(self):
        """Main execution method."""
        logger.info("Starting Seabank generation data retrieval")
        start_time = time.time()
        
        try:
            df, all_succeeded = await self.retrieve_all_data()
            
            if not df.is_empty():
                self.save_to_csv(df, all_succeeded)
                
                # Log summary statistics
                logger.info(f"Summary statistics:")
                logger.info(f"  Total records: {df.height}")
                logger.info(f"  Date range: {df.select(pl.col('settlementDate').min())[0,0]} to {df.select(pl.col('settlementDate').max())[0,0]}")
                logger.info(f"  Units: {df.select(pl.col('bmUnit').unique())[0,0]}")
                total_generation = df.select(pl.col('quantity').sum())[0,0] or 0
                logger.info(f"  Total generation (MWh): {total_generation:.2f}")
                
            elapsed_time = time.time() - start_time
            logger.info(f"Script completed in {elapsed_time:.2f} seconds")
            
        except Exception as e:
            logger.error(f"Script failed with error: {e}")
            raise


async def main():
    """Main entry point."""
    retriever = SeabankDataRetriever()
    await retriever.run()


if __name__ == "__main__":
    asyncio.run(main())
