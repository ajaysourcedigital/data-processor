#!/usr/bin/env python3
"""
Data Processing CronJob
A comprehensive example of a Dockerfile-based cronjob that processes data
"""

import os
import sys
import json
import time
import logging
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from typing import Dict, List, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

class DataProcessor:
    def __init__(self):
        self.cronjob_name = os.getenv('CRONJOB_NAME', 'dockerfile-data-processor')
        self.execution_uuid = os.getenv('CRONJOB_EXECUTION_UUID', 'local-run')
        self.manual_trigger = os.getenv('MANUAL_TRIGGER', 'false').lower() == 'true'
        self.start_time = datetime.now(timezone.utc)
        
        logger.info("🚀 Data Processing CronJob Started!")
        logger.info(f"📋 CronJob Details:")
        logger.info(f"   Name: {self.cronjob_name}")
        logger.info(f"   Execution UUID: {self.execution_uuid}")
        logger.info(f"   Manual Trigger: {self.manual_trigger}")
        logger.info(f"   Start Time: {self.start_time.isoformat()}")

    def fetch_sample_data(self) -> List[Dict[str, Any]]:
        """Fetch sample data from a public API"""
        logger.info("📡 Fetching sample data from API...")
        
        try:
            # Using JSONPlaceholder for demo data
            response = requests.get(
                'https://jsonplaceholder.typicode.com/posts',
                timeout=30
            )
            response.raise_for_status()
            
            data = response.json()[:10]  # Limit to first 10 posts
            logger.info(f"✅ Successfully fetched {len(data)} records")
            return data
            
        except requests.RequestException as e:
            logger.error(f"❌ Failed to fetch data: {e}")
            # Return mock data as fallback
            return self._generate_mock_data()

    def _generate_mock_data(self) -> List[Dict[str, Any]]:
        """Generate mock data as fallback"""
        logger.info("🔄 Generating mock data as fallback...")
        
        mock_data = []
        for i in range(10):
            mock_data.append({
                'id': i + 1,
                'title': f'Sample Data Entry {i + 1}',
                'body': f'This is mock data entry number {i + 1}',
                'userId': (i % 3) + 1
            })
        
        return mock_data

    def process_data(self, raw_data: List[Dict[str, Any]]) -> pd.DataFrame:
        """Process the raw data using pandas"""
        logger.info("⚙️ Processing data with pandas...")
        
        # Convert to DataFrame
        df = pd.DataFrame(raw_data)
        
        # Add processing timestamp
        df['processed_at'] = datetime.now(timezone.utc).isoformat()
        
        # Add some computed fields
        df['title_length'] = df['title'].str.len()
        df['body_word_count'] = df['body'].str.split().str.len()
        
        # Group by userId and add statistics
        user_stats = df.groupby('userId').agg({
            'id': 'count',
            'title_length': 'mean',
            'body_word_count': 'sum'
        }).round(2)
        
        logger.info("📊 Data processing statistics:")
        logger.info(f"   Total records: {len(df)}")
        logger.info(f"   Unique users: {df['userId'].nunique()}")
        logger.info(f"   Average title length: {df['title_length'].mean():.2f}")
        logger.info(f"   Total words: {df['body_word_count'].sum()}")
        
        # Log user statistics
        logger.info("👥 User statistics:")
        for user_id, stats in user_stats.iterrows():
            logger.info(f"   User {user_id}: {stats['id']} posts, "
                       f"avg title length: {stats['title_length']}, "
                       f"total words: {stats['body_word_count']}")
        
        return df

    def perform_analysis(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Perform data analysis"""
        logger.info("🔍 Performing data analysis...")
        
        analysis = {
            'execution_info': {
                'cronjob_name': self.cronjob_name,
                'execution_uuid': self.execution_uuid,
                'manual_trigger': self.manual_trigger,
                'processed_at': datetime.now(timezone.utc).isoformat()
            },
            'data_summary': {
                'total_records': len(df),
                'unique_users': int(df['userId'].nunique()),
                'avg_title_length': float(df['title_length'].mean()),
                'total_words': int(df['body_word_count'].sum()),
                'min_title_length': int(df['title_length'].min()),
                'max_title_length': int(df['title_length'].max())
            },
            'user_breakdown': df.groupby('userId').agg({
                'id': 'count',
                'title_length': 'mean',
                'body_word_count': 'sum'
            }).round(2).to_dict('index')
        }
        
        return analysis

    def save_results(self, df: pd.DataFrame, analysis: Dict[str, Any]):
        """Save processing results"""
        logger.info("💾 Saving processing results...")
        
        # Create output directory
        output_dir = '/tmp/cronjob_output'
        os.makedirs(output_dir, exist_ok=True)
        
        # Save DataFrame as CSV
        csv_path = f"{output_dir}/processed_data_{self.execution_uuid}.csv"
        df.to_csv(csv_path, index=False)
        logger.info(f"📄 Saved processed data to: {csv_path}")
        
        # Save analysis as JSON
        json_path = f"{output_dir}/analysis_{self.execution_uuid}.json"
        with open(json_path, 'w') as f:
            json.dump(analysis, f, indent=2)
        logger.info(f"📊 Saved analysis to: {json_path}")
        
        # Log file sizes
        csv_size = os.path.getsize(csv_path)
        json_size = os.path.getsize(json_path)
        logger.info(f"📏 File sizes: CSV={csv_size} bytes, JSON={json_size} bytes")

    def simulate_work(self):
        """Simulate some processing work"""
        logger.info("⏳ Simulating additional processing work...")
        
        # Simulate CPU-intensive work
        for i in range(3):
            logger.info(f"   Processing batch {i + 1}/3...")
            
            # Generate some random data and perform operations
            data = np.random.rand(1000, 100)
            result = np.mean(data, axis=1)
            
            # Simulate I/O wait
            time.sleep(1)
            
            logger.info(f"   Batch {i + 1} completed. Mean result: {np.mean(result):.4f}")

    def run(self):
        """Main execution method"""
        try:
            logger.info("🎯 Starting data processing pipeline...")
            
            # Step 1: Fetch data
            raw_data = self.fetch_sample_data()
            
            # Step 2: Process data
            processed_df = self.process_data(raw_data)
            
            # Step 3: Perform analysis
            analysis = self.perform_analysis(processed_df)
            
            # Step 4: Save results
            self.save_results(processed_df, analysis)
            
            # Step 5: Simulate additional work
            self.simulate_work()
            
            # Calculate execution time
            end_time = datetime.now(timezone.utc)
            execution_time = (end_time - self.start_time).total_seconds()
            
            logger.info("✅ Data processing completed successfully!")
            logger.info(f"⏱️  Total execution time: {execution_time:.2f} seconds")
            logger.info("🎉 CronJob finished successfully!")
            
            return 0
            
        except Exception as e:
            logger.error(f"❌ CronJob failed with error: {e}")
            logger.exception("Full error traceback:")
            return 1

def main():
    """Main entry point"""
    processor = DataProcessor()
    exit_code = processor.run()
    sys.exit(exit_code)

if __name__ == "__main__":
    main()
