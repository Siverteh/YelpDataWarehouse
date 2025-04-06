"""
Main initialization script for Yelp Data Warehouse
This script orchestrates the initialization of all database systems with data limits.
"""

import os
import time
import json
from datetime import datetime

# Import database-specific loaders
from mysql_loader import check_and_initialize_mysql
from mongodb_loader import check_and_initialize_mongodb
from neo4j_loader import check_and_initialize_neo4j
from utils import wait_for_databases, get_config

def main():
    """Main execution function for data initialization"""
    print("Starting Yelp Data Warehouse Initializer")
    
    # Data subsetting configuration
    data_limits = {
        'business_limit': 10000,  # Load first 10,000 businesses
        'review_limit': 100000,    # Load up to 50,000 reviews
        'user_limit': 50000,      # Load up to 10,000 users
        'filter_by_business': True  # Filter related data by loaded businesses
    }
    
    # Wait for databases to be ready
    wait_for_databases()
    
    # Check and initialize each database
    mysql_initialized = check_and_initialize_mysql(data_limits)
    mongodb_initialized = check_and_initialize_mongodb(data_limits)
    neo4j_initialized = check_and_initialize_neo4j(data_limits)
    
    # Summary
    print("\nInitialization Summary:")
    print(f"MySQL:   {'✓ Initialized' if mysql_initialized else '✓ Already had data'}")
    print(f"MongoDB: {'✓ Initialized' if mongodb_initialized else '✓ Already had data'}")
    print(f"Neo4j:   {'✓ Initialized' if neo4j_initialized else '✓ Already had data'}")
    
    # Wait for 10 seconds to ensure all data is committed
    print("\nWaiting 10 seconds for databases to finalize...")
    time.sleep(10)
    
    print("\nData Initializer completed successfully.")
    print("Note: Only a subset of data was loaded. The remaining data can be used for Kafka streaming.")

if __name__ == "__main__":
    main()