"""
Main initialization script for Yelp Data Warehouse
This script orchestrates the initialization of all database systems.
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
    
    # Wait for databases to be ready
    wait_for_databases()
    
    # Check and initialize each database
    mysql_initialized = check_and_initialize_mysql()
    mongodb_initialized = check_and_initialize_mongodb()
    neo4j_initialized = check_and_initialize_neo4j()
    
    # Summary
    print("\nInitialization Summary:")
    print(f"MySQL:   {'✓ Initialized' if mysql_initialized else '✓ Already had data'}")
    print(f"MongoDB: {'✓ Initialized' if mongodb_initialized else '✓ Already had data'}")
    print(f"Neo4j:   {'✓ Initialized' if neo4j_initialized else '✓ Already had data'}")
    
    # Wait for 10 seconds to ensure all data is committed
    print("\nWaiting 10 seconds for databases to finalize...")
    time.sleep(10)
    
    print("\nData Initializer completed successfully.")

if __name__ == "__main__":
    main()