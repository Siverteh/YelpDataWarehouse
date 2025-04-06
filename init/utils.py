"""
Utility functions for Yelp Data Warehouse initialization
"""

import os
import time
import pymysql
import pymongo
from neo4j import GraphDatabase

def get_config():
    """Get configuration from environment variables"""
    config = {
        'mysql': {
            'host': os.environ.get('MYSQL_HOST', 'mysql'),
            'port': int(os.environ.get('MYSQL_PORT', 3306)),
            'user': os.environ.get('MYSQL_USER', 'root'),
            'password': os.environ.get('MYSQL_PASSWORD', 'user'),
            'db': os.environ.get('MYSQL_DATABASE', 'yelp_dw'),
            'charset': 'utf8mb4'
        },
        'mongodb': {
            'host': os.environ.get('MONGO_HOST', 'mongodb'),
            'port': int(os.environ.get('MONGO_PORT', 27017)),
            'user': os.environ.get('MONGO_USER', 'user'),
            'password': os.environ.get('MONGO_PASSWORD', 'user'),
            'uri': f"mongodb://{os.environ.get('MONGO_USER', 'user')}:{os.environ.get('MONGO_PASSWORD', 'user')}@{os.environ.get('MONGO_HOST', 'mongodb')}:{os.environ.get('MONGO_PORT', 27017)}/"
        },
        'neo4j': {
            'host': os.environ.get('NEO4J_HOST', 'neo4j'),
            'port': int(os.environ.get('NEO4J_PORT', 7687)),
            'user': os.environ.get('NEO4J_USER', 'neo4j'),
            'password': os.environ.get('NEO4J_PASSWORD', 'user'),
            'uri': f"bolt://{os.environ.get('NEO4J_HOST', 'neo4j')}:{os.environ.get('NEO4J_PORT', 7687)}"
        },
        'data': {
            'dir': 'data',
            'business_file': os.path.join('data', 'yelp_academic_dataset_business.json'),
            'user_file': os.path.join('data', 'yelp_academic_dataset_user.json'),
            'review_file': os.path.join('data', 'yelp_academic_dataset_review.json'),
            'checkin_file': os.path.join('data', 'yelp_academic_dataset_checkin.json'),
            'tip_file': os.path.join('data', 'yelp_academic_dataset_tip.json')
        }
    }
    return config

def wait_for_databases():
    """Wait for all databases to be ready"""
    print("Waiting for databases to be ready...")
    
    config = get_config()
    # Wait longer and try more times
    max_attempts = 60  # Increase from 30 to 60 attempts
    
    # Check MySQL
    mysql_ready = False
    for i in range(max_attempts):
        try:
            conn = pymysql.connect(**config['mysql'])
            conn.close()
            mysql_ready = True
            print(f"MySQL is ready (attempt {i+1}/{max_attempts})")
            break
        except Exception as e:
            print(f"MySQL not ready: {e} (attempt {i+1}/{max_attempts})")
            time.sleep(2)  # Wait longer between attempts
    
    # Check MongoDB
    mongo_ready = False
    for i in range(max_attempts):
        try:
            client = pymongo.MongoClient(config['mongodb']['uri'], serverSelectionTimeoutMS=5000)
            client.admin.command('ping')
            client.close()
            mongo_ready = True
            print(f"MongoDB is ready (attempt {i+1}/{max_attempts})")
            break
        except Exception as e:
            print(f"MongoDB not ready: {e} (attempt {i+1}/{max_attempts})")
            time.sleep(2)
    
    # Check Neo4j
    neo4j_ready = False
    for i in range(max_attempts):
        try:
            driver = GraphDatabase.driver(
                config['neo4j']['uri'], 
                auth=(config['neo4j']['user'], config['neo4j']['password'])
            )
            with driver.session() as session:
                session.run("RETURN 1")
            driver.close()
            neo4j_ready = True
            print(f"Neo4j is ready (attempt {i+1}/{max_attempts})")
            break
        except Exception as e:
            print(f"Neo4j not ready: {e} (attempt {i+1}/{max_attempts})")
            time.sleep(2)
    
    if not (mysql_ready and mongo_ready and neo4j_ready):
        if not mysql_ready:
            print("WARNING: MySQL is not ready. Initialization may fail.")
        if not mongo_ready:
            print("WARNING: MongoDB is not ready. Initialization may fail.")
        if not neo4j_ready:
            print("WARNING: Neo4j is not ready. Initialization may fail.")
    else:
        print("All databases are ready!")
    
    # Wait an additional 10 seconds to ensure services are fully initialized
    print("Waiting 10 more seconds for services to stabilize...")
    time.sleep(10)
    
    return mysql_ready and mongo_ready and neo4j_ready