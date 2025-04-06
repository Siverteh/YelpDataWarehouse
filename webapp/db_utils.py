"""
Database utility functions for connecting to various database systems
"""
import os
import time
import logging
import pymysql
import pymongo
from neo4j import GraphDatabase
from datetime import datetime, date
import decimal
import json

# Set up logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('webapp')

# Get environment variables with defaults
MYSQL_HOST = os.environ.get('MYSQL_HOST', 'mysql')
MYSQL_PORT = int(os.environ.get('MYSQL_PORT', 3306))
MYSQL_USER = os.environ.get('MYSQL_USER', 'root')
MYSQL_PASSWORD = os.environ.get('MYSQL_PASSWORD', 'user')
MYSQL_DATABASE = os.environ.get('MYSQL_DATABASE', 'yelp_dw')

MONGO_HOST = os.environ.get('MONGO_HOST', 'mongodb')
MONGO_PORT = int(os.environ.get('MONGO_PORT', 27017))
MONGO_USER = os.environ.get('MONGO_USER', 'user')
MONGO_PASSWORD = os.environ.get('MONGO_PASSWORD', 'user')

NEO4J_HOST = os.environ.get('NEO4J_HOST', 'neo4j')
NEO4J_PORT = int(os.environ.get('NEO4J_PORT', 7687))
NEO4J_USER = os.environ.get('NEO4J_USER', 'neo4j')
NEO4J_PASSWORD = os.environ.get('NEO4J_PASSWORD', 'user')

# Helper function to convert Decimal/Date types for JSON serialization
def json_serialize(obj):
    """Handle non-serializable types."""
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    elif isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

# Database connections with retry
def get_mysql_connection(max_retries=5, retry_delay=2):
    """Get MySQL connection with retry logic"""
    for attempt in range(max_retries):
        try:
            connection = pymysql.connect(
                host=MYSQL_HOST,
                port=MYSQL_PORT,
                user=MYSQL_USER,
                password=MYSQL_PASSWORD,
                db=MYSQL_DATABASE,
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
            )
            logger.info("Successfully connected to MySQL")
            return connection
        except Exception as e:
            logger.warning(f"MySQL connection attempt {attempt+1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                logger.error(f"Failed to connect to MySQL after {max_retries} attempts")
                # Return dummy connection/data for development
                if os.environ.get('FLASK_ENV') == 'development' or os.environ.get('FLASK_DEBUG') == '1':
                    logger.info("Returning None for development")
                    return None
                return None

def get_mongodb_connection(max_retries=5, retry_delay=2):
    """Get MongoDB connection with retry logic"""
    for attempt in range(max_retries):
        try:
            connection_string = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/"
            client = pymongo.MongoClient(
                connection_string,
                serverSelectionTimeoutMS=5000  # 5 second timeout
            )
            # Force a connection to verify it works
            client.admin.command('ping')
            logger.info("Successfully connected to MongoDB")
            return client
        except Exception as e:
            logger.warning(f"MongoDB connection attempt {attempt+1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                logger.error(f"Failed to connect to MongoDB after {max_retries} attempts")
                return None

def get_neo4j_driver(max_retries=5, retry_delay=2):
    """Get Neo4j driver with retry logic"""
    for attempt in range(max_retries):
        try:
            driver = GraphDatabase.driver(
                f"bolt://{NEO4J_HOST}:{NEO4J_PORT}",
                auth=(NEO4J_USER, NEO4J_PASSWORD)
            )
            # Test connection
            with driver.session() as session:
                session.run("RETURN 1")
            logger.info("Successfully connected to Neo4j")
            return driver
        except Exception as e:
            logger.warning(f"Neo4j connection attempt {attempt+1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                logger.error(f"Failed to connect to Neo4j after {max_retries} attempts")
                return None