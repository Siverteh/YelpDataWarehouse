#!/usr/bin/env python3
"""
Yelp Data Loader Script
Loads data from Yelp dataset files into MySQL, MongoDB, and Neo4j databases.

Usage:
  python yelp_data_loader.py [--limit N]

Options:
  --limit N    Load only the first N records from each file (default: 1000)
"""

import os
import sys
import json
import time
import argparse
import pymysql
import pymongo
from neo4j import GraphDatabase
from datetime import datetime
from tqdm import tqdm

# Parse command line arguments
parser = argparse.ArgumentParser(description='Load Yelp dataset into databases')
parser.add_argument('--limit', type=int, default=1000, help='Limit number of records to load')
args = parser.parse_args()

# Database connection parameters
MYSQL_CONFIG = {
    'host': 'localhost',  # Use localhost when running outside Docker
    'port': 3306,
    'user': 'root',
    'password': 'user',
    'db': 'yelp_dw',
    'charset': 'utf8mb4'
}

MONGODB_URI = "mongodb://user:user@localhost:27017/"
NEO4J_URI = "bolt://localhost:7687"
NEO4J_AUTH = ("neo4j", "user")

# Paths to dataset files
BUSINESS_FILE = 'data/yelp_academic_dataset_business.json'
USER_FILE = 'data/yelp_academic_dataset_user.json'
REVIEW_FILE = 'data/yelp_academic_dataset_review.json'
CHECKIN_FILE = 'data/yelp_academic_dataset_checkin.json'
TIP_FILE = 'data/yelp_academic_dataset_tip.json'

# Verify files exist
for file_path in [BUSINESS_FILE, USER_FILE, REVIEW_FILE, CHECKIN_FILE, TIP_FILE]:
    if not os.path.isfile(file_path):
        print(f"Error: File not found: {file_path}")
        print("Make sure the Yelp dataset files are in the data directory.")
        sys.exit(1)

print("Yelp dataset files found. Starting data loading process.")

def wait_for_databases():
    """Wait for all databases to be ready"""
    print("Checking database connections...")
    
    # Check MySQL
    mysql_ready = False
    retries = 0
    while not mysql_ready and retries < 5:
        try:
            conn = pymysql.connect(**MYSQL_CONFIG)
            conn.close()
            mysql_ready = True
            print("MySQL connection successful")
        except Exception as e:
            print(f"MySQL connection failed: {e}")
            retries += 1
            print(f"Retrying in 5 seconds... (attempt {retries}/5)")
            time.sleep(5)
    
    # Check MongoDB
    mongo_ready = False
    retries = 0
    while not mongo_ready and retries < 5:
        try:
            client = pymongo.MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
            client.admin.command('ping')
            client.close()
            mongo_ready = True
            print("MongoDB connection successful")
        except Exception as e:
            print(f"MongoDB connection failed: {e}")
            retries += 1
            print(f"Retrying in 5 seconds... (attempt {retries}/5)")
            time.sleep(5)
    
    # Check Neo4j
    neo4j_ready = False
    retries = 0
    while not neo4j_ready and retries < 5:
        try:
            driver = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)
            with driver.session() as session:
                session.run("RETURN 1")
            driver.close()
            neo4j_ready = True
            print("Neo4j connection successful")
        except Exception as e:
            print(f"Neo4j connection failed: {e}")
            retries += 1
            print(f"Retrying in 5 seconds... (attempt {retries}/5)")
            time.sleep(5)
    
    if not (mysql_ready and mongo_ready and neo4j_ready):
        print("Error: Failed to connect to one or more databases")
        return False
    
    print("All database connections successful")
    return True

def setup_mysql_schema():
    """Set up MySQL schema"""
    print("\nSetting up MySQL schema...")
    conn = pymysql.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()
    
    # Create dimension tables
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_time (
            time_id INT PRIMARY KEY AUTO_INCREMENT,
            date_actual DATE NOT NULL,
            day_of_week VARCHAR(10) NOT NULL,
            day_of_month INT NOT NULL,
            month_actual INT NOT NULL,
            month_name VARCHAR(10) NOT NULL,
            quarter_actual INT NOT NULL,
            year_actual INT NOT NULL,
            UNIQUE KEY uk_date (date_actual)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_location (
            location_id INT PRIMARY KEY AUTO_INCREMENT,
            city VARCHAR(255),
            state VARCHAR(255),
            postal_code VARCHAR(20),
            latitude DECIMAL(9,6),
            longitude DECIMAL(9,6),
            UNIQUE KEY uk_location (city(250), state(250), postal_code)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_business (
            business_id VARCHAR(255) PRIMARY KEY,
            business_name VARCHAR(255),
            location_id INT,
            stars DECIMAL(2,1),
            review_count INT,
            is_open BOOLEAN,
            FOREIGN KEY (location_id) REFERENCES dim_location(location_id)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_user (
            user_id VARCHAR(255) PRIMARY KEY,
            name VARCHAR(255),
            review_count INT,
            yelping_since DATE,
            fans INT,
            average_stars DECIMAL(3,2)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_category (
            category_id INT PRIMARY KEY AUTO_INCREMENT,
            category_name VARCHAR(255),
            UNIQUE KEY uk_category (category_name(250))
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS business_category (
            business_id VARCHAR(255),
            category_id INT,
            PRIMARY KEY (business_id, category_id),
            FOREIGN KEY (business_id) REFERENCES dim_business(business_id),
            FOREIGN KEY (category_id) REFERENCES dim_category(category_id)
        )
    """)
    
    # Create fact tables
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fact_review (
            review_id VARCHAR(255) PRIMARY KEY,
            business_id VARCHAR(255),
            user_id VARCHAR(255),
            time_id INT,
            stars INT,
            useful_votes INT,
            funny_votes INT,
            cool_votes INT,
            FOREIGN KEY (business_id) REFERENCES dim_business(business_id),
            FOREIGN KEY (user_id) REFERENCES dim_user(user_id),
            FOREIGN KEY (time_id) REFERENCES dim_time(time_id)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fact_checkin (
            checkin_id INT PRIMARY KEY AUTO_INCREMENT,
            business_id VARCHAR(255),
            time_id INT,
            checkin_count INT,
            FOREIGN KEY (business_id) REFERENCES dim_business(business_id),
            FOREIGN KEY (time_id) REFERENCES dim_time(time_id)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fact_tip (
            tip_id INT PRIMARY KEY AUTO_INCREMENT,
            business_id VARCHAR(255),
            user_id VARCHAR(255),
            time_id INT,
            compliment_count INT,
            FOREIGN KEY (business_id) REFERENCES dim_business(business_id),
            FOREIGN KEY (user_id) REFERENCES dim_user(user_id),
            FOREIGN KEY (time_id) REFERENCES dim_time(time_id)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS summary_business_performance (
            business_id VARCHAR(255) PRIMARY KEY,
            total_reviews INT,
            avg_rating DECIMAL(2,1),
            total_checkins INT,
            FOREIGN KEY (business_id) REFERENCES dim_business(business_id)
        )
    """)
    
    # Create time dimension
    print("Creating time dimension...")
    years = range(2004, 2022)
    months = range(1, 13)
    
    for year in years:
        for month in months:
            # Determine days in month
            if month in [4, 6, 9, 11]:
                days = 30
            elif month == 2:
                if (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0):
                    days = 29
                else:
                    days = 28
            else:
                days = 31
            
            for day in range(1, days + 1):
                date_str = f"{year}-{month:02d}-{day:02d}"
                
                # Get day of week and month name
                try:
                    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
                    day_of_week = date_obj.strftime("%A")
                    month_name = date_obj.strftime("%B")
                    quarter = ((month - 1) // 3) + 1
                    
                    cursor.execute("""
                        INSERT IGNORE INTO dim_time 
                        (date_actual, day_of_week, day_of_month, month_actual, month_name, quarter_actual, year_actual) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (
                        date_str, day_of_week, day, month, month_name, quarter, year
                    ))
                except Exception as e:
                    print(f"Error creating time dimension for {date_str}: {e}")
    
    conn.commit()
    cursor.close()
    conn.close()
    print("MySQL schema setup complete")

def setup_mongodb_schema():
    """Set up MongoDB schema"""
    print("\nSetting up MongoDB schema...")
    client = pymongo.MongoClient(MONGODB_URI)
    db = client.yelp_db
    
    # Drop existing collections
    for collection in db.list_collection_names():
        db[collection].drop()
    
    # Create collections
    db.create_collection("businesses")
    db.create_collection("users")
    db.create_collection("reviews")
    db.create_collection("checkins")
    db.create_collection("tips")
    
    # Create indexes
    db.businesses.create_index([("business_id", pymongo.ASCENDING)], unique=True)
    db.businesses.create_index([("categories", pymongo.ASCENDING)])
    db.users.create_index([("user_id", pymongo.ASCENDING)], unique=True)
    db.reviews.create_index([("review_id", pymongo.ASCENDING)], unique=True)
    db.reviews.create_index([("business_id", pymongo.ASCENDING)])
    db.reviews.create_index([("user_id", pymongo.ASCENDING)])
    db.checkins.create_index([("business_id", pymongo.ASCENDING)])
    db.tips.create_index([("business_id", pymongo.ASCENDING)])
    db.tips.create_index([("user_id", pymongo.ASCENDING)])
    
    client.close()
    print("MongoDB schema setup complete")

def setup_neo4j_schema():
    """Set up Neo4j schema"""
    print("\nSetting up Neo4j schema...")
    driver = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)
    with driver.session() as session:
        # Clear existing data
        session.run("MATCH (n) DETACH DELETE n")
        
        # Create constraints
        constraints = [
            "CREATE CONSTRAINT business_id IF NOT EXISTS ON (b:Business) ASSERT b.business_id IS UNIQUE",
            "CREATE CONSTRAINT user_id IF NOT EXISTS ON (u:User) ASSERT u.user_id IS UNIQUE",
            "CREATE CONSTRAINT review_id IF NOT EXISTS ON (r:Review) ASSERT r.review_id IS UNIQUE",
            "CREATE CONSTRAINT category_name IF NOT EXISTS ON (c:Category) ASSERT c.name IS UNIQUE",
            "CREATE CONSTRAINT location_id IF NOT EXISTS ON (l:Location) ASSERT (l.city, l.state, l.postal_code) IS NODE KEY"
        ]
        
        for constraint in constraints:
            try:
                session.run(constraint)
            except Exception as e:
                # Some Neo4j versions handle constraints differently
                print(f"Note: Constraint creation modified: {e}")
                try:
                    if "business_id" in constraint:
                        session.run("CREATE CONSTRAINT ON (b:Business) ASSERT b.business_id IS UNIQUE")
                    elif "user_id" in constraint:
                        session.run("CREATE CONSTRAINT ON (u:User) ASSERT u.user_id IS UNIQUE")
                    elif "review_id" in constraint:
                        session.run("CREATE CONSTRAINT ON (r:Review) ASSERT r.review_id IS UNIQUE")
                    elif "category_name" in constraint:
                        session.run("CREATE CONSTRAINT ON (c:Category) ASSERT c.name IS UNIQUE")
                except Exception as e2:
                    print(f"Warning: Could not create constraint: {e2}")
        
        # Create indexes
        indexes = [
            "CREATE INDEX business_stars IF NOT EXISTS FOR (b:Business) ON (b.stars)",
            "CREATE INDEX business_name IF NOT EXISTS FOR (b:Business) ON (b.name)",
            "CREATE INDEX location_city IF NOT EXISTS FOR (l:Location) ON (l.city)",
            "CREATE INDEX user_name IF NOT EXISTS FOR (u:User) ON (u.name)",
            "CREATE INDEX review_stars IF NOT EXISTS FOR (r:Review) ON (r.stars)"
        ]
        
        for index in indexes:
            try:
                session.run(index)
            except Exception as e:
                # Some Neo4j versions handle indexes differently
                print(f"Note: Index creation modified: {e}")
                try:
                    if "business_stars" in index:
                        session.run("CREATE INDEX ON :Business(stars)")
                    elif "business_name" in index:
                        session.run("CREATE INDEX ON :Business(name)")
                    elif "location_city" in index:
                        session.run("CREATE INDEX ON :Location(city)")
                    elif "user_name" in index:
                        session.run("CREATE INDEX ON :User(name)")
                    elif "review_stars" in index:
                        session.run("CREATE INDEX ON :Review(stars)")
                except Exception as e2:
                    print(f"Warning: Could not create index: {e2}")
    
    driver.close()
    print("Neo4j schema setup complete")

def load_businesses():
    """Load businesses into all databases"""
    print("\nLoading businesses into databases...")
    
    # Load into MySQL
    mysql_conn = pymysql.connect(**MYSQL_CONFIG)
    mysql_cursor = mysql_conn.cursor()
    
    # Load into MongoDB
    mongo_client = pymongo.MongoClient(MONGODB_URI)
    mongo_db = mongo_client.yelp_db
    
    # Load into Neo4j
    neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)
    
    # Location cache to avoid duplicates
    location_cache = {}
    
    # Category cache to avoid duplicates
    category_cache = {}
    
    # Load businesses
    count = 0
    mongo_batch = []
    
    with open(BUSINESS_FILE, 'r', encoding='utf-8') as f:
        # Use tqdm for progress bar
        for line in tqdm(f, desc="Businesses", unit="business"):
            if count >= args.limit:
                break
            
            business = json.loads(line)
            business_id = business['business_id']
            
            # Process for MySQL
            try:
                # Handle location
                city = business.get('city', '')
                state = business.get('state', '')
                postal_code = business.get('postal_code', '')
                
                location_key = f"{city}|{state}|{postal_code}"
                
                if location_key not in location_cache:
                    # Insert location if new
                    mysql_cursor.execute("""
                        INSERT IGNORE INTO dim_location (city, state, postal_code, latitude, longitude)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (
                        city, state, postal_code,
                        business.get('latitude'),
                        business.get('longitude')
                    ))
                    
                    # Get location ID
                    mysql_cursor.execute("""
                        SELECT location_id FROM dim_location
                        WHERE city = %s AND state = %s AND postal_code = %s
                    """, (city, state, postal_code))
                    
                    result = mysql_cursor.fetchone()
                    if result:
                        location_id = result[0]
                        location_cache[location_key] = location_id
                    else:
                        location_id = None
                else:
                    location_id = location_cache[location_key]
                
                # Insert business
                if location_id:
                    mysql_cursor.execute("""
                        INSERT IGNORE INTO dim_business 
                        (business_id, business_name, location_id, stars, review_count, is_open)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                        business_id,
                        business.get('name', ''),
                        location_id,
                        business.get('stars'),
                        business.get('review_count', 0),
                        business.get('is_open', 0) == 1
                    ))
                
                # Process categories
                if business.get('categories'):
                    categories = [c.strip() for c in business.get('categories', '').split(',')]
                    
                    for category in categories:
                        if not category:
                            continue
                        
                        if category not in category_cache:
                            # Insert category if new
                            mysql_cursor.execute("""
                                INSERT IGNORE INTO dim_category (category_name)
                                VALUES (%s)
                            """, (category,))
                            
                            # Get category ID
                            mysql_cursor.execute("""
                                SELECT category_id FROM dim_category
                                WHERE category_name = %s
                            """, (category,))
                            
                            result = mysql_cursor.fetchone()
                            if result:
                                category_id = result[0]
                                category_cache[category] = category_id
                            else:
                                continue
                        else:
                            category_id = category_cache[category]
                        
                        # Link business to category
                        mysql_cursor.execute("""
                            INSERT IGNORE INTO business_category (business_id, category_id)
                            VALUES (%s, %s)
                        """, (business_id, category_id))
            
            except Exception as e:
                print(f"Error loading business {business_id} into MySQL: {e}")
            
            # Process for MongoDB
            try:
                # Convert categories string to array
                if business.get('categories'):
                    business['categories'] = [c.strip() for c in business.get('categories', '').split(',')]
                
                # Restructure location into a subdocument
                business['location'] = {
                    'address': business.get('address'),
                    'city': business.get('city'),
                    'state': business.get('state'),
                    'postal_code': business.get('postal_code'),
                    'latitude': business.get('latitude'),
                    'longitude': business.get('longitude')
                }
                
                # Clean up original fields
                for key in ['address', 'city', 'state', 'postal_code', 'latitude', 'longitude']:
                    if key in business:
                        del business[key]
                
                # Convert hours to subdocument if it exists
                if business.get('hours') and isinstance(business['hours'], str):
                    try:
                        business['hours'] = json.loads(business['hours'].replace("'", '"'))
                    except:
                        pass
                
                # Add to batch
                mongo_batch.append(business)
                
                # Insert batch when it reaches size
                if len(mongo_batch) >= 100:
                    mongo_db.businesses.insert_many(mongo_batch)
                    mongo_batch = []
            
            except Exception as e:
                print(f"Error loading business {business_id} into MongoDB: {e}")
            
            # Process for Neo4j
            try:
                with neo4j_driver.session() as session:
                    # Create business node
                    session.run("""
                        MERGE (b:Business {business_id: $business_id})
                        SET b.name = $name,
                            b.stars = $stars,
                            b.review_count = $review_count,
                            b.is_open = $is_open
                    """, {
                        'business_id': business_id,
                        'name': business.get('name', ''),
                        'stars': business.get('stars'),
                        'review_count': business.get('review_count', 0),
                        'is_open': business.get('is_open', 0) == 1
                    })
                    
                    # Create location node and relationship
                    if city and state:
                        session.run("""
                            MERGE (l:Location {city: $city, state: $state, postal_code: $postal_code})
                            SET l.latitude = $latitude,
                                l.longitude = $longitude
                            WITH l
                            MATCH (b:Business {business_id: $business_id})
                            MERGE (b)-[:LOCATED_IN]->(l)
                        """, {
                            'business_id': business_id,
                            'city': city,
                            'state': state,
                            'postal_code': postal_code,
                            'latitude': business.get('latitude'),
                            'longitude': business.get('longitude')
                        })
                    
                    # Create category nodes and relationships
                    if business.get('categories'):
                        # Check if categories is already a list
                        if isinstance(business['categories'], list):
                            categories = business['categories']
                        else:
                            categories = [c.strip() for c in business.get('categories', '').split(',')]
                        
                        for category in categories:
                            if category:
                                session.run("""
                                    MERGE (c:Category {name: $category})
                                    WITH c
                                    MATCH (b:Business {business_id: $business_id})
                                    MERGE (b)-[:IN_CATEGORY]->(c)
                                """, {
                                    'business_id': business_id,
                                    'category': category
                                })
            
            except Exception as e:
                print(f"Error loading business {business_id} into Neo4j: {e}")
            
            count += 1
            
            # Commit every 100 records to avoid transaction size issues
            if count % 100 == 0:
                mysql_conn.commit()
    
    # Insert remaining MongoDB batch
    if mongo_batch:
        mongo_db.businesses.insert_many(mongo_batch)
    
    # Commit and close connections
    mysql_conn.commit()
    mysql_cursor.close()
    mysql_conn.close()
    
    mongo_client.close()
    neo4j_driver.close()
    
    print(f"Loaded {count} businesses into all databases")

def load_users():
    """Load users into all databases"""
    print("\nLoading users into databases...")
    
    # Load into MySQL
    mysql_conn = pymysql.connect(**MYSQL_CONFIG)
    mysql_cursor = mysql_conn.cursor()
    
    # Load into MongoDB
    mongo_client = pymongo.MongoClient(MONGODB_URI)
    mongo_db = mongo_client.yelp_db
    
    # Load into Neo4j
    neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)
    
    # Load users
    count = 0
    mongo_batch = []
    
    with open(USER_FILE, 'r', encoding='utf-8') as f:
        # Use tqdm for progress bar
        for line in tqdm(f, desc="Users", unit="user"):
            if count >= args.limit:
                break
            
            user = json.loads(line)
            user_id = user['user_id']
            
            # Process for MySQL
            try:
                # Parse yelping_since date
                yelping_since = datetime.strptime(user.get('yelping_since', '2010-01-01'), '%Y-%m-%d %H:%M:%S').date() if 'yelping_since' in user else None
                
                # Insert user
                mysql_cursor.execute("""
                    INSERT IGNORE INTO dim_user 
                    (user_id, name, review_count, yelping_since, fans, average_stars)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    user_id,
                    user.get('name', ''),
                    user.get('review_count', 0),
                    yelping_since,
                    user.get('fans', 0),
                    user.get('average_stars', 0)
                ))
            
            except Exception as e:
                print(f"Error loading user {user_id} into MySQL: {e}")
            
            # Process for MongoDB
            try:
                # Convert friends to array
                if 'friends' in user and user['friends'] != 'None':
                    user['friends'] = user['friends'].split(', ')
                else:
                    user['friends'] = []
                
                # Convert elite years to array of integers
                if 'elite' in user and user['elite'] and user['elite'] != 'None':
                    # First handle multiple separators (could be ',' or ', ')
                    if ', ' in user['elite']:
                        years_list = user['elite'].split(', ')
                    else:
                        years_list = user['elite'].split(',')
                    
                    # Safely convert each element to integer, skipping invalid ones
                    elite_years = []
                    for year_str in years_list:
                        try:
                            if year_str.strip().isdigit():  # Only process if it's a valid digit string
                                elite_years.append(int(year_str.strip()))
                        except ValueError:
                            # Skip invalid year entries
                            pass
                    
                    user['elite'] = elite_years
                else:
                    user['elite'] = []
                
                # Add to batch
                mongo_batch.append(user)
                
                # Insert batch when it reaches size
                if len(mongo_batch) >= 100:
                    mongo_db.users.insert_many(mongo_batch)
                    mongo_batch = []
            
            except Exception as e:
                print(f"Error loading user {user_id} into MongoDB: {e}")
            
            # Process for Neo4j
            try:
                # Parse yelping_since date for Neo4j
                yelping_since_str = user.get('yelping_since', '2010-01-01')
                
                with neo4j_driver.session() as session:
                    # Create user node
                    session.run("""
                        MERGE (u:User {user_id: $user_id})
                        SET u.name = $name,
                            u.review_count = $review_count,
                            u.yelping_since = date($yelping_since),
                            u.fans = $fans,
                            u.average_stars = $average_stars
                    """, {
                        'user_id': user_id,
                        'name': user.get('name', ''),
                        'review_count': user.get('review_count', 0),
                        'yelping_since': yelping_since_str.split(' ')[0],  # Just take the date part
                        'fans': user.get('fans', 0),
                        'average_stars': user.get('average_stars', 0)
                    })
                    
                    # Create friendship relationships (very selectively to keep the graph manageable)
                    if 'friends' in user and user['friends'] != 'None' and count % 10 == 0:  # Only for 10% of users
                        friends = user['friends'].split(', ') if isinstance(user['friends'], str) else user['friends']
                        # Take at most 5 friends to avoid overloading
                        for friend_id in friends[:5]:
                            session.run("""
                                MATCH (u:User {user_id: $user_id})
                                MERGE (f:User {user_id: $friend_id})
                                MERGE (u)-[:FRIENDS_WITH]->(f)
                            """, {
                                'user_id': user_id,
                                'friend_id': friend_id
                            })
            
            except Exception as e:
                print(f"Error loading user {user_id} into Neo4j: {e}")
            
            count += 1
            
            # Commit every 100 records to avoid transaction size issues
            if count % 100 == 0:
                mysql_conn.commit()
    
    # Insert remaining MongoDB batch
    if mongo_batch:
        mongo_db.users.insert_many(mongo_batch)
    
    # Commit and close connections
    mysql_conn.commit()
    mysql_cursor.close()
    mysql_conn.close()
    
    mongo_client.close()
    neo4j_driver.close()
    
    print(f"Loaded {count} users into all databases")

def load_reviews():
    """Load reviews into all databases"""
    print("\nLoading reviews into databases...")
    
    # Load into MySQL
    mysql_conn = pymysql.connect(**MYSQL_CONFIG)
    mysql_cursor = mysql_conn.cursor()
    
    # Get time_id mapping
    mysql_cursor.execute("SELECT time_id, date_actual FROM dim_time")
    time_mapping = {row[1].strftime('%Y-%m-%d'): row[0] for row in mysql_cursor.fetchall()}
    
    # Load into MongoDB
    mongo_client = pymongo.MongoClient(MONGODB_URI)
    mongo_db = mongo_client.yelp_db
    
    # Load into Neo4j
    neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)
    
    # Load reviews
    count = 0
    mongo_batch = []
    
    with open(REVIEW_FILE, 'r', encoding='utf-8') as f:
        # Use tqdm for progress bar
        for line in tqdm(f, desc="Reviews", unit="review"):
            if count >= args.limit:
                break
            
            review = json.loads(line)
            review_id = review['review_id']
            
            # Process for MySQL
            try:
                # Parse date and get time_id
                review_date = datetime.strptime(review.get('date', '2010-01-01'), '%Y-%m-%d %H:%M:%S').date()
                date_str = review_date.strftime('%Y-%m-%d')
                
                if date_str in time_mapping:
                    time_id = time_mapping[date_str]
                    
                    # Insert review
                    mysql_cursor.execute("""
                        INSERT IGNORE INTO fact_review 
                        (review_id, business_id, user_id, time_id, stars, useful_votes, funny_votes, cool_votes)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        review_id,
                        review.get('business_id'),
                        review.get('user_id'),
                        time_id,
                        review.get('stars', 0),
                        review.get('useful', 0),
                        review.get('funny', 0),
                        review.get('cool', 0)
                    ))
            
            except Exception as e:
                print(f"Error loading review {review_id} into MySQL: {e}")
            
            # Process for MongoDB
            try:
                # Convert date string to datetime object
                if 'date' in review:
                    try:
                        review['date'] = datetime.strptime(review['date'], '%Y-%m-%d %H:%M:%S')
                    except:
                        pass
                
                # Add to batch
                mongo_batch.append(review)
                
                # Insert batch when it reaches size
                if len(mongo_batch) >= 100:
                    mongo_db.reviews.insert_many(mongo_batch)
                    mongo_batch = []
            
            except Exception as e:
                print(f"Error loading review {review_id} into MongoDB: {e}")
            
            # Process for Neo4j
            try:
                # Only add a subset of reviews to Neo4j to keep it manageable
                if count % 10 == 0:  # Only add 10% of reviews
                    with neo4j_driver.session() as session:
                        # Create review node and relationships
                        session.run("""
                            MATCH (b:Business {business_id: $business_id})
                            MATCH (u:User {user_id: $user_id})
                            CREATE (r:Review {review_id: $review_id})
                            SET r.stars = $stars,
                                r.useful = $useful,
                                r.funny = $funny,
                                r.cool = $cool,
                                r.date = date($date)
                            CREATE (u)-[:WROTE]->(r)
                            CREATE (r)-[:REVIEWS]->(b)
                        """, {
                            'review_id': review_id,
                            'business_id': review.get('business_id'),
                            'user_id': review.get('user_id'),
                            'stars': review.get('stars', 0),
                            'useful': review.get('useful', 0),
                            'funny': review.get('funny', 0),
                            'cool': review.get('cool', 0),
                            'date': review_date.strftime('%Y-%m-%d')
                        })
            
            except Exception as e:
                if "not found" not in str(e):  # Ignore errors about missing business or user nodes
                    print(f"Error loading review {review_id} into Neo4j: {e}")
            
            count += 1
            
            # Commit every 100 records to avoid transaction size issues
            if count % 100 == 0:
                mysql_conn.commit()
    
    # Insert remaining MongoDB batch
    if mongo_batch:
        mongo_db.reviews.insert_many(mongo_batch)
    
    # Update business review counts and stars in Neo4j
    try:
        with neo4j_driver.session() as session:
            session.run("""
                MATCH (b:Business)<-[:REVIEWS]-(r:Review)
                WITH b, count(r) as review_count, avg(r.stars) as avg_stars
                SET b.review_count = review_count,
                    b.stars = avg_stars
            """)
    except Exception as e:
        print(f"Error updating business review counts in Neo4j: {e}")
    
    # Commit and close connections
    mysql_conn.commit()
    mysql_cursor.close()
    mysql_conn.close()
    
    mongo_client.close()
    neo4j_driver.close()
    
    print(f"Loaded {count} reviews into all databases")

def load_checkins():
    """Load checkins into all databases"""
    print("\nLoading checkins into databases...")
    
    # Load into MySQL
    mysql_conn = pymysql.connect(**MYSQL_CONFIG)
    mysql_cursor = mysql_conn.cursor()
    
    # Cache existing business_ids to check foreign key constraints
    mysql_cursor.execute("SELECT business_id FROM dim_business")
    business_ids = {row[0] for row in mysql_cursor.fetchall()}
    
    # Get time_id mapping
    mysql_cursor.execute("SELECT time_id, date_actual FROM dim_time")
    time_mapping = {row[1].strftime('%Y-%m-%d'): row[0] for row in mysql_cursor.fetchall()}
    
    # Load into MongoDB
    mongo_client = pymongo.MongoClient(MONGODB_URI)
    mongo_db = mongo_client.yelp_db
    
    # Load into Neo4j
    neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)
    
    # Load checkins
    count = 0
    mongo_batch = []
    
    with open(CHECKIN_FILE, 'r', encoding='utf-8') as f:
        # Use tqdm for progress bar
        for line in tqdm(f, desc="Checkins", unit="checkin"):
            if count >= args.limit:
                break
            
            checkin = json.loads(line)
            business_id = checkin['business_id']
            
            # Process for MySQL
            try:
                # Only process if the business exists in the dimension table
                if business_id in business_ids:
                    # Process date strings
                    if 'date' in checkin:
                        # Count occurrences of each date
                        date_counts = {}
                        
                        for date_str in checkin['date'].split(', '):
                            try:
                                # Extract just the date part (not time)
                                date_obj = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
                                date_only = date_obj.strftime('%Y-%m-%d')
                                
                                if date_only in date_counts:
                                    date_counts[date_only] += 1
                                else:
                                    date_counts[date_only] = 1
                            except:
                                continue
                        
                        # Insert each unique date as a separate checkin record
                        for date_str, count_val in date_counts.items():
                            if date_str in time_mapping:
                                time_id = time_mapping[date_str]
                                
                                # Insert checkin
                                mysql_cursor.execute("""
                                    INSERT INTO fact_checkin 
                                    (business_id, time_id, checkin_count)
                                    VALUES (%s, %s, %s)
                                """, (
                                    business_id,
                                    time_id,
                                    count_val
                                ))
            
            except Exception as e:
                print(f"Error loading checkin for business {business_id} into MySQL: {e}")
            
            # Process for MongoDB
            try:
                # Convert date string to array of datetime objects
                if 'date' in checkin:
                    date_array = []
                    for date_str in checkin['date'].split(', '):
                        try:
                            date_obj = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
                            date_array.append(date_obj)
                        except:
                            continue
                    
                    checkin['dates'] = date_array
                    del checkin['date']
                
                # Add to batch
                mongo_batch.append(checkin)
                
                # Insert batch when it reaches size
                if len(mongo_batch) >= 100:
                    mongo_db.checkins.insert_many(mongo_batch)
                    mongo_batch = []
            
            except Exception as e:
                print(f"Error loading checkin for business {business_id} into MongoDB: {e}")
            
            # Process for Neo4j
            try:
                # Only add a subset of checkins to Neo4j to keep it manageable
                if count % 10 == 0:  # Only add 10% of checkins
                    # First check if the business exists in Neo4j
                    with neo4j_driver.session() as session:
                        check_result = session.run("""
                            MATCH (b:Business {business_id: $business_id})
                            RETURN count(b) as count
                        """, {
                            'business_id': business_id
                        }).single()
                        
                        # Only proceed if the business exists
                        if check_result and check_result["count"] > 0:
                            if 'date' in checkin:
                                # Count occurrences of each date
                                date_counts = {}
                                
                                for date_str in checkin['date'].split(', '):
                                    try:
                                        date_obj = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
                                        date_only = date_obj.strftime('%Y-%m-%d')
                                        
                                        if date_only in date_counts:
                                            date_counts[date_only] += 1
                                        else:
                                            date_counts[date_only] = 1
                                    except:
                                        continue
                                
                                # Create checkin relationships
                                for date_str, count_val in date_counts.items():
                                    # Create time node if it doesn't exist
                                    session.run("""
                                        MERGE (t:Time {date: date($date)})
                                        SET t.day = date($date).day,
                                            t.month = date($date).month,
                                            t.year = date($date).year
                                    """, {'date': date_str})
                                    
                                    # Create checkin relationship
                                    session.run("""
                                        MATCH (b:Business {business_id: $business_id})
                                        MATCH (t:Time {date: date($date)})
                                        MERGE (b)-[c:HAD_CHECKIN]->(t)
                                        ON CREATE SET c.count = $count
                                        ON MATCH SET c.count = c.count + $count
                                    """, {
                                        'business_id': business_id,
                                        'date': date_str,
                                        'count': count_val
                                    })
            
            except Exception as e:
                if "not found" not in str(e):  # Ignore errors about missing business nodes
                    print(f"Error loading checkin for business {business_id} into Neo4j: {e}")
            
            count += 1
            
            # Commit every 100 records to avoid transaction size issues
            if count % 100 == 0:
                mysql_conn.commit()
    
    # Insert remaining MongoDB batch
    if mongo_batch:
        mongo_db.checkins.insert_many(mongo_batch)
    
    # Commit and close connections
    mysql_conn.commit()
    mysql_cursor.close()
    mysql_conn.close()
    
    mongo_client.close()
    neo4j_driver.close()
    
    print(f"Loaded {count} checkins into all databases")

def load_tips():
    """Load tips into all databases"""
    print("\nLoading tips into databases...")
    
    # Load into MySQL
    mysql_conn = pymysql.connect(**MYSQL_CONFIG)
    mysql_cursor = mysql_conn.cursor()
    
    # Cache existing business_ids and user_ids to check foreign key constraints
    mysql_cursor.execute("SELECT business_id FROM dim_business")
    business_ids = {row[0] for row in mysql_cursor.fetchall()}
    
    mysql_cursor.execute("SELECT user_id FROM dim_user")
    user_ids = {row[0] for row in mysql_cursor.fetchall()}
    
    # Get time_id mapping
    mysql_cursor.execute("SELECT time_id, date_actual FROM dim_time")
    time_mapping = {row[1].strftime('%Y-%m-%d'): row[0] for row in mysql_cursor.fetchall()}
    
    # Load into MongoDB
    mongo_client = pymongo.MongoClient(MONGODB_URI)
    mongo_db = mongo_client.yelp_db
    
    # Load into Neo4j
    neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)
    
    # Load tips
    count = 0
    mongo_batch = []
    
    with open(TIP_FILE, 'r', encoding='utf-8') as f:
        # Use tqdm for progress bar
        for line in tqdm(f, desc="Tips", unit="tip"):
            if count >= args.limit:
                break
            
            tip = json.loads(line)
            business_id = tip['business_id']
            user_id = tip['user_id']
            
            # Process for MySQL
            try:
                # Parse date
                tip_date = datetime.strptime(tip.get('date', '2010-01-01'), '%Y-%m-%d %H:%M:%S').date()
                date_str = tip_date.strftime('%Y-%m-%d')
                
                # Check if business and user exist before inserting
                if (business_id in business_ids and 
                    user_id in user_ids and 
                    date_str in time_mapping):
                    
                    time_id = time_mapping[date_str]
                    
                    # Insert tip
                    mysql_cursor.execute("""
                        INSERT INTO fact_tip 
                        (business_id, user_id, time_id, compliment_count)
                        VALUES (%s, %s, %s, %s)
                    """, (
                        business_id,
                        user_id,
                        time_id,
                        tip.get('compliment_count', 0)
                    ))
            
            except Exception as e:
                print(f"Error loading tip from user {user_id} for business {business_id} into MySQL: {e}")
            
            # Process for MongoDB
            try:
                # Convert date string to datetime object
                if 'date' in tip:
                    try:
                        tip['date'] = datetime.strptime(tip['date'], '%Y-%m-%d %H:%M:%S')
                    except:
                        pass
                
                # Add to batch
                mongo_batch.append(tip)
                
                # Insert batch when it reaches size
                if len(mongo_batch) >= 100:
                    mongo_db.tips.insert_many(mongo_batch)
                    mongo_batch = []
            
            except Exception as e:
                print(f"Error loading tip from user {user_id} for business {business_id} into MongoDB: {e}")
            
            # Process for Neo4j
            try:
                # Only add a subset of tips to Neo4j to keep it manageable
                if count % 20 == 0:  # Only add 5% of tips
                    with neo4j_driver.session() as session:
                        # First check if both the user and business nodes exist
                        check_result = session.run("""
                            MATCH (u:User {user_id: $user_id})
                            MATCH (b:Business {business_id: $business_id})
                            RETURN count(*) as count
                        """, {
                            'user_id': user_id,
                            'business_id': business_id
                        }).single()
                        
                        # Only create the relationship if both nodes exist
                        if check_result and check_result["count"] > 0:
                            # Create tip relationship
                            session.run("""
                                MATCH (u:User {user_id: $user_id})
                                MATCH (b:Business {business_id: $business_id})
                                MERGE (u)-[t:TIPPED]->(b)
                                SET t.date = date($date),
                                    t.text = $text,
                                    t.compliment_count = $compliment_count
                            """, {
                                'user_id': user_id,
                                'business_id': business_id,
                                'date': tip_date.strftime('%Y-%m-%d'),
                                'text': tip.get('text', ''),
                                'compliment_count': tip.get('compliment_count', 0)
                            })
            
            except Exception as e:
                # Ignore "not found" errors since we're now checking
                if "not found" not in str(e):
                    print(f"Error loading tip from user {user_id} for business {business_id} into Neo4j: {e}")
            
            count += 1
            
            # Commit every 100 records to avoid transaction size issues
            if count % 100 == 0:
                mysql_conn.commit()
    
    # Insert remaining MongoDB batch
    if mongo_batch:
        mongo_db.tips.insert_many(mongo_batch)
    
    # Commit and close connections
    mysql_conn.commit()
    mysql_cursor.close()
    mysql_conn.close()
    
    mongo_client.close()
    neo4j_driver.close()
    
    print(f"Loaded {count} tips into all databases")

def create_summary_tables():
    """Create and populate summary tables"""
    print("\nCreating summary tables...")
    
    # MySQL summary tables
    try:
        mysql_conn = pymysql.connect(**MYSQL_CONFIG)
        mysql_cursor = mysql_conn.cursor()
        
        # Populate summary_business_performance table
        mysql_cursor.execute("""
            INSERT INTO summary_business_performance (business_id, total_reviews, avg_rating, total_checkins)
            SELECT 
                b.business_id,
                COUNT(r.review_id) as total_reviews,
                AVG(r.stars) as avg_rating,
                COALESCE(SUM(c.checkin_count), 0) as total_checkins
            FROM 
                dim_business b
                LEFT JOIN fact_review r ON b.business_id = r.business_id
                LEFT JOIN fact_checkin c ON b.business_id = c.business_id
            GROUP BY 
                b.business_id
            ON DUPLICATE KEY UPDATE
                total_reviews = VALUES(total_reviews),
                avg_rating = VALUES(avg_rating),
                total_checkins = VALUES(total_checkins)
        """)
        
        mysql_conn.commit()
        mysql_cursor.close()
        mysql_conn.close()
        print("MySQL summary tables created")
    except Exception as e:
        print(f"Error creating MySQL summary tables: {e}")
    
    # MongoDB aggregations
    try:
        mongo_client = pymongo.MongoClient(MONGODB_URI)
        mongo_db = mongo_client.yelp_db
        
        # Create business_summaries collection
        if "business_summaries" in mongo_db.list_collection_names():
            mongo_db.business_summaries.drop()
        
        # Aggregate business metrics
        pipeline = [
            {
                "$lookup": {
                    "from": "reviews",
                    "localField": "business_id",
                    "foreignField": "business_id",
                    "as": "reviews"
                }
            },
            {
                "$lookup": {
                    "from": "checkins",
                    "localField": "business_id",
                    "foreignField": "business_id",
                    "as": "checkins"
                }
            },
            {
                "$project": {
                    "business_id": 1,
                    "name": 1,
                    "location": 1,
                    "categories": 1,
                    "stars": 1,
                    "review_count": 1,
                    "is_open": 1,
                    "review_stats": {
                        "avg_stars": {"$avg": "$reviews.stars"},
                        "review_count": {"$size": "$reviews"},
                        "five_star_count": {
                            "$size": {
                                "$filter": {
                                    "input": "$reviews",
                                    "as": "review",
                                    "cond": {"$eq": ["$$review.stars", 5]}
                                }
                            }
                        },
                        "one_star_count": {
                            "$size": {
                                "$filter": {
                                    "input": "$reviews",
                                    "as": "review",
                                    "cond": {"$eq": ["$$review.stars", 1]}
                                }
                            }
                        }
                    },
                    "checkin_count": {"$size": {"$ifNull": ["$checkins.dates", []]}}
                }
            },
            {"$out": "business_summaries"}
        ]
        
        mongo_db.businesses.aggregate(pipeline)
        
        # Create indexes on the summary collection
        mongo_db.business_summaries.create_index([("business_id", pymongo.ASCENDING)], unique=True)
        mongo_db.business_summaries.create_index([("categories", pymongo.ASCENDING)])
        mongo_db.business_summaries.create_index([("stars", pymongo.DESCENDING)])
        mongo_db.business_summaries.create_index([("review_stats.avg_stars", pymongo.DESCENDING)])
        
        mongo_client.close()
        print("MongoDB aggregations created")
    except Exception as e:
        print(f"Error creating MongoDB aggregations: {e}")
    
    # Neo4j summary calculations
    try:
        neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)
        with neo4j_driver.session() as session:
            # Add summary properties to Business nodes
            session.run("""
                MATCH (b:Business)
                OPTIONAL MATCH (b)<-[r:REVIEWS]-(:Review)
                WITH b, count(r) as review_count, avg(r.stars) as avg_stars
                
                OPTIONAL MATCH (b)-[c:HAD_CHECKIN]->(:Time)
                WITH b, review_count, avg_stars, sum(c.count) as checkin_count
                
                SET b.summary_review_count = review_count,
                    b.summary_avg_stars = avg_stars,
                    b.summary_checkin_count = checkin_count
            """)
            
            # Add summary properties to Category nodes
            session.run("""
                MATCH (c:Category)<-[:IN_CATEGORY]-(b:Business)
                WITH c, count(b) as business_count
                
                OPTIONAL MATCH (c)<-[:IN_CATEGORY]-(b:Business)<-[:REVIEWS]-(r:Review)
                WITH c, business_count, count(r) as review_count, avg(r.stars) as avg_stars
                
                SET c.business_count = business_count,
                    c.review_count = review_count,
                    c.avg_stars = avg_stars
            """)
        
        neo4j_driver.close()
        print("Neo4j summary properties created")
    except Exception as e:
        print(f"Error creating Neo4j summary properties: {e}")

def main():
    """Main function to load all data"""
    print("=== Yelp Data Warehouse Loader ===")
    print(f"Loading up to {args.limit} records from each file")
    
    # Check database connections
    if not wait_for_databases():
        print("Exiting due to database connection issues")
        return
    
    # Set up schemas
    setup_mysql_schema()
    setup_mongodb_schema()
    setup_neo4j_schema()
    
    # Load data
    load_businesses()
    load_users()
    load_reviews()
    load_checkins()
    load_tips()
    
    # Create summary tables and aggregations
    create_summary_tables()
    
    print("\n=== Data loading complete ===")
    print("You can now access the Yelp Data Warehouse through the web dashboard")

if __name__ == "__main__":
    main()