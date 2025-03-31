"""
Data Initializer for Yelp Data Warehouse
This script checks if data exists in each database and loads sample data if needed.
"""

import os
import time
import json
import pymysql
import pymongo
from neo4j import GraphDatabase
import pandas as pd
from datetime import datetime

# Environment variables
MYSQL_CONFIG = {
    'host': os.environ.get('MYSQL_HOST', 'mysql'),
    'port': int(os.environ.get('MYSQL_PORT', 3306)),
    'user': os.environ.get('MYSQL_USER', 'root'),
    'password': os.environ.get('MYSQL_PASSWORD', 'user'),
    'db': os.environ.get('MYSQL_DATABASE', 'yelp_dw'),
    'charset': 'utf8mb4'
}

MONGO_HOST = os.environ.get('MONGO_HOST', 'mongodb')
MONGO_PORT = int(os.environ.get('MONGO_PORT', 27017))
MONGO_USER = os.environ.get('MONGO_USER', 'user')
MONGO_PASSWORD = os.environ.get('MONGO_PASSWORD', 'user')
MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/"

NEO4J_HOST = os.environ.get('NEO4J_HOST', 'neo4j')
NEO4J_PORT = int(os.environ.get('NEO4J_PORT', 7687))
NEO4J_USER = os.environ.get('NEO4J_USER', 'neo4j')
NEO4J_PASSWORD = os.environ.get('NEO4J_PASSWORD', 'user')
NEO4J_URI = f"bolt://{NEO4J_HOST}:{NEO4J_PORT}"

def wait_for_databases():
    """Wait for all databases to be ready"""
    print("Waiting for databases to be ready...")
    
    # Check MySQL
    mysql_ready = False
    for _ in range(30):  # Try for 30 seconds
        try:
            conn = pymysql.connect(**MYSQL_CONFIG)
            conn.close()
            mysql_ready = True
            print("MySQL is ready")
            break
        except:
            time.sleep(1)
    
    # Check MongoDB
    mongo_ready = False
    for _ in range(30):  # Try for 30 seconds
        try:
            client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
            client.admin.command('ping')
            client.close()
            mongo_ready = True
            print("MongoDB is ready")
            break
        except:
            time.sleep(1)
    
    # Check Neo4j
    neo4j_ready = False
    for _ in range(30):  # Try for 30 seconds
        try:
            driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
            with driver.session() as session:
                session.run("RETURN 1")
            driver.close()
            neo4j_ready = True
            print("Neo4j is ready")
            break
        except:
            time.sleep(1)
    
    if not (mysql_ready and mongo_ready and neo4j_ready):
        print("Warning: Not all databases are ready. Proceeding anyway.")

def check_and_initialize_mysql():
    """Check if MySQL has data, initialize if needed"""
    print("\nChecking MySQL data...")
    
    conn = None
    try:
        conn = pymysql.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()
        
        # Check if tables exist
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        tables = [t[0] for t in tables]
        
        # If tables exist, check if they have data
        if tables:
            print(f"MySQL tables found: {tables}")
            
            # Check dim_business table
            if 'dim_business' in tables:
                cursor.execute("SELECT COUNT(*) FROM dim_business")
                count = cursor.fetchone()[0]
                print(f"Business count: {count}")
                
                if count > 0:
                    print("MySQL has data. Skipping initialization.")
                    return False  # No need to initialize
        
        # Initialize MySQL schema
        print("Initializing MySQL schema...")
        
        # Execute SQL schema files
        try:
            schema_file = '/app/scripts/etl/sql/schema/star_schema.sql'
            with open(schema_file, 'r') as f:
                sql = f.read()
                cursor.execute(sql)
            print("Schema created successfully.")
        except Exception as e:
            print(f"Error creating schema: {e}")
            
            # Fallback: Create basic schema
            print("Creating basic schema...")
            
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
                    city VARCHAR(100) NOT NULL,
                    state VARCHAR(50) NOT NULL,
                    postal_code VARCHAR(20) NOT NULL,
                    latitude DECIMAL(9,6),
                    longitude DECIMAL(9,6),
                    UNIQUE KEY uk_location (city, state, postal_code)
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS dim_business (
                    business_id VARCHAR(100) PRIMARY KEY,
                    business_name VARCHAR(255) NOT NULL,
                    location_id INT NOT NULL,
                    stars DECIMAL(2,1),
                    review_count INT,
                    is_open BOOLEAN,
                    FOREIGN KEY (location_id) REFERENCES dim_location(location_id)
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS dim_category (
                    category_id INT PRIMARY KEY AUTO_INCREMENT,
                    category_name VARCHAR(100) NOT NULL,
                    UNIQUE KEY uk_category (category_name)
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS business_category (
                    business_id VARCHAR(100) NOT NULL,
                    category_id INT NOT NULL,
                    PRIMARY KEY (business_id, category_id),
                    FOREIGN KEY (business_id) REFERENCES dim_business(business_id),
                    FOREIGN KEY (category_id) REFERENCES dim_category(category_id)
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS summary_business_performance (
                    business_id VARCHAR(100) PRIMARY KEY,
                    total_reviews INT,
                    avg_rating DECIMAL(2,1),
                    total_checkins INT,
                    FOREIGN KEY (business_id) REFERENCES dim_business(business_id)
                )
            """)
        
        # Initialize with example data
        print("Loading sample data into MySQL...")
        
        # Insert sample location
        cursor.execute("""
            INSERT INTO dim_location (city, state, postal_code, latitude, longitude)
            VALUES 
            ('San Francisco', 'CA', '94103', 37.7749, -122.4194),
            ('Los Angeles', 'CA', '90001', 34.0522, -118.2437),
            ('New York', 'NY', '10001', 40.7128, -74.0060)
        """)
        
        # Get location IDs
        cursor.execute("SELECT location_id FROM dim_location")
        location_ids = [row[0] for row in cursor.fetchall()]
        
        # Insert sample businesses
        for i, location_id in enumerate(location_ids, 1):
            cursor.execute("""
                INSERT INTO dim_business 
                (business_id, business_name, location_id, stars, review_count, is_open)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                f"sample_business_{i}",
                f"Sample Business {i}",
                location_id,
                4.0 + (i % 2) * 0.5,
                100 * i,
                1
            ))
        
        # Insert sample categories
        categories = ['Restaurants', 'Shopping', 'Food', 'Beauty & Spas', 'Home Services']
        for category in categories:
            cursor.execute("""
                INSERT INTO dim_category (category_name)
                VALUES (%s)
            """, (category,))
        
        # Link businesses to categories
        cursor.execute("SELECT business_id FROM dim_business")
        business_ids = [row[0] for row in cursor.fetchall()]
        
        cursor.execute("SELECT category_id FROM dim_category")
        category_ids = [row[0] for row in cursor.fetchall()]
        
        for business_id in business_ids:
            for category_id in category_ids[:2]:  # Assign first two categories to each business
                cursor.execute("""
                    INSERT INTO business_category (business_id, category_id)
                    VALUES (%s, %s)
                """, (business_id, category_id))
        
        conn.commit()
        print("MySQL initialized with sample data.")
        return True  # Initialized
        
    except Exception as e:
        print(f"Error initializing MySQL: {e}")
        return False
    finally:
        if conn:
            conn.close()

def check_and_initialize_mongodb():
    """Check if MongoDB has data, initialize if needed"""
    print("\nChecking MongoDB data...")
    
    try:
        client = pymongo.MongoClient(MONGO_URI)
        db = client.yelp_db
        
        # Check if collections exist and have data
        collections = db.list_collection_names()
        print(f"MongoDB collections found: {collections}")
        
        if 'businesses' in collections:
            count = db.businesses.count_documents({})
            print(f"Business count: {count}")
            
            if count > 0:
                print("MongoDB has data. Skipping initialization.")
                client.close()
                return False  # No need to initialize
        
        # Initialize MongoDB
        print("Initializing MongoDB...")
        
        # Create collections
        if 'businesses' not in collections:
            db.create_collection('businesses')
        if 'reviews' not in collections:
            db.create_collection('reviews')
        if 'users' not in collections:
            db.create_collection('users')
        
        # Create indexes
        db.businesses.create_index([('business_id', pymongo.ASCENDING)], unique=True)
        db.businesses.create_index([('categories', pymongo.ASCENDING)])
        db.reviews.create_index([('business_id', pymongo.ASCENDING)])
        db.reviews.create_index([('user_id', pymongo.ASCENDING)])
        db.users.create_index([('user_id', pymongo.ASCENDING)], unique=True)
        
        # Insert sample data
        print("Loading sample data into MongoDB...")
        
        # Sample businesses
        businesses = []
        cities = ['San Francisco', 'Los Angeles', 'New York']
        states = ['CA', 'CA', 'NY']
        categories = [
            ['Restaurants', 'Italian'],
            ['Shopping', 'Clothing'],
            ['Food', 'Grocery'],
            ['Beauty & Spas', 'Hair Salons'],
            ['Restaurants', 'Mexican']
        ]
        
        for i in range(1, 6):
            city_idx = (i - 1) % len(cities)
            cat_idx = (i - 1) % len(categories)
            
            business = {
                "business_id": f"sample_business_mongo_{i}",
                "name": f"Sample MongoDB Business {i}",
                "stars": 4.0 + (i % 2) * 0.5,
                "review_count": 100 * i,
                "categories": categories[cat_idx],
                "location": {
                    "city": cities[city_idx],
                    "state": states[city_idx],
                    "postal_code": f"9{i}001"
                },
                "is_open": 1
            }
            businesses.append(business)
        
        if businesses:
            db.businesses.insert_many(businesses)
        
        # Sample users
        users = []
        for i in range(1, 6):
            user = {
                "user_id": f"sample_user_mongo_{i}",
                "name": f"User {i}",
                "review_count": 50 * i,
                "yelping_since": "2015-01-01",
                "fans": 10 * i,
                "average_stars": 3.5 + (i % 3) * 0.5,
                "elite": ["2019", "2020"] if i % 2 == 0 else []
            }
            users.append(user)
        
        if users:
            db.users.insert_many(users)
        
        # Sample reviews
        reviews = []
        for i in range(1, 11):
            business_idx = (i - 1) % len(businesses)
            user_idx = (i - 1) % len(users)
            
            review = {
                "review_id": f"sample_review_mongo_{i}",
                "business_id": businesses[business_idx]["business_id"],
                "user_id": users[user_idx]["user_id"],
                "stars": (i % 5) + 1,
                "date": "2023-01-01",
                "text": f"This is a sample review {i}.",
                "useful": i % 3,
                "funny": i % 2,
                "cool": i % 4
            }
            reviews.append(review)
        
        if reviews:
            db.reviews.insert_many(reviews)
        
        print("MongoDB initialized with sample data.")
        client.close()
        return True  # Initialized
        
    except Exception as e:
        print(f"Error initializing MongoDB: {e}")
        return False

def check_and_initialize_neo4j():
    """Check if Neo4j has data, initialize if needed"""
    print("\nChecking Neo4j data...")
    
    driver = None
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        
        with driver.session() as session:
            # Check if nodes exist
            result = session.run("MATCH (n) RETURN count(n) as count")
            count = result.single()["count"]
            print(f"Node count: {count}")
            
            if count > 0:
                print("Neo4j has data. Skipping initialization.")
                return False  # No need to initialize
            
            # Initialize Neo4j
            print("Initializing Neo4j...")
            
            # Create constraints and indexes
            constraints = [
                "CREATE CONSTRAINT IF NOT EXISTS ON (b:Business) ASSERT b.business_id IS UNIQUE",
                "CREATE CONSTRAINT IF NOT EXISTS ON (u:User) ASSERT u.user_id IS UNIQUE",
                "CREATE CONSTRAINT IF NOT EXISTS ON (r:Review) ASSERT r.review_id IS UNIQUE",
                "CREATE CONSTRAINT IF NOT EXISTS ON (c:Category) ASSERT c.name IS UNIQUE"
            ]
            
            for constraint in constraints:
                try:
                    session.run(constraint)
                except Exception as e:
                    print(f"Error creating constraint: {e}")
            
            # Create sample data
            print("Loading sample data into Neo4j...")
            
            # Create locations
            cities = ['San Francisco', 'Los Angeles', 'New York']
            states = ['CA', 'CA', 'NY']
            
            for i, (city, state) in enumerate(zip(cities, states)):
                session.run("""
                    MERGE (l:Location {city: $city, state: $state})
                """, city=city, state=state)
            
            # Create categories
            categories = ['Restaurants', 'Shopping', 'Food', 'Beauty & Spas', 'Home Services']
            for category in categories:
                session.run("""
                    MERGE (c:Category {name: $name})
                """, name=category)
            
            # Create businesses with relationships
            for i in range(1, 6):
                city_idx = (i - 1) % len(cities)
                
                # Create business
                session.run("""
                    CREATE (b:Business {
                        business_id: $business_id,
                        name: $name,
                        stars: $stars,
                        review_count: $review_count,
                        is_open: $is_open
                    })
                """, {
                    'business_id': f"sample_business_neo4j_{i}",
                    'name': f"Sample Neo4j Business {i}",
                    'stars': 4.0 + (i % 2) * 0.5,
                    'review_count': 100 * i,
                    'is_open': True
                })
                
                # Connect to location
                session.run("""
                    MATCH (b:Business {business_id: $business_id})
                    MATCH (l:Location {city: $city, state: $state})
                    MERGE (b)-[:LOCATED_IN]->(l)
                """, {
                    'business_id': f"sample_business_neo4j_{i}",
                    'city': cities[city_idx],
                    'state': states[city_idx]
                })
                
                # Connect to categories (first two for each business)
                for j in range(2):
                    cat_idx = (i + j) % len(categories)
                    session.run("""
                        MATCH (b:Business {business_id: $business_id})
                        MATCH (c:Category {name: $category})
                        MERGE (b)-[:IN_CATEGORY]->(c)
                    """, {
                        'business_id': f"sample_business_neo4j_{i}",
                        'category': categories[cat_idx]
                    })
            
            # Create some time nodes
            for i in range(1, 13):  # One for each month of 2023
                session.run("""
                    CREATE (t:Time {
                        date: date('2023-' + $month + '-01'),
                        year: 2023,
                        month: toInteger($month),
                        day: 1,
                        quarter: CASE 
                            WHEN toInteger($month) <= 3 THEN 1
                            WHEN toInteger($month) <= 6 THEN 2
                            WHEN toInteger($month) <= 9 THEN 3
                            ELSE 4
                        END
                    })
                """, month=str(i).zfill(2))
            
            print("Neo4j initialized with sample data.")
            return True  # Initialized
        
    except Exception as e:
        print(f"Error initializing Neo4j: {e}")
        return False
    finally:
        if driver:
            driver.close()

if __name__ == "__main__":
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
    
    print("\nData Initializer completed successfully.")