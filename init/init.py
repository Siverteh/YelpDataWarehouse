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
from datetime import datetime, timedelta

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
    
    # Wait longer and try more times
    max_attempts = 60  # Increase from 30 to 60 attempts
    
    # Check MySQL
    mysql_ready = False
    for i in range(max_attempts):
        try:
            conn = pymysql.connect(**MYSQL_CONFIG)
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
            client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
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
            driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
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
            # Try to find the schema file in multiple possible locations
            schema_paths = [
                '/app/databases/mysql/schema/star_schema.sql',
                '/databases/mysql/schema/star_schema.sql',
                './databases/mysql/schema/star_schema.sql'
            ]
            
            schema_loaded = False
            for schema_path in schema_paths:
                if os.path.exists(schema_path):
                    print(f"Loading schema from {schema_path}")
                    with open(schema_path, 'r') as f:
                        sql = f.read()
                        cursor.execute(sql)
                    schema_loaded = True
                    print("Schema created successfully from file.")
                    break
            
            if not schema_loaded:
                raise FileNotFoundError("Could not find schema file in any expected location")
                
        except Exception as e:
            print(f"Error creating schema from file: {e}")
            
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
                CREATE TABLE IF NOT EXISTS fact_review (
                    review_id VARCHAR(100) PRIMARY KEY,
                    business_id VARCHAR(100) NOT NULL,
                    user_id VARCHAR(100) NOT NULL,
                    time_id INT NOT NULL,
                    stars INT NOT NULL,
                    useful_votes INT NOT NULL,
                    funny_votes INT NOT NULL,
                    cool_votes INT NOT NULL,
                    FOREIGN KEY (business_id) REFERENCES dim_business(business_id),
                    FOREIGN KEY (time_id) REFERENCES dim_time(time_id)
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS fact_checkin (
                    checkin_id INT PRIMARY KEY AUTO_INCREMENT,
                    business_id VARCHAR(100) NOT NULL,
                    time_id INT NOT NULL,
                    checkin_count INT NOT NULL,
                    FOREIGN KEY (business_id) REFERENCES dim_business(business_id),
                    FOREIGN KEY (time_id) REFERENCES dim_time(time_id)
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
        
        # Create time dimension (this is essential for the dashboard)
        print("Creating time dimension...")
        start_date = datetime(2015, 1, 1)
        end_date = datetime(2023, 12, 31)
        current_date = start_date
        
        while current_date <= end_date:
            day_name = current_date.strftime('%A')
            month_name = current_date.strftime('%B')
            quarter = ((current_date.month - 1) // 3) + 1
            
            cursor.execute("""
                INSERT IGNORE INTO dim_time 
                (date_actual, day_of_week, day_of_month, month_actual, month_name, quarter_actual, year_actual)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                current_date.strftime('%Y-%m-%d'),
                day_name,
                current_date.day,
                current_date.month,
                month_name,
                quarter,
                current_date.year
            ))
            
            current_date += timedelta(days=1)
        
        # Initialize with example data
        print("Loading sample data into MySQL...")
        
        # Insert sample location
        cursor.execute("""
            INSERT INTO dim_location (city, state, postal_code, latitude, longitude)
            VALUES 
            ('San Francisco', 'CA', '94103', 37.7749, -122.4194),
            ('Los Angeles', 'CA', '90001', 34.0522, -118.2437),
            ('New York', 'NY', '10001', 40.7128, -74.0060),
            ('Chicago', 'IL', '60611', 41.8781, -87.6298),
            ('Houston', 'TX', '77001', 29.7604, -95.3698)
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
                round(3.5 + (i % 3) * 0.5, 1),  # Ratings between 3.5 and 5.0
                50 + (i * 25),  # Review counts between 50 and several hundred
                1
            ))
        
        # Insert more sample businesses for diversity
        for i in range(6, 16):
            random_location = location_ids[i % len(location_ids)]
            cursor.execute("""
                INSERT INTO dim_business 
                (business_id, business_name, location_id, stars, review_count, is_open)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                f"sample_business_{i}",
                f"Business Example {i}",
                random_location,
                round(2.0 + (i % 6) * 0.5, 1),  # More varied ratings 2.0-5.0
                30 + (i * 20),  # Different review counts
                i % 5 != 0  # Some closed businesses
            ))
        
        # Insert sample categories (expanded list)
        categories = [
            'Restaurants', 'Shopping', 'Food', 'Beauty & Spas', 'Home Services',
            'Coffee & Tea', 'Bars', 'Fast Food', 'Pizza', 'Italian', 
            'Mexican', 'Chinese', 'Japanese', 'Thai', 'Burgers',
            'Clothing', 'Grocery', 'Bakeries', 'Gyms', 'Health'
        ]
        for category in categories:
            cursor.execute("""
                INSERT IGNORE INTO dim_category (category_name)
                VALUES (%s)
            """, (category,))
        
        # Link businesses to categories
        cursor.execute("SELECT business_id FROM dim_business")
        business_ids = [row[0] for row in cursor.fetchall()]
        
        cursor.execute("SELECT category_id, category_name FROM dim_category")
        category_data = cursor.fetchall()
        
        # Assign multiple categories to each business in a more realistic way
        for i, business_id in enumerate(business_ids):
            # Assign 2-4 categories to each business
            num_categories = min(len(category_data), 2 + (i % 3))
            
            # Use different patterns of categories based on business_id number
            if i % 5 == 0:  # Restaurant-type
                relevant_categories = [cat for cat in category_data if cat[1] in 
                                      ['Restaurants', 'Food', 'Italian', 'Mexican', 'Chinese', 'Japanese', 'Thai', 'Pizza', 'Burgers']]
            elif i % 5 == 1:  # Shopping-type
                relevant_categories = [cat for cat in category_data if cat[1] in 
                                      ['Shopping', 'Clothing', 'Grocery']]
            elif i % 5 == 2:  # Service-type
                relevant_categories = [cat for cat in category_data if cat[1] in 
                                      ['Beauty & Spas', 'Home Services', 'Health', 'Gyms']]
            elif i % 5 == 3:  # Food & Drink
                relevant_categories = [cat for cat in category_data if cat[1] in 
                                      ['Coffee & Tea', 'Bars', 'Bakeries', 'Food']]
            else:  # Mixed
                relevant_categories = category_data
            
            # If no relevant categories found, use all categories
            if not relevant_categories:
                relevant_categories = category_data
            
            # Select a subset of categories
            selected_categories = relevant_categories[:num_categories]
            
            # Link business to categories
            for category_id, _ in selected_categories:
                cursor.execute("""
                    INSERT IGNORE INTO business_category (business_id, category_id)
                    VALUES (%s, %s)
                """, (business_id, category_id))
        
        # Insert user dimension (essential for reviews)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dim_user (
                user_id VARCHAR(100) PRIMARY KEY,
                name VARCHAR(255),
                review_count INT,
                yelping_since DATE,
                fans INT,
                average_stars DECIMAL(2,1)
            )
        """)
        
        # Insert sample users
        for i in range(1, 20):
            cursor.execute("""
                INSERT IGNORE INTO dim_user
                (user_id, name, review_count, yelping_since, fans, average_stars)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                f"user_{i}",
                f"User {i}",
                10 + (i * 5),
                datetime(2015, 1, 1) + timedelta(days=(i * 30)),
                i % 10,
                round(3.0 + (i % 4) * 0.5, 1)
            ))
        
        # Get time_ids for different dates
        cursor.execute("SELECT time_id, date_actual FROM dim_time WHERE date_actual BETWEEN '2022-01-01' AND '2023-01-01'")
        time_data = cursor.fetchall()
        time_ids = [row[0] for row in time_data]
        
        # Get user_ids
        cursor.execute("SELECT user_id FROM dim_user")
        user_ids = [row[0] for row in cursor.fetchall()]
        
        # Insert sample reviews
        for i, business_id in enumerate(business_ids):
            # Insert multiple reviews per business
            num_reviews = min(5 + (i % 5), len(time_ids), len(user_ids))
            
            for j in range(num_reviews):
                time_id = time_ids[j % len(time_ids)]
                user_id = user_ids[j % len(user_ids)]
                stars = 3 + (j % 3)  # Ratings 3-5
                
                cursor.execute("""
                    INSERT IGNORE INTO fact_review
                    (review_id, business_id, user_id, time_id, stars, useful_votes, funny_votes, cool_votes)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    f"review_{business_id}_{j}",
                    business_id,
                    user_id,
                    time_id,
                    stars,
                    j % 5,  # useful votes
                    j % 3,  # funny votes
                    j % 4   # cool votes
                ))
        
        # Insert sample checkins
        for i, business_id in enumerate(business_ids):
            # Insert multiple checkins per business
            num_checkins = min(3 + (i % 4), len(time_ids))
            
            for j in range(num_checkins):
                time_id = time_ids[j % len(time_ids)]
                checkin_count = 1 + (j % 5)  # 1-5 checkins
                
                cursor.execute("""
                    INSERT INTO fact_checkin 
                    (business_id, time_id, checkin_count)
                    VALUES (%s, %s, %s)
                """, (
                    business_id,
                    time_id,
                    checkin_count
                ))
        
        # Create summary data
        cursor.execute("""
            INSERT INTO summary_business_performance (business_id, total_reviews, avg_rating, total_checkins)
            SELECT 
                b.business_id,
                COUNT(r.review_id) as total_reviews,
                IFNULL(AVG(r.stars), b.stars) as avg_rating,
                IFNULL(SUM(c.checkin_count), 0) as total_checkins
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
        if 'checkins' not in collections:
            db.create_collection('checkins')
        
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
        cities = ['San Francisco', 'Los Angeles', 'New York', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia']
        states = ['CA', 'CA', 'NY', 'IL', 'TX', 'AZ', 'PA']
        
        # More varied business categories for better visualization
        category_groups = [
            ['Restaurants', 'Italian'],
            ['Restaurants', 'Mexican'],
            ['Restaurants', 'Chinese'],
            ['Restaurants', 'Japanese'],
            ['Restaurants', 'Thai'],
            ['Restaurants', 'American'],
            ['Shopping', 'Clothing'],
            ['Shopping', 'Department Stores'],
            ['Food', 'Grocery'],
            ['Food', 'Bakeries'],
            ['Beauty & Spas', 'Hair Salons'],
            ['Beauty & Spas', 'Nail Salons'],
            ['Home Services', 'Contractors'],
            ['Home Services', 'Plumbing'],
            ['Nightlife', 'Bars'],
            ['Nightlife', 'Clubs'],
            ['Health & Medical', 'Doctors'],
            ['Automotive', 'Auto Repair'],
            ['Active Life', 'Gyms']
        ]
        
        # Create more sample businesses (20 businesses)
        for i in range(1, 21):
            city_idx = (i - 1) % len(cities)
            cat_idx = (i - 1) % len(category_groups)
            
            stars = round(2.0 + (i % 6) * 0.5, 1)  # Ratings from 2.0-5.0
            review_count = 30 + (i * 15)  # Different review counts
            
            business = {
                "business_id": f"sample_business_mongo_{i}",
                "name": f"MongoDB Sample {i}",
                "stars": stars,
                "review_count": review_count,
                "categories": category_groups[cat_idx],
                "location": {
                    "city": cities[city_idx],
                    "state": states[city_idx],
                    "postal_code": f"9{i:02d}01"
                },
                "is_open": i % 7 != 0,  # Most are open
                "attributes": {
                    "TakesReservations": i % 2 == 0,
                    "NoiseLevel": ["quiet", "average", "loud"][i % 3],
                    "WiFi": ["no", "free", "paid"][i % 3]
                }
            }
            businesses.append(business)
        
        if businesses:
            db.businesses.insert_many(businesses)
        
        # Sample users
        users = []
        for i in range(1, 25):
            yelping_since = datetime(2015, 1, 1) + timedelta(days=(i * 30))
            review_count = 5 + (i * 3)
            
            user = {
                "user_id": f"sample_user_mongo_{i}",
                "name": f"User {i}",
                "review_count": review_count,
                "yelping_since": yelping_since,
                "fans": i % 10,
                "average_stars": round(3.0 + (i % 4) * 0.5, 1),
                "elite": ["2019", "2020"] if i % 3 == 0 else []
            }
            users.append(user)
        
        if users:
            db.users.insert_many(users)
        
        # Sample reviews - create more with varied dates for better visualization
        reviews = []
        dates = [
            "2022-01-15", "2022-02-20", "2022-03-10", "2022-04-05", "2022-05-15",
            "2022-06-22", "2022-07-18", "2022-08-30", "2022-09-12", "2022-10-25",
            "2022-11-11", "2022-12-28", "2023-01-05", "2023-02-14"
        ]
        
        # Generate 3-5 reviews per business with different dates
        for business in businesses:
            num_reviews = 3 + (businesses.index(business) % 3)  # 3-5 reviews per business
            for j in range(num_reviews):
                user_idx = (businesses.index(business) + j) % len(users)
                date_idx = (businesses.index(business) + j) % len(dates)
                stars = max(1, min(5, round(business["stars"] + (j % 3 - 1))))  # Vary around business rating
                
                review = {
                    "review_id": f"review_mongo_{business['business_id']}_{j}",
                    "business_id": business["business_id"],
                    "user_id": users[user_idx]["user_id"],
                    "stars": stars,
                    "date": datetime.strptime(dates[date_idx], "%Y-%m-%d"),
                    "text": f"This is sample review {j+1} for {business['name']}. {'Great experience!' if stars >= 4 else 'Average place.' if stars == 3 else 'Disappointing service.'}",
                    "useful": j % 5,
                    "funny": j % 3,
                    "cool": j % 4
                }
                reviews.append(review)
        
        if reviews:
            db.reviews.insert_many(reviews)
        
        # Sample checkins - more data for visualization
        checkins = []
        for business in businesses:
            # Create checkins with dates array
            checkin_dates = []
            for i in range(5 + (businesses.index(business) % 5)):  # 5-9 checkins per business
                date_idx = (businesses.index(business) + i) % len(dates)
                # Add multiple checkins for some dates
                for j in range(1 + (i % 3)):
                    checkin_date = datetime.strptime(dates[date_idx], "%Y-%m-%d")
                    checkin_dates.append(checkin_date)
            
            checkin = {
                "business_id": business["business_id"],
                "dates": checkin_dates
            }
            checkins.append(checkin)
        
        if checkins:
            db.checkins.insert_many(checkins)
        
        # Create business_summaries collection with aggregated data
        if 'business_summaries' in db.list_collection_names():
            db.business_summaries.drop()
        
        business_summaries = []
        for business in businesses:
            # Count reviews and checkins for this business
            business_reviews = [r for r in reviews if r["business_id"] == business["business_id"]]
            business_checkins = next((c for c in checkins if c["business_id"] == business["business_id"]), {"dates": []})
            
            five_star_count = len([r for r in business_reviews if r["stars"] == 5])
            one_star_count = len([r for r in business_reviews if r["stars"] == 1])
            
            summary = {
                "business_id": business["business_id"],
                "name": business["name"],
                "location": business["location"],
                "categories": business["categories"],
                "stars": business["stars"],
                "review_count": business["review_count"],
                "is_open": business["is_open"],
                "review_stats": {
                    "avg_stars": round(sum(r["stars"] for r in business_reviews) / len(business_reviews), 1) if business_reviews else business["stars"],
                    "review_count": len(business_reviews),
                    "five_star_count": five_star_count,
                    "one_star_count": one_star_count
                },
                "checkin_count": len(business_checkins["dates"])
            }
            business_summaries.append(summary)
        
        if business_summaries:
            db.business_summaries.insert_many(business_summaries)
        
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
                    # Try alternative syntax for older Neo4j versions
                    if "ON ... IS" in str(e):
                        try:
                            constraint = constraint.replace(" IF NOT EXISTS", "")
                            if "business_id" in constraint:
                                session.run("CREATE CONSTRAINT ON (b:Business) ASSERT b.business_id IS UNIQUE")
                            elif "user_id" in constraint:
                                session.run("CREATE CONSTRAINT ON (u:User) ASSERT u.user_id IS UNIQUE")
                            elif "review_id" in constraint:
                                session.run("CREATE CONSTRAINT ON (r:Review) ASSERT r.review_id IS UNIQUE")
                            elif "name" in constraint and "Category" in constraint:
                                session.run("CREATE CONSTRAINT ON (c:Category) ASSERT c.name IS UNIQUE")
                        except Exception as e2:
                            print(f"Error with alternate constraint syntax: {e2}")
            
            # Create sample data
            print("Loading sample data into Neo4j...")
            
            # Create locations
            cities = ['San Francisco', 'Los Angeles', 'New York', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia']
            states = ['CA', 'CA', 'NY', 'IL', 'TX', 'AZ', 'PA']
            
            for i, (city, state) in enumerate(zip(cities, states)):
                session.run("""
                    MERGE (l:Location {city: $city, state: $state})
                """, city=city, state=state)
            
            # Create categories - expanded list
            categories = [
                'Restaurants', 'Italian', 'Mexican', 'Chinese', 'Japanese', 'Thai', 'American',
                'Shopping', 'Clothing', 'Department Stores', 'Food', 'Grocery', 'Bakeries', 
                'Beauty & Spas', 'Hair Salons', 'Nail Salons', 'Home Services', 'Contractors', 
                'Plumbing', 'Nightlife', 'Bars', 'Clubs', 'Health & Medical', 'Doctors', 
                'Automotive', 'Auto Repair', 'Active Life', 'Gyms'
            ]
            for category in categories:
                session.run("""
                    MERGE (c:Category {name: $name})
                """, name=category)
            
            # Create businesses with relationships
            for i in range(1, 21):
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
                    'name': f"Neo4j Sample {i}",
                    'stars': round(2.0 + (i % 6) * 0.5, 1),  # Ratings from 2.0-5.0
                    'review_count': 25 + (i * 15),  # Different review counts
                    'is_open': i % 7 != 0  # Most are open
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
                
                # Connect to categories - assign 2-3 categories
                category_count = 2 + (i % 2)  # 2-3 categories
                category_offset = (i * 3) % (len(categories) - category_count)
                
                for j in range(category_count):
                    cat_idx = (category_offset + j) % len(categories)
                    session.run("""
                        MATCH (b:Business {business_id: $business_id})
                        MATCH (c:Category {name: $category})
                        MERGE (b)-[:IN_CATEGORY]->(c)
                    """, {
                        'business_id': f"sample_business_neo4j_{i}",
                        'category': categories[cat_idx]
                    })
            
            # Create time nodes for the last 2 years (monthly)
            for year in [2022, 2023]:
                for month in range(1, 13):
                    # Skip future months in 2023
                    if year == 2023 and month > datetime.now().month:
                        continue
                        
                    day = 15  # Middle of month
                    
                    # Adjust day for February
                    if month == 2 and day > 28:
                        day = 28
                        
                    # Determine quarter
                    quarter = ((month - 1) // 3) + 1
                    
                    session.run("""
                        CREATE (t:Time {
                            date: date($date),
                            year: $year,
                            month: $month,
                            day: $day,
                            quarter: $quarter
                        })
                    """, {
                        'date': f"{year}-{month:02d}-{day:02d}",
                        'year': year,
                        'month': month,
                        'day': day,
                        'quarter': quarter
                    })
            
            # Create users
            for i in range(1, 21):
                session.run("""
                    CREATE (u:User {
                        user_id: $user_id,
                        name: $name,
                        yelping_since: date($yelping_since),
                        review_count: $review_count,
                        fans: $fans,
                        average_stars: $avg_stars
                    })
                """, {
                    'user_id': f"user_neo4j_{i}",
                    'name': f"Neo4j User {i}",
                    'yelping_since': "2020-01-01",
                    'review_count': 10 + (i * 3),
                    'fans': i % 10,
                    'avg_stars': round(3.0 + (i % 4) * 0.5, 1)
                })
            
            # Create reviews with relationships to businesses, users, and time
            # First, check if necessary nodes exist
            print("Creating reviews...")
            
            dates = [
                "2022-01-15", "2022-02-15", "2022-03-15", "2022-04-15", "2022-05-15",
                "2022-06-15", "2022-07-15", "2022-08-15", "2022-09-15", "2022-10-15",
                "2022-11-15", "2022-12-15", "2023-01-15", "2023-02-15"
            ]
            
            # Get all business IDs
            businesses = session.run("""
                MATCH (b:Business) RETURN b.business_id AS id
            """).values()
            business_ids = [record[0] for record in businesses]
            
            # Get all user IDs
            users = session.run("""
                MATCH (u:User) RETURN u.user_id AS id
            """).values()
            user_ids = [record[0] for record in users]
            
            # Create reviews
            review_count = 0
            for i, business_id in enumerate(business_ids):
                # Create 3-5 reviews per business
                num_reviews = 3 + (i % 3)
                
                for j in range(num_reviews):
                    user_id = user_ids[j % len(user_ids)]
                    date_idx = (i + j) % len(dates)
                    date = dates[date_idx]
                    stars = max(1, min(5, 3 + ((i + j) % 3) - 1))  # Ratings 2-5
                    
                    # Create review and connect to business, user, and time
                    session.run("""
                        MATCH (b:Business {business_id: $business_id})
                        MATCH (u:User {user_id: $user_id})
                        MATCH (t:Time {date: date($date)})
                        CREATE (r:Review {
                            review_id: $review_id,
                            stars: $stars,
                            useful: $useful,
                            funny: $funny,
                            cool: $cool,
                            text: $text,
                            date: date($date)
                        })
                        CREATE (u)-[:WROTE]->(r)
                        CREATE (r)-[:REVIEWS]->(b)
                        CREATE (r)-[:ON_DATE]->(t)
                    """, {
                        'review_id': f"review_{business_id}_{j}",
                        'business_id': business_id,
                        'user_id': user_id,
                        'date': date,
                        'stars': stars,
                        'useful': j % 5,
                        'funny': j % 3,
                        'cool': j % 4,
                        'text': f"Neo4j review {j+1} for business {i+1}."
                    })
                    
                    review_count += 1
            
            print(f"Created {review_count} reviews in Neo4j")
            
            # Create checkins
            print("Creating checkins...")
            checkin_count = 0
            
            for i, business_id in enumerate(business_ids):
                # Create 2-4 checkins per business
                num_checkins = 2 + (i % 3)
                
                for j in range(num_checkins):
                    date_idx = (i + j * 2) % len(dates)  # Spread out the dates
                    date = dates[date_idx]
                    count = 1 + (j % 4)  # 1-4 checkins
                    
                    # Create or update checkin relationship
                    session.run("""
                        MATCH (b:Business {business_id: $business_id})
                        MATCH (t:Time {date: date($date)})
                        MERGE (b)-[c:HAD_CHECKIN]->(t)
                        ON CREATE SET c.count = $count
                        ON MATCH SET c.count = c.count + $count
                    """, {
                        'business_id': business_id,
                        'date': date,
                        'count': count
                    })
                    
                    checkin_count += 1
            
            print(f"Created {checkin_count} checkins in Neo4j")
            
            # Update business summary stats
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
    
    # Wait for 10 seconds to ensure all data is committed
    print("\nWaiting 10 seconds for databases to finalize...")
    time.sleep(10)
    
    print("\nData Initializer completed successfully.")