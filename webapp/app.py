from flask import Flask, render_template, request, jsonify
import pymysql
import pymongo
from neo4j import GraphDatabase
import os
import json
import time
from datetime import datetime, timedelta

app = Flask(__name__)

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

# Database connections
def get_mysql_connection():
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
        print("Successfully connected to MySQL")
        return connection
    except Exception as e:
        print(f"MySQL connection error: {e}")
        # Return dummy connection/data for development
        if os.environ.get('FLASK_ENV') == 'development' or os.environ.get('FLASK_DEBUG') == '1':
            print("Returning dummy connection for development")
            return None
        return None

def get_mongodb_connection():
    try:
        connection_string = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/"
        client = pymongo.MongoClient(
            connection_string,
            serverSelectionTimeoutMS=5000  # 5 second timeout
        )
        # Force a connection to verify it works
        client.admin.command('ping')
        print("Successfully connected to MongoDB")
        return client
    except Exception as e:
        print(f"MongoDB connection error: {e}")
        return None

def get_neo4j_driver():
    try:
        driver = GraphDatabase.driver(
            f"bolt://{NEO4J_HOST}:{NEO4J_PORT}",
            auth=(NEO4J_USER, NEO4J_PASSWORD)
        )
        # Test connection
        with driver.session() as session:
            session.run("RETURN 1")
        print("Successfully connected to Neo4j")
        return driver
    except Exception as e:
        print(f"Neo4j connection error: {e}")
        return None

# Database Initialization Functions
def initialize_mysql():
    """Initialize MySQL with schema and sample data"""
    print("\nInitializing MySQL...")
    
    conn = get_mysql_connection()
    if not conn:
        print("Cannot connect to MySQL for initialization")
        return False
    
    try:
        cursor = conn.cursor()
        
        # Check if tables exist
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        tables = [list(t.values())[0] for t in tables]
        
        # If tables exist and have data, skip initialization
        if 'dim_business' in tables:
            cursor.execute("SELECT COUNT(*) as count FROM dim_business")
            count = cursor.fetchone()['count']
            if count > 0:
                print("MySQL already has data. Skipping initialization.")
                return True
        
        # Create basic schema
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
        
        # Create summary table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS summary_business_performance (
                business_id VARCHAR(100) PRIMARY KEY,
                total_reviews INT,
                avg_rating DECIMAL(2,1),
                total_checkins INT,
                FOREIGN KEY (business_id) REFERENCES dim_business(business_id)
            )
        """)
        
        # Insert sample data
        print("Inserting sample data...")
        
        # Insert sample locations
        cursor.execute("""
            INSERT INTO dim_location (city, state, postal_code, latitude, longitude)
            VALUES 
            ('San Francisco', 'CA', '94103', 37.7749, -122.4194),
            ('Los Angeles', 'CA', '90001', 34.0522, -118.2437),
            ('New York', 'NY', '10001', 40.7128, -74.0060)
        """)
        
        # Get location IDs
        cursor.execute("SELECT location_id FROM dim_location")
        location_ids = [row['location_id'] for row in cursor.fetchall()]
        
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
        business_ids = [row['business_id'] for row in cursor.fetchall()]
        
        cursor.execute("SELECT category_id FROM dim_category")
        category_ids = [row['category_id'] for row in cursor.fetchall()]
        
        for business_id in business_ids:
            for category_id in category_ids[:2]:  # Assign first two categories to each business
                cursor.execute("""
                    INSERT INTO business_category (business_id, category_id)
                    VALUES (%s, %s)
                """, (business_id, category_id))
        
        conn.commit()
        print("MySQL initialization completed successfully")
        return True
    
    except Exception as e:
        print(f"Error initializing MySQL: {e}")
        return False
    finally:
        if conn:
            conn.close()

def initialize_mongodb():
    """Initialize MongoDB with collections and sample data"""
    print("\nInitializing MongoDB...")
    
    client = get_mongodb_connection()
    if not client:
        print("Cannot connect to MongoDB for initialization")
        return False
    
    try:
        db = client.yelp_db
        
        # Check if collections exist and have data
        collections = db.list_collection_names()
        print(f"MongoDB collections found: {collections}")
        
        if 'businesses' in collections:
            count = db.businesses.count_documents({})
            print(f"Business count: {count}")
            
            if count > 0:
                print("MongoDB already has data. Skipping initialization.")
                client.close()
                return True
        
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
        print("Inserting sample data into MongoDB...")
        
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
        
        print("MongoDB initialization completed successfully")
        client.close()
        return True
    
    except Exception as e:
        print(f"Error initializing MongoDB: {e}")
        return False

def initialize_neo4j():
    """Initialize Neo4j with constraints and sample data"""
    print("\nInitializing Neo4j...")
    
    driver = get_neo4j_driver()
    if not driver:
        print("Cannot connect to Neo4j for initialization")
        return False
    
    try:
        with driver.session() as session:
            # Check if nodes exist
            result = session.run("MATCH (n) RETURN count(n) as count")
            count = result.single()["count"]
            print(f"Node count: {count}")
            
            if count > 0:
                print("Neo4j already has data. Skipping initialization.")
                return True
            
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
            print("Inserting sample data into Neo4j...")
            
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
        
        print("Neo4j initialization completed successfully")
        driver.close()
        return True
    
    except Exception as e:
        print(f"Error initializing Neo4j: {e}")
        return False

def initialize_databases():
    """Initialize all databases with schema and sample data"""
    print("Starting database initialization...")
    
    # Wait for databases to be ready
    print("Waiting for databases to be ready...")
    time.sleep(15)  # Give databases time to start
    
    # Initialize each database
    mysql_initialized = initialize_mysql()
    mongodb_initialized = initialize_mongodb()
    neo4j_initialized = initialize_neo4j()
    
    # Summary
    print("\nInitialization Summary:")
    print(f"MySQL:   {'✓ Success' if mysql_initialized else '✗ Failed'}")
    print(f"MongoDB: {'✓ Success' if mongodb_initialized else '✗ Failed'}")
    print(f"Neo4j:   {'✓ Success' if neo4j_initialized else '✗ Failed'}")
    
    return mysql_initialized and mongodb_initialized and neo4j_initialized

# Routes
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/mysql/top_businesses')
def mysql_top_businesses():
    category = request.args.get('category', 'Restaurants')
    limit = int(request.args.get('limit', 10))
    
    conn = get_mysql_connection()
    if not conn:
        # Return dummy data for development
        if os.environ.get('FLASK_ENV') == 'development' or os.environ.get('FLASK_DEBUG') == '1':
            print("Returning dummy data for MySQL top businesses")
            dummy_data = [
                {
                    "business_id": "sample_business_1",
                    "business_name": "Sample Business 1",
                    "stars": 4.5,
                    "review_count": 100,
                    "city": "San Francisco",
                    "state": "CA"
                },
                {
                    "business_id": "sample_business_2",
                    "business_name": "Sample Business 2",
                    "stars": 4.0,
                    "review_count": 200,
                    "city": "Los Angeles",
                    "state": "CA"
                },
                {
                    "business_id": "sample_business_3",
                    "business_name": "Sample Business 3",
                    "stars": 4.5,
                    "review_count": 300,
                    "city": "New York",
                    "state": "NY"
                }
            ]
            return jsonify(dummy_data)
        return jsonify({"error": "MySQL connection failed"}), 503
    
    try:
        cursor = conn.cursor()
        
        query = """
            SELECT b.business_id, b.business_name, b.stars, b.review_count, 
                   l.city, l.state
            FROM dim_business b
            JOIN dim_location l ON b.location_id = l.location_id
            JOIN business_category bc ON b.business_id = bc.business_id
            JOIN dim_category c ON bc.category_id = c.category_id
            WHERE c.category_name = %s
            ORDER BY b.stars DESC, b.review_count DESC
            LIMIT %s
        """
        
        cursor.execute(query, (category, limit))
        results = cursor.fetchall()
        
        # Handle potential serialization issues with Decimal and datetime types
        import decimal
        from datetime import datetime, date
        
        serializable_results = []
        for row in results:
            serializable_row = {}
            for key, value in row.items():
                if isinstance(value, decimal.Decimal):
                    serializable_row[key] = float(value)
                elif isinstance(value, datetime):
                    serializable_row[key] = value.isoformat()
                elif isinstance(value, date):
                    serializable_row[key] = value.isoformat()
                else:
                    serializable_row[key] = value
            serializable_results.append(serializable_row)
        
        cursor.close()
        conn.close()
        
        return jsonify(serializable_results)
    except Exception as e:
        if conn:
            conn.close()
        print(f"Error in mysql_top_businesses: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/mongodb/top_businesses')
def mongodb_top_businesses():
    category = request.args.get('category', 'Restaurants')
    limit = int(request.args.get('limit', 10))
    
    client = get_mongodb_connection()
    if not client:
        # Return dummy data for development
        if os.environ.get('FLASK_ENV') == 'development' or os.environ.get('FLASK_DEBUG') == '1':
            print("Returning dummy data for MongoDB top businesses")
            dummy_data = [
                {
                    "business_id": "sample_business_mongo_1",
                    "name": "Sample MongoDB Business 1",
                    "stars": 4.5,
                    "review_count": 100,
                    "city": "San Francisco",
                    "state": "CA"
                },
                {
                    "business_id": "sample_business_mongo_2",
                    "name": "Sample MongoDB Business 2",
                    "stars": 4.0,
                    "review_count": 200,
                    "city": "Los Angeles",
                    "state": "CA"
                }
            ]
            return jsonify(dummy_data)
        return jsonify({"error": "MongoDB connection failed"}), 503
    
    try:
        db = client.yelp_db
        results = list(db.businesses.find(
            {"categories": {"$in": [category]}},
            {"_id": 0, "business_id": 1, "name": 1, "stars": 1, "review_count": 1, 
             "city": "$location.city", "state": "$location.state"}
        ).sort([("stars", -1), ("review_count", -1)]).limit(limit))
        
        return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if client:
            client.close()

@app.route('/api/neo4j/top_businesses')
def neo4j_top_businesses():
    category = request.args.get('category', 'Restaurants')
    limit = int(request.args.get('limit', 10))
    
    driver = get_neo4j_driver()
    if not driver:
        # Return dummy data for development
        if os.environ.get('FLASK_ENV') == 'development' or os.environ.get('FLASK_DEBUG') == '1':
            print("Returning dummy data for Neo4j top businesses")
            dummy_data = [
                {
                    "business_id": "sample_business_neo4j_1",
                    "business_name": "Sample Neo4j Business 1",
                    "stars": 4.5,
                    "review_count": 100,
                    "city": "San Francisco",
                    "state": "CA"
                },
                {
                    "business_id": "sample_business_neo4j_2",
                    "business_name": "Sample Neo4j Business 2",
                    "stars": 4.0,
                    "review_count": 200,
                    "city": "Los Angeles",
                    "state": "CA"
                }
            ]
            return jsonify(dummy_data)
        return jsonify({"error": "Neo4j connection failed"}), 503
    
    try:
        with driver.session() as session:
            query = """
                MATCH (b:Business)-[:IN_CATEGORY]->(c:Category {name: $category})
                MATCH (b)-[:LOCATED_IN]->(l:Location)
                RETURN b.business_id as business_id, b.name as business_name, 
                       b.stars as stars, b.review_count as review_count,
                       l.city as city, l.state as state
                ORDER BY b.stars DESC, b.review_count DESC
                LIMIT $limit
            """
            
            result = session.run(query, category=category, limit=limit)
            records = [record.data() for record in result]
            
            return jsonify(records)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if driver:
            driver.close()

@app.route('/api/health')
def health():
    """Check health of database connections."""
    status = {
        "mysql": get_mysql_connection() is not None,
        "mongodb": get_mongodb_connection() is not None,
        "neo4j": get_neo4j_driver() is not None
    }
    overall = all(status.values())
    
    return jsonify({
        "status": "healthy" if overall else "unhealthy",
        "services": status,
        "timestamp": datetime.now().isoformat()
    }), 200 if overall else 503

@app.route('/api/test')
def test():
    """Simple test endpoint to verify the API is working."""
    return jsonify({
        "status": "ok",
        "message": "API is working",
        "timestamp": datetime.now().isoformat()
    })

if __name__ == '__main__':
    # Check if we should initialize databases
    if os.environ.get('INITIALIZE_DB', 'false').lower() == 'true':
        initialize_databases()
    else:
        print("Database initialization skipped (INITIALIZE_DB not set to true)")
    
    # Add a delay to ensure databases are ready
    print("Starting Flask application on 0.0.0.0:8080...")
    app.run(host='0.0.0.0', port=8080, debug=True)