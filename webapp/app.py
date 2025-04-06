from flask import Flask, render_template, request, jsonify
import pymysql
import pymongo
from neo4j import GraphDatabase
import os
import json
import time
import logging
from datetime import datetime, timedelta
import decimal
import random
from collections import defaultdict
from datetime import datetime, date


# Set up logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('webapp')

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

# Routes
@app.route('/')
def index():
    """Render the main dashboard page"""
    return render_template('index.html')

# ---------------------------
# Health and Debug Endpoints
# ---------------------------

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

@app.route('/api/debug')
def debug():
    """Endpoint for checking database connection details and configuration."""
    debug_info = {
        "environment": {
            "FLASK_APP": os.environ.get('FLASK_APP'),
            "FLASK_ENV": os.environ.get('FLASK_ENV'),
            "FLASK_DEBUG": os.environ.get('FLASK_DEBUG'),
        },
        "mysql_config": {
            "host": MYSQL_HOST,
            "port": MYSQL_PORT,
            "user": MYSQL_USER,
            "database": MYSQL_DATABASE,
        },
        "mongodb_config": {
            "host": MONGO_HOST,
            "port": MONGO_PORT,
            "user": MONGO_USER,
        },
        "neo4j_config": {
            "host": NEO4J_HOST,
            "port": NEO4J_PORT,
            "user": NEO4J_USER,
        },
        "mysql_tables": [],
        "mongodb_collections": [],
        "neo4j_node_counts": {}
    }
    
    # Try to get MySQL tables
    conn = get_mysql_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("SHOW TABLES")
            debug_info["mysql_tables"] = [row[list(row.keys())[0]] for row in cursor.fetchall()]
            
            # Get row counts for important tables
            table_counts = {}
            for table in debug_info["mysql_tables"]:
                cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
                table_counts[table] = cursor.fetchone()['count']
            debug_info["mysql_table_counts"] = table_counts
            
            cursor.close()
            conn.close()
        except Exception as e:
            debug_info["mysql_error"] = str(e)
    
    # Try to get MongoDB collections
    client = get_mongodb_connection()
    if client:
        try:
            db = client.yelp_db
            debug_info["mongodb_collections"] = db.list_collection_names()
            
            # Get document counts for each collection
            collection_counts = {}
            for collection in debug_info["mongodb_collections"]:
                collection_counts[collection] = db[collection].count_documents({})
            debug_info["mongodb_collection_counts"] = collection_counts
            
            client.close()
        except Exception as e:
            debug_info["mongodb_error"] = str(e)
    
    # Try to get Neo4j node counts
    driver = get_neo4j_driver()
    if driver:
        try:
            with driver.session() as session:
                result = session.run("""
                    MATCH (n)
                    RETURN labels(n)[0] as label, count(*) as count
                    ORDER BY label
                """)
                debug_info["neo4j_node_counts"] = {record["label"]: record["count"] for record in result}
                
                # Get relationship counts
                result = session.run("""
                    MATCH ()-[r]->()
                    RETURN type(r) as type, count(*) as count
                    ORDER BY type
                """)
                debug_info["neo4j_relationship_counts"] = {record["type"]: record["count"] for record in result}
            
            driver.close()
        except Exception as e:
            debug_info["neo4j_error"] = str(e)
    
    return jsonify(debug_info)

@app.route('/api/test')
def test():
    """Simple test endpoint to verify the API is working."""
    return jsonify({
        "status": "ok",
        "message": "API is working",
        "timestamp": datetime.now().isoformat()
    })

# ---------------------------
# MySQL API Endpoints
# ---------------------------

@app.route('/api/mysql/overview_stats')
def mysql_overview_stats():
    """Get overview statistics from MySQL for dashboard"""
    conn = get_mysql_connection()
    if not conn:
        # Return dummy data
        dummy_data = {
            "business_count": 15,
            "review_count": 75,
            "user_count": 20,
            "category_stats": [
                {"category_name": "Restaurants", "count": 5},
                {"category_name": "Shopping", "count": 3},
                {"category_name": "Food", "count": 2},
                {"category_name": "Beauty & Spas", "count": 2},
                {"category_name": "Home Services", "count": 1}
            ],
            "star_distribution": [
                {"stars": 5, "count": 10},
                {"stars": 4, "count": 25},
                {"stars": 3, "count": 20},
                {"stars": 2, "count": 15},
                {"stars": 1, "count": 5}
            ]
        }
        return jsonify(dummy_data)
    
    try:
        cursor = conn.cursor()
        
        # Get business count
        cursor.execute("SELECT COUNT(*) as count FROM dim_business")
        business_count = cursor.fetchone()['count']
        
        # Get review count
        cursor.execute("SELECT COUNT(*) as count FROM fact_review")
        review_count = cursor.fetchone()['count']
        
        # Get user count
        cursor.execute("SELECT COUNT(*) as count FROM dim_user")
        user_count = cursor.fetchone()['count']
        
        # Get category stats
        cursor.execute("""
            SELECT c.category_name, COUNT(bc.business_id) as count
            FROM dim_category c
            JOIN business_category bc ON c.category_id = bc.category_id
            GROUP BY c.category_name
            ORDER BY count DESC
            LIMIT 10
        """)
        category_stats = cursor.fetchall()
        
        # Get star distribution
        cursor.execute("""
            SELECT stars, COUNT(*) as count
            FROM fact_review
            GROUP BY stars
            ORDER BY stars
        """)
        star_distribution = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        result = {
            "business_count": business_count,
            "review_count": review_count,
            "user_count": user_count,
            "category_stats": category_stats,
            "star_distribution": star_distribution
        }
        
        return jsonify(result)
    except Exception as e:
        if conn:
            conn.close()
        logger.error(f"Error in mysql_overview_stats: {str(e)}")
        
        # Return dummy data on error
        dummy_data = {
            "business_count": 15,
            "review_count": 75,
            "user_count": 20,
            "category_stats": [
                {"category_name": "Restaurants", "count": 5},
                {"category_name": "Shopping", "count": 3},
                {"category_name": "Food", "count": 2},
                {"category_name": "Beauty & Spas", "count": 2},
                {"category_name": "Home Services", "count": 1}
            ],
            "star_distribution": [
                {"stars": 5, "count": 10},
                {"stars": 4, "count": 25},
                {"stars": 3, "count": 20},
                {"stars": 2, "count": 15},
                {"stars": 1, "count": 5}
            ]
        }
        return jsonify(dummy_data)

@app.route('/api/mysql/top_businesses')
def mysql_top_businesses():
    category = request.args.get('category', 'Restaurants')
    limit = int(request.args.get('limit', 10))
    
    conn = get_mysql_connection()
    if not conn:
        # Return dummy data
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
        logger.error(f"Error in mysql_top_businesses: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/mysql/business_performance')
def mysql_business_performance():
    business_id = request.args.get('business_id')
    
    if not business_id:
        return jsonify({"error": "Business ID is required"}), 400
    
    conn = get_mysql_connection()
    if not conn:
        # Return dummy data
        dummy_data = {
            "business": {
                "business_id": business_id,
                "business_name": "Sample Business",
                "city": "San Francisco",
                "state": "CA",
                "stars": 4.5,
                "review_count": 120,
                "avg_rating": 4.3,
                "total_checkins": 250
            },
            "reviews_by_month": [
                {"year": 2022, "month": 1, "review_count": 5},
                {"year": 2022, "month": 2, "review_count": 8},
                {"year": 2022, "month": 3, "review_count": 6},
                {"year": 2022, "month": 4, "review_count": 7},
                {"year": 2022, "month": 5, "review_count": 10}
            ],
            "checkins_by_month": [
                {"year": 2022, "month": 1, "checkin_count": 12},
                {"year": 2022, "month": 2, "checkin_count": 18},
                {"year": 2022, "month": 3, "checkin_count": 15},
                {"year": 2022, "month": 4, "checkin_count": 20},
                {"year": 2022, "month": 5, "checkin_count": 25}
            ]
        }
        return jsonify(dummy_data)
    
    try:
        cursor = conn.cursor()
        
        # Get business details
        cursor.execute("""
            SELECT b.*, l.city, l.state, 
                   s.total_reviews, s.avg_rating, s.total_checkins
            FROM dim_business b
            JOIN dim_location l ON b.location_id = l.location_id
            LEFT JOIN summary_business_performance s ON b.business_id = s.business_id
            WHERE b.business_id = %s
        """, (business_id,))
        
        business = cursor.fetchone()
        
        if not business:
            cursor.close()
            conn.close()
            return jsonify({"error": "Business not found"}), 404
        
        # Get reviews by month
        cursor.execute("""
            SELECT YEAR(t.date_actual) as year, 
                   MONTH(t.date_actual) as month, 
                   COUNT(*) as review_count
            FROM fact_review r
            JOIN dim_time t ON r.time_id = t.time_id
            WHERE r.business_id = %s
            GROUP BY YEAR(t.date_actual), MONTH(t.date_actual)
            ORDER BY year, month
        """, (business_id,))
        
        reviews_by_month = cursor.fetchall()
        
        # Get checkins by month
        cursor.execute("""
            SELECT YEAR(t.date_actual) as year, 
                   MONTH(t.date_actual) as month, 
                   SUM(c.checkin_count) as checkin_count
            FROM fact_checkin c
            JOIN dim_time t ON c.time_id = t.time_id
            WHERE c.business_id = %s
            GROUP BY YEAR(t.date_actual), MONTH(t.date_actual)
            ORDER BY year, month
        """, (business_id,))
        
        checkins_by_month = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        # Convert values for JSON serialization
        business = json.loads(json.dumps(business, default=json_serialize))
        reviews_by_month = json.loads(json.dumps(reviews_by_month, default=json_serialize))
        checkins_by_month = json.loads(json.dumps(checkins_by_month, default=json_serialize))
        
        result = {
            "business": business,
            "reviews_by_month": reviews_by_month,
            "checkins_by_month": checkins_by_month
        }
        
        return jsonify(result)
    except Exception as e:
        if conn:
            conn.close()
        logger.error(f"Error in mysql_business_performance: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/mysql/review_trends')
def mysql_review_trends():
    """Get review trends over time for MySQL analytics tab"""
    conn = get_mysql_connection()
    if not conn:
        # Return dummy data
        months = 12
        dates = [(datetime.now() - timedelta(days=30*i)).strftime('%Y-%m-01') for i in range(months)]
        dates.reverse()
        
        dummy_data = {
            "dates": dates,
            "review_counts": [random.randint(20, 100) for _ in range(months)]
        }
        return jsonify(dummy_data)
    
    try:
        cursor = conn.cursor()
        
        # Get review counts by month
        cursor.execute("""
            SELECT DATE_FORMAT(t.date_actual, '%Y-%m-01') as month_date, 
                   COUNT(*) as review_count
            FROM fact_review r
            JOIN dim_time t ON r.time_id = t.time_id
            GROUP BY month_date
            ORDER BY month_date
            LIMIT 12
        """)
        
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        
        dates = [row['month_date'] for row in results]
        review_counts = [row['review_count'] for row in results]
        
        # Convert dates for JSON serialization
        dates = [date.strftime('%Y-%m-%d') if isinstance(date, datetime) else date for date in dates]
        
        return jsonify({
            "dates": dates,
            "review_counts": review_counts
        })
    except Exception as e:
        if conn:
            conn.close()
        logger.error(f"Error in mysql_review_trends: {str(e)}")
        
        # Return dummy data on error
        months = 12
        dates = [(datetime.now() - timedelta(days=30*i)).strftime('%Y-%m-01') for i in range(months)]
        dates.reverse()
        
        dummy_data = {
            "dates": dates,
            "review_counts": [random.randint(20, 100) for _ in range(months)]
        }
        return jsonify(dummy_data)

@app.route('/api/mysql/category_ratings')
def mysql_category_ratings():
    """Get average ratings by category for MySQL analytics tab"""
    conn = get_mysql_connection()
    if not conn:
        # Return dummy data
        categories = ["Restaurants", "Shopping", "Food", "Beauty & Spas", "Home Services", 
                      "Coffee & Tea", "Bars", "Fast Food", "Pizza", "Italian"]
        
        dummy_data = {
            "categories": categories,
            "avg_ratings": [round(random.uniform(3.0, 4.8), 1) for _ in range(len(categories))]
        }
        return jsonify(dummy_data)
    
    try:
        cursor = conn.cursor()
        
        # Get average ratings by category
        cursor.execute("""
            SELECT c.category_name, AVG(r.stars) as avg_rating
            FROM dim_category c
            JOIN business_category bc ON c.category_id = bc.category_id
            JOIN fact_review r ON bc.business_id = r.business_id
            GROUP BY c.category_name
            ORDER BY avg_rating DESC
            LIMIT 10
        """)
        
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        
        categories = [row['category_name'] for row in results]
        avg_ratings = [float(row['avg_rating']) for row in results]
        
        return jsonify({
            "categories": categories,
            "avg_ratings": avg_ratings
        })
    except Exception as e:
        if conn:
            conn.close()
        logger.error(f"Error in mysql_category_ratings: {str(e)}")
        
        # Return dummy data on error
        categories = ["Restaurants", "Shopping", "Food", "Beauty & Spas", "Home Services", 
                      "Coffee & Tea", "Bars", "Fast Food", "Pizza", "Italian"]
        
        dummy_data = {
            "categories": categories,
            "avg_ratings": [round(random.uniform(3.0, 4.8), 1) for _ in range(len(categories))]
        }
        return jsonify(dummy_data)

# ---------------------------
# MongoDB API Endpoints
# ---------------------------

@app.route('/api/mongodb/overview_stats')
def mongodb_overview_stats():
    """Get overview statistics from MongoDB for dashboard"""
    client = get_mongodb_connection()
    if not client:
        # Return dummy data
        dummy_data = {
            "business_count": 18,
            "review_count": 85,
            "user_count": 25,
            "category_stats": [
                {"category": "Restaurants", "count": 6},
                {"category": "Shopping", "count": 4},
                {"category": "Food", "count": 3},
                {"category": "Beauty & Spas", "count": 2},
                {"category": "Home Services", "count": 2}
            ],
            "star_distribution": [
                {"stars": 5, "count": 15},
                {"stars": 4, "count": 30},
                {"stars": 3, "count": 25},
                {"stars": 2, "count": 10},
                {"stars": 1, "count": 5}
            ]
        }
        return jsonify(dummy_data)
    
    try:
        db = client.yelp_db
        
        # Get business count
        business_count = db.businesses.count_documents({})
        
        # Get review count
        review_count = db.reviews.count_documents({})
        
        # Get user count
        user_count = db.users.count_documents({})
        
        # Get category stats using aggregation
        pipeline = [
            {"$unwind": "$categories"},
            {"$group": {"_id": "$categories", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 10},
            {"$project": {"category": "$_id", "count": 1, "_id": 0}}
        ]
        category_stats = list(db.businesses.aggregate(pipeline))
        
        # Get star distribution using aggregation
        pipeline = [
            {"$group": {"_id": "$stars", "count": {"$sum": 1}}},
            {"$sort": {"_id": 1}},
            {"$project": {"stars": "$_id", "count": 1, "_id": 0}}
        ]
        star_distribution = list(db.reviews.aggregate(pipeline))
        
        client.close()
        
        result = {
            "business_count": business_count,
            "review_count": review_count,
            "user_count": user_count,
            "category_stats": category_stats,
            "star_distribution": star_distribution
        }
        
        return jsonify(result)
    except Exception as e:
        if client:
            client.close()
        logger.error(f"Error in mongodb_overview_stats: {str(e)}")
        
        # Return dummy data on error
        dummy_data = {
            "business_count": 18,
            "review_count": 85,
            "user_count": 25,
            "category_stats": [
                {"category": "Restaurants", "count": 6},
                {"category": "Shopping", "count": 4},
                {"category": "Food", "count": 3},
                {"category": "Beauty & Spas", "count": 2},
                {"category": "Home Services", "count": 2}
            ],
            "star_distribution": [
                {"stars": 5, "count": 15},
                {"stars": 4, "count": 30},
                {"stars": 3, "count": 25},
                {"stars": 2, "count": 10},
                {"stars": 1, "count": 5}
            ]
        }
        return jsonify(dummy_data)

@app.route('/api/mongodb/top_businesses')
def mongodb_top_businesses():
    category = request.args.get('category', 'Restaurants')
    limit = int(request.args.get('limit', 10))
    
    client = get_mongodb_connection()
    if not client:
        # Return dummy data
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
            },
            {
                "business_id": "sample_business_mongo_3",
                "name": "Sample MongoDB Business 3",
                "stars": 4.8,
                "review_count": 150,
                "city": "New York",
                "state": "NY"
            }
        ]
        return jsonify(dummy_data)
    
    try:
        db = client.yelp_db
        
        pipeline = [
            {"$match": {"categories": {"$in": [category]}}},
            {"$project": {
                "_id": 0,
                "business_id": 1,
                "name": 1,
                "stars": 1,
                "review_count": 1,
                "city": "$location.city",
                "state": "$location.state"
            }},
            {"$sort": {"stars": -1, "review_count": -1}},
            {"$limit": limit}
        ]
        
        results = list(db.businesses.aggregate(pipeline))
        client.close()
        
        return jsonify(results)
    except Exception as e:
        if client:
            client.close()
        logger.error(f"Error in mongodb_top_businesses: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/mongodb/business_performance')
def mongodb_business_performance():
    business_id = request.args.get('business_id')
    
    if not business_id:
        return jsonify({"error": "Business ID is required"}), 400
    
    client = get_mongodb_connection()
    if not client:
        # Return dummy data
        dummy_data = {
            "business": {
                "business_id": business_id,
                "name": "Sample MongoDB Business",
                "city": "San Francisco", 
                "state": "CA",
                "stars": 4.5,
                "review_count": 120,
                "review_stats": {
                    "avg_stars": 4.3,
                    "review_count": 120,
                    "five_star_count": 60,
                    "one_star_count": 5
                },
                "checkins": {
                    "total_checkins": 250
                }
            },
            "reviews_by_month": [
                {"year": 2022, "month": 1, "review_count": 5},
                {"year": 2022, "month": 2, "review_count": 8},
                {"year": 2022, "month": 3, "review_count": 6},
                {"year": 2022, "month": 4, "review_count": 7},
                {"year": 2022, "month": 5, "review_count": 10}
            ],
            "checkins_by_month": [
                {"year": 2022, "month": 1, "checkin_count": 12},
                {"year": 2022, "month": 2, "checkin_count": 18},
                {"year": 2022, "month": 3, "checkin_count": 15},
                {"year": 2022, "month": 4, "checkin_count": 20},
                {"year": 2022, "month": 5, "checkin_count": 25}
            ]
        }
        return jsonify(dummy_data)
    
    try:
        db = client.yelp_db
        
        # Get business details
        business = db.business_summaries.find_one(
            {"business_id": business_id},
            {"_id": 0}
        )
        
        if not business:
            # Try to get from businesses collection
            business = db.businesses.find_one(
                {"business_id": business_id},
                {"_id": 0}
            )
            
            if not business:
                client.close()
                return jsonify({"error": "Business not found"}), 404
        
        # Extract city and state
        if 'location' in business and isinstance(business['location'], dict):
            business['city'] = business['location'].get('city')
            business['state'] = business['location'].get('state')
        
        # Get reviews by month
        pipeline = [
            {"$match": {"business_id": business_id}},
            {"$project": {
                "year": {"$year": "$date"},
                "month": {"$month": "$date"}
            }},
            {"$group": {
                "_id": {"year": "$year", "month": "$month"},
                "review_count": {"$sum": 1}
            }},
            {"$project": {
                "_id": 0,
                "year": "$_id.year",
                "month": "$_id.month",
                "review_count": 1
            }},
            {"$sort": {"year": 1, "month": 1}}
        ]
        
        reviews_by_month = list(db.reviews.aggregate(pipeline))
        
        # Get checkins by month
        pipeline = [
            {"$match": {"business_id": business_id}},
            {"$unwind": "$dates"},
            {"$project": {
                "year": {"$year": "$dates"},
                "month": {"$month": "$dates"}
            }},
            {"$group": {
                "_id": {"year": "$year", "month": "$month"},
                "checkin_count": {"$sum": 1}
            }},
            {"$project": {
                "_id": 0,
                "year": "$_id.year",
                "month": "$_id.month",
                "checkin_count": 1
            }},
            {"$sort": {"year": 1, "month": 1}}
        ]
        
        checkins_by_month = list(db.checkins.aggregate(pipeline))
        
        client.close()
        
        result = {
            "business": business,
            "reviews_by_month": reviews_by_month,
            "checkins_by_month": checkins_by_month
        }
        
        return jsonify(result)
    except Exception as e:
        if client:
            client.close()
        logger.error(f"Error in mongodb_business_performance: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/mongodb/document_size_stats')
def mongodb_document_size_stats():
    """Get document size distribution for MongoDB analytics"""
    client = get_mongodb_connection()
    if not client:
        # Return dummy data
        size_ranges = ["<1KB", "1-5KB", "5-10KB", "10-50KB", "50-100KB", ">100KB"]
        
        dummy_data = {
            "size_ranges": size_ranges,
            "document_counts": [random.randint(5, 50) for _ in range(len(size_ranges))]
        }
        return jsonify(dummy_data)
    
    try:
        db = client.yelp_db
        
        # For this demonstration, we'll compute approximate sizes
        size_ranges = ["<1KB", "1-5KB", "5-10KB", "10-50KB", "50-100KB", ">100KB"]
        document_counts = [0, 0, 0, 0, 0, 0]
        
        # Sample some documents from each collection to estimate size
        collections = ["businesses", "reviews", "users", "checkins"]
        
        for collection_name in collections:
            collection = db[collection_name]
            sample_size = min(100, collection.count_documents({}))
            
            if sample_size == 0:
                continue
                
            for doc in collection.find().limit(sample_size):
                # Estimate document size in bytes
                size_in_kb = len(json.dumps(doc)) / 1024
                
                if size_in_kb < 1:
                    document_counts[0] += 1
                elif size_in_kb < 5:
                    document_counts[1] += 1
                elif size_in_kb < 10:
                    document_counts[2] += 1
                elif size_in_kb < 50:
                    document_counts[3] += 1
                elif size_in_kb < 100:
                    document_counts[4] += 1
                else:
                    document_counts[5] += 1
        
        client.close()
        
        return jsonify({
            "size_ranges": size_ranges,
            "document_counts": document_counts
        })
    except Exception as e:
        if client:
            client.close()
        logger.error(f"Error in mongodb_document_size_stats: {str(e)}")
        
        # Return dummy data on error
        size_ranges = ["<1KB", "1-5KB", "5-10KB", "10-50KB", "50-100KB", ">100KB"]
        
        dummy_data = {
            "size_ranges": size_ranges,
            "document_counts": [random.randint(5, 50) for _ in range(len(size_ranges))]
        }
        return jsonify(dummy_data)

@app.route('/api/mongodb/business_attributes')
def mongodb_business_attributes():
    """Get business attributes analysis for MongoDB analytics"""
    client = get_mongodb_connection()
    if not client:
        # Return dummy data
        attribute_names = ["TakesReservations", "Delivery", "Takeout", "WiFi", 
                          "OutdoorSeating", "GoodForKids", "Parking", "BikeParking"]
        
        dummy_data = {
            "attribute_names": attribute_names,
            "attribute_counts": [random.randint(10, 100) for _ in range(len(attribute_names))]
        }
        return jsonify(dummy_data)
    
    try:
        db = client.yelp_db
        
        # Get all attribute keys
        pipeline = [
            {"$match": {"attributes": {"$exists": true}}},
            {"$project": {"attributeKeys": {"$objectToArray": "$attributes"}}},
            {"$unwind": "$attributeKeys"},
            {"$group": {"_id": "$attributeKeys.k", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 10}
        ]
        
        results = list(db.businesses.aggregate(pipeline))
        
        attribute_names = [result["_id"] for result in results]
        attribute_counts = [result["count"] for result in results]
        
        client.close()
        
        return jsonify({
            "attribute_names": attribute_names,
            "attribute_counts": attribute_counts
        })
    except Exception as e:
        if client:
            client.close()
        logger.error(f"Error in mongodb_business_attributes: {str(e)}")
        
        # Return dummy data on error
        attribute_names = ["TakesReservations", "Delivery", "Takeout", "WiFi", 
                          "OutdoorSeating", "GoodForKids", "Parking", "BikeParking"]
        
        dummy_data = {
            "attribute_names": attribute_names,
            "attribute_counts": [random.randint(10, 100) for _ in range(len(attribute_names))]
        }
        return jsonify(dummy_data)

# ---------------------------
# Neo4j API Endpoints
# ---------------------------

@app.route('/api/neo4j/overview_stats')
def neo4j_overview_stats():
    """Get overview statistics from Neo4j for dashboard"""
    driver = get_neo4j_driver()
    if not driver:
        # Return dummy data
        dummy_data = {
            "business_count": 15,
            "review_count": 75,
            "user_count": 20,
            "relationship_count": 150,
            "category_stats": [
                {"category": "Restaurants", "count": 5},
                {"category": "Shopping", "count": 3},
                {"category": "Food", "count": 2},
                {"category": "Beauty & Spas", "count": 2},
                {"category": "Home Services", "count": 1}
            ],
            "relationship_stats": [
                {"type": "LOCATED_IN", "count": 15},
                {"type": "IN_CATEGORY", "count": 30},
                {"type": "WROTE", "count": 75},
                {"type": "REVIEWS", "count": 75},
                {"type": "HAD_CHECKIN", "count": 30}
            ]
        }
        return jsonify(dummy_data)
    
    try:
        with driver.session() as session:
            # Get node counts
            result = session.run("""
                MATCH (b:Business) WITH count(b) AS business_count
                MATCH (r:Review) WITH business_count, count(r) AS review_count
                MATCH (u:User) WITH business_count, review_count, count(u) AS user_count
                RETURN business_count, review_count, user_count
            """)
            counts = result.single()
            
            business_count = counts["business_count"]
            review_count = counts["review_count"]
            user_count = counts["user_count"]
            
            # Get relationship count
            result = session.run("MATCH ()-[r]->() RETURN count(r) AS relationship_count")
            relationship_count = result.single()["relationship_count"]
            
            # Get category stats
            result = session.run("""
                MATCH (c:Category)<-[r:IN_CATEGORY]-(b:Business)
                WITH c.name AS category, count(b) AS count
                ORDER BY count DESC
                LIMIT 10
                RETURN category, count
            """)
            category_stats = [{"category": record["category"], "count": record["count"]} for record in result]
            
            # Get relationship type distribution
            result = session.run("""
                MATCH ()-[r]->()
                WITH type(r) AS relationship_type, count(r) AS count
                ORDER BY count DESC
                RETURN relationship_type, count
            """)
            relationship_stats = [{"type": record["relationship_type"], "count": record["count"]} for record in result]
        
        driver.close()
        
        result = {
            "business_count": business_count,
            "review_count": review_count,
            "user_count": user_count,
            "relationship_count": relationship_count,
            "category_stats": category_stats,
            "relationship_stats": relationship_stats
        }
        
        return jsonify(result)
    except Exception as e:
        if driver:
            driver.close()
        logger.error(f"Error in neo4j_overview_stats: {str(e)}")
        
        # Return dummy data on error
        dummy_data = {
            "business_count": 15,
            "review_count": 75,
            "user_count": 20,
            "relationship_count": 150,
            "category_stats": [
                {"category": "Restaurants", "count": 5},
                {"category": "Shopping", "count": 3},
                {"category": "Food", "count": 2},
                {"category": "Beauty & Spas", "count": 2},
                {"category": "Home Services", "count": 1}
            ],
            "relationship_stats": [
                {"type": "LOCATED_IN", "count": 15},
                {"type": "IN_CATEGORY", "count": 30},
                {"type": "WROTE", "count": 75},
                {"type": "REVIEWS", "count": 75},
                {"type": "HAD_CHECKIN", "count": 30}
            ]
        }
        return jsonify(dummy_data)

@app.route('/api/neo4j/top_businesses')
def neo4j_top_businesses():
    category = request.args.get('category', 'Restaurants')
    limit = int(request.args.get('limit', 10))
    
    driver = get_neo4j_driver()
    if not driver:
        # Return dummy data
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
            },
            {
                "business_id": "sample_business_neo4j_3",
                "business_name": "Sample Neo4j Business 3",
                "stars": 4.8,
                "review_count": 150,
                "city": "New York",
                "state": "NY"
            }
        ]
        return jsonify(dummy_data)
    
    try:
        with driver.session() as session:
            query = """
                MATCH (b:Business)-[:IN_CATEGORY]->(c:Category {name: $category})
                OPTIONAL MATCH (b)-[:LOCATED_IN]->(l:Location)
                RETURN b.business_id as business_id, b.name as business_name, 
                       b.stars as stars, b.review_count as review_count,
                       l.city as city, l.state as state
                ORDER BY b.stars DESC, b.review_count DESC
                LIMIT $limit
            """
            
            result = session.run(query, category=category, limit=limit)
            records = [record.data() for record in result]
            
            driver.close()
            return jsonify(records)
    except Exception as e:
        if driver:
            driver.close()
        logger.error(f"Error in neo4j_top_businesses: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/neo4j/business_performance')
def neo4j_business_performance():
    business_id = request.args.get('business_id')
    
    if not business_id:
        return jsonify({"error": "Business ID is required"}), 400
    
    driver = get_neo4j_driver()
    if not driver:
        # Return dummy data
        dummy_data = {
            "business": {
                "business_id": business_id,
                "business_name": "Sample Neo4j Business",
                "city": "San Francisco",
                "state": "CA",
                "stars": 4.5,
                "review_count": 120,
                "avg_stars": 4.3,
                "checkin_count": 250
            },
            "reviews_by_month": [
                {"year": 2022, "month": 1, "review_count": 5},
                {"year": 2022, "month": 2, "review_count": 8},
                {"year": 2022, "month": 3, "review_count": 6},
                {"year": 2022, "month": 4, "review_count": 7},
                {"year": 2022, "month": 5, "review_count": 10}
            ],
            "checkins_by_month": [
                {"year": 2022, "month": 1, "checkin_count": 12},
                {"year": 2022, "month": 2, "checkin_count": 18},
                {"year": 2022, "month": 3, "checkin_count": 15},
                {"year": 2022, "month": 4, "checkin_count": 20},
                {"year": 2022, "month": 5, "checkin_count": 25}
            ]
        }
        return jsonify(dummy_data)
    
    try:
        with driver.session() as session:
            # Get business details
            query = """
                MATCH (b:Business {business_id: $business_id})
                OPTIONAL MATCH (b)-[:LOCATED_IN]->(l:Location)
                RETURN b.business_id as business_id, b.name as business_name,
                       b.stars as stars, b.review_count as review_count,
                       b.summary_avg_stars as avg_stars, b.summary_checkin_count as checkin_count,
                       l.city as city, l.state as state
            """
            
            result = session.run(query, business_id=business_id)
            business = result.single()
            
            if not business:
                driver.close()
                return jsonify({"error": "Business not found"}), 404
            
            business = dict(business)
            
            # Get reviews by month
            query = """
                MATCH (b:Business {business_id: $business_id})<-[:REVIEWS]-(r:Review)-[:ON_DATE]->(t:Time)
                WITH t.year as year, t.month as month, count(r) as review_count
                ORDER BY year, month
                RETURN year, month, review_count
            """
            
            result = session.run(query, business_id=business_id)
            reviews_by_month = [dict(record) for record in result]
            
            # Get checkins by month
            query = """
                MATCH (b:Business {business_id: $business_id})-[c:HAD_CHECKIN]->(t:Time)
                WITH t.year as year, t.month as month, sum(c.count) as checkin_count
                ORDER BY year, month
                RETURN year, month, checkin_count
            """
            
            result = session.run(query, business_id=business_id)
            checkins_by_month = [dict(record) for record in result]
        
        driver.close()
        
        result = {
            "business": business,
            "reviews_by_month": reviews_by_month,
            "checkins_by_month": checkins_by_month
        }
        
        return jsonify(result)
    except Exception as e:
        if driver:
            driver.close()
        logger.error(f"Error in neo4j_business_performance: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/neo4j/business_network')
def neo4j_business_network():
    business_id = request.args.get('business_id')
    
    if not business_id:
        return jsonify({"error": "Business ID is required"}), 400
    
    driver = get_neo4j_driver()
    if not driver:
        # Return dummy data
        dummy_data = {
            "category_related": [
                {
                    "business_name": "Related Business 1",
                    "city": "San Francisco",
                    "state": "CA",
                    "stars": 4.2,
                    "common_categories": 3
                },
                {
                    "business_name": "Related Business 2",
                    "city": "San Francisco",
                    "state": "CA",
                    "stars": 3.8,
                    "common_categories": 2
                }
            ],
            "user_related": [
                {
                    "business_name": "User Related Business 1",
                    "city": "Los Angeles",
                    "state": "CA",
                    "stars": 4.5,
                    "common_users": 5
                },
                {
                    "business_name": "User Related Business 2",
                    "city": "Chicago",
                    "state": "IL",
                    "stars": 4.0,
                    "common_users": 3
                }
            ]
        }
        return jsonify(dummy_data)
    
    try:
        with driver.session() as session:
            # Get businesses related by category
            query = """
                MATCH (b:Business {business_id: $business_id})-[:IN_CATEGORY]->(c:Category)<-[:IN_CATEGORY]-(other:Business)
                WHERE other.business_id <> $business_id
                WITH other, count(c) AS common_categories
                ORDER BY common_categories DESC
                LIMIT 5
                MATCH (other)-[:LOCATED_IN]->(l:Location)
                RETURN other.business_id as business_id, other.name as business_name,
                       other.stars as stars, l.city as city, l.state as state,
                       common_categories
            """
            
            result = session.run(query, business_id=business_id)
            category_related = [dict(record) for record in result]
            
            # Get businesses related by user reviews
            query = """
                MATCH (b:Business {business_id: $business_id})<-[:REVIEWS]-(:Review)<-[:WROTE]-(u:User)-[:WROTE]->(:Review)-[:REVIEWS]->(other:Business)
                WHERE other.business_id <> $business_id
                WITH other, count(DISTINCT u) AS common_users
                ORDER BY common_users DESC
                LIMIT 5
                MATCH (other)-[:LOCATED_IN]->(l:Location)
                RETURN other.business_id as business_id, other.name as business_name,
                       other.stars as stars, l.city as city, l.state as state,
                       common_users
            """
            
            result = session.run(query, business_id=business_id)
            user_related = [dict(record) for record in result]
        
        driver.close()
        
        result = {
            "category_related": category_related,
            "user_related": user_related
        }
        
        return jsonify(result)
    except Exception as e:
        if driver:
            driver.close()
        logger.error(f"Error in neo4j_business_network: {str(e)}")
        return jsonify({"error": str(e)}), 500

# Run the application
if __name__ == '__main__':
    # Add a delay to ensure databases are ready
    logger.info("Starting Flask application on 0.0.0.0:8080...")
    app.run(host='0.0.0.0', port=8080, debug=True)