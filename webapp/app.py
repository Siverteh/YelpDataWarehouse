from flask import Flask, render_template, request, jsonify
import pymysql
import pymongo
from neo4j import GraphDatabase
import os
import json
import time
import logging
from datetime import datetime, timedelta

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
                    logger.info("Returning dummy connection for development")
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
    return render_template('index.html')

@app.route('/api/mysql/top_businesses')
def mysql_top_businesses():
    category = request.args.get('category', 'Restaurants')
    limit = int(request.args.get('limit', 10))
    
    conn = get_mysql_connection()
    if not conn:
        # Return dummy data for development
        if os.environ.get('FLASK_ENV') == 'development' or os.environ.get('FLASK_DEBUG') == '1':
            logger.info("Returning dummy data for MySQL top businesses")
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
        logger.error(f"Error in mysql_top_businesses: {str(e)}")
        return jsonify({"error": str(e)}), 500

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
                {"category": "Restaurants", "count": 5},
                {"category": "Shopping", "count": 3},
                {"category": "Food", "count": 2},
                {"category": "Beauty & Spas", "count": 2},
                {"category": "Home Services", "count": 1}
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

@app.route('/api/mongodb/overview_stats')
def mongodb_overview_stats():
    """Get overview statistics from MongoDB for dashboard"""
    client = get_mongodb_connection()
    if not client:
        # Return dummy data
        dummy_data = {
            "business_count": 15,
            "review_count": 75,
            "user_count": 20,
            "category_stats": [
                {"category": "Restaurants", "count": 5},
                {"category": "Shopping", "count": 3},
                {"category": "Food", "count": 2},
                {"category": "Beauty & Spas", "count": 2},
                {"category": "Home Services", "count": 1}
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
            "business_count": 15,
            "review_count": 75,
            "user_count": 20,
            "category_stats": [
                {"category": "Restaurants", "count": 5},
                {"category": "Shopping", "count": 3},
                {"category": "Food", "count": 2},
                {"category": "Beauty & Spas", "count": 2},
                {"category": "Home Services", "count": 1}
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

@app.route('/api/mongodb/top_businesses')
def mongodb_top_businesses():
    category = request.args.get('category', 'Restaurants')
    limit = int(request.args.get('limit', 10))
    
    client = get_mongodb_connection()
    if not client:
        # Return dummy data for development
        if os.environ.get('FLASK_ENV') == 'development' or os.environ.get('FLASK_DEBUG') == '1':
            logger.info("Returning dummy data for MongoDB top businesses")
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
        
        # Extract city and state from location if needed
        for business in results:
            if 'location' in business and isinstance(business['location'], dict):
                business['city'] = business['location'].get('city')
                business['state'] = business['location'].get('state')
                del business['location']
        
        return jsonify(results)
    except Exception as e:
        logger.error(f"Error in mongodb_top_businesses: {str(e)}")
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
            logger.info("Returning dummy data for Neo4j top businesses")
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
                OPTIONAL MATCH (b)-[:LOCATED_IN]->(l:Location)
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
        logger.error(f"Error in neo4j_top_businesses: {str(e)}")
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

if __name__ == '__main__':
    # Add a delay to ensure databases are ready
    logger.info("Starting Flask application on 0.0.0.0:8080...")
    app.run(host='0.0.0.0', port=8080, debug=True)