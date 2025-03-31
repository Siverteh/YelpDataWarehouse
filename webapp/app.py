from flask import Flask, render_template, request, jsonify
import pymysql
import pymongo
from neo4j import GraphDatabase
import os
import json

app = Flask(__name__)

# Get environment variables with defaults
MYSQL_HOST = os.environ.get('MYSQL_HOST', 'mysql')
MYSQL_PORT = int(os.environ.get('MYSQL_PORT', 3306))
MYSQL_USER = os.environ.get('MYSQL_USER', 'root')
MYSQL_PASSWORD = os.environ.get('MYSQL_PASSWORD', 'your_password')
MYSQL_DATABASE = os.environ.get('MYSQL_DATABASE', 'yelp_dw')

MONGO_HOST = os.environ.get('MONGO_HOST', 'mongodb')
MONGO_PORT = int(os.environ.get('MONGO_PORT', 27017))
MONGO_USER = os.environ.get('MONGO_USER', 'admin')
MONGO_PASSWORD = os.environ.get('MONGO_PASSWORD', 'your_password')

NEO4J_HOST = os.environ.get('NEO4J_HOST', 'neo4j')
NEO4J_PORT = int(os.environ.get('NEO4J_PORT', 7687))
NEO4J_USER = os.environ.get('NEO4J_USER', 'neo4j')
NEO4J_PASSWORD = os.environ.get('NEO4J_PASSWORD', 'your_password')

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

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/mysql/top_businesses')
def mysql_top_businesses():
    category = request.args.get('category', 'Restaurants')
    limit = int(request.args.get('limit', 10))
    
    conn = get_mysql_connection()
    if not conn:
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
        
        cursor.close()
        conn.close()
        
        return jsonify(results)
    except Exception as e:
        if conn:
            conn.close()
        return jsonify({"error": str(e)}), 500

@app.route('/api/mongodb/top_businesses')
def mongodb_top_businesses():
    category = request.args.get('category', 'Restaurants')
    limit = int(request.args.get('limit', 10))
    
    client = get_mongodb_connection()
    if not client:
        return jsonify({"error": "MongoDB connection failed"}), 503
    
    try:
        db = client.yelp_db
        results = list(db.businesses.find(
            {"categories": {"$regex": category, "$options": "i"}},
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

@app.route('/health')
def health():
    status = {
        "mysql": get_mysql_connection() is not None,
        "mongodb": get_mongodb_connection() is not None,
        "neo4j": get_neo4j_driver() is not None
    }
    overall = all(status.values())
    
    return jsonify({
        "status": "healthy" if overall else "unhealthy",
        "services": status
    }), 200 if overall else 503

if __name__ == '__main__':
    # Add a small delay to ensure databases are ready
    import time
    print("Waiting for databases to be ready...")
    time.sleep(10)
    
    # Try to connect to each database once before starting
    print("Testing database connections...")
    mysql_conn = get_mysql_connection()
    if mysql_conn:
        mysql_conn.close()
    
    mongo_client = get_mongodb_connection()
    if mongo_client:
        mongo_client.close()
    
    neo4j_driver = get_neo4j_driver()
    if neo4j_driver:
        neo4j_driver.close()
    
    print("Starting Flask application...")
    app.run(host='0.0.0.0', port=8080, debug=True)