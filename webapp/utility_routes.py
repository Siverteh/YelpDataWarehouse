"""
Utility routes for the Yelp Data Warehouse API
"""
from flask import Blueprint, jsonify
from datetime import datetime
from db_utils import (
    get_mysql_connection, 
    get_mongodb_connection, 
    get_neo4j_driver,
    logger
)
import os

# Create Blueprint for utility routes
utility_bp = Blueprint('utility', __name__, url_prefix='/api')

@utility_bp.route('/health')
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

@utility_bp.route('/debug')
def debug():
    """Endpoint for checking database connection details and configuration."""
    from db_utils import (
        MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_DATABASE,
        MONGO_HOST, MONGO_PORT, MONGO_USER,
        NEO4J_HOST, NEO4J_PORT, NEO4J_USER
    )
    
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

@utility_bp.route('/test')
def test():
    """Simple test endpoint to verify the API is working."""
    return jsonify({
        "status": "ok",
        "message": "API is working",
        "timestamp": datetime.now().isoformat()
    })