"""
Neo4j-specific routes for the Yelp Data Warehouse API
"""
from flask import Blueprint, jsonify, request
from db_utils import get_neo4j_driver, logger

# Create Blueprint for Neo4j routes
neo4j_bp = Blueprint('neo4j', __name__, url_prefix='/api/neo4j')

# Neo4j API Endpoints
@neo4j_bp.route('/overview_stats')
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

@neo4j_bp.route('/top_businesses')
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

@neo4j_bp.route('/business_performance')
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

@neo4j_bp.route('/business_network')
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