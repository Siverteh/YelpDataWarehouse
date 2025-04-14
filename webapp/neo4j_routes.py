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
        return jsonify({"error": "Failed to connect to Neo4j database"}), 503
    
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
            
            # Get user activity (new)
            result = session.run("""
                MATCH (u:User)-[:WROTE]->()
                WITH u, count(*) as review_count
                ORDER BY review_count DESC
                LIMIT 5
                RETURN u.name as name, review_count
            """)
            top_users = [{"name": record["name"], "review_count": record["review_count"]} for record in result]
            
            # Get density metrics (new)
            result = session.run("""
                MATCH (b:Business)
                OPTIONAL MATCH (b)-[:IN_CATEGORY]->(c:Category)
                WITH b, count(c) as category_count
                RETURN 
                    avg(category_count) as avg_categories_per_business,
                    max(category_count) as max_categories_per_business
            """)
            density_metrics = result.single()
        
        driver.close()
        
        result = {
            "business_count": business_count,
            "review_count": review_count,
            "user_count": user_count,
            "relationship_count": relationship_count,
            "category_stats": category_stats,
            "relationship_stats": relationship_stats,
            "top_users": top_users,
            "density_metrics": {
                "avg_categories_per_business": density_metrics["avg_categories_per_business"],
                "max_categories_per_business": density_metrics["max_categories_per_business"]
            }
        }
        
        return jsonify(result)
    except Exception as e:
        if driver:
            driver.close()
        logger.error(f"Error in neo4j_overview_stats: {str(e)}")
        return jsonify({"error": str(e)}), 500

@neo4j_bp.route('/search_businesses')
def neo4j_search_businesses():
    """Search businesses with advanced graph-based filtering"""
    # Get search parameters
    name = request.args.get('name', '')
    category = request.args.get('category', '')
    location = request.args.get('location', '')
    min_rating = request.args.get('min_rating')
    user_reviewed = request.args.get('user_reviewed', '')
    connected_to = request.args.get('connected_to', '')
    common_category_with = request.args.get('common_category_with', '')
    sort_by = request.args.get('sort_by', 'stars')
    limit = int(request.args.get('limit', 10))
    page = int(request.args.get('page', 1))
    offset = (page - 1) * limit
    
    driver = get_neo4j_driver()
    if not driver:
        return jsonify({"error": "Failed to connect to Neo4j database"}), 503
    
    try:
        with driver.session() as session:
            # Build query based on parameters
            query_parts = ["MATCH (b:Business)"]
            where_clauses = []
            params = {}
            
            if name:
                where_clauses.append("b.name =~ $name_pattern")
                params["name_pattern"] = f"(?i).*{name}.*"
            
            if category:
                query_parts.append("MATCH (b)-[:IN_CATEGORY]->(c:Category)")
                where_clauses.append("c.name = $category")
                params["category"] = category
            
            if location:
                query_parts.append("MATCH (b)-[:LOCATED_IN]->(l:Location)")
                where_clauses.append("(l.city =~ $location_pattern OR l.state =~ $location_pattern)")
                params["location_pattern"] = f"(?i).*{location}.*"
            
            if min_rating:
                where_clauses.append("b.stars >= $min_rating")
                params["min_rating"] = float(min_rating)
            
            if user_reviewed:
                query_parts.append("MATCH (u:User {user_id: $user_id})-[:WROTE]->(:Review)-[:REVIEWS]->(b)")
                params["user_id"] = user_reviewed
            
            if connected_to:
                query_parts.append("""
                    MATCH (otherBiz:Business {business_id: $connected_to})
                    MATCH (otherBiz)-[:IN_CATEGORY]->(c:Category)<-[:IN_CATEGORY]-(b)
                """)
                params["connected_to"] = connected_to
            
            if common_category_with:
                query_parts.append("""
                    MATCH (otherBiz:Business {business_id: $common_with})
                    MATCH (otherBiz)-[:IN_CATEGORY]->(c:Category)<-[:IN_CATEGORY]-(b)
                """)
                params["common_with"] = common_category_with
            
            # Add WHERE clause if any conditions exist
            if where_clauses:
                query_parts.append("WHERE " + " AND ".join(where_clauses))
            
            # Add optional location match for display
            query_parts.append("OPTIONAL MATCH (b)-[:LOCATED_IN]->(l:Location)")
            
            # Determine sort order
            if sort_by == 'stars':
                order_by = "b.stars DESC, b.review_count DESC"
            elif sort_by == 'review_count':
                order_by = "b.review_count DESC, b.stars DESC"
            else:
                order_by = "b.name"
            
            # Count total results
            count_query = " ".join(query_parts) + " RETURN count(DISTINCT b) as total"
            result = session.run(count_query, params)
            total = result.single()["total"]
            
            # Final query with pagination
            final_query = " ".join(query_parts) + f"""
                RETURN DISTINCT b.business_id as business_id, b.name as business_name, 
                       b.stars as stars, b.review_count as review_count,
                       l.city as city, l.state as state
                ORDER BY {order_by}
                SKIP $skip LIMIT $limit
            """
            params["skip"] = offset
            params["limit"] = limit
            
            result = session.run(final_query, params)
            businesses = [record.data() for record in result]
            
            # Calculate total pages
            total_pages = (total + limit - 1) // limit if total > 0 else 1
            
            driver.close()
            
            return jsonify({
                "businesses": businesses,
                "pagination": {
                    "total": total,
                    "page": page,
                    "limit": limit,
                    "pages": total_pages
                }
            })
    except Exception as e:
        if driver:
            driver.close()
        logger.error(f"Error in neo4j_search_businesses: {str(e)}")
        return jsonify({"error": str(e)}), 500

@neo4j_bp.route('/top_businesses')
def neo4j_top_businesses():
    category = request.args.get('category', 'Restaurants')
    limit = int(request.args.get('limit', 10))
    
    driver = get_neo4j_driver()
    if not driver:
        return jsonify({"error": "Failed to connect to Neo4j database"}), 503
    
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
        return jsonify({"error": "Failed to connect to Neo4j database"}), 503
    
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
            
            # Get category counts (new)
            query = """
                MATCH (b:Business {business_id: $business_id})-[:IN_CATEGORY]->(c:Category)
                RETURN c.name as category
            """
            
            result = session.run(query, business_id=business_id)
            categories = [record["category"] for record in result]
            
            # Get recent reviews with user details (new)
            query = """
                MATCH (b:Business {business_id: $business_id})<-[:REVIEWS]-(r:Review)<-[:WROTE]-(u:User)
                MATCH (r)-[:ON_DATE]->(t:Time)
                RETURN 
                    r.review_id as review_id,
                    u.name as user_name,
                    u.user_id as user_id,
                    r.stars as stars,
                    r.text as text,
                    t.date_actual as date
                ORDER BY t.date_actual DESC
                LIMIT 5
            """
            
            result = session.run(query, business_id=business_id)
            recent_reviews = [dict(record) for record in result]
        
        driver.close()
        
        result = {
            "business": business,
            "reviews_by_month": reviews_by_month,
            "checkins_by_month": checkins_by_month,
            "categories": categories,
            "recent_reviews": recent_reviews
        }
        
        return jsonify(result)
    except Exception as e:
        if driver:
            driver.close()
        logger.error(f"Error in neo4j_business_performance: {str(e)}")
        return jsonify({"error": str(e)}), 500

@neo4j_bp.route('/business_reviews')
def neo4j_business_reviews():
    business_id = request.args.get('business_id')
    page = int(request.args.get('page', 1))
    limit = int(request.args.get('limit', 10))
    sort = request.args.get('sort', 'date_desc')
    
    if not business_id:
        return jsonify({"error": "Business ID is required"}), 400
    
    driver = get_neo4j_driver()
    if not driver:
        return jsonify({"error": "Failed to connect to Neo4j database"}), 503
    
    try:
        with driver.session() as session:
            # Determine sort order
            sort_clause = ""
            if sort == 'date_desc':
                sort_clause = "ORDER BY t.date_actual DESC"
            elif sort == 'date_asc':
                sort_clause = "ORDER BY t.date_actual ASC"
            elif sort == 'stars_desc':
                sort_clause = "ORDER BY r.stars DESC"
            elif sort == 'stars_asc':
                sort_clause = "ORDER BY r.stars ASC"
            elif sort == 'useful_desc':
                sort_clause = "ORDER BY r.useful DESC"
            else:
                sort_clause = "ORDER BY t.date_actual DESC"
            
            # Count total reviews
            count_query = """
                MATCH (b:Business {business_id: $business_id})<-[:REVIEWS]-(r:Review)
                RETURN count(r) as total
            """
            
            result = session.run(count_query, business_id=business_id)
            total = result.single()["total"]
            
            # Get reviews with pagination
            query = f"""
                MATCH (b:Business {{business_id: $business_id}})<-[:REVIEWS]-(r:Review)<-[:WROTE]-(u:User)
                MATCH (r)-[:ON_DATE]->(t:Time)
                RETURN 
                    r.review_id as review_id,
                    u.name as user_name,
                    u.user_id as user_id,
                    r.stars as stars,
                    r.text as text,
                    r.useful as useful_votes,
                    r.funny as funny_votes,
                    r.cool as cool_votes,
                    t.date_actual as review_date
                {sort_clause}
                SKIP $skip LIMIT $limit
            """
            
            result = session.run(query, business_id=business_id, skip=(page-1)*limit, limit=limit)
            reviews = [dict(record) for record in result]
            
            # Calculate total pages
            total_pages = (total + limit - 1) // limit if total > 0 else 1
        
        driver.close()
        
        return jsonify({
            "reviews": reviews,
            "pagination": {
                "total": total,
                "page": page,
                "limit": limit,
                "pages": total_pages
            }
        })
    except Exception as e:
        if driver:
            driver.close()
        logger.error(f"Error in neo4j_business_reviews: {str(e)}")
        return jsonify({"error": str(e)}), 500

@neo4j_bp.route('/business_network')
def neo4j_business_network():
    business_id = request.args.get('business_id')
    
    if not business_id:
        return jsonify({"error": "Business ID is required"}), 400
    
    driver = get_neo4j_driver()
    if not driver:
        return jsonify({"error": "Failed to connect to Neo4j database"}), 503
    
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
            
            # Get shared categories (new)
            query = """
                MATCH (b:Business {business_id: $business_id})-[:IN_CATEGORY]->(c:Category)
                MATCH (c)<-[:IN_CATEGORY]-(other:Business)
                WHERE other.business_id <> $business_id
                RETURN c.name as category, count(DISTINCT other) as business_count
                ORDER BY business_count DESC
                LIMIT 8
            """
            
            result = session.run(query, business_id=business_id)
            shared_categories = [dict(record) for record in result]
            
            # Get most connected users (new)
            query = """
                MATCH (b:Business {business_id: $business_id})<-[:REVIEWS]-(:Review)<-[:WROTE]-(u:User)
                MATCH (u)-[:WROTE]->(:Review)-[:REVIEWS]->(other:Business)
                WHERE other.business_id <> $business_id
                WITH u, count(DISTINCT other) as business_count
                ORDER BY business_count DESC
                LIMIT 5
                RETURN u.user_id as user_id, u.name as user_name, business_count
            """
            
            result = session.run(query, business_id=business_id)
            connected_users = [dict(record) for record in result]
        
        driver.close()
        
        result = {
            "category_related": category_related,
            "user_related": user_related,
            "shared_categories": shared_categories,
            "connected_users": connected_users
        }
        
        return jsonify(result)
    except Exception as e:
        if driver:
            driver.close()
        logger.error(f"Error in neo4j_business_network: {str(e)}")
        return jsonify({"error": str(e)}), 500

@neo4j_bp.route('/business_recommendations')
def neo4j_business_recommendations():
    """Get recommendations for a business using graph relationships"""
    business_id = request.args.get('business_id')
    
    if not business_id:
        return jsonify({"error": "Business ID is required"}), 400
    
    driver = get_neo4j_driver()
    if not driver:
        return jsonify({"error": "Failed to connect to Neo4j database"}), 503
    
    try:
        with driver.session() as session:
            # Get similar highly rated businesses by category
            query = """
                MATCH (b:Business {business_id: $business_id})-[:IN_CATEGORY]->(c:Category)<-[:IN_CATEGORY]-(other:Business)
                WHERE other.business_id <> $business_id AND other.stars >= 4.0
                WITH other, collect(c.name) as shared_categories, count(c) as category_count
                ORDER BY category_count DESC, other.stars DESC
                LIMIT 5
                MATCH (other)-[:LOCATED_IN]->(l:Location)
                RETURN 
                    other.business_id as business_id,
                    other.name as business_name,
                    other.stars as stars,
                    other.review_count as review_count,
                    l.city as city,
                    l.state as state,
                    shared_categories,
                    category_count
            """
            
            result = session.run(query, business_id=business_id)
            similar_businesses = [dict(record) for record in result]
            
            # Get complementary businesses (visited by same users but different categories)
            query = """
                MATCH (b:Business {business_id: $business_id})<-[:REVIEWS]-(:Review)<-[:WROTE]-(u:User)-[:WROTE]->(:Review)-[:REVIEWS]->(other:Business)
                WHERE other.business_id <> $business_id
                MATCH (b)-[:IN_CATEGORY]->(bc:Category)
                MATCH (other)-[:IN_CATEGORY]->(oc:Category)
                WHERE NOT (other)-[:IN_CATEGORY]->(:Category)<-[:IN_CATEGORY]-(b)
                WITH other, count(DISTINCT u) as common_users, collect(DISTINCT oc.name) as categories
                ORDER BY common_users DESC
                LIMIT 5
                MATCH (other)-[:LOCATED_IN]->(l:Location)
                RETURN 
                    other.business_id as business_id,
                    other.name as business_name,
                    other.stars as stars,
                    l.city as city,
                    l.state as state,
                    categories,
                    common_users
            """
            
            result = session.run(query, business_id=business_id)
            complementary_businesses = [dict(record) for record in result]
            
            # Get "people who reviewed X also reviewed Y" recommendations
            query = """
                MATCH (b:Business {business_id: $business_id})<-[:REVIEWS]-(:Review)<-[:WROTE]-(u:User)-[:WROTE]->(:Review)-[:REVIEWS]->(other:Business)
                WHERE other.business_id <> $business_id
                WITH other, count(DISTINCT u) as common_reviewers
                ORDER BY common_reviewers DESC
                LIMIT 5
                MATCH (other)-[:LOCATED_IN]->(l:Location)
                RETURN 
                    other.business_id as business_id,
                    other.name as business_name,
                    other.stars as stars,
                    l.city as city,
                    l.state as state,
                    common_reviewers
            """
            
            result = session.run(query, business_id=business_id)
            also_reviewed = [dict(record) for record in result]
        
        driver.close()
        
        return jsonify({
            "similar_businesses": similar_businesses,
            "complementary_businesses": complementary_businesses,
            "also_reviewed": also_reviewed
        })
    except Exception as e:
        if driver:
            driver.close()
        logger.error(f"Error in neo4j_business_recommendations: {str(e)}")
        return jsonify({"error": str(e)}), 500

@neo4j_bp.route('/user_recommendations')
def neo4j_user_recommendations():
    """Get business recommendations for a specific user"""
    user_id = request.args.get('user_id')
    
    if not user_id:
        return jsonify({"error": "User ID is required"}), 400
    
    driver = get_neo4j_driver()
    if not driver:
        return jsonify({"error": "Failed to connect to Neo4j database"}), 503
    
    try:
        with driver.session() as session:
            # Get the user's preferences (categories of businesses they've reviewed)
            query = """
                MATCH (u:User {user_id: $user_id})-[:WROTE]->(:Review)-[:REVIEWS]->(b:Business)-[:IN_CATEGORY]->(c:Category)
                WITH c, count(DISTINCT b) as category_count
                ORDER BY category_count DESC
                LIMIT 5
                RETURN c.name as category, category_count
            """
            
            result = session.run(query, user_id=user_id)
            preferred_categories = [dict(record) for record in result]
            
            # Get businesses in user's preferred categories
            category_names = [cat["category"] for cat in preferred_categories]
            if not category_names:
                preferred_recommendations = []
            else:
                query = """
                    MATCH (u:User {user_id: $user_id})
                    MATCH (c:Category)<-[:IN_CATEGORY]-(b:Business)
                    WHERE c.name IN $categories
                    AND NOT (u)-[:WROTE]->(:Review)-[:REVIEWS]->(b)
                    WITH b, count(c) as relevance
                    ORDER BY b.stars DESC, relevance DESC
                    LIMIT 5
                    MATCH (b)-[:LOCATED_IN]->(l:Location)
                    RETURN 
                        b.business_id as business_id,
                        b.name as business_name,
                        b.stars as stars,
                        l.city as city,
                        l.state as state
                """
                
                result = session.run(query, user_id=user_id, categories=category_names)
                preferred_recommendations = [dict(record) for record in result]
            
            # Get recommendations based on similar users
            query = """
                MATCH (u:User {user_id: $user_id})-[:WROTE]->(:Review)-[:REVIEWS]->(b1:Business)
                MATCH (u2:User)-[:WROTE]->(:Review)-[:REVIEWS]->(b1)
                MATCH (u2)-[:WROTE]->(:Review)-[:REVIEWS]->(b2:Business)
                WHERE NOT (u)-[:WROTE]->(:Review)-[:REVIEWS]->(b2)
                WITH b2, count(DISTINCT u2) as common_users
                ORDER BY common_users DESC, b2.stars DESC
                LIMIT 5
                MATCH (b2)-[:LOCATED_IN]->(l:Location)
                RETURN 
                    b2.business_id as business_id,
                    b2.name as business_name,
                    b2.stars as stars,
                    l.city as city,
                    l.state as state,
                    common_users
            """
            
            result = session.run(query, user_id=user_id)
            collaborative_recommendations = [dict(record) for record in result]
        
        driver.close()
        
        return jsonify({
            "preferred_categories": preferred_categories,
            "category_recommendations": preferred_recommendations,
            "collaborative_recommendations": collaborative_recommendations
        })
    except Exception as e:
        if driver:
            driver.close()
        logger.error(f"Error in neo4j_user_recommendations: {str(e)}")
        return jsonify({"error": str(e)}), 500

@neo4j_bp.route('/connection_path')
def neo4j_connection_path():
    """Find paths connecting two businesses through users or categories"""
    business_id1 = request.args.get('business_id1')
    business_id2 = request.args.get('business_id2')
    path_type = request.args.get('path_type', 'user')  # 'user' or 'category'
    
    if not business_id1 or not business_id2:
        return jsonify({"error": "Both business IDs are required"}), 400
    
    if business_id1 == business_id2:
        return jsonify({"error": "Business IDs must be different"}), 400
    
    driver = get_neo4j_driver()
    if not driver:
        return jsonify({"error": "Failed to connect to Neo4j database"}), 503
    
    try:
        with driver.session() as session:
            # Get business names for display
            query = """
                MATCH (b1:Business {business_id: $id1})
                MATCH (b2:Business {business_id: $id2})
                RETURN b1.name as name1, b2.name as name2
            """
            
            result = session.run(query, id1=business_id1, id2=business_id2)
            names = result.single()
            
            if not names:
                driver.close()
                return jsonify({"error": "One or both businesses not found"}), 404
            
            business_name1 = names["name1"]
            business_name2 = names["name2"]
            
            paths = []
            
            if path_type == 'user':
                # Find paths through users (business1 <- user -> business2)
                query = """
                    MATCH path = (b1:Business {business_id: $id1})<-[:REVIEWS]-(:Review)<-[:WROTE]-(u:User)-[:WROTE]->(:Review)-[:REVIEWS]->(b2:Business {business_id: $id2})
                    RETURN u.name as user_name, u.user_id as user_id
                    LIMIT 10
                """
                
                result = session.run(query, id1=business_id1, id2=business_id2)
                user_paths = [dict(record) for record in result]
                
                for path in user_paths:
                    paths.append({
                        "type": "user",
                        "connection": path["user_name"],
                        "user_id": path["user_id"]
                    })
            
            elif path_type == 'category':
                # Find paths through categories (business1 -> category <- business2)
                query = """
                    MATCH (b1:Business {business_id: $id1})-[:IN_CATEGORY]->(c:Category)<-[:IN_CATEGORY]-(b2:Business {business_id: $id2})
                    RETURN c.name as category
                """
                
                result = session.run(query, id1=business_id1, id2=business_id2)
                category_paths = [dict(record) for record in result]
                
                for path in category_paths:
                    paths.append({
                        "type": "category",
                        "connection": path["category"]
                    })
            
            # Find shortest path using Neo4j's shortestPath algorithm
            query = """
                MATCH (b1:Business {business_id: $id1}), (b2:Business {business_id: $id2})
                CALL {
                    MATCH p = shortestPath((b1)-[*..6]-(b2))
                    RETURN p as path, length(p) as length
                    LIMIT 1
                }
                UNWIND nodes(path) as node
                RETURN labels(node)[0] as type, node.name as name, node.business_id as business_id, node.user_id as user_id
            """
            
            result = session.run(query, id1=business_id1, id2=business_id2)
            shortest_path = [dict(record) for record in result]
        
        driver.close()
        
        return jsonify({
            "business1": {
                "id": business_id1,
                "name": business_name1
            },
            "business2": {
                "id": business_id2,
                "name": business_name2
            },
            "paths": paths,
            "shortest_path": shortest_path
        })
    except Exception as e:
        if driver:
            driver.close()
        logger.error(f"Error in neo4j_connection_path: {str(e)}")
        return jsonify({"error": str(e)}), 500

@neo4j_bp.route('/graph_analytics')
def neo4j_graph_analytics():
    """Get analytical insights from the graph structure"""
    driver = get_neo4j_driver()
    if not driver:
        return jsonify({"error": "Failed to connect to Neo4j database"}), 503
    
    try:
        with driver.session() as session:
            # Get most central businesses (with most connections)
            query = """
                MATCH (b:Business)-[r]-()
                WITH b, count(r) as connection_count
                ORDER BY connection_count DESC
                LIMIT 5
                MATCH (b)-[:LOCATED_IN]->(l:Location)
                RETURN 
                    b.business_id as business_id,
                    b.name as business_name,
                    b.stars as stars,
                    l.city as city,
                    l.state as state,
                    connection_count
            """
            
            result = session.run(query)
            central_businesses = [dict(record) for record in result]
            
            # Get most influential users (with most connections)
            query = """
                MATCH (u:User)-[r]-()
                WITH u, count(r) as connection_count
                ORDER BY connection_count DESC
                LIMIT 5
                RETURN 
                    u.user_id as user_id,
                    u.name as user_name,
                    connection_count
            """
            
            result = session.run(query)
            influential_users = [dict(record) for record in result]
            
            # Get most connected categories
            query = """
                MATCH (c:Category)<-[:IN_CATEGORY]-(b:Business)
                WITH c, count(b) as business_count
                ORDER BY business_count DESC
                LIMIT 10
                RETURN 
                    c.name as category,
                    business_count
            """
            
            result = session.run(query)
            top_categories = [dict(record) for record in result]
            
            # Get category co-occurrence (which categories often appear together)
            query = """
                MATCH (c1:Category)<-[:IN_CATEGORY]-(b:Business)-[:IN_CATEGORY]->(c2:Category)
                WHERE c1.name < c2.name
                WITH c1.name as category1, c2.name as category2, count(b) as business_count
                ORDER BY business_count DESC
                LIMIT 10
                RETURN category1, category2, business_count
            """
            
            result = session.run(query)
            category_pairs = [dict(record) for record in result]
        
        driver.close()
        
        return jsonify({
            "central_businesses": central_businesses,
            "influential_users": influential_users,
            "top_categories": top_categories,
            "category_pairs": category_pairs
        })
    except Exception as e:
        if driver:
            driver.close()
        logger.error(f"Error in neo4j_graph_analytics: {str(e)}")
        return jsonify({"error": str(e)}), 500