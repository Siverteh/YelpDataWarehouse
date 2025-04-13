"""
MySQL-specific routes for the Yelp Data Warehouse API
"""
from flask import Blueprint, jsonify, request
import json
import random
from datetime import datetime, date, timedelta
import decimal
from db_utils import get_mysql_connection, json_serialize, logger

# Create Blueprint for MySQL routes
mysql_bp = Blueprint('mysql', __name__, url_prefix='/api/mysql')

# MySQL API Endpoints
@mysql_bp.route('/overview_stats')
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

@mysql_bp.route('/top_businesses')
def mysql_top_businesses():
    category = request.args.get('category', 'Restaurants')
    limit = int(request.args.get('limit', 10))
    page = int(request.args.get('page', 1))
    offset = (page - 1) * limit
    
    conn = get_mysql_connection()
    if not conn:
        # Return dummy data
        dummy_data = {
            "businesses": [
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
            ],
            "pagination": {
                "total": 50,
                "page": page,
                "limit": limit,
                "pages": 5
            }
        }
        return jsonify(dummy_data)
    
    try:
        cursor = conn.cursor()
        
        # Query to get businesses for the current page
        query = """
            SELECT b.business_id, b.business_name, b.stars, b.review_count, 
                   l.city, l.state
            FROM dim_business b
            JOIN dim_location l ON b.location_id = l.location_id
            JOIN business_category bc ON b.business_id = bc.business_id
            JOIN dim_category c ON bc.category_id = c.category_id
            WHERE c.category_name = %s
            ORDER BY b.stars DESC, b.review_count DESC
            LIMIT %s OFFSET %s
        """
        
        cursor.execute(query, (category, limit, offset))
        businesses = cursor.fetchall()
        
        # Query to get total count for pagination
        count_query = """
            SELECT COUNT(*) as count
            FROM dim_business b
            JOIN business_category bc ON b.business_id = bc.business_id
            JOIN dim_category c ON bc.category_id = c.category_id
            WHERE c.category_name = %s
        """
        
        cursor.execute(count_query, (category,))
        total = cursor.fetchone()['count']
        
        # Calculate total pages
        total_pages = (total + limit - 1) // limit
        
        # Handle potential serialization issues with Decimal and datetime types
        serializable_businesses = []
        for row in businesses:
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
            serializable_businesses.append(serializable_row)
        
        cursor.close()
        conn.close()
        
        result = {
            "businesses": serializable_businesses,
            "pagination": {
                "total": total,
                "page": page,
                "limit": limit,
                "pages": total_pages
            }
        }
        
        return jsonify(result)
    except Exception as e:
        if conn:
            conn.close()
        logger.error(f"Error in mysql_top_businesses: {str(e)}")
        return jsonify({"error": str(e)}), 500

@mysql_bp.route('/business_checkins')
def mysql_business_checkins():
    """Get checkin patterns for a specific business"""
    business_id = request.args.get('business_id')
    
    if not business_id:
        return jsonify({"error": "Business ID is required"}), 400
    
    conn = get_mysql_connection()
    if not conn:
        # Return dummy data
        dummy_data = {
            "day_distribution": [15, 12, 18, 22, 35, 45, 30],  # Sun through Sat
            "month_distribution": [20, 18, 25, 30, 35, 42, 45, 40, 32, 28, 25, 30],  # Jan through Dec
            "hour_distribution": [
                2, 1, 0, 0, 0, 1, 5, 10, 15, 18, 20, 25,
                30, 28, 22, 20, 23, 28, 35, 30, 25, 18, 12, 5
            ]  # Hours 0-23
        }
        return jsonify(dummy_data)
    
    try:
        cursor = conn.cursor()
        
        # Get checkin day distribution
        cursor.execute("""
            SELECT 
                DAYOFWEEK(t.date_actual) as day_of_week,
                SUM(c.checkin_count) as checkin_count
            FROM fact_checkin c
            JOIN dim_time t ON c.time_id = t.time_id
            WHERE c.business_id = %s
            GROUP BY day_of_week
            ORDER BY day_of_week
        """, (business_id,))
        
        days_result = cursor.fetchall()
        
        # Initialize with zeros for all days
        day_distribution = [0] * 7
        
        # Fill in actual values (DAYOFWEEK returns 1=Sunday, 2=Monday, etc.)
        for row in days_result:
            day_index = row['day_of_week'] - 1  # Convert to 0-based index
            day_distribution[day_index] = row['checkin_count']
        
        # Get checkin month distribution
        cursor.execute("""
            SELECT 
                t.month_actual as month,
                SUM(c.checkin_count) as checkin_count
            FROM fact_checkin c
            JOIN dim_time t ON c.time_id = t.time_id
            WHERE c.business_id = %s
            GROUP BY month
            ORDER BY month
        """, (business_id,))
        
        months_result = cursor.fetchall()
        
        # Initialize with zeros for all months
        month_distribution = [0] * 12
        
        # Fill in actual values
        for row in months_result:
            month_index = row['month'] - 1  # Convert to 0-based index
            month_distribution[month_index] = row['checkin_count']
        
        cursor.close()
        conn.close()
        
        result = {
            "day_distribution": day_distribution,
            "month_distribution": month_distribution
            # We can't include hour distribution since we don't have that data in our schema
        }
        
        return jsonify(result)
    except Exception as e:
        if conn:
            conn.close()
        logger.error(f"Error in mysql_business_checkins: {str(e)}")
        
        # Return dummy data on error
        dummy_data = {
            "day_distribution": [15, 12, 18, 22, 35, 45, 30],  # Sun through Sat
            "month_distribution": [20, 18, 25, 30, 35, 42, 45, 40, 32, 28, 25, 30],  # Jan through Dec
            "hour_distribution": [
                2, 1, 0, 0, 0, 1, 5, 10, 15, 18, 20, 25,
                30, 28, 22, 20, 23, 28, 35, 30, 25, 18, 12, 5
            ]  # Hours 0-23
        }
        return jsonify(dummy_data)

@mysql_bp.route('/city_ratings')
def mysql_city_ratings():
    """Get rating distribution by city"""
    state = request.args.get('state')
    limit = int(request.args.get('limit', 20))
    
    conn = get_mysql_connection()
    if not conn:
        # Return dummy data
        dummy_data = [
            {"city": "Las Vegas", "state": "NV", "business_count": 450, "avg_rating": 4.2, "total_reviews": 25000, 
             "five_star_count": 200, "four_star_count": 150, "three_star_count": 75, "two_star_count": 20, "one_star_count": 5},
            {"city": "Phoenix", "state": "AZ", "business_count": 350, "avg_rating": 4.0, "total_reviews": 18000,
             "five_star_count": 150, "four_star_count": 120, "three_star_count": 60, "two_star_count": 15, "one_star_count": 5},
            {"city": "Philadelphia", "state": "PA", "business_count": 300, "avg_rating": 3.9, "total_reviews": 15000,
             "five_star_count": 120, "four_star_count": 100, "three_star_count": 60, "two_star_count": 15, "one_star_count": 5}
        ]
        return jsonify(dummy_data)
    
    try:
        cursor = conn.cursor()
        
        # Base query
        query = """
            SELECT 
                l.city,
                l.state,
                COUNT(DISTINCT b.business_id) as business_count,
                AVG(b.stars) as avg_rating,
                SUM(b.review_count) as total_reviews,
                COUNT(CASE WHEN b.stars >= 4.5 THEN 1 END) as five_star_count,
                COUNT(CASE WHEN b.stars >= 3.5 AND b.stars < 4.5 THEN 1 END) as four_star_count,
                COUNT(CASE WHEN b.stars >= 2.5 AND b.stars < 3.5 THEN 1 END) as three_star_count,
                COUNT(CASE WHEN b.stars >= 1.5 AND b.stars < 2.5 THEN 1 END) as two_star_count,
                COUNT(CASE WHEN b.stars < 1.5 THEN 1 END) as one_star_count
            FROM 
                dim_business b
                JOIN dim_location l ON b.location_id = l.location_id
        """
        
        params = []
        
        # Add state filter if provided
        if state:
            query += " WHERE l.state = %s"
            params.append(state)
        
        query += """
            GROUP BY 
                l.city, l.state
            HAVING 
                COUNT(DISTINCT b.business_id) > 5
            ORDER BY 
                business_count DESC
            LIMIT %s
        """
        params.append(limit)
        
        cursor.execute(query, params)
        cities = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        # Convert Decimal objects for JSON serialization
        result = json.loads(json.dumps(cities, default=json_serialize))
        
        return jsonify(result)
    except Exception as e:
        if conn:
            conn.close()
        logger.error(f"Error in mysql_city_ratings: {str(e)}")
        
        # Return dummy data on error
        dummy_data = [
            {"city": "Las Vegas", "state": "NV", "business_count": 450, "avg_rating": 4.2, "total_reviews": 25000, 
             "five_star_count": 200, "four_star_count": 150, "three_star_count": 75, "two_star_count": 20, "one_star_count": 5},
            {"city": "Phoenix", "state": "AZ", "business_count": 350, "avg_rating": 4.0, "total_reviews": 18000,
             "five_star_count": 150, "four_star_count": 120, "three_star_count": 60, "two_star_count": 15, "one_star_count": 5},
            {"city": "Philadelphia", "state": "PA", "business_count": 300, "avg_rating": 3.9, "total_reviews": 15000,
             "five_star_count": 120, "four_star_count": 100, "three_star_count": 60, "two_star_count": 15, "one_star_count": 5}
        ]
        return jsonify(dummy_data)

@mysql_bp.route('/business_performance')
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

@mysql_bp.route('/business_reviews')
def mysql_business_reviews():
    """Get reviews for a specific business with pagination"""
    business_id = request.args.get('business_id')
    page = int(request.args.get('page', 1))
    limit = int(request.args.get('limit', 10))
    sort = request.args.get('sort', 'date_desc')
    offset = (page - 1) * limit
    
    if not business_id:
        return jsonify({"error": "Business ID is required"}), 400
    
    conn = get_mysql_connection()
    if not conn:
        # Return dummy data
        dummy_data = {
            "reviews": [
                {
                    "review_id": "sample_review_1",
                    "user_id": "user_1",
                    "user_name": "John Doe",
                    "stars": 5,
                    "review_date": "2022-05-15",
                    "text": "This place is amazing! The food was delicious and the service was excellent.",
                    "useful_votes": 3,
                    "funny_votes": 1,
                    "cool_votes": 2
                },
                {
                    "review_id": "sample_review_2",
                    "user_id": "user_2",
                    "user_name": "Jane Smith",
                    "stars": 4,
                    "review_date": "2022-04-20",
                    "text": "Great experience overall. Would come back again.",
                    "useful_votes": 2,
                    "funny_votes": 0,
                    "cool_votes": 1
                }
            ],
            "pagination": {
                "total": 25,
                "page": page,
                "limit": limit,
                "pages": 3
            }
        }
        return jsonify(dummy_data)
    
    try:
        cursor = conn.cursor()
        
        # Determine sort order
        sort_column = "t.date_actual"
        sort_direction = "DESC"
        
        if sort == 'date_asc':
            sort_direction = "ASC"
        elif sort == 'stars_desc':
            sort_column = "r.stars"
            sort_direction = "DESC"
        elif sort == 'stars_asc':
            sort_column = "r.stars"
            sort_direction = "ASC"
        elif sort == 'useful_desc':
            sort_column = "r.useful_votes"
            sort_direction = "DESC"
        
        # Get reviews with user details
        cursor.execute(f"""
            SELECT r.review_id, r.stars, r.useful_votes, r.funny_votes, 
                   r.cool_votes, t.date_actual as review_date,
                   u.name as user_name, u.user_id,
                   r.text
            FROM fact_review r
            JOIN dim_time t ON r.time_id = t.time_id
            JOIN dim_user u ON r.user_id = u.user_id
            WHERE r.business_id = %s
            ORDER BY {sort_column} {sort_direction}
            LIMIT %s OFFSET %s
        """, (business_id, limit, offset))
        
        reviews = cursor.fetchall()
        
        # Get total count for pagination
        cursor.execute("""
            SELECT COUNT(*) as count
            FROM fact_review
            WHERE business_id = %s
        """, (business_id,))
        
        total = cursor.fetchone()['count']
        
        # Calculate total pages
        total_pages = (total + limit - 1) // limit
        
        cursor.close()
        conn.close()
        
        # Convert values for JSON serialization
        reviews = json.loads(json.dumps(reviews, default=json_serialize))
        
        result = {
            "reviews": reviews,
            "pagination": {
                "total": total,
                "page": page,
                "limit": limit,
                "pages": total_pages
            }
        }
        
        return jsonify(result)
    except Exception as e:
        if conn:
            conn.close()
        logger.error(f"Error in mysql_business_reviews: {str(e)}")
        return jsonify({"error": str(e)}), 500

@mysql_bp.route('/search_businesses')
def mysql_search_businesses():
    """Search businesses by name, location and filters"""
    query = request.args.get('query', '')
    location = request.args.get('location', '')
    category = request.args.get('category', '')
    min_rating = request.args.get('min_rating', '')
    min_reviews = request.args.get('min_reviews', '')
    sort_by = request.args.get('sort_by', 'stars')
    page = int(request.args.get('page', 1))
    limit = int(request.args.get('limit', 20))
    offset = (page - 1) * limit
    
    conn = get_mysql_connection()
    if not conn:
        # Return dummy data
        dummy_data = {
            "businesses": [
                {
                    "business_id": "search_result_1",
                    "business_name": "Search Result 1",
                    "stars": 4.5,
                    "review_count": 120,
                    "city": "San Francisco",
                    "state": "CA"
                },
                {
                    "business_id": "search_result_2",
                    "business_name": "Search Result 2",
                    "stars": 4.0,
                    "review_count": 85,
                    "city": "Los Angeles",
                    "state": "CA"
                }
            ],
            "pagination": {
                "total": 10,
                "page": page,
                "limit": limit,
                "pages": 1
            }
        }
        return jsonify(dummy_data)
    
    try:
        cursor = conn.cursor()
        
        # Base query
        sql = """
            SELECT b.business_id, b.business_name, b.stars, b.review_count,
                   l.city, l.state
            FROM dim_business b
            JOIN dim_location l ON b.location_id = l.location_id
        """
        
        params = []
        where_clauses = []
        
        # Add search conditions
        if query:
            where_clauses.append("(b.business_name LIKE %s)")
            params.append(f"%{query}%")
        
        # Add location filter
        if location:
            where_clauses.append("(l.city LIKE %s OR l.state LIKE %s)")
            params.extend([f"%{location}%", f"%{location}%"])
        
        # Add category filter if specified
        if category:
            sql += " JOIN business_category bc ON b.business_id = bc.business_id"
            sql += " JOIN dim_category c ON bc.category_id = c.category_id"
            where_clauses.append("c.category_name = %s")
            params.append(category)
        
        # Add minimum rating filter
        if min_rating:
            where_clauses.append("b.stars >= %s")
            params.append(float(min_rating))
        
        # Add minimum reviews filter
        if min_reviews:
            where_clauses.append("b.review_count >= %s")
            params.append(int(min_reviews))
        
        # Combine where clauses if any
        if where_clauses:
            sql += " WHERE " + " AND ".join(where_clauses)
        
        # Order by
        if sort_by == 'stars':
            sql += " ORDER BY b.stars DESC, b.review_count DESC"
        elif sort_by == 'review_count':
            sql += " ORDER BY b.review_count DESC, b.stars DESC"
        else:  # name
            sql += " ORDER BY b.business_name ASC"
        
        # Add limit and offset
        sql += " LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        
        # Execute query
        cursor.execute(sql, params)
        businesses = cursor.fetchall()
        
        # Get total count for pagination (without limit/offset)
        count_sql = """
            SELECT COUNT(*) as count
            FROM dim_business b
            JOIN dim_location l ON b.location_id = l.location_id
        """
        
        if category:
            count_sql += " JOIN business_category bc ON b.business_id = bc.business_id"
            count_sql += " JOIN dim_category c ON bc.category_id = c.category_id"
        
        if where_clauses:
            count_sql += " WHERE " + " AND ".join(where_clauses)
        
        cursor.execute(count_sql, params[:-2] if params else [])
        total = cursor.fetchone()['count']
        
        # Calculate total pages
        total_pages = (total + limit - 1) // limit
        
        cursor.close()
        conn.close()
        
        # Convert values for JSON serialization
        businesses = json.loads(json.dumps(businesses, default=json_serialize))
        
        result = {
            "businesses": businesses,
            "pagination": {
                "total": total,
                "page": page,
                "limit": limit,
                "pages": total_pages
            }
        }
        
        return jsonify(result)
    except Exception as e:
        if conn:
            conn.close()
        logger.error(f"Error in mysql_search_businesses: {str(e)}")
        return jsonify({"error": str(e)}), 500

@mysql_bp.route('/review_trends')
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
        
        dates = [row['month_date'].strftime('%Y-%m-%d') for row in results]
        review_counts = [row['review_count'] for row in results]
        
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

@mysql_bp.route('/category_ratings')
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