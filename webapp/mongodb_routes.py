"""
MongoDB-specific routes for the Yelp Data Warehouse API
"""
from flask import Blueprint, jsonify, request
import json
import random
from db_utils import get_mongodb_connection, logger

# Create Blueprint for MongoDB routes
mongodb_bp = Blueprint('mongodb', __name__, url_prefix='/api/mongodb')

# MongoDB API Endpoints
@mongodb_bp.route('/overview_stats')
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

@mongodb_bp.route('/top_businesses')
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

@mongodb_bp.route('/business_performance')
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

@mongodb_bp.route('/document_size_stats')
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

@mongodb_bp.route('/business_attributes')
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