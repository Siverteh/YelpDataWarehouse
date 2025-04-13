"""
MongoDB-specific routes for the Yelp Data Warehouse API with no dummy data
"""
from flask import Blueprint, jsonify, request
import json
from db_utils import get_mongodb_connection, logger

# Create Blueprint for MongoDB routes
mongodb_bp = Blueprint('mongodb', __name__, url_prefix='/api/mongodb')

# MongoDB API Endpoints
@mongodb_bp.route('/overview_stats')
def mongodb_overview_stats():
    """Get overview statistics from MongoDB for dashboard"""
    client = get_mongodb_connection()
    if not client:
        return jsonify({"error": "Failed to connect to MongoDB database"}), 503
    
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
        return jsonify({"error": str(e)}), 500

@mongodb_bp.route('/top_businesses')
def mongodb_top_businesses():
    """Get top businesses filtered by category"""
    # Get query parameters
    category = request.args.get('category', 'Restaurants')
    limit = int(request.args.get('limit', 10))
    query = request.args.get('query')
    location = request.args.get('location')
    min_rating = request.args.get('min_rating')
    attribute_key = request.args.get('attribute_key')
    attribute_value = request.args.get('attribute_value')
    sort_by = request.args.get('sort_by', 'stars')
    
    client = get_mongodb_connection()
    if not client:
        return jsonify({"error": "Failed to connect to MongoDB database"}), 503
    
    try:
        db = client.yelp_db
        
        # Build match criteria
        match_criteria = {}
        
        # Add category filter if provided
        if category:
            match_criteria["categories"] = {"$in": [category]}
        
        # Add text search if provided
        if query:
            # Use regex for name search
            match_criteria["name"] = {"$regex": query, "$options": "i"}
        
        # Add location filter if provided
        if location:
            location_regex = {"$regex": location, "$options": "i"}
            match_criteria["$or"] = [
                {"location.city": location_regex},
                {"location.state": location_regex}
            ]
        
        # Add star rating filter if provided
        if min_rating:
            match_criteria["stars"] = {"$gte": float(min_rating)}
        
        # Add attribute filter if both key and value are provided
        if attribute_key and attribute_value:
            attribute_path = f"attributes.{attribute_key}"
            
            # Handle boolean values
            if attribute_value.lower() in ('true', 'false'):
                attribute_value = attribute_value.lower() == 'true'
                
            match_criteria[attribute_path] = attribute_value
        
        # Determine sort order
        if sort_by == 'stars':
            sort_options = {"stars": -1, "review_count": -1}
        elif sort_by == 'review_count':
            sort_options = {"review_count": -1, "stars": -1}
        else:  # name
            sort_options = {"name": 1}
        
        # Build the pipeline to properly handle location properties
        pipeline = [
            {"$match": match_criteria},
            {"$project": {
                "_id": 0,
                "business_id": 1,
                "name": 1,
                "stars": 1,
                "review_count": 1,
                "location": 1,
                "attributes": 1
            }},
            {"$sort": sort_options},
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
    """Get business performance details with reviews and checkins"""
    business_id = request.args.get('business_id')
    
    if not business_id:
        return jsonify({"error": "Business ID is required"}), 400
    
    client = get_mongodb_connection()
    if not client:
        return jsonify({"error": "Failed to connect to MongoDB database"}), 503
    
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

@mongodb_bp.route('/business_reviews')
def mongodb_business_reviews():
    """Get reviews for a specific business with pagination"""
    business_id = request.args.get('business_id')
    page = int(request.args.get('page', 1))
    limit = int(request.args.get('limit', 10))
    sort = request.args.get('sort', 'date_desc')
    
    if not business_id:
        return jsonify({"error": "Business ID is required"}), 400
    
    client = get_mongodb_connection()
    if not client:
        return jsonify({"error": "Failed to connect to MongoDB database"}), 503
    
    try:
        db = client.yelp_db
        
        # Determine sort order
        sort_field = None
        sort_direction = None
        
        if sort == 'date_desc':
            sort_field = "date"
            sort_direction = -1
        elif sort == 'date_asc':
            sort_field = "date"
            sort_direction = 1
        elif sort == 'stars_desc':
            sort_field = "stars"
            sort_direction = -1
        elif sort == 'stars_asc':
            sort_field = "stars"
            sort_direction = 1
        elif sort == 'useful_desc':
            sort_field = "useful"
            sort_direction = -1
        else:
            sort_field = "date"
            sort_direction = -1
        
        # Count total reviews
        total_count = db.reviews.count_documents({"business_id": business_id})
        
        # Calculate total pages
        total_pages = (total_count + limit - 1) // limit if total_count > 0 else 1
        
        # Calculate skip value
        skip = (page - 1) * limit
        
        # Get reviews with sort and pagination
        pipeline = [
            {"$match": {"business_id": business_id}},
            {"$lookup": {
                "from": "users",
                "localField": "user_id",
                "foreignField": "user_id",
                "as": "user"
            }},
            {"$unwind": {"path": "$user", "preserveNullAndEmptyArrays": True}},
            {"$sort": {sort_field: sort_direction}},
            {"$skip": skip},
            {"$limit": limit},
            {"$project": {
                "_id": 0,
                "review_id": 1,
                "user_id": 1,
                "user_name": "$user.name",
                "stars": 1,
                "date": 1,
                "text": 1,
                "useful": 1,
                "funny": 1,
                "cool": 1
            }}
        ]
        
        reviews = list(db.reviews.aggregate(pipeline))
        
        # Add user_name if it doesn't exist (fallback)
        for review in reviews:
            if 'user_name' not in review or not review['user_name']:
                review['user_name'] = 'Anonymous'
        
        client.close()
        
        # Return with pagination info
        result = {
            "reviews": reviews,
            "pagination": {
                "total": total_count,
                "page": page,
                "limit": limit,
                "pages": total_pages
            }
        }
        
        return jsonify(result)
    except Exception as e:
        if client:
            client.close()
        logger.error(f"Error in mongodb_business_reviews: {str(e)}")
        return jsonify({"error": str(e)}), 500

@mongodb_bp.route('/business_checkins')
def mongodb_business_checkins():
    """Get checkin patterns for a specific business"""
    business_id = request.args.get('business_id')
    
    if not business_id:
        return jsonify({"error": "Business ID is required"}), 400
    
    client = get_mongodb_connection()
    if not client:
        return jsonify({"error": "Failed to connect to MongoDB database"}), 503
    
    try:
        db = client.yelp_db
        
        # Find the checkin document for this business
        checkin = db.checkins.find_one({"business_id": business_id})
        
        # Initialize distributions with zeros
        day_distribution = [0] * 7  # Sunday through Saturday
        month_distribution = [0] * 12  # January through December
        hour_distribution = [0] * 24  # 0-23 hours
        
        # Process dates to get distributions if checkin data exists
        if checkin and 'dates' in checkin and checkin['dates']:
            from datetime import datetime
            
            for date_str in checkin['dates']:
                try:
                    date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                    
                    # Day of week (0 = Monday in Python's datetime, we want 0 = Sunday)
                    day_index = (date.weekday() + 1) % 7
                    day_distribution[day_index] += 1
                    
                    # Month (0 = January)
                    month_index = date.month - 1
                    month_distribution[month_index] += 1
                    
                    # Hour
                    hour_index = date.hour
                    hour_distribution[hour_index] += 1
                except ValueError:
                    # Skip invalid dates
                    continue
        
        client.close()
        
        result = {
            "day_distribution": day_distribution,
            "month_distribution": month_distribution,
            "hour_distribution": hour_distribution
        }
        
        return jsonify(result)
    except Exception as e:
        if client:
            client.close()
        logger.error(f"Error in mongodb_business_checkins: {str(e)}")
        return jsonify({"error": str(e)}), 500

@mongodb_bp.route('/document_size_stats')
def mongodb_document_size_stats():
    """Get document size distribution for MongoDB analytics"""
    client = get_mongodb_connection()
    if not client:
        return jsonify({"error": "Failed to connect to MongoDB database"}), 503
    
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
        return jsonify({"error": str(e)}), 500

@mongodb_bp.route('/business_attributes')
def mongodb_business_attributes():
    """Get business attributes analysis for MongoDB analytics"""
    client = get_mongodb_connection()
    if not client:
        return jsonify({"error": "Failed to connect to MongoDB database"}), 503
    
    try:
        db = client.yelp_db
        
        # Get top level attribute keys
        # MongoDB's $objectToArray converts a document to an array of k,v pairs
        pipeline = [
            {"$match": {"attributes": {"$exists": true, "$ne": None}}},
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
        return jsonify({"error": str(e)}), 500

@mongodb_bp.route('/schema_analysis')
def mongodb_schema_analysis():
    """Get schema analysis for MongoDB collections"""
    client = get_mongodb_connection()
    if not client:
        return jsonify({"error": "Failed to connect to MongoDB database"}), 503
    
    try:
        db = client.yelp_db
        
        # This would require a more complex analysis of actual documents
        # We'll do a basic field count analysis
        collections = ["businesses", "reviews", "users", "checkins"]
        collection_stats = []
        
        for collection_name in collections:
            collection = db[collection_name]
            
            # Sample some documents
            sample_size = min(100, collection.count_documents({}))
            if sample_size == 0:
                continue
                
            # Analyze field structure
            all_fields = {}
            total_field_count = 0
            max_field_count = 0
            min_field_count = float('inf')
            
            for doc in collection.find().limit(sample_size):
                # Count fields at all levels (flatten the document)
                field_count = 0
                
                def count_fields(obj, prefix=""):
                    nonlocal field_count
                    if isinstance(obj, dict):
                        for k, v in obj.items():
                            field_path = f"{prefix}.{k}" if prefix else k
                            field_count += 1
                            all_fields[field_path] = all_fields.get(field_path, 0) + 1
                            if isinstance(v, (dict, list)):
                                count_fields(v, field_path)
                    elif isinstance(obj, list) and len(obj) > 0:
                        field_count += 1
                        # For simplicity, just check first item if it's a complex type
                        if isinstance(obj[0], (dict, list)):
                            count_fields(obj[0], f"{prefix}[0]")
                
                count_fields(doc)
                total_field_count += field_count
                max_field_count = max(max_field_count, field_count)
                min_field_count = min(min_field_count, field_count)
            
            # Calculate stats
            common_fields = [field for field, count in all_fields.items() 
                            if count > sample_size * 0.8]  # Fields in >80% of docs
            schema_variation = (len(all_fields) - len(common_fields)) / len(all_fields) if len(all_fields) > 0 else 0
            
            collection_stats.append({
                "collection": collection_name,
                "avg_field_count": total_field_count / sample_size if sample_size > 0 else 0,
                "min_field_count": min_field_count if min_field_count != float('inf') else 0,
                "max_field_count": max_field_count,
                "total_unique_fields": len(all_fields),
                "common_fields": len(common_fields),
                "schema_variation": schema_variation * 100  # as percentage
            })
        
        client.close()
        
        return jsonify(collection_stats)
    except Exception as e:
        if client:
            client.close()
        logger.error(f"Error in mongodb_schema_analysis: {str(e)}")
        return jsonify({"error": str(e)}), 500

@mongodb_bp.route('/array_field_analysis')
def mongodb_array_field_analysis():
    """Get array field analysis for MongoDB documents"""
    client = get_mongodb_connection()
    if not client:
        return jsonify({"error": "Failed to connect to MongoDB database"}), 503
    
    try:
        db = client.yelp_db
        
        # Identify common array fields
        pipeline = [
            # First find the businesses collection fields that are arrays
            {"$sample": {"size": 100}},
            {"$project": {
                "categories": {"$cond": [{"$isArray": "$categories"}, {"$size": "$categories"}, 0]},
                "friends_count": {"$cond": [{"$isArray": "$friends"}, {"$size": "$friends"}, 0]},
                "photos_count": {"$cond": [{"$isArray": "$photos"}, {"$size": "$photos"}, 0]}
            }},
            {"$group": {
                "_id": null,
                "avg_categories": {"$avg": "$categories"},
                "max_categories": {"$max": "$categories"},
                "avg_friends": {"$avg": "$friends_count"},
                "max_friends": {"$max": "$friends_count"},
                "avg_photos": {"$avg": "$photos_count"},
                "max_photos": {"$max": "$photos_count"}
            }}
        ]
        
        array_stats = list(db.businesses.aggregate(pipeline))
        
        # If we got no results, return empty stats
        if not array_stats:
            array_stats = [{
                "avg_categories": 0,
                "max_categories": 0,
                "avg_friends": 0,
                "max_friends": 0,
                "avg_photos": 0,
                "max_photos": 0
            }]
        
        # Format the results
        result = {
            "array_fields": [
                {"field": "categories", "avg_length": array_stats[0].get("avg_categories", 0), "max_length": array_stats[0].get("max_categories", 0)},
                {"field": "friends", "avg_length": array_stats[0].get("avg_friends", 0), "max_length": array_stats[0].get("max_friends", 0)},
                {"field": "photos", "avg_length": array_stats[0].get("avg_photos", 0), "max_length": array_stats[0].get("max_photos", 0)}
            ]
        }
        
        client.close()
        
        return jsonify(result)
    except Exception as e:
        if client:
            client.close()
        logger.error(f"Error in mongodb_array_field_analysis: {str(e)}")
        return jsonify({"error": str(e)}), 500

@mongodb_bp.route('/document_structure')
def mongodb_document_structure():
    """Get sample document structure for visualization"""
    client = get_mongodb_connection()
    if not client:
        return jsonify({"error": "Failed to connect to MongoDB database"}), 503
    
    try:
        db = client.yelp_db
        
        # Find a sample business document
        sample_business = db.businesses.find_one({}, {"_id": 0})
        
        # If no real document exists, return an empty structure
        if not sample_business:
            return jsonify({"error": "No business documents found"}), 404
            
        # Analyze and describe the structure
        structure = {}
        
        def analyze_structure(obj, parent_key=""):
            if isinstance(obj, dict):
                result = {"type": "Object", "fields": {}}
                for k, v in obj.items():
                    if parent_key:
                        field_key = f"{parent_key}.{k}"
                    else:
                        field_key = k
                    result["fields"][k] = analyze_structure(v, field_key)
                return result
            elif isinstance(obj, list):
                if len(obj) > 0:
                    # Analyze first item to determine array type
                    sample = obj[0]
                    if isinstance(sample, (dict, list)):
                        item_type = analyze_structure(sample, f"{parent_key}[0]")
                        return {"type": "Array", "items": item_type, "length": len(obj)}
                    else:
                        return {"type": f"Array of {type(sample).__name__}", "length": len(obj)}
                else:
                    return {"type": "Empty Array"}
            else:
                # For scalar values, return their type and example value
                return {"type": type(obj).__name__, "example": str(obj)[:50] if obj is not None else "null"}
        
        structure = analyze_structure(sample_business)
        
        client.close()
        
        return jsonify(structure)
    except Exception as e:
        if client:
            client.close()
        logger.error(f"Error in mongodb_document_structure: {str(e)}")
        return jsonify({"error": str(e)}), 500