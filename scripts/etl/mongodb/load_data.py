import json
import pymongo
from tqdm import tqdm
from datetime import datetime

# Connect to MongoDB
def get_mongodb_connection():
    client = pymongo.MongoClient('mongodb://user:user@mongodb:27017/')
    return client

def load_businesses(chunk_size=1000):
    """
    Load businesses from JSON into MongoDB
    """
    client = get_mongodb_connection()
    db = client['yelp_db']
    
    # Clear existing collection
    db.businesses.drop()
    
    # Create indexes for better query performance
    db.businesses.create_index([('business_id', pymongo.ASCENDING)], unique=True)
    db.businesses.create_index([('categories', pymongo.ASCENDING)])
    db.businesses.create_index([('stars', pymongo.DESCENDING)])
    db.businesses.create_index([('review_count', pymongo.DESCENDING)])
    
    print("Loading businesses into MongoDB...")
    batch = []
    
    with open('data/yelp_academic_dataset_business.json', 'r', encoding='utf-8') as f:
        for i, line in enumerate(tqdm(f)):
            business = json.loads(line)
            
            # Convert categories string to array
            if business.get('categories'):
                business['categories'] = [c.strip() for c in business['categories'].split(',')]
            
            # Structure location as subdocument
            location = {
                'city': business.get('city', ''),
                'state': business.get('state', ''),
                'postal_code': business.get('postal_code', ''),
                'latitude': business.get('latitude'),
                'longitude': business.get('longitude'),
                'address': business.get('address', '')
            }
            business['location'] = location
            
            # Flatten attributes if they exist
            if 'attributes' in business and business['attributes']:
                for key, value in business['attributes'].items():
                    # Convert JSON strings to actual objects
                    if isinstance(value, str) and value.startswith('{'):
                        try:
                            value = json.loads(value.replace("'", '"'))
                        except:
                            pass
                    business[f"attr_{key}"] = value
                
                # Remove the original attributes object
                business.pop('attributes')
            
            # Clean up fields we don't need or have restructured
            for key in ['city', 'state', 'postal_code', 'latitude', 'longitude', 'address']:
                if key in business:
                    business.pop(key)
            
            batch.append(business)
            
            if len(batch) >= chunk_size:
                db.businesses.insert_many(batch)
                batch = []
                print(f"Processed {i+1} businesses")
            
            # For testing, limit to a smaller number
            if i >= 50000:  # Comment this out for full dataset
                break
        
        # Insert any remaining businesses
        if batch:
            db.businesses.insert_many(batch)
    
    print("Businesses loaded successfully")

def load_users(chunk_size=1000):
    """
    Load users from JSON into MongoDB
    """
    client = get_mongodb_connection()
    db = client['yelp_db']
    
    # Clear existing collection
    db.users.drop()
    
    # Create indexes for better query performance
    db.users.create_index([('user_id', pymongo.ASCENDING)], unique=True)
    db.users.create_index([('review_count', pymongo.DESCENDING)])
    db.users.create_index([('yelping_since', pymongo.ASCENDING)])
    
    print("Loading users into MongoDB...")
    batch = []
    
    with open('data/yelp_academic_dataset_user.json', 'r', encoding='utf-8') as f:
        for i, line in enumerate(tqdm(f)):
            user = json.loads(line)
            
            # Convert friends to array
            if 'friends' in user and user['friends']:
                if user['friends'] != 'None':
                    user['friends'] = user['friends'].split(', ')
                else:
                    user['friends'] = []
            
            # Convert elite years to array of integers
            if 'elite' in user and user['elite']:
                if user['elite'] != 'None':
                    user['elite'] = [int(year) for year in user['elite'].split(', ')]
                else:
                    user['elite'] = []
            
            batch.append(user)
            
            if len(batch) >= chunk_size:
                db.users.insert_many(batch)
                batch = []
                print(f"Processed {i+1} users")
            
            # For testing, limit to a smaller number
            if i >= 50000:  # Comment this out for full dataset
                break
        
        # Insert any remaining users
        if batch:
            db.users.insert_many(batch)
    
    print("Users loaded successfully")

def load_reviews(chunk_size=1000):
    """
    Load reviews from JSON into MongoDB
    """
    client = get_mongodb_connection()
    db = client['yelp_db']
    
    # Clear existing collection
    db.reviews.drop()
    
    # Create indexes for better query performance
    db.reviews.create_index([('review_id', pymongo.ASCENDING)], unique=True)
    db.reviews.create_index([('business_id', pymongo.ASCENDING)])
    db.reviews.create_index([('user_id', pymongo.ASCENDING)])
    db.reviews.create_index([('stars', pymongo.DESCENDING)])
    db.reviews.create_index([('date', pymongo.DESCENDING)])
    
    print("Loading reviews into MongoDB...")
    batch = []
    
    with open('data/yelp_academic_dataset_review.json', 'r', encoding='utf-8') as f:
        for i, line in enumerate(tqdm(f)):
            review = json.loads(line)
            
            # Convert date strings to proper format
            if 'date' in review:
                try:
                    review['date'] = datetime.fromisoformat(review['date'])
                except:
                    pass
            
            batch.append(review)
            
            if len(batch) >= chunk_size:
                db.reviews.insert_many(batch)
                batch = []
                print(f"Processed {i+1} reviews")
            
            # For testing, limit to a smaller number
            if i >= 50000:  # Comment this out for full dataset
                break
        
        # Insert any remaining reviews
        if batch:
            db.reviews.insert_many(batch)
    
    print("Reviews loaded successfully")

def load_checkins(chunk_size=1000):
    """
    Load checkins from JSON into MongoDB
    """
    client = get_mongodb_connection()
    db = client['yelp_db']
    
    # Clear existing collection
    db.checkins.drop()
    
    # Create indexes for better query performance
    db.checkins.create_index([('business_id', pymongo.ASCENDING)])
    
    print("Loading checkins into MongoDB...")
    batch = []
    
    with open('data/yelp_academic_dataset_checkin.json', 'r', encoding='utf-8') as f:
        for i, line in enumerate(tqdm(f)):
            checkin = json.loads(line)
            
            # Process date strings into structured format
            if 'date' in checkin:
                dates = []
                for date_str in checkin['date'].split(', '):
                    try:
                        # Store full date-time
                        dates.append(date_str)
                    except:
                        continue
                
                # Replace string with array of dates
                checkin['dates'] = dates
                del checkin['date']
            
            batch.append(checkin)
            
            if len(batch) >= chunk_size:
                db.checkins.insert_many(batch)
                batch = []
                print(f"Processed {i+1} checkins")
            
            # For testing, limit to a smaller number
            if i >= 50000:  # Comment this out for full dataset
                break
        
        # Insert any remaining checkins
        if batch:
            db.checkins.insert_many(batch)
    
    print("Checkins loaded successfully")

def load_tips(chunk_size=1000):
    """
    Load tips from JSON into MongoDB
    """
    client = get_mongodb_connection()
    db = client['yelp_db']
    
    # Clear existing collection
    db.tips.drop()
    
    # Create indexes for better query performance
    db.tips.create_index([('business_id', pymongo.ASCENDING)])
    db.tips.create_index([('user_id', pymongo.ASCENDING)])
    db.tips.create_index([('date', pymongo.DESCENDING)])
    
    print("Loading tips into MongoDB...")
    batch = []
    
    with open('data/yelp_academic_dataset_tip.json', 'r', encoding='utf-8') as f:
        for i, line in enumerate(tqdm(f)):
            tip = json.loads(line)
            
            # Convert date strings to proper format
            if 'date' in tip:
                try:
                    tip['date'] = datetime.fromisoformat(tip['date'])
                except:
                    pass
            
            batch.append(tip)
            
            if len(batch) >= chunk_size:
                db.tips.insert_many(batch)
                batch = []
                print(f"Processed {i+1} tips")
            
            # For testing, limit to a smaller number
            if i >= 50000:  # Comment this out for full dataset
                break
        
        # Insert any remaining tips
        if batch:
            db.tips.insert_many(batch)
    
    print("Tips loaded successfully")

def create_aggregations():
    """
    Create pre-aggregated summary collections
    """
    client = get_mongodb_connection()
    db = client['yelp_db']
    
    print("Creating pre-aggregated summaries...")
    
    # Create business_summaries collection
    db.business_summaries.drop()
    
    # Aggregate business metrics
    pipeline = [
        {
            "$lookup": {
                "from": "reviews",
                "localField": "business_id",
                "foreignField": "business_id",
                "as": "all_reviews"
            }
        },
        {
            "$lookup": {
                "from": "checkins",
                "localField": "business_id",
                "foreignField": "business_id",
                "as": "all_checkins"
            }
        },
        {
            "$project": {
                "business_id": 1,
                "name": 1,
                "location": 1,
                "categories": 1,
                "stars": 1,
                "review_count": 1,
                "is_open": 1,
                "review_stats": {
                    "avg_stars": {"$avg": "$all_reviews.stars"},
                    "review_count": {"$size": "$all_reviews"},
                    "five_star_count": {
                        "$size": {
                            "$filter": {
                                "input": "$all_reviews",
                                "as": "review",
                                "cond": {"$eq": ["$$review.stars", 5]}
                            }
                        }
                    },
                    "one_star_count": {
                        "$size": {
                            "$filter": {
                                "input": "$all_reviews",
                                "as": "review",
                                "cond": {"$eq": ["$$review.stars", 1]}
                            }
                        }
                    }
                },
                "checkin_count": {"$size": {"$ifNull": ["$all_checkins.dates", []]}}
            }
        },
        {"$out": "business_summaries"}
    ]
    
    db.businesses.aggregate(pipeline)
    
    # Create indexes on the summary collection
    db.business_summaries.create_index([('business_id', pymongo.ASCENDING)], unique=True)
    db.business_summaries.create_index([('categories', pymongo.ASCENDING)])
    db.business_summaries.create_index([('stars', pymongo.DESCENDING)])
    db.business_summaries.create_index([('review_stats.avg_stars', pymongo.DESCENDING)])
    
    print("Pre-aggregated summaries created successfully")

if __name__ == "__main__":
    load_businesses()
    load_users()
    load_reviews()
    load_checkins()
    load_tips()
    create_aggregations()
    print("MongoDB ETL process completed successfully!")