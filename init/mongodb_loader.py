"""
MongoDB data loading functions for Yelp Data Warehouse
"""

import os
import json
import pymongo
from datetime import datetime
from utils import get_config

def check_and_initialize_mongodb():
    """Check if MongoDB has data, initialize if needed"""
    print("\nChecking MongoDB data...")
    
    config = get_config()
    mongo_uri = config['mongodb']['uri']
    data_config = config['data']
    
    try:
        client = pymongo.MongoClient(mongo_uri)
        db = client.yelp_db
        
        # Check if collections exist and have data
        collections = db.list_collection_names()
        print(f"MongoDB collections found: {collections}")
        
        if 'businesses' in collections:
            count = db.businesses.count_documents({})
            print(f"Business count: {count}")
            
            if count > 0:
                print("MongoDB has data. Skipping initialization.")
                client.close()
                return False  # No need to initialize
        
        # Initialize MongoDB
        print("Initializing MongoDB...")
        
        # Create collections
        if 'businesses' not in collections:
            db.create_collection('businesses')
        if 'reviews' not in collections:
            db.create_collection('reviews')
        if 'users' not in collections:
            db.create_collection('users')
        if 'checkins' not in collections:
            db.create_collection('checkins')
        if 'tips' not in collections:
            db.create_collection('tips')
        
        # Create indexes
        db.businesses.create_index([('business_id', pymongo.ASCENDING)], unique=True)
        db.businesses.create_index([('categories', pymongo.ASCENDING)])
        db.businesses.create_index([('stars', pymongo.DESCENDING)])
        
        db.reviews.create_index([('business_id', pymongo.ASCENDING)])
        db.reviews.create_index([('user_id', pymongo.ASCENDING)])
        db.reviews.create_index([('date', pymongo.DESCENDING)])
        
        db.users.create_index([('user_id', pymongo.ASCENDING)], unique=True)
        
        db.checkins.create_index([('business_id', pymongo.ASCENDING)])
        
        db.tips.create_index([('business_id', pymongo.ASCENDING)])
        db.tips.create_index([('user_id', pymongo.ASCENDING)])
        
        # Load businesses
        load_businesses(db, data_config['business_file'])
        
        # Load users
        load_users(db, data_config['user_file'])
        
        # Load reviews
        load_reviews(db, data_config['review_file'])
        
        # Load checkins
        load_checkins(db, data_config['checkin_file'])
        
        # Load tips
        load_tips(db, data_config['tip_file'])
        
        # Create business_summaries collection for pre-aggregated data
        create_business_summaries(db)
        
        print("MongoDB initialized with data from Yelp dataset.")
        client.close()
        return True  # Initialized
        
    except Exception as e:
        print(f"Error initializing MongoDB: {e}")
        return False

def load_businesses(db, file_path, batch_size=1000):
    """Load businesses from Yelp dataset file into MongoDB"""
    if os.path.exists(file_path):
        print(f"Loading businesses from {file_path}...")
        batch_size = 1000
        loaded_count = 0
        batch_count = 0
        
        with open(file_path, 'r', encoding='utf-8') as f:
            batch = []
            for line in f:
                if line.strip():
                    business = json.loads(line)
                    
                    # Fix categories to be array
                    if 'categories' in business and business['categories']:
                        business['categories'] = [c.strip() for c in business['categories'].split(',')]
                    else:
                        business['categories'] = []
                    
                    batch.append(business)
                    
                    if len(batch) >= batch_size:
                        try:
                            db.businesses.insert_many(batch)
                            loaded_count += len(batch)
                            batch_count += 1
                            print(f"Loaded {loaded_count} businesses (batch {batch_count})...")
                        except Exception as e:
                            print(f"Error loading business batch: {e}")
                        batch = []
            
            # Load any remaining businesses
            if batch:
                try:
                    db.businesses.insert_many(batch)
                    loaded_count += len(batch)
                    print(f"Loaded {loaded_count} total businesses...")
                except Exception as e:
                    print(f"Error loading business batch: {e}")
    else:
        print(f"Business file not found: {file_path}")

def load_users(db, file_path, batch_size=1000):
    """Load users from Yelp dataset file into MongoDB"""
    if os.path.exists(file_path):
        print(f"Loading users from {file_path}...")
        batch_size = 1000
        loaded_count = 0
        batch_count = 0
        
        with open(file_path, 'r', encoding='utf-8') as f:
            batch = []
            for line in f:
                if line.strip():
                    user = json.loads(line)
                    
                    # Convert yelping_since to date object
                    if 'yelping_since' in user:
                        try:
                            user['yelping_since'] = datetime.strptime(user['yelping_since'], '%Y-%m-%d')
                        except:
                            pass
                    
                    batch.append(user)
                    
                    if len(batch) >= batch_size:
                        try:
                            db.users.insert_many(batch)
                            loaded_count += len(batch)
                            batch_count += 1
                            print(f"Loaded {loaded_count} users (batch {batch_count})...")
                        except Exception as e:
                            print(f"Error loading user batch: {e}")
                        batch = []
            
            # Load any remaining users
            if batch:
                try:
                    db.users.insert_many(batch)
                    loaded_count += len(batch)
                    print(f"Loaded {loaded_count} total users...")
                except Exception as e:
                    print(f"Error loading user batch: {e}")
    else:
        print(f"User file not found: {file_path}")

def load_reviews(db, file_path, batch_size=1000):
    """Load reviews from Yelp dataset file into MongoDB"""
    if os.path.exists(file_path):
        print(f"Loading reviews from {file_path}...")
        batch_size = 1000
        loaded_count = 0
        batch_count = 0
        
        with open(file_path, 'r', encoding='utf-8') as f:
            batch = []
            for line in f:
                if line.strip():
                    review = json.loads(line)
                    
                    # Convert date to date object
                    if 'date' in review:
                        try:
                            review['date'] = datetime.strptime(review['date'], '%Y-%m-%d')
                        except:
                            pass
                    
                    batch.append(review)
                    
                    if len(batch) >= batch_size:
                        try:
                            db.reviews.insert_many(batch)
                            loaded_count += len(batch)
                            batch_count += 1
                            print(f"Loaded {loaded_count} reviews (batch {batch_count})...")
                        except Exception as e:
                            print(f"Error loading review batch: {e}")
                        batch = []
            
            # Load any remaining reviews
            if batch:
                try:
                    db.reviews.insert_many(batch)
                    loaded_count += len(batch)
                    print(f"Loaded {loaded_count} total reviews...")
                except Exception as e:
                    print(f"Error loading review batch: {e}")
    else:
        print(f"Review file not found: {file_path}")

def load_checkins(db, file_path, batch_size=1000):
    """Load checkins from Yelp dataset file into MongoDB"""
    if os.path.exists(file_path):
        print(f"Loading checkins from {file_path}...")
        batch_size = 1000
        loaded_count = 0
        batch_count = 0
        
        with open(file_path, 'r', encoding='utf-8') as f:
            batch = []
            for line in f:
                if line.strip():
                    checkin = json.loads(line)
                    
                    # Process the date field to array of dates
                    dates = []
                    if isinstance(checkin.get('date'), str):
                        # Old format: string of date-times
                        for date_str in checkin['date'].split(', '):
                            try:
                                dates.append(datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S'))
                            except:
                                pass
                    elif isinstance(checkin.get('date'), dict):
                        # New format: dict with date as key and times as values
                        for date_str, times in checkin['date'].items():
                            for time_str in times.split(', '):
                                try:
                                    full_str = f"{date_str} {time_str}"
                                    dates.append(datetime.strptime(full_str, '%Y-%m-%d %H:%M:%S'))
                                except:
                                    pass
                    
                    checkin['dates'] = dates
                    if 'date' in checkin:
                        del checkin['date']  # Remove old format
                    
                    batch.append(checkin)
                    
                    if len(batch) >= batch_size:
                        try:
                            db.checkins.insert_many(batch)
                            loaded_count += len(batch)
                            batch_count += 1
                            print(f"Loaded {loaded_count} checkins (batch {batch_count})...")
                        except Exception as e:
                            print(f"Error loading checkin batch: {e}")
                        batch = []
            
            # Load any remaining checkins
            if batch:
                try:
                    db.checkins.insert_many(batch)
                    loaded_count += len(batch)
                    print(f"Loaded {loaded_count} total checkins...")
                except Exception as e:
                    print(f"Error loading checkin batch: {e}")
    else:
        print(f"Checkin file not found: {file_path}")

def load_tips(db, file_path, batch_size=1000):
    """Load tips from Yelp dataset file into MongoDB"""
    if os.path.exists(file_path):
        print(f"Loading tips from {file_path}...")
        batch_size = 1000
        loaded_count = 0
        batch_count = 0
        
        with open(file_path, 'r', encoding='utf-8') as f:
            batch = []
            for line in f:
                if line.strip():
                    tip = json.loads(line)
                    
                    # Convert date to date object
                    if 'date' in tip:
                        try:
                            tip['date'] = datetime.strptime(tip['date'], '%Y-%m-%d')
                        except:
                            pass
                    
                    batch.append(tip)
                    
                    if len(batch) >= batch_size:
                        try:
                            db.tips.insert_many(batch)
                            loaded_count += len(batch)
                            batch_count += 1
                            print(f"Loaded {loaded_count} tips (batch {batch_count})...")
                        except Exception as e:
                            print(f"Error loading tip batch: {e}")
                        batch = []
            
            # Load any remaining tips
            if batch:
                try:
                    db.tips.insert_many(batch)
                    loaded_count += len(batch)
                    print(f"Loaded {loaded_count} total tips...")
                except Exception as e:
                    print(f"Error loading tip batch: {e}")
    else:
        print(f"Tip file not found: {file_path}")

def create_business_summaries(db):
    """Create business_summaries collection for pre-aggregated data"""
    print("Creating business_summaries collection...")
    collections = db.list_collection_names()
    if 'business_summaries' in collections:
        db.business_summaries.drop()
    
    # Create aggregation pipeline for business summaries
    pipeline = [
        # Match businesses
        {"$match": {"business_id": {"$exists": True}}},
        
        # Lookup reviews
        {"$lookup": {
            "from": "reviews",
            "localField": "business_id",
            "foreignField": "business_id",
            "as": "reviews"
        }},
        
        # Lookup checkins
        {"$lookup": {
            "from": "checkins",
            "localField": "business_id",
            "foreignField": "business_id",
            "as": "checkins"
        }},
        
        # Lookup tips
        {"$lookup": {
            "from": "tips",
            "localField": "business_id",
            "foreignField": "business_id",
            "as": "tips"
        }},
        
        # Add summary fields
        {"$addFields": {
            "review_stats": {
                "avg_stars": {"$avg": "$reviews.stars"},
                "review_count": {"$size": "$reviews"},
                "five_star_count": {
                    "$size": {
                        "$filter": {
                            "input": "$reviews",
                            "as": "review",
                            "cond": {"$eq": ["$$review.stars", 5]}
                        }
                    }
                },
                "one_star_count": {
                    "$size": {
                        "$filter": {
                            "input": "$reviews",
                            "as": "review",
                            "cond": {"$eq": ["$$review.stars", 1]}
                        }
                    }
                }
            },
            "checkin_count": {
                "$sum": {
                    "$map": {
                        "input": "$checkins",
                        "as": "checkin",
                        "in": {"$size": "$$checkin.dates"}
                    }
                }
            },
            "tip_count": {"$size": "$tips"}
        }},
        
        # Project to remove nested arrays (for better performance)
        {"$project": {
            "_id": 0,
            "business_id": 1,
            "name": 1,
            "stars": 1,
            "review_count": 1,
            "attributes": 1,
            "categories": 1,
            "location": 1,
            "is_open": 1,
            "review_stats": 1,
            "checkin_count": 1,
            "tip_count": 1
        }},
        
        # Output to business_summaries collection
        {"$out": "business_summaries"}
    ]
    
    # Execute aggregation (this might take a while)
    print("Running aggregation to create business_summaries...")
    db.businesses.aggregate(pipeline, allowDiskUse=True)
    print("Business summaries created successfully.")