import json
import pymongo
from tqdm import tqdm

# Connect to MongoDB
client = pymongo.MongoClient('mongodb://admin:your_password@localhost:27017/')
db = client['yelp_dw']

# Clear existing collections
db.businesses.drop()
db.users.drop()
db.reviews.drop()
db.checkins.drop()

# Create indexes
db.businesses.create_index([('business_id', pymongo.ASCENDING)], unique=True)
db.businesses.create_index([('categories', pymongo.ASCENDING)])
db.users.create_index([('user_id', pymongo.ASCENDING)], unique=True)
db.reviews.create_index([('review_id', pymongo.ASCENDING)], unique=True)
db.reviews.create_index([('business_id', pymongo.ASCENDING)])
db.reviews.create_index([('user_id', pymongo.ASCENDING)])

def load_businesses():
    print("Loading businesses into MongoDB...")
    batch_size = 1000
    batch = []
    
    with open('data/yelp_academic_dataset_business.json', 'r') as f:
        for i, line in enumerate(tqdm(f)):
            business = json.loads(line)
            
            # Convert categories string to array
            if business.get('categories'):
                business['categories'] = [c.strip() for c in business['categories'].split(',')]
            
            batch.append(business)
            
            if len(batch) >= batch_size:
                db.businesses.insert_many(batch)
                batch = []
        
        # Insert any remaining businesses
        if batch:
            db.businesses.insert_many(batch)
    
    print("Businesses loaded successfully")

def load_users():
    # Similar implementation for users
    pass

def load_reviews():
    # Similar implementation for reviews
    pass

if __name__ == "__main__":
    load_businesses()
    load_users()
    load_reviews()