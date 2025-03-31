// Create a yelp_db database
db = db.getSiblingDB('yelp_db');

// Create collections
db.createCollection('businesses');
db.createCollection('reviews');
db.createCollection('users');

// Create indexes
db.businesses.createIndex({ "business_id": 1 }, { unique: true });
db.businesses.createIndex({ "categories": 1 });
db.businesses.createIndex({ "stars": -1 });
db.reviews.createIndex({ "business_id": 1 });
db.reviews.createIndex({ "user_id": 1 });
db.users.createIndex({ "user_id": 1 }, { unique: true });

// Insert a sample business to test
db.businesses.insertOne({
  "business_id": "sample_business",
  "name": "Sample Business",
  "stars": 4.5,
  "review_count": 100,
  "categories": ["Restaurants", "Italian"],
  "location": {
    "city": "San Francisco",
    "state": "CA"
  }
});

// Print confirmation
print("MongoDB initialization completed successfully!");