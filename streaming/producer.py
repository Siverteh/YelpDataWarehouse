#!/usr/bin/env python3
"""
Enhanced Kafka Producer for Yelp Data Warehouse
Generates simulated Yelp data events and sends them to Kafka topics,
including businesses and users.
"""

import os
import json
import time
import random
import string
import pymysql
from kafka import KafkaProducer
from datetime import datetime, timedelta

class YelpKafkaProducer:
    """
    Producer that generates simulated Yelp data events and sends them to Kafka topics.
    Now includes business and user generation.
    """
    
    def __init__(self, bootstrap_servers='kafka:9092'):
        """Initialize the Kafka producer with server address."""
        print(f"Initializing Kafka producer with bootstrap servers: {bootstrap_servers}")
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        
        # Load sample business and user IDs
        self.business_ids = self._load_business_ids()
        self.user_ids = self._load_user_ids()
        
        # Load cities and states for business generation
        self.cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", 
                      "San Antonio", "San Diego", "Dallas", "San Jose", "Austin", "Seattle", 
                      "Boston", "Las Vegas", "Portland", "Denver", "Miami", "Atlanta"]
        self.states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", 
                       "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", 
                       "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", 
                       "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", 
                       "WI", "WY"]
        self.categories = ["Restaurants", "Fast Food", "Coffee & Tea", "Bars", "Nightlife", 
                          "Shopping", "Beauty & Spas", "Health & Medical", "Home Services", 
                          "Auto Services", "Local Services", "Hotels & Travel", "Arts & Entertainment", 
                          "Active Life", "Professional Services", "Education", "Real Estate", "Financial Services"]
        
        print(f"Loaded {len(self.business_ids)} businesses and {len(self.user_ids)} users")
    
    def _load_business_ids(self):
        """Load a list of business IDs from the database or sample dataset."""
        try:
            # Try connecting to MySQL to get business IDs
            conn = pymysql.connect(
                host=os.environ.get('MYSQL_HOST', 'mysql'),
                port=int(os.environ.get('MYSQL_PORT', 3306)),
                user=os.environ.get('MYSQL_USER', 'root'),
                password=os.environ.get('MYSQL_PASSWORD', 'user'),
                db=os.environ.get('MYSQL_DATABASE', 'yelp_dw'),
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
            )
            
            cursor = conn.cursor()
            cursor.execute("SELECT business_id FROM dim_business LIMIT 100")
            business_ids = [row['business_id'] for row in cursor.fetchall()]
            cursor.close()
            conn.close()
            
            if business_ids:
                return business_ids
        except Exception as e:
            print(f"Error loading business IDs from database: {e}")
        
        # Fallback to loading from file if available
        try:
            data_dir = os.environ.get('DATA_DIR', '/app/data')
            business_file = os.path.join(data_dir, 'yelp_academic_dataset_business.json')
            
            if os.path.exists(business_file):
                business_ids = []
                with open(business_file, 'r', encoding='utf-8') as f:
                    for i, line in enumerate(f):
                        if i >= 100:  # Limit to 100 businesses for demo
                            break
                        data = json.loads(line)
                        business_ids.append(data['business_id'])
                return business_ids
        except Exception as e:
            print(f"Error loading business IDs from file: {e}")
        
        # Fallback to sample IDs
        return ["sample_business_id_1", "sample_business_id_2", "sample_business_id_3"]
    
    def _load_user_ids(self):
        """Load a list of user IDs from the database or sample dataset."""
        try:
            # Try connecting to MySQL to get user IDs
            conn = pymysql.connect(
                host=os.environ.get('MYSQL_HOST', 'mysql'),
                port=int(os.environ.get('MYSQL_PORT', 3306)),
                user=os.environ.get('MYSQL_USER', 'root'),
                password=os.environ.get('MYSQL_PASSWORD', 'user'),
                db=os.environ.get('MYSQL_DATABASE', 'yelp_dw'),
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
            )
            
            cursor = conn.cursor()
            cursor.execute("SELECT user_id FROM dim_user LIMIT 100")
            user_ids = [row['user_id'] for row in cursor.fetchall()]
            cursor.close()
            conn.close()
            
            if user_ids:
                return user_ids
        except Exception as e:
            print(f"Error loading user IDs from database: {e}")
        
        # Fallback to loading from file if available
        try:
            data_dir = os.environ.get('DATA_DIR', '/app/data')
            user_file = os.path.join(data_dir, 'yelp_academic_dataset_user.json')
            
            if os.path.exists(user_file):
                user_ids = []
                with open(user_file, 'r', encoding='utf-8') as f:
                    for i, line in enumerate(f):
                        if i >= 100:  # Limit to 100 users for demo
                            break
                        data = json.loads(line)
                        user_ids.append(data['user_id'])
                return user_ids
        except Exception as e:
            print(f"Error loading user IDs from file: {e}")
        
        # Fallback to sample IDs
        return ["sample_user_id_1", "sample_user_id_2", "sample_user_id_3"]
    
    def _generate_random_id(self, prefix=""):
        """Generate a random ID string."""
        random_id = ''.join(random.choices(string.ascii_letters + string.digits, k=22))
        return f"{prefix}{random_id}"
    
    def generate_business(self):
        """Generate a simulated business."""
        business_id = self._generate_random_id()
        city = random.choice(self.cities)
        state = random.choice(self.states)
        postal_code = f"{random.randint(10000, 99999)}"
        
        # Generate 1-3 random categories
        num_categories = random.randint(1, 3)
        business_categories = random.sample(self.categories, num_categories)
        
        # Create business name parts - fixed to avoid f-string backslash issues
        prefix_options = ["The", "New", "Original", "Classic", "Urban", "Golden", "Silver", "Blue", "Green"]
        owner_options = ["Joe's", "Mike's", "Amy's", "John's", "Sarah's", "David's"]
        type_options = ["Cafe", "Restaurant", "Diner", "Bistro", "Grill", "Place", "House", "Kitchen"]
        location_prefixes = ["Downtown", "Uptown", "Riverside", "Lakeside", "Hillside", "Seaside", "Central"]
        
        business_name_templates = [
            # Template 1: The/New/etc + Cafe/Restaurant/etc
            random.choice(prefix_options) + " " + random.choice(type_options),
            # Template 2: Joe's/Mike's/etc + Cafe/Restaurant/etc
            random.choice(owner_options) + " " + random.choice(type_options),
            # Template 3: [City] + Cafe/Restaurant/etc
            city + " " + random.choice(type_options),
            # Template 4: Downtown/Uptown/etc + Cafe/Restaurant/etc
            random.choice(location_prefixes) + " " + random.choice(type_options)
        ]
        
        business = {
            'business_id': business_id,
            'name': random.choice(business_name_templates),
            'city': city,
            'state': state,
            'postal_code': postal_code,
            'latitude': round(random.uniform(25, 49), 6),
            'longitude': round(random.uniform(-125, -70), 6),
            'stars': round(random.uniform(1, 5), 1),
            'review_count': 0,
            'is_open': 1,
            'categories': ','.join(business_categories),
            'attributes': {},  # Simplified attributes
            'hours': {},  # Simplified hours
            'source': 'streaming'
        }
        
        # Add to our local cache of business IDs
        self.business_ids.append(business_id)
        
        return business
    
    def generate_user(self):
        """Generate a simulated user."""
        user_id = self._generate_random_id()
        
        # Generate a random "yelping_since" date in the past 10 years
        days_ago = random.randint(1, 3650)  # Up to ~10 years ago
        yelping_since = (datetime.now() - timedelta(days=days_ago)).strftime('%Y-%m-%d')
        
        first_names = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", 
                      "William", "Elizabeth", "David", "Susan", "Richard", "Jessica", "Joseph", "Sarah", 
                      "Thomas", "Karen", "Charles", "Nancy", "Christopher", "Lisa", "Daniel", "Betty", 
                      "Matthew", "Dorothy", "Anthony", "Sandra", "Mark", "Ashley", "Donald", "Kimberly"]
                      
        last_names = ["Smith", "Johnson", "Williams", "Jones", "Brown", "Davis", "Miller", "Wilson", 
                     "Moore", "Taylor", "Anderson", "Thomas", "Jackson", "White", "Harris", "Martin", 
                     "Thompson", "Garcia", "Martinez", "Robinson", "Clark", "Rodriguez", "Lewis", "Lee", 
                     "Walker", "Hall", "Allen", "Young", "Hernandez", "King", "Wright", "Lopez"]
        
        name = f"{random.choice(first_names)} {random.choice(last_names)}"
        
        user = {
            'user_id': user_id,
            'name': name,
            'review_count': 0,
            'yelping_since': yelping_since,
            'fans': 0,
            'average_stars': 0.0,
            'source': 'streaming'
        }
        
        # Add to our local cache of user IDs
        self.user_ids.append(user_id)
        
        return user
    
    def generate_review(self):
        """Generate a simulated review."""
        business_id = random.choice(self.business_ids)
        user_id = random.choice(self.user_ids)
        stars = random.randint(1, 5)
        
        review = {
            'review_id': f"STREAM_{int(time.time())}_{random.randint(1000, 9999)}",
            'business_id': business_id,
            'user_id': user_id,
            'stars': stars,
            'date': datetime.now().isoformat(),
            'text': self._generate_review_text(stars),
            'useful': 0,
            'funny': 0,
            'cool': 0,
            'source': 'streaming'
        }
        
        return review
    
    def generate_checkin(self):
        """Generate a simulated checkin."""
        business_id = random.choice(self.business_ids)
        
        checkin = {
            'business_id': business_id,
            'date': datetime.now().isoformat(),
            'count': random.randint(1, 5),
            'source': 'streaming'
        }
        
        return checkin
    
    def _generate_review_text(self, stars):
        """Generate realistic review text based on star rating."""
        positive_phrases = [
            "Amazing experience! Highly recommend.",
            "The service was excellent.",
            "Best food I've had in a long time.",
            "Will definitely come back again.",
            "Great value for money.",
            "Friendly staff and fast service.",
            "Exceeded my expectations.",
            "Such a wonderful place!",
            "The atmosphere was perfect."
        ]
        
        negative_phrases = [
            "Disappointing experience overall.",
            "The service was slow and inattentive.",
            "Food was cold and undercooked.",
            "Would not recommend.",
            "Overpriced for what you get.",
            "Staff was unfriendly.",
            "Did not meet expectations.",
            "The place was too noisy and crowded.",
            "Long wait times and mediocre food."
        ]
        
        neutral_phrases = [
            "It was okay, nothing special.",
            "Average experience overall.",
            "Some things were good, others not so much.",
            "Might give it another try.",
            "Food was decent but service could be better.",
            "Reasonable prices but nothing extraordinary.",
            "Has potential but needs improvement."
        ]
        
        if stars >= 4:
            phrases = positive_phrases
        elif stars <= 2:
            phrases = negative_phrases
        else:
            phrases = neutral_phrases
        
        # Combine 2-3 phrases for a more realistic review
        num_phrases = random.randint(2, 3)
        selected_phrases = random.sample(phrases, num_phrases)
        
        return " ".join(selected_phrases)
    
    def stream_events(self, interval=1.0, count=None):
        """
        Stream events to Kafka topics.
        
        Args:
            interval: Time between events in seconds
            count: Number of events to generate (None for infinite)
        """
        i = 0
        try:
            print(f"Starting to stream events with interval {interval}s. Press Ctrl+C to stop.")
            
            while count is None or i < count:
                # Determine which type of event to send
                event_type = random.choices(
                    ["review", "checkin", "business", "user"],
                    weights=[0.6, 0.2, 0.1, 0.1],  # 60% reviews, 20% checkins, 10% businesses, 10% users
                    k=1
                )[0]
                
                if event_type == "review":
                    # Generate a review
                    review = self.generate_review()
                    self.producer.send('yelp-reviews', key=review['business_id'], value=review)
                    print(f"Sent review: {review['review_id']} for business {review['business_id']}")
                
                elif event_type == "checkin":
                    # Generate a checkin
                    checkin = self.generate_checkin()
                    self.producer.send('yelp-checkins', key=checkin['business_id'], value=checkin)
                    print(f"Sent checkin for business {checkin['business_id']}")
                
                elif event_type == "business":
                    # Generate a new business
                    business = self.generate_business()
                    self.producer.send('yelp-businesses', key=business['business_id'], value=business)
                    print(f"Sent new business: {business['name']} (ID: {business['business_id']})")
                
                elif event_type == "user":
                    # Generate a new user
                    user = self.generate_user()
                    self.producer.send('yelp-users', key=user['user_id'], value=user)
                    print(f"Sent new user: {user['name']} (ID: {user['user_id']})")
                
                self.producer.flush()
                i += 1
                
                # Sleep for the specified interval
                time.sleep(interval)
                
                # Print progress every 10 events
                if i % 10 == 0:
                    print(f"Sent {i} events so far...")
        
        except KeyboardInterrupt:
            print("\nStreaming stopped by user")
        finally:
            self.producer.close()
            print(f"Producer closed. Streamed {i} events.")


if __name__ == "__main__":
    import argparse
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Kafka producer for Yelp Data Warehouse")
    parser.add_argument("--interval", type=float, default=1.0, help="Interval between events in seconds (default: 1.0)")
    parser.add_argument("--count", type=int, default=100, help="Number of events to produce (default: 100)")
    
    args = parser.parse_args()
    
    # Initialize and run producer
    bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    producer = YelpKafkaProducer(bootstrap_servers=bootstrap_servers)
    producer.stream_events(interval=args.interval, count=args.count)