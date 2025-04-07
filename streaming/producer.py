#!/usr/bin/env python3
"""
Kafka Producer for Yelp Data Warehouse
Generates simulated Yelp data events and sends them to Kafka topics.
"""

import os
import json
import time
import random
import pymysql
from kafka import KafkaProducer
from datetime import datetime

class YelpKafkaProducer:
    """
    Producer that generates simulated Yelp data events and sends them to Kafka topics.
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
                # Generate a review (70% probability)
                if random.random() < 0.7:
                    review = self.generate_review()
                    self.producer.send('yelp-reviews', key=review['business_id'], value=review)
                    print(f"Sent review: {review['review_id']} for business {review['business_id']}")
                
                # Generate a checkin (30% probability)
                else:
                    checkin = self.generate_checkin()
                    self.producer.send('yelp-checkins', key=checkin['business_id'], value=checkin)
                    print(f"Sent checkin for business {checkin['business_id']}")
                
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