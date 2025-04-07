#!/usr/bin/env python3
"""
Dataset Kafka Streamer for Yelp Data Warehouse

This script reads from the Yelp dataset files and streams records not already loaded
into the database to Kafka topics, allowing for real-time data updates.
"""

import os
import sys
import json
import time
import random
import argparse
import pymysql
import pymongo
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError
from datetime import datetime


class YelpDatasetStreamer:
    """
    Streams data from Yelp dataset files to Kafka topics, skipping records
    that have already been loaded into the databases during initialization.
    """
    
    def __init__(self, bootstrap_servers='localhost:9092'):
        """Initialize Kafka producer and database connections for filtering."""
        # Use environment variable if available
        actual_bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', bootstrap_servers)
        print(f"Connecting to Kafka at {actual_bootstrap_servers}")
        
        # Add retry logic for Kafka connection
        max_retries = 5
        retry_interval = 3
        for attempt in range(max_retries):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=actual_bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    key_serializer=lambda k: k.encode('utf-8') if k else None
                )
                print("Successfully connected to Kafka broker")
                break
            except Exception as e:
                print(f"Attempt {attempt+1}/{max_retries} - Failed to connect to Kafka: {e}")
                if attempt < max_retries - 1:
                    print(f"Retrying in {retry_interval} seconds...")
                    time.sleep(retry_interval)
                else:
                    print("Failed to connect to Kafka after multiple attempts")
                    raise
            
        # Initialize database connections for checking existing records
        self._init_mysql()
        self._init_mongodb()
        
        # Load configuration
        self.data_dir = os.environ.get('DATA_DIR', '/app/data')
        
        # Set file paths
        self.review_file = os.path.join(self.data_dir, 'yelp_academic_dataset_review.json')
        self.checkin_file = os.path.join(self.data_dir, 'yelp_academic_dataset_checkin.json')
        self.business_file = os.path.join(self.data_dir, 'yelp_academic_dataset_business.json')
        
        print(f"Initialized YelpDatasetStreamer with data directory: {self.data_dir}")
        
        # Get existing IDs to avoid duplicates
        self.loaded_business_ids = self._get_loaded_business_ids()
        self.loaded_review_ids = self._get_loaded_review_ids(limit=50000)  # Limit to avoid memory issues
        
        print(f"Found {len(self.loaded_business_ids)} existing businesses")
        print(f"Found {len(self.loaded_review_ids)} existing reviews")
    
    def _init_mysql(self):
        """Initialize MySQL connection."""
        try:
            self.mysql_conn = pymysql.connect(
                host=os.environ.get('MYSQL_HOST', 'mysql'),
                port=int(os.environ.get('MYSQL_PORT', 3306)),
                user=os.environ.get('MYSQL_USER', 'root'),
                password=os.environ.get('MYSQL_PASSWORD', 'user'),
                db=os.environ.get('MYSQL_DATABASE', 'yelp_dw'),
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
            )
            print("MySQL connection established")
        except Exception as e:
            print(f"MySQL connection error: {e}")
            self.mysql_conn = None
    
    def _init_mongodb(self):
        """Initialize MongoDB connection."""
        try:
            mongo_host = os.environ.get('MONGO_HOST', 'mongodb')
            mongo_port = int(os.environ.get('MONGO_PORT', 27017))
            mongo_user = os.environ.get('MONGO_USER', 'user')
            mongo_password = os.environ.get('MONGO_PASSWORD', 'user')
            
            mongo_uri = f"mongodb://{mongo_user}:{mongo_password}@{mongo_host}:{mongo_port}/"
            self.mongodb_client = pymongo.MongoClient(mongo_uri)
            self.mongodb_db = self.mongodb_client['yelp_db']
            print("MongoDB connection established")
        except Exception as e:
            print(f"MongoDB connection error: {e}")
            self.mongodb_client = None
            self.mongodb_db = None
    
    def _get_loaded_business_ids(self):
        """Get business IDs that have already been loaded into the database."""
        loaded_ids = set()
        
        # Get IDs from MySQL
        if self.mysql_conn:
            try:
                cursor = self.mysql_conn.cursor()
                cursor.execute("SELECT business_id FROM dim_business")
                results = cursor.fetchall()
                cursor.close()
                for row in results:
                    loaded_ids.add(row['business_id'])
                print(f"Found {len(loaded_ids)} businesses in MySQL")
            except Exception as e:
                print(f"Error querying MySQL for business IDs: {e}")
        
        # Get IDs from MongoDB as a backup or supplement
        if self.mongodb_db:
            try:
                count_before = len(loaded_ids)
                mongo_ids = {doc['business_id'] for doc in self.mongodb_db.businesses.find({}, {'business_id': 1})}
                loaded_ids.update(mongo_ids)
                print(f"Found {len(mongo_ids)} businesses in MongoDB (added {len(loaded_ids) - count_before} new IDs)")
            except Exception as e:
                print(f"Error querying MongoDB for business IDs: {e}")
        
        return loaded_ids
    
    def _get_loaded_review_ids(self, limit=50000):
        """Get review IDs that have already been loaded into the database."""
        loaded_ids = set()
        
        # Get IDs from MySQL
        if self.mysql_conn:
            try:
                cursor = self.mysql_conn.cursor()
                # Get a sample of IDs to determine if we need the full set
                cursor.execute("SELECT COUNT(*) as count FROM fact_review")
                count = cursor.fetchone()['count']
                
                # If we have many reviews, just load a sample to avoid memory issues
                if count > limit:
                    cursor.execute(f"SELECT review_id FROM fact_review LIMIT {limit}")
                    print(f"Sampling {limit} review IDs from {count} total reviews in MySQL")
                else:
                    cursor.execute("SELECT review_id FROM fact_review")
                    print(f"Loading all {count} review IDs from MySQL")
                
                results = cursor.fetchall()
                cursor.close()
                for row in results:
                    loaded_ids.add(row['review_id'])
            except Exception as e:
                print(f"Error querying MySQL for review IDs: {e}")
        
        # We won't load MongoDB review IDs to avoid memory issues
        # The streaming logic will check for duplicates in both databases
        
        return loaded_ids
    
    def _check_review_exists(self, review_id):
        """Check if a review exists in any database, including those not in memory."""
        # First check in-memory cache
        if review_id in self.loaded_review_ids:
            return True
        
        # Then check MySQL directly
        if self.mysql_conn:
            try:
                cursor = self.mysql_conn.cursor()
                cursor.execute("SELECT 1 FROM fact_review WHERE review_id = %s LIMIT 1", (review_id,))
                result = cursor.fetchone()
                cursor.close()
                if result:
                    # Add to in-memory cache for future checks
                    self.loaded_review_ids.add(review_id)
                    return True
            except Exception as e:
                print(f"Error checking review existence in MySQL: {e}")
        
        # Check MongoDB
        if self.mongodb_db:
            try:
                result = self.mongodb_db.reviews.find_one({'review_id': review_id}, {'_id': 1})
                if result:
                    # Add to in-memory cache for future checks
                    self.loaded_review_ids.add(review_id)
                    return True
            except Exception as e:
                print(f"Error checking review existence in MongoDB: {e}")
        
        return False
    
    def stream_reviews(self, count=None, interval_range=(0.1, 1.0)):
        """
        Stream reviews from the dataset file to the Kafka topic.
        
        Args:
            count: Number of reviews to stream (None for unlimited)
            interval_range: Tuple of (min, max) seconds between messages
        """
        if not os.path.exists(self.review_file):
            print(f"Review file not found: {self.review_file}")
            return
        
        print(f"Starting to stream reviews from: {self.review_file}")
        
        sent_count = 0
        skipped_count = 0
        
        with open(self.review_file, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f):
                if not line.strip():
                    continue
                
                review = json.loads(line)
                
                # Skip if this review is already loaded (check memory cache first, then DB)
                if review['review_id'] in self.loaded_review_ids or self._check_review_exists(review['review_id']):
                    skipped_count += 1
                    if skipped_count % 1000 == 0:
                        print(f"Skipped {skipped_count} reviews so far...")
                    continue
                
                # Skip if the business is not in our loaded set
                if review['business_id'] not in self.loaded_business_ids:
                    skipped_count += 1
                    continue
                
                # Add a source field to identify this as streaming data
                review['source'] = 'streaming'
                
                # Send to Kafka
                try:
                    self.producer.send('yelp-reviews', key=review['business_id'], value=review)
                    sent_count += 1
                    
                    # Print progress
                    if sent_count % 10 == 0:
                        print(f"Sent {sent_count} reviews, skipped {skipped_count}")
                    
                    # Flush for every message to ensure delivery
                    self.producer.flush()
                    
                    # Sleep for a random interval to simulate real-time
                    time.sleep(random.uniform(*interval_range))
                    
                    # Check if we've reached the desired count
                    if count is not None and sent_count >= count:
                        break
                    
                except KafkaError as e:
                    print(f"Error sending review to Kafka: {e}")
                except Exception as e:
                    print(f"Unexpected error: {e}")
        
        print(f"Finished streaming reviews. Sent {sent_count}, skipped {skipped_count}")
    
    def stream_checkins(self, count=None, interval_range=(0.5, 2.0)):
        """
        Stream checkins from the dataset file to the Kafka topic.
        
        Args:
            count: Number of checkins to stream (None for unlimited)
            interval_range: Tuple of (min, max) seconds between messages
        """
        if not os.path.exists(self.checkin_file):
            print(f"Checkin file not found: {self.checkin_file}")
            return
        
        print(f"Starting to stream checkins from: {self.checkin_file}")
        
        sent_count = 0
        skipped_count = 0
        
        with open(self.checkin_file, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f):
                if not line.strip():
                    continue
                
                checkin = json.loads(line)
                
                # Skip if the business is not in our loaded set
                if checkin['business_id'] not in self.loaded_business_ids:
                    skipped_count += 1
                    continue
                
                # Add a source field to identify this as streaming data
                checkin['source'] = 'streaming'
                
                # Process dates
                if isinstance(checkin['date'], str):
                    # Old format: we'll split the dates and send each one individually
                    all_dates = checkin['date'].split(', ')
                    for date_batch in [all_dates[i:i+5] for i in range(0, len(all_dates), 5)]:
                        # Create a new checkin with a subset of dates
                        single_checkin = {
                            'business_id': checkin['business_id'],
                            'date': datetime.now().isoformat(),
                            'count': len(date_batch),
                            'source': 'streaming'
                        }
                        
                        try:
                            self.producer.send('yelp-checkins', key=checkin['business_id'], value=single_checkin)
                            sent_count += 1
                            
                            # Print progress
                            if sent_count % 10 == 0:
                                print(f"Sent {sent_count} checkins, skipped {skipped_count}")
                            
                            # Flush for every message to ensure delivery
                            self.producer.flush()
                            
                            # Sleep for a random interval to simulate real-time
                            time.sleep(random.uniform(*interval_range))
                            
                            # Check if we've reached the desired count
                            if count is not None and sent_count >= count:
                                break
                        
                        except KafkaError as e:
                            print(f"Error sending checkin to Kafka: {e}")
                        except Exception as e:
                            print(f"Unexpected error: {e}")
                
                else:
                    # New format with dictionary of dates:times
                    for date, times in checkin['date'].items():
                        time_count = len(times.split(', '))
                        single_checkin = {
                            'business_id': checkin['business_id'],
                            'date': date,
                            'count': time_count,
                            'source': 'streaming'
                        }
                        
                        try:
                            self.producer.send('yelp-checkins', key=checkin['business_id'], value=single_checkin)
                            sent_count += 1
                            
                            # Print progress
                            if sent_count % 10 == 0:
                                print(f"Sent {sent_count} checkins, skipped {skipped_count}")
                            
                            # Flush for every message to ensure delivery
                            self.producer.flush()
                            
                            # Sleep for a random interval to simulate real-time
                            time.sleep(random.uniform(*interval_range))
                            
                            # Check if we've reached the desired count
                            if count is not None and sent_count >= count:
                                break
                        
                        except KafkaError as e:
                            print(f"Error sending checkin to Kafka: {e}")
                        except Exception as e:
                            print(f"Unexpected error: {e}")
                
                # Check if we've reached the desired count
                if count is not None and sent_count >= count:
                    break
        
        print(f"Finished streaming checkins. Sent {sent_count}, skipped {skipped_count}")
    
    def stream_mixed(self, count=None, review_ratio=0.7, interval_range=(0.5, 2.0)):
        """
        Stream a mix of reviews and checkins, approximately maintaining the given ratio.
        
        Args:
            count: Total number of events to stream (None for unlimited)
            review_ratio: Approximate ratio of reviews to total events (0.7 = 70% reviews)
            interval_range: Tuple of (min, max) seconds between messages
        """
        # Open file handles
        if not os.path.exists(self.review_file) or not os.path.exists(self.checkin_file):
            print("Review or checkin file not found")
            return
        
        review_file = open(self.review_file, 'r', encoding='utf-8')
        checkin_file = open(self.checkin_file, 'r', encoding='utf-8')
        
        try:
            sent_count = 0
            skipped_count = 0
            review_count = 0
            checkin_count = 0
            
            print(f"Starting mixed streaming with review ratio: {review_ratio}")
            
            # Keep track of reviews we've already seen
            seen_reviews = set()
            
            # Read through review file first
            review_buffer = []
            for line in review_file:
                if not line.strip():
                    continue
                
                review = json.loads(line)
                
                # Skip if already processed or business not loaded
                if (review['review_id'] in self.loaded_review_ids or 
                    review['review_id'] in seen_reviews or
                    self._check_review_exists(review['review_id']) or
                    review['business_id'] not in self.loaded_business_ids):
                    continue
                
                seen_reviews.add(review['review_id'])
                review['source'] = 'streaming'
                review_buffer.append(review)
                
                # Stop once we have a decent buffer
                if len(review_buffer) >= 1000:
                    break
            
            # Read through checkin file
            checkin_buffer = []
            for line in checkin_file:
                if not line.strip():
                    continue
                
                checkin = json.loads(line)
                
                # Skip if business not loaded
                if checkin['business_id'] not in self.loaded_business_ids:
                    continue
                
                checkin['source'] = 'streaming'
                
                # Process dates
                if isinstance(checkin['date'], str):
                    # Old format with comma-separated dates
                    all_dates = checkin['date'].split(', ')
                    for i in range(0, len(all_dates), 5):
                        date_batch = all_dates[i:i+5]
                        single_checkin = {
                            'business_id': checkin['business_id'],
                            'date': datetime.now().isoformat(),
                            'count': len(date_batch),
                            'source': 'streaming'
                        }
                        checkin_buffer.append(single_checkin)
                else:
                    # New format with dictionary
                    for date, times in checkin['date'].items():
                        time_count = len(times.split(', '))
                        single_checkin = {
                            'business_id': checkin['business_id'],
                            'date': date,
                            'count': time_count,
                            'source': 'streaming'
                        }
                        checkin_buffer.append(single_checkin)
                
                # Stop once we have a decent buffer
                if len(checkin_buffer) >= 1000:
                    break
            
            print(f"Loaded {len(review_buffer)} reviews and {len(checkin_buffer)} checkins into buffers")
            
            # Main streaming loop
            while True:
                # Decide whether to send a review or checkin based on the ratio
                should_send_review = (random.random() < review_ratio)
                
                if should_send_review and review_buffer:
                    # Send a review
                    review = review_buffer.pop(0)
                    try:
                        self.producer.send('yelp-reviews', key=review['business_id'], value=review)
                        self.producer.flush()
                        sent_count += 1
                        review_count += 1
                    except Exception as e:
                        print(f"Error sending review: {e}")
                        skipped_count += 1
                    
                    # Replenish review buffer if needed
                    if len(review_buffer) < 100:
                        for _ in range(min(50, 1000 - len(review_buffer))):
                            line = review_file.readline()
                            if not line.strip():
                                continue
                            
                            try:
                                review = json.loads(line)
                                
                                # Skip if already processed or business not loaded
                                if (review['review_id'] in self.loaded_review_ids or 
                                    review['review_id'] in seen_reviews or
                                    self._check_review_exists(review['review_id']) or
                                    review['business_id'] not in self.loaded_business_ids):
                                    continue
                                
                                seen_reviews.add(review['review_id'])
                                review['source'] = 'streaming'
                                review_buffer.append(review)
                            except Exception as e:
                                print(f"Error parsing review: {e}")
                
                elif checkin_buffer:
                    # Send a checkin
                    checkin = checkin_buffer.pop(0)
                    try:
                        self.producer.send('yelp-checkins', key=checkin['business_id'], value=checkin)
                        self.producer.flush()
                        sent_count += 1
                        checkin_count += 1
                    except Exception as e:
                        print(f"Error sending checkin: {e}")
                        skipped_count += 1
                    
                    # Replenish checkin buffer if needed
                    if len(checkin_buffer) < 100:
                        for _ in range(min(50, 1000 - len(checkin_buffer))):
                            line = checkin_file.readline()
                            if not line.strip():
                                continue
                            
                            try:
                                checkin = json.loads(line)
                                
                                # Skip if business not loaded
                                if checkin['business_id'] not in self.loaded_business_ids:
                                    continue
                                
                                checkin['source'] = 'streaming'
                                
                                # Process dates (similar to above)
                                if isinstance(checkin['date'], str):
                                    all_dates = checkin['date'].split(', ')
                                    for i in range(0, len(all_dates), 5):
                                        date_batch = all_dates[i:i+5]
                                        single_checkin = {
                                            'business_id': checkin['business_id'],
                                            'date': datetime.now().isoformat(),
                                            'count': len(date_batch),
                                            'source': 'streaming'
                                        }
                                        checkin_buffer.append(single_checkin)
                                else:
                                    for date, times in checkin['date'].items():
                                        time_count = len(times.split(', '))
                                        single_checkin = {
                                            'business_id': checkin['business_id'],
                                            'date': date,
                                            'count': time_count,
                                            'source': 'streaming'
                                        }
                                        checkin_buffer.append(single_checkin)
                            except Exception as e:
                                print(f"Error parsing checkin: {e}")
                
                else:
                    # If both buffers are empty, we're done
                    print("Both buffers empty, ending stream")
                    break
                
                # Print progress periodically
                if sent_count % 10 == 0:
                    print(f"Sent {sent_count} events ({review_count} reviews, {checkin_count} checkins), skipped {skipped_count}")
                
                # Check if we've reached the desired count
                if count is not None and sent_count >= count:
                    print(f"Reached target count of {count} events")
                    break
                
                # Sleep for a random interval to simulate real-time
                time.sleep(random.uniform(*interval_range))
        
        finally:
            # Close file handles
            review_file.close()
            checkin_file.close()
            
            print(f"Finished mixed streaming. Sent {sent_count} events ({review_count} reviews, {checkin_count} checkins), skipped {skipped_count}")
    
    def close(self):
        """Close connections."""
        self.producer.close()
        
        if self.mysql_conn:
            self.mysql_conn.close()
        
        if self.mongodb_client:
            self.mongodb_client.close()


if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Stream Yelp dataset to Kafka topics")
    parser.add_argument("--mode", choices=["reviews", "checkins", "mixed"], default="mixed",
                      help="Type of data to stream (default: mixed)")
    parser.add_argument("--count", type=int, default=None,
                      help="Number of events to stream (default: unlimited)")
    parser.add_argument("--interval", type=float, default=0.5,
                      help="Base interval between events in seconds (default: 0.5)")
    parser.add_argument("--bootstrap-servers", default="localhost:9092",
                      help="Kafka bootstrap servers (default: localhost:9092)")
    
    args = parser.parse_args()
    
    try:
        # Initialize streamer
        streamer = YelpDatasetStreamer(bootstrap_servers=args.bootstrap_servers)
        
        # Stream data based on mode
        if args.mode == "reviews":
            streamer.stream_reviews(count=args.count, interval_range=(args.interval, args.interval * 2))
        elif args.mode == "checkins":
            streamer.stream_checkins(count=args.count, interval_range=(args.interval, args.interval * 2))
        else:  # mixed
            streamer.stream_mixed(count=args.count, interval_range=(args.interval, args.interval * 2))
    
    except KeyboardInterrupt:
        print("\nStreaming interrupted by user")
    except Exception as e:
        print(f"Error during streaming: {e}")
    finally:
        if 'streamer' in locals():
            streamer.close()