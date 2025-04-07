#!/usr/bin/env python3
"""
Kafka Consumer for Yelp Data Warehouse
Processes messages from Kafka topics and updates all database systems.
"""

import os
import json
import time
import pymysql
import pymongo
import requests
from neo4j import GraphDatabase
from kafka import KafkaConsumer
from datetime import datetime

class YelpKafkaConsumer:
    """
    Consumer that processes Yelp data events from Kafka topics and loads them into databases.
    """
    
    def __init__(self, bootstrap_servers='kafka:9092', topics=['yelp-reviews', 'yelp-checkins']):
        """Initialize Kafka consumer and database connections."""
        print(f"Initializing Kafka consumer with bootstrap servers: {bootstrap_servers}")
        self.consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=bootstrap_servers,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='latest',
            group_id='yelp-consumers'
        )
        
        # Initialize database connections
        self._init_mysql()
        self._init_mongodb()
        self._init_neo4j()
        
        # Processing state
        self.running = True
    
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
            
            # Create a lookup dictionary for date_actual to time_id
            cursor = self.mysql_conn.cursor()
            cursor.execute("SELECT time_id, date_actual FROM dim_time")
            self.time_lookup = {row['date_actual'].strftime('%Y-%m-%d'): row['time_id'] for row in cursor.fetchall()}
            cursor.close()
            
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
    
    def _init_neo4j(self):
        """Initialize Neo4j connection."""
        try:
            neo4j_host = os.environ.get('NEO4J_HOST', 'neo4j')
            neo4j_port = int(os.environ.get('NEO4J_PORT', 7687))
            neo4j_user = os.environ.get('NEO4J_USER', 'neo4j')
            neo4j_password = os.environ.get('NEO4J_PASSWORD', 'user')
            
            self.neo4j_driver = GraphDatabase.driver(
                f"bolt://{neo4j_host}:{neo4j_port}", 
                auth=(neo4j_user, neo4j_password)
            )
            # Test connection
            with self.neo4j_driver.session() as session:
                result = session.run("RETURN 1")
                result.single()
            print("Neo4j connection established")
        except Exception as e:
            print(f"Neo4j connection error: {e}")
            self.neo4j_driver = None
    
    def _broadcast_event(self, database, event_type, data):
        """
        Send event data to the web application for broadcasting to connected clients
        """
        # Get the webapp URL from environment or use default
        webapp_url = os.environ.get('WEBAPP_URL', "http://webapp:8080")
        broadcast_endpoint = f"{webapp_url}/api/broadcast"
        
        payload = {
            'database': database,
            'event_type': event_type,
            'data': data
        }
        
        try:
            response = requests.post(broadcast_endpoint, json=payload, timeout=5)
            if response.status_code == 200:
                print(f"Successfully broadcast {event_type} event for {database}")
            else:
                print(f"Failed to broadcast event: {response.status_code} - {response.text}")
        except Exception as e:
            print(f"Error broadcasting event: {e}")

    def process_messages(self):
        """Process messages from Kafka topics."""
        print("Starting to consume messages from Kafka. Press Ctrl+C to stop.")
        try:
            for message in self.consumer:
                topic = message.topic
                value = message.value
                
                # Process message based on topic
                if topic == 'yelp-reviews':
                    if self.mysql_conn:
                        self._process_review_mysql(value)
                    if self.mongodb_db:
                        self._process_review_mongodb(value)
                    if self.neo4j_driver:
                        with self.neo4j_driver.session() as session:
                            self._process_review_neo4j(session, value)
                            
                elif topic == 'yelp-checkins':
                    if self.mysql_conn:
                        self._process_checkin_mysql(value)
                    if self.mongodb_db:
                        self._process_checkin_mongodb(value)
                    if self.neo4j_driver:
                        with self.neo4j_driver.session() as session:
                            self._process_checkin_neo4j(session, value)
                
                print(f"Processed {topic} message for business {value.get('business_id', '')}")
                
        except KeyboardInterrupt:
            print("Message processing stopped by user")
        finally:
            self.running = False
            self._close_connections()
    
    def _close_connections(self):
        """Close all database connections."""
        if self.mysql_conn:
            self.mysql_conn.close()
            print("MySQL connection closed")
        
        if self.mongodb_client:
            self.mongodb_client.close()
            print("MongoDB connection closed")
        
        if self.neo4j_driver:
            self.neo4j_driver.close()
            print("Neo4j connection closed")
            
    # Database-specific message processing methods
    
    def _process_review_mysql(self, review):
        """Process a review message for MySQL."""
        if not self.mysql_conn:
            return
        
        cursor = self.mysql_conn.cursor()
        
        try:
            # Parse date and get time_id
            date_str = datetime.fromisoformat(review.get('date')).strftime('%Y-%m-%d')
            time_id = self.time_lookup.get(date_str)
            
            if not time_id:
                print(f"Time ID not found for date: {date_str}")
                return
            
            # Insert review into fact_review table
            cursor.execute("""
                INSERT IGNORE INTO fact_review 
                (review_id, business_id, user_id, time_id, stars, useful_votes, funny_votes, cool_votes)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                review['review_id'],
                review['business_id'],
                review['user_id'],
                time_id,
                review.get('stars', 0),
                review.get('useful', 0),
                review.get('funny', 0),
                review.get('cool', 0)
            ))
            
            # Update business review count and stars
            cursor.execute("""
                UPDATE dim_business
                SET review_count = review_count + 1
                WHERE business_id = %s
            """, (review['business_id'],))
            
            # Update summary table
            cursor.execute("""
                INSERT INTO summary_business_performance (business_id, total_reviews, avg_rating, total_checkins)
                VALUES (%s, 1, %s, 0)
                ON DUPLICATE KEY UPDATE
                total_reviews = total_reviews + 1,
                avg_rating = (avg_rating * total_reviews + %s) / (total_reviews + 1)
            """, (
                review['business_id'],
                review.get('stars', 0),
                review.get('stars', 0)
            ))
            
            # Commit the transaction
            self.mysql_conn.commit()
            
            # Broadcast the event
            self._broadcast_event('mysql', 'review', {
                'review_id': review['review_id'],
                'business_id': review['business_id'],
                'user_id': review['user_id'],
                'stars': review.get('stars', 0),
                'date': review.get('date')
            })
        
        except Exception as e:
            print(f"Error processing review in MySQL: {e}")
            self.mysql_conn.rollback()
        finally:
            cursor.close()
    
    def _process_checkin_mysql(self, checkin):
        """Process a checkin message for MySQL."""
        if not self.mysql_conn:
            return
        
        cursor = self.mysql_conn.cursor()
        
        try:
            # Parse date and get time_id
            date_str = datetime.fromisoformat(checkin.get('date')).strftime('%Y-%m-%d')
            time_id = self.time_lookup.get(date_str)
            
            if not time_id:
                print(f"Time ID not found for date: {date_str}")
                return
            
            # Insert checkin into fact_checkin table
            cursor.execute("""
                INSERT INTO fact_checkin 
                (business_id, time_id, checkin_count)
                VALUES (%s, %s, %s)
            """, (
                checkin['business_id'],
                time_id,
                checkin.get('count', 1)
            ))
            
            # Update summary table
            cursor.execute("""
                INSERT INTO summary_business_performance (business_id, total_reviews, avg_rating, total_checkins)
                VALUES (%s, 0, 0, %s)
                ON DUPLICATE KEY UPDATE
                total_checkins = total_checkins + %s
            """, (
                checkin['business_id'],
                checkin.get('count', 1),
                checkin.get('count', 1)
            ))
            
            # Commit the transaction
            self.mysql_conn.commit()
            
            # Broadcast the event
            self._broadcast_event('mysql', 'checkin', {
                'business_id': checkin['business_id'],
                'date': checkin.get('date'),
                'count': checkin.get('count', 1)
            })
        
        except Exception as e:
            print(f"Error processing checkin in MySQL: {e}")
            self.mysql_conn.rollback()
        finally:
            cursor.close()
    
    def _process_review_mongodb(self, review):
        """Process a review message for MongoDB."""
        if not self.mongodb_db:
            return
        
        try:
            # Convert date string to datetime
            if 'date' in review:
                try:
                    review['date'] = datetime.fromisoformat(review['date'])
                except:
                    pass
            
            # Insert review into reviews collection
            self.mongodb_db.reviews.insert_one(review)
            
            # Update business document
            self.mongodb_db.businesses.update_one(
                {'business_id': review['business_id']},
                {'$inc': {'review_count': 1}}
            )
            
            # Update business summary
            self.mongodb_db.business_summaries.update_one(
                {'business_id': review['business_id']},
                {
                    '$inc': {
                        'review_stats.review_count': 1,
                        f"review_stats.{'five_star_count' if review.get('stars') == 5 else 'one_star_count' if review.get('stars') == 1 else 'other_star_count'}": 1
                    },
                    '$set': {
                        'review_stats.avg_stars': {
                            '$avg': [
                                '$review_stats.avg_stars',
                                review.get('stars', 0)
                            ]
                        }
                    }
                },
                upsert=True
            )
            
            # Broadcast the event
            self._broadcast_event('mongodb', 'review', {
                'review_id': review['review_id'],
                'business_id': review['business_id'],
                'user_id': review['user_id'],
                'stars': review.get('stars', 0),
                'date': review.get('date').isoformat() if isinstance(review.get('date'), datetime) else review.get('date')
            })
        
        except Exception as e:
            print(f"Error processing review in MongoDB: {e}")
    
    def _process_checkin_mongodb(self, checkin):
        """Process a checkin message for MongoDB."""
        if not self.mongodb_db:
            return
        
        try:
            # Convert date to datetime and store in dates array
            dates = []
            if 'date' in checkin:
                try:
                    dates = [datetime.fromisoformat(checkin['date'])] * checkin.get('count', 1)
                except:
                    pass
            
            checkin['dates'] = dates
            if 'date' in checkin:
                del checkin['date']
            
            # Insert checkin into checkins collection
            self.mongodb_db.checkins.insert_one(checkin)
            
            # Update business summary
            self.mongodb_db.business_summaries.update_one(
                {'business_id': checkin['business_id']},
                {'$inc': {'checkin_count': checkin.get('count', 1)}},
                upsert=True
            )
            
            # Broadcast the event
            self._broadcast_event('mongodb', 'checkin', {
                'business_id': checkin['business_id'],
                'count': checkin.get('count', 1)
            })
        
        except Exception as e:
            print(f"Error processing checkin in MongoDB: {e}")
    
    def _process_review_neo4j(self, session, review):
        """Process a review message for Neo4j."""
        try:
            # Create review node and connect to Business, User and Time
            query = """
            MATCH (b:Business {business_id: $business_id})
            MATCH (u:User {user_id: $user_id})
            MATCH (t:Time {date: date($date)})
            
            CREATE (r:Review {review_id: $review_id})
            SET r.stars = $stars,
                r.useful = $useful,
                r.funny = $funny,
                r.cool = $cool,
                r.text = $text,
                r.date = date($date),
                r.source = 'streaming'
            
            CREATE (u)-[:WROTE]->(r)
            CREATE (r)-[:REVIEWS]->(b)
            CREATE (r)-[:ON_DATE]->(t)
            
            // Update business stats
            SET b.review_count = coalesce(b.review_count, 0) + 1,
                b.summary_review_count = coalesce(b.summary_review_count, 0) + 1,
                b.summary_avg_stars = (coalesce(b.summary_avg_stars, 0) * coalesce(b.summary_review_count, 0) + $stars) / (coalesce(b.summary_review_count, 0) + 1)
            """
            
            session.run(query, {
                'review_id': review['review_id'],
                'business_id': review['business_id'],
                'user_id': review['user_id'],
                'stars': review.get('stars', 0),
                'useful': review.get('useful', 0),
                'funny': review.get('funny', 0),
                'cool': review.get('cool', 0),
                'text': review.get('text', ''),
                'date': review.get('date')
            })
            
            # Broadcast the event
            self._broadcast_event('neo4j', 'review', {
                'review_id': review['review_id'],
                'business_id': review['business_id'],
                'user_id': review['user_id'],
                'stars': review.get('stars', 0),
                'date': review.get('date')
            })
        
        except Exception as e:
            print(f"Error processing review in Neo4j: {e}")
    
    def _process_checkin_neo4j(self, session, checkin):
        """Process a checkin message for Neo4j."""
        try:
            # Create or update checkin relationship
            query = """
            MATCH (b:Business {business_id: $business_id})
            MATCH (t:Time {date: date($date)})
            
            MERGE (b)-[c:HAD_CHECKIN]->(t)
            ON CREATE SET c.count = $count
            ON MATCH SET c.count = c.count + $count
            
            // Update business stats
            SET b.summary_checkin_count = coalesce(b.summary_checkin_count, 0) + $count
            """
            
            session.run(query, {
                'business_id': checkin['business_id'],
                'date': checkin.get('date'),
                'count': checkin.get('count', 1)
            })
            
            # Broadcast the event
            self._broadcast_event('neo4j', 'checkin', {
                'business_id': checkin['business_id'],
                'date': checkin.get('date'),
                'count': checkin.get('count', 1)
            })
        
        except Exception as e:
            print(f"Error processing checkin in Neo4j: {e}")


if __name__ == "__main__":
    # Initialize and run consumer
    bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    consumer = YelpKafkaConsumer(bootstrap_servers=bootstrap_servers)
    print("Starting to consume events from Kafka. Press Ctrl+C to stop.")
    consumer.process_messages()