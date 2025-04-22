#!/usr/bin/env python3
"""
Enhanced Kafka Consumer for Yelp Data Warehouse
Processes messages from Kafka topics and updates all database systems.
This version handles business and user creation messages too.
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
    
    def __init__(self, bootstrap_servers='kafka:9092', 
                 topics=['yelp-reviews', 'yelp-checkins', 'yelp-businesses', 'yelp-users']):
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
                    if self.mysql_conn is not None:
                        self._process_review_mysql(value)
                    if self.mongodb_db is not None:
                        self._process_review_mongodb(value)
                    if self.neo4j_driver is not None:
                        with self.neo4j_driver.session() as session:
                            self._process_review_neo4j(session, value)
                            
                elif topic == 'yelp-checkins':
                    if self.mysql_conn is not None:
                        self._process_checkin_mysql(value)
                    if self.mongodb_db is not None:
                        self._process_checkin_mongodb(value)
                    if self.neo4j_driver is not None:
                        with self.neo4j_driver.session() as session:
                            self._process_checkin_neo4j(session, value)
                
                elif topic == 'yelp-businesses':
                    if self.mysql_conn is not None:
                        self._process_business_mysql(value)
                    if self.mongodb_db is not None:
                        self._process_business_mongodb(value)
                    if self.neo4j_driver is not None:
                        with self.neo4j_driver.session() as session:
                            self._process_business_neo4j(session, value)
                
                elif topic == 'yelp-users':
                    if self.mysql_conn is not None:
                        self._process_user_mysql(value)
                    if self.mongodb_db is not None:
                        self._process_user_mongodb(value)
                    if self.neo4j_driver is not None:
                        with self.neo4j_driver.session() as session:
                            self._process_user_neo4j(session, value)
                
                print(f"Processed {topic} message for {value.get('business_id', value.get('user_id', 'unknown'))}")
                
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
    
    def _process_business_mysql(self, business):
        """Process a business message for MySQL."""
        if not self.mysql_conn:
            return
        
        cursor = self.mysql_conn.cursor()
        
        try:
            # First, check if the location exists
            cursor.execute("""
                SELECT location_id FROM dim_location 
                WHERE city = %s AND state = %s AND postal_code = %s
            """, (
                business.get('city', ''),
                business.get('state', ''),
                business.get('postal_code', '')
            ))
            
            location = cursor.fetchone()
            
            if location:
                location_id = location['location_id']
            else:
                # Insert new location
                cursor.execute("""
                    INSERT INTO dim_location (city, state, postal_code, latitude, longitude)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    business.get('city', ''),
                    business.get('state', ''),
                    business.get('postal_code', ''),
                    business.get('latitude'),
                    business.get('longitude')
                ))
                
                # Get the newly created location_id
                cursor.execute("SELECT LAST_INSERT_ID() as location_id")
                location_id = cursor.fetchone()['location_id']
            
            # Insert business
            cursor.execute("""
                INSERT IGNORE INTO dim_business 
                (business_id, business_name, location_id, stars, review_count, is_open)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                business['business_id'],
                business['name'],
                location_id,
                business.get('stars', 0),
                business.get('review_count', 0),
                business.get('is_open', 1)
            ))
            
            # Process categories if present
            if 'categories' in business and business['categories']:
                categories = business['categories'].split(',')
                
                for category in categories:
                    category = category.strip()
                    if not category:
                        continue
                    
                    # Check if category exists
                    cursor.execute("SELECT category_id FROM dim_category WHERE category_name = %s", (category,))
                    category_row = cursor.fetchone()
                    
                    if category_row:
                        category_id = category_row['category_id']
                    else:
                        # Insert new category
                        cursor.execute("INSERT INTO dim_category (category_name) VALUES (%s)", (category,))
                        cursor.execute("SELECT LAST_INSERT_ID() as category_id")
                        category_id = cursor.fetchone()['category_id']
                    
                    # Link business to category
                    cursor.execute("""
                        INSERT IGNORE INTO business_category (business_id, category_id)
                        VALUES (%s, %s)
                    """, (business['business_id'], category_id))
            
            # Create summary entry for the business
            cursor.execute("""
                INSERT IGNORE INTO summary_business_performance 
                (business_id, total_reviews, avg_rating, total_checkins, total_tips)
                VALUES (%s, 0, 0, 0, 0)
            """, (business['business_id'],))
            
            # Commit the transaction
            self.mysql_conn.commit()
            
            # Broadcast the event
            self._broadcast_event('mysql', 'business', {
                'business_id': business['business_id'],
                'name': business['name'],
                'city': business.get('city', ''),
                'state': business.get('state', '')
            })
            
            print(f"MySQL: Added business {business['name']} (ID: {business['business_id']})")
        
        except Exception as e:
            print(f"Error processing business in MySQL: {e}")
            self.mysql_conn.rollback()
        finally:
            cursor.close()
    
    def _process_user_mysql(self, user):
        """Process a user message for MySQL."""
        if not self.mysql_conn:
            return
        
        cursor = self.mysql_conn.cursor()
        
        try:
            # Parse yelping_since if present
            yelping_since = None
            if 'yelping_since' in user:
                try:
                    yelping_since = datetime.strptime(user['yelping_since'], '%Y-%m-%d')
                except:
                    pass
            
            # Insert user
            cursor.execute("""
                INSERT IGNORE INTO dim_user 
                (user_id, name, review_count, yelping_since, fans, average_stars)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                user['user_id'],
                user.get('name', ''),
                user.get('review_count', 0),
                yelping_since,
                user.get('fans', 0),
                user.get('average_stars', 0)
            ))
            
            # Commit the transaction
            self.mysql_conn.commit()
            
            # Broadcast the event
            self._broadcast_event('mysql', 'user', {
                'user_id': user['user_id'],
                'name': user.get('name', ''),
                'yelping_since': user.get('yelping_since', '')
            })
            
            print(f"MySQL: Added user {user.get('name', '')} (ID: {user['user_id']})")
        
        except Exception as e:
            print(f"Error processing user in MySQL: {e}")
            self.mysql_conn.rollback()
        finally:
            cursor.close()
    
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
                # Try to add the date to dim_time
                try:
                    date_obj = datetime.fromisoformat(review.get('date'))
                    day_name = date_obj.strftime('%A')
                    month_name = date_obj.strftime('%B')
                    quarter = ((date_obj.month - 1) // 3) + 1
                    
                    cursor.execute("""
                        INSERT IGNORE INTO dim_time 
                        (date_actual, day_of_week, day_of_month, month_actual, month_name, quarter_actual, year_actual)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (
                        date_str,
                        day_name,
                        date_obj.day,
                        date_obj.month,
                        month_name,
                        quarter,
                        date_obj.year
                    ))
                    
                    # Get the ID of the newly added time
                    cursor.execute("SELECT time_id FROM dim_time WHERE date_actual = %s", (date_str,))
                    result = cursor.fetchone()
                    if result:
                        time_id = result['time_id']
                        # Also add to our lookup
                        self.time_lookup[date_str] = time_id
                    else:
                        return  # Can't process without a time_id
                except Exception as e:
                    print(f"Error adding date to dim_time: {e}")
                    return
            
            # Insert review into fact_review table
            cursor.execute("""
                INSERT IGNORE INTO fact_review 
                (review_id, business_id, user_id, time_id, stars, useful_votes, funny_votes, cool_votes, text)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                review['review_id'],
                review['business_id'],
                review['user_id'],
                time_id,
                review.get('stars', 0),
                review.get('useful', 0),
                review.get('funny', 0),
                review.get('cool', 0),
                review.get('text', '')
            ))
            
            # Update business review count and stars
            cursor.execute("""
                UPDATE dim_business
                SET review_count = review_count + 1
                WHERE business_id = %s
            """, (review['business_id'],))
            
            # Update user review count
            cursor.execute("""
                UPDATE dim_user
                SET review_count = review_count + 1
                WHERE user_id = %s
            """, (review['user_id'],))
            
            # Update summary table
            cursor.execute("""
                INSERT INTO summary_business_performance (business_id, total_reviews, avg_rating, total_checkins, total_tips)
                VALUES (%s, 1, %s, 0, 0)
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
            
            print(f"MySQL: Added review {review['review_id']} for business {review['business_id']}")
        
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
                # Try to add the date to dim_time
                try:
                    date_obj = datetime.fromisoformat(checkin.get('date'))
                    day_name = date_obj.strftime('%A')
                    month_name = date_obj.strftime('%B')
                    quarter = ((date_obj.month - 1) // 3) + 1
                    
                    cursor.execute("""
                        INSERT IGNORE INTO dim_time 
                        (date_actual, day_of_week, day_of_month, month_actual, month_name, quarter_actual, year_actual)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (
                        date_str,
                        day_name,
                        date_obj.day,
                        date_obj.month,
                        month_name,
                        quarter,
                        date_obj.year
                    ))
                    
                    # Get the ID of the newly added time
                    cursor.execute("SELECT time_id FROM dim_time WHERE date_actual = %s", (date_str,))
                    result = cursor.fetchone()
                    if result:
                        time_id = result['time_id']
                        # Also add to our lookup
                        self.time_lookup[date_str] = time_id
                    else:
                        return  # Can't process without a time_id
                except Exception as e:
                    print(f"Error adding date to dim_time: {e}")
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
                INSERT INTO summary_business_performance (business_id, total_reviews, avg_rating, total_checkins, total_tips)
                VALUES (%s, 0, 0, %s, 0)
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
            
            print(f"MySQL: Added checkin for business {checkin['business_id']} with count {checkin.get('count', 1)}")
        
        except Exception as e:
            print(f"Error processing checkin in MySQL: {e}")
            self.mysql_conn.rollback()
        finally:
            cursor.close()
    
    def _process_business_mongodb(self, business):
        """Process a business message for MongoDB."""
        if self.mongodb_db is None:
            return
        
        try:
            # Insert business into businesses collection
            self.mongodb_db.businesses.insert_one(business)
            
            # Create an empty business summary
            summary = {
                'business_id': business['business_id'],
                'name': business['name'],
                'review_stats': {
                    'avg_stars': 0,
                    'review_count': 0,
                    'five_star_count': 0,
                    'one_star_count': 0,
                    'other_star_count': 0
                },
                'checkin_count': 0,
                'tip_count': 0
            }
            
            self.mongodb_db.business_summaries.insert_one(summary)
            
            # Broadcast the event
            self._broadcast_event('mongodb', 'business', {
                'business_id': business['business_id'],
                'name': business['name'],
                'city': business.get('city', ''),
                'state': business.get('state', '')
            })
            
            print(f"MongoDB: Added business {business['name']} (ID: {business['business_id']})")
        
        except Exception as e:
            print(f"Error processing business in MongoDB: {e}")
    
    def _process_user_mongodb(self, user):
        """Process a user message for MongoDB."""
        if self.mongodb_db is None:
            return
        
        try:
            # Convert yelping_since to datetime if present
            if 'yelping_since' in user:
                try:
                    user['yelping_since'] = datetime.strptime(user['yelping_since'], '%Y-%m-%d')
                except:
                    pass
            
            # Insert user into users collection
            self.mongodb_db.users.insert_one(user)
            
            # Broadcast the event
            self._broadcast_event('mongodb', 'user', {
                'user_id': user['user_id'],
                'name': user.get('name', ''),
                'yelping_since': user.get('yelping_since', '')
            })
            
            print(f"MongoDB: Added user {user.get('name', '')} (ID: {user['user_id']})")
        
        except Exception as e:
            print(f"Error processing user in MongoDB: {e}")
    
    def _process_review_mongodb(self, review):
        """Process a review message for MongoDB."""
        if self.mongodb_db is None:
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
            
            # Update user document
            self.mongodb_db.users.update_one(
                {'user_id': review['user_id']},
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
            
            print(f"MongoDB: Added review {review['review_id']} for business {review['business_id']}")
        
        except Exception as e:
            print(f"Error processing review in MongoDB: {e}")
    
    def _process_checkin_mongodb(self, checkin):
        """Process a checkin message for MongoDB."""
        if self.mongodb_db is None:
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
            
            print(f"MongoDB: Added checkin for business {checkin['business_id']} with count {checkin.get('count', 1)}")
        
        except Exception as e:
            print(f"Error processing checkin in MongoDB: {e}")
    
    def _process_business_neo4j(self, session, business):
        """Process a business message for Neo4j."""
        if self.neo4j_driver is None:
            return
            
        try:
            # Create Business node
            query = """
            CREATE (b:Business {business_id: $business_id})
            SET b.name = $name,
                b.stars = $stars,
                b.review_count = $review_count,
                b.is_open = $is_open,
                b.summary_review_count = 0,
                b.summary_avg_stars = 0,
                b.summary_checkin_count = 0
            
            // Create or merge Location node and relate to Business
            WITH b
            MERGE (l:Location {city: $city, state: $state, postal_code: $postal_code})
            ON CREATE SET l.latitude = $latitude, l.longitude = $longitude
            MERGE (b)-[:LOCATED_IN]->(l)
            
            // Handle categories
            WITH b
            UNWIND $categories as category
            MERGE (c:Category {name: category})
            MERGE (b)-[:IN_CATEGORY]->(c)
            
            RETURN b.business_id
            """
            
            # Parse categories
            categories = []
            if 'categories' in business and business['categories']:
                categories = [c.strip() for c in business['categories'].split(',') if c.strip()]
            
            result = session.run(query, {
                'business_id': business['business_id'],
                'name': business['name'],
                'stars': business.get('stars', 0),
                'review_count': business.get('review_count', 0),
                'is_open': business.get('is_open', 1) == 1,  # Convert to boolean
                'city': business.get('city', ''),
                'state': business.get('state', ''),
                'postal_code': business.get('postal_code', ''),
                'latitude': business.get('latitude'),
                'longitude': business.get('longitude'),
                'categories': categories
            })
            
            # Check if the business was created
            if result.single():
                # Broadcast the event
                self._broadcast_event('neo4j', 'business', {
                    'business_id': business['business_id'],
                    'name': business['name'],
                    'city': business.get('city', ''),
                    'state': business.get('state', '')
                })
                
                print(f"Neo4j: Added business {business['name']} (ID: {business['business_id']})")
            else:
                print(f"Neo4j: Failed to add business {business['business_id']}")
        
        except Exception as e:
            print(f"Error processing business in Neo4j: {e}")
    
    def _process_user_neo4j(self, session, user):
        """Process a user message for Neo4j."""
        if self.neo4j_driver is None:
            return
            
        try:
            # Create User node
            query = """
            CREATE (u:User {user_id: $user_id})
            SET u.name = $name,
                u.review_count = $review_count,
                u.yelping_since = date($yelping_since),
                u.fans = $fans,
                u.average_stars = $average_stars
            
            RETURN u.user_id
            """
            
            # Parse yelping_since date
            yelping_since = None
            if 'yelping_since' in user:
                yelping_since = user['yelping_since']
            
            result = session.run(query, {
                'user_id': user['user_id'],
                'name': user.get('name', ''),
                'review_count': user.get('review_count', 0),
                'yelping_since': yelping_since,
                'fans': user.get('fans', 0),
                'average_stars': user.get('average_stars', 0)
            })
            
            # Check if the user was created
            if result.single():
                # Broadcast the event
                self._broadcast_event('neo4j', 'user', {
                    'user_id': user['user_id'],
                    'name': user.get('name', ''),
                    'yelping_since': user.get('yelping_since', '')
                })
                
                print(f"Neo4j: Added user {user.get('name', '')} (ID: {user['user_id']})")
            else:
                print(f"Neo4j: Failed to add user {user['user_id']}")
        
        except Exception as e:
            print(f"Error processing user in Neo4j: {e}")
    
    def _process_review_neo4j(self, session, review):
        """Process a review message for Neo4j."""
        if self.neo4j_driver is None:
            return
            
        try:
            # Create review node and connect to Business, User and Time
            query = """
            MATCH (b:Business {business_id: $business_id})
            MATCH (u:User {user_id: $user_id})
            
            // Try to find time with exact date first, then fall back to year-month
            OPTIONAL MATCH (t:Time {date: date($date)})
            WITH b, u, t
            WHERE t IS NOT NULL
            
            // If no exact date match, try year-month
            CALL {
                WITH b, u, t
                WITH b, u, t
                WHERE t IS NULL
                MATCH (t2:Time)
                WHERE t2.year = $year AND t2.month = $month
                RETURN t2 as fallback_time
                LIMIT 1
            }
            
            WITH b, u, COALESCE(t, fallback_time) as time_node
            
            // Only proceed if we have a valid time node
            WHERE time_node IS NOT NULL
            
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
            CREATE (r)-[:ON_DATE]->(time_node)
            
            // Update business stats
            SET b.review_count = COALESCE(b.review_count, 0) + 1,
                b.summary_review_count = COALESCE(b.summary_review_count, 0) + 1,
                b.summary_avg_stars = (COALESCE(b.summary_avg_stars, 0) * COALESCE(b.summary_review_count, 0) + $stars) / (COALESCE(b.summary_review_count, 0) + 1)
            
            // Update user stats
            SET u.review_count = COALESCE(u.review_count, 0) + 1
                
            RETURN r.review_id
            """
            
            # Get year and month from date
            date_obj = datetime.fromisoformat(review.get('date'))
            year = date_obj.year
            month = date_obj.month
            
            result = session.run(query, {
                'review_id': review['review_id'],
                'business_id': review['business_id'],
                'user_id': review['user_id'],
                'stars': review.get('stars', 0),
                'useful': review.get('useful', 0),
                'funny': review.get('funny', 0),
                'cool': review.get('cool', 0),
                'text': review.get('text', ''),
                'date': review.get('date'),
                'year': year,
                'month': month
            })
            
            # Check if the review was created
            record = result.single()
            if record:
                # Broadcast the event
                self._broadcast_event('neo4j', 'review', {
                    'review_id': review['review_id'],
                    'business_id': review['business_id'],
                    'user_id': review['user_id'],
                    'stars': review.get('stars', 0),
                    'date': review.get('date')
                })
                
                print(f"Neo4j: Added review {review['review_id']} for business {review['business_id']}")
            else:
                print(f"Neo4j: Failed to add review - perhaps missing Business, User, or Time node?")
        
        except Exception as e:
            print(f"Error processing review in Neo4j: {e}")
    
    def _process_checkin_neo4j(self, session, checkin):
        """Process a checkin message for Neo4j."""
        if self.neo4j_driver is None:
            return
            
        try:
            # Create or update checkin relationship
            query = """
            MATCH (b:Business {business_id: $business_id})
            
            // Try to find time with exact date first, then fall back to year-month
            OPTIONAL MATCH (t:Time {date: date($date)})
            WITH b, t
            WHERE t IS NOT NULL
            
            // If no exact date match, try year-month
            CALL {
                WITH b, t
                WITH b, t
                WHERE t IS NULL
                MATCH (t2:Time)
                WHERE t2.year = $year AND t2.month = $month
                RETURN t2 as fallback_time
                LIMIT 1
            }
            
            WITH b, COALESCE(t, fallback_time) as time_node
            
            // Only proceed if we have a valid time node
            WHERE time_node IS NOT NULL
            
            MERGE (b)-[c:HAD_CHECKIN]->(time_node)
            ON CREATE SET c.count = $count
            ON MATCH SET c.count = c.count + $count
            
            // Update business stats
            SET b.summary_checkin_count = COALESCE(b.summary_checkin_count, 0) + $count
            
            RETURN b.business_id, time_node.date, c.count
            """
            
            # Get year and month from date
            date_obj = datetime.fromisoformat(checkin.get('date'))
            year = date_obj.year
            month = date_obj.month
            
            result = session.run(query, {
                'business_id': checkin['business_id'],
                'date': checkin.get('date'),
                'count': checkin.get('count', 1),
                'year': year,
                'month': month
            })
            
            # Check if the checkin was created/updated
            record = result.single()
            if record:
                # Broadcast the event
                self._broadcast_event('neo4j', 'checkin', {
                    'business_id': checkin['business_id'],
                    'date': checkin.get('date'),
                    'count': checkin.get('count', 1)
                })
                
                print(f"Neo4j: Added checkin for business {checkin['business_id']} with count {checkin.get('count', 1)}")
            else:
                print(f"Neo4j: Failed to add checkin - perhaps missing Business or Time node?")
        
        except Exception as e:
            print(f"Error processing checkin in Neo4j: {e}")


if __name__ == "__main__":
    # Initialize and run consumer
    bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    consumer = YelpKafkaConsumer(bootstrap_servers=bootstrap_servers)
    print("Starting to consume events from Kafka. Press Ctrl+C to stop.")
    consumer.process_messages()