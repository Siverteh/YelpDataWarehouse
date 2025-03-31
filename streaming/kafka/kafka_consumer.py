import json
import time
import threading
import pymysql
import pymongo
from neo4j import GraphDatabase
from kafka import KafkaConsumer
from datetime import datetime

class YelpKafkaConsumer:
    """
    Consumer that processes Yelp data events from Kafka topics and loads them into databases.
    """
    
    def __init__(self, bootstrap_servers='localhost:9092', topics=['yelp-reviews', 'yelp-checkins']):
        """Initialize Kafka consumer and database connections."""
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
        
        # Create processor threads
        self.mysql_thread = None
        self.mongodb_thread = None
        self.neo4j_thread = None
        
        # Flags for stopping threads
        self.running = True
        
        # Message queues for each database
        self.mysql_queue = []
        self.mongodb_queue = []
        self.neo4j_queue = []
        
        # Locks for thread synchronization
        self.mysql_lock = threading.Lock()
        self.mongodb_lock = threading.Lock()
        self.neo4j_lock = threading.Lock()
    
    def _init_mysql(self):
        """Initialize MySQL connection."""
        try:
            self.mysql_conn = pymysql.connect(
                host='mysql',
                port=3306,
                user='root',
                password='user',
                db='yelp_dw',
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
            self.mongodb_client = pymongo.MongoClient('mongodb://user:user@mongodb:27017/')
            self.mongodb_db = self.mongodb_client['yelp_db']
            print("MongoDB connection established")
        except Exception as e:
            print(f"MongoDB connection error: {e}")
            self.mongodb_client = None
            self.mongodb_db = None
    
    def _init_neo4j(self):
        """Initialize Neo4j connection."""
        try:
            self.neo4j_driver = GraphDatabase.driver("bolt://neo4j:7687", auth=("neo4j", "user"))
            # Test connection
            with self.neo4j_driver.session() as session:
                result = session.run("RETURN 1")
                result.single()
            print("Neo4j connection established")
        except Exception as e:
            print(f"Neo4j connection error: {e}")
            self.neo4j_driver = None
    
    def process_messages(self):
        """Process messages from Kafka topics."""
        try:
            # Start processor threads
            self.mysql_thread = threading.Thread(target=self._mysql_processor)
            self.mongodb_thread = threading.Thread(target=self._mongodb_processor)
            self.neo4j_thread = threading.Thread(target=self._neo4j_processor)
            
            self.mysql_thread.daemon = True
            self.mongodb_thread.daemon = True
            self.neo4j_thread.daemon = True
            
            self.mysql_thread.start()
            self.mongodb_thread.start()
            self.neo4j_thread.start()
            
            # Main thread reads from Kafka and dispatches messages
            for message in self.consumer:
                topic = message.topic
                value = message.value
                
                # Add message to appropriate queues
                with self.mysql_lock:
                    self.mysql_queue.append((topic, value))
                
                with self.mongodb_lock:
                    self.mongodb_queue.append((topic, value))
                
                with self.neo4j_lock:
                    self.neo4j_queue.append((topic, value))
                
                # Optionally print message for debugging
                print(f"Received message on {topic}: {value.get('business_id', '')}")
        
        except KeyboardInterrupt:
            print("Processing stopped by user")
        finally:
            self.running = False
            
            # Close connections
            if self.mysql_conn:
                self.mysql_conn.close()
            
            if self.mongodb_client:
                self.mongodb_client.close()
            
            if self.neo4j_driver:
                self.neo4j_driver.close()
            
            # Wait for threads to finish
            if self.mysql_thread:
                self.mysql_thread.join(timeout=5)
            
            if self.mongodb_thread:
                self.mongodb_thread.join(timeout=5)
            
            if self.neo4j_thread:
                self.neo4j_thread.join(timeout=5)
    
    def _mysql_processor(self):
        """Process messages for MySQL database."""
        print("MySQL processor thread started")
        
        while self.running or len(self.mysql_queue) > 0:
            # Get messages to process
            messages = []
            with self.mysql_lock:
                if self.mysql_queue:
                    # Get up to 10 messages to process in batch
                    messages = self.mysql_queue[:10]
                    self.mysql_queue = self.mysql_queue[10:]
            
            if messages:
                try:
                    # Process messages
                    for topic, value in messages:
                        if topic == 'yelp-reviews':
                            self._process_review_mysql(value)
                        elif topic == 'yelp-checkins':
                            self._process_checkin_mysql(value)
                    
                    # Commit the transaction
                    self.mysql_conn.commit()
                    print(f"Processed {len(messages)} messages for MySQL")
                
                except Exception as e:
                    print(f"Error processing MySQL messages: {e}")
                    # Try to reconnect if connection was lost
                    try:
                        self.mysql_conn.ping(reconnect=True)
                    except:
                        self._init_mysql()
            
            # Sleep to avoid excessive CPU usage
            time.sleep(0.1)
    
    def _mongodb_processor(self):
        """Process messages for MongoDB database."""
        print("MongoDB processor thread started")
        
        while self.running or len(self.mongodb_queue) > 0:
            # Get messages to process
            messages = []
            with self.mongodb_lock:
                if self.mongodb_queue:
                    # Get up to 10 messages to process in batch
                    messages = self.mongodb_queue[:10]
                    self.mongodb_queue = self.mongodb_queue[10:]
            
            if messages:
                try:
                    # Process messages
                    for topic, value in messages:
                        if topic == 'yelp-reviews':
                            self._process_review_mongodb(value)
                        elif topic == 'yelp-checkins':
                            self._process_checkin_mongodb(value)
                    
                    print(f"Processed {len(messages)} messages for MongoDB")
                
                except Exception as e:
                    print(f"Error processing MongoDB messages: {e}")
                    # Try to reconnect if connection was lost
                    self._init_mongodb()
            
            # Sleep to avoid excessive CPU usage
            time.sleep(0.1)
    
    def _neo4j_processor(self):
        """Process messages for Neo4j database."""
        print("Neo4j processor thread started")
        
        while self.running or len(self.neo4j_queue) > 0:
            # Get messages to process
            messages = []
            with self.neo4j_lock:
                if self.neo4j_queue:
                    # Get up to 10 messages to process in batch
                    messages = self.neo4j_queue[:10]
                    self.neo4j_queue = self.neo4j_queue[10:]
            
            if messages:
                try:
                    # Process messages
                    with self.neo4j_driver.session() as session:
                        for topic, value in messages:
                            if topic == 'yelp-reviews':
                                self._process_review_neo4j(session, value)
                            elif topic == 'yelp-checkins':
                                self._process_checkin_neo4j(session, value)
                    
                    print(f"Processed {len(messages)} messages for Neo4j")
                
                except Exception as e:
                    print(f"Error processing Neo4j messages: {e}")
                    # Try to reconnect if connection was lost
                    self._init_neo4j()
            
            # Sleep to avoid excessive CPU usage
            time.sleep(0.1)
    
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
        
        finally:
            cursor.close()
    
    def _process_review_mongodb(self, review):
        """Process a review message for MongoDB."""
        if not self.mongodb_db:
            return
        
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
    
    def _process_checkin_mongodb(self, checkin):
        """Process a checkin message for MongoDB."""
        if not self.mongodb_db:
            return
        
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
    
    def _process_review_neo4j(self, session, review):
        """Process a review message for Neo4j."""
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
    
    def _process_checkin_neo4j(self, session, checkin):
        """Process a checkin message for Neo4j."""
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

if __name__ == "__main__":
    # Initialize and run consumer
    consumer = YelpKafkaConsumer()
    print("Starting to consume events from Kafka. Press Ctrl+C to stop.")
    consumer.process_messages()