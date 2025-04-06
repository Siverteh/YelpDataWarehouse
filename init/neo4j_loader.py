"""
Neo4j data loading functions for Yelp Data Warehouse
"""

import os
import json
from datetime import datetime
from neo4j import GraphDatabase
from utils import get_config

def check_and_initialize_neo4j():
    """Check if Neo4j has data, initialize if needed"""
    print("\nChecking Neo4j data...")
    
    config = get_config()
    neo4j_uri = config['neo4j']['uri']
    neo4j_user = config['neo4j']['user']
    neo4j_password = config['neo4j']['password']
    data_config = config['data']
    
    driver = None
    try:
        driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        
        with driver.session() as session:
            # Check if nodes exist
            result = session.run("MATCH (n) RETURN count(n) as count")
            count = result.single()["count"]
            print(f"Node count: {count}")
            
            if count > 0:
                print("Neo4j has data. Skipping initialization.")
                return False  # No need to initialize
            
            # Initialize Neo4j
            print("Initializing Neo4j...")
            
            # Create constraints and indexes
            create_constraints_and_indexes(session)
            
            # Create Time nodes first
            create_time_nodes(session)
            
            # Load businesses with batch processing
            load_businesses(session, data_config['business_file'])
            
            # Load users with batch processing
            load_users(session, data_config['user_file'])
            
            # Load reviews with batch processing
            load_reviews(session, data_config['review_file'])
            
            # Load checkins with batch processing
            load_checkins(session, data_config['checkin_file'])
            
            # Load tips with batch processing
            load_tips(session, data_config['tip_file'])
            
            # Update business summary stats
            update_business_summaries(session)
            
            print("Neo4j initialized with data from Yelp dataset.")
            return True  # Initialized
        
    except Exception as e:
        print(f"Error initializing Neo4j: {e}")
        return False
    finally:
        if driver:
            driver.close()

def create_constraints_and_indexes(session):
    """Create constraints and indexes for Neo4j"""
    print("Creating constraints and indexes...")
    
    constraints = [
        "CREATE CONSTRAINT IF NOT EXISTS ON (b:Business) ASSERT b.business_id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS ON (u:User) ASSERT u.user_id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS ON (r:Review) ASSERT r.review_id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS ON (c:Category) ASSERT c.name IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS ON (l:Location) ASSERT (l.city, l.state) IS NODE KEY"
    ]
    
    for constraint in constraints:
        try:
            session.run(constraint)
        except Exception as e:
            print(f"Error creating constraint: {e}")
            # Try alternative syntax for older Neo4j versions
            if "ON ... IS" in str(e):
                try:
                    constraint = constraint.replace(" IF NOT EXISTS", "")
                    if "business_id" in constraint:
                        session.run("CREATE CONSTRAINT ON (b:Business) ASSERT b.business_id IS UNIQUE")
                    elif "user_id" in constraint:
                        session.run("CREATE CONSTRAINT ON (u:User) ASSERT u.user_id IS UNIQUE")
                    elif "review_id" in constraint:
                        session.run("CREATE CONSTRAINT ON (r:Review) ASSERT r.review_id IS UNIQUE")
                    elif "name" in constraint and "Category" in constraint:
                        session.run("CREATE CONSTRAINT ON (c:Category) ASSERT c.name IS UNIQUE")
                    elif "Location" in constraint:
                        session.run("CREATE CONSTRAINT ON (l:Location) ASSERT l.city IS NOT NULL")
                        session.run("CREATE CONSTRAINT ON (l:Location) ASSERT l.state IS NOT NULL")
                except Exception as e2:
                    print(f"Error with alternate constraint syntax: {e2}")

def create_time_nodes(session):
    """Create Time nodes for Neo4j"""
    print("Creating Time nodes...")
    
    for year in range(2004, 2024):  # Yelp founded in 2004 through 2023
        for month in range(1, 13):
            # Skip future months in 2023
            if year == 2023 and month > datetime.now().month:
                continue
                
            day = 15  # Middle of month
            
            # Adjust day for February
            if month == 2 and day > 28:
                day = 28
                
            # Determine quarter
            quarter = ((month - 1) // 3) + 1
            
            session.run("""
                CREATE (t:Time {
                    date: date($date),
                    year: $year,
                    month: $month,
                    day: $day,
                    quarter: $quarter
                })
            """, {
                'date': f"{year}-{month:02d}-{day:02d}",
                'year': year,
                'month': month,
                'day': day,
                'quarter': quarter
            })

def load_businesses(session, file_path, batch_size=100):
    """Load businesses from Yelp dataset file into Neo4j"""
    if os.path.exists(file_path):
        print(f"Loading businesses from {file_path}...")
        batch_size = 100  # Smaller batch for Neo4j
        loaded_count = 0
        
        with open(file_path, 'r', encoding='utf-8') as f:
            batch = []
            for line in f:
                if not line.strip():
                    continue
                
                business = json.loads(line)
                batch.append(business)
                
                if len(batch) >= batch_size:
                    _load_business_batch_neo4j(session, batch)
                    loaded_count += len(batch)
                    print(f"Loaded {loaded_count} businesses...")
                    batch = []
            
            # Load any remaining businesses
            if batch:
                _load_business_batch_neo4j(session, batch)
                loaded_count += len(batch)
                print(f"Loaded {loaded_count} total businesses")
    else:
        print(f"Business file not found: {file_path}")

def _load_business_batch_neo4j(session, businesses):
    """Load a batch of businesses into Neo4j"""
    try:
        # Process businesses
        for business in businesses:
            if not all(k in business for k in ['business_id', 'name']):
                continue
                
            # Create Business node
            session.run("""
                CREATE (b:Business {
                    business_id: $business_id,
                    name: $name,
                    stars: $stars,
                    review_count: $review_count,
                    is_open: $is_open
                })
            """, {
                'business_id': business['business_id'],
                'name': business['name'],
                'stars': business.get('stars', 0),
                'review_count': business.get('review_count', 0),
                'is_open': business.get('is_open', 0) == 1
            })
            
            # Create or merge Location node and relate it to Business
            if all(k in business for k in ['city', 'state']):
                session.run("""
                    MERGE (l:Location {city: $city, state: $state})
                    ON CREATE SET l.postal_code = $postal_code,
                                  l.latitude = $latitude,
                                  l.longitude = $longitude
                    WITH l
                    MATCH (b:Business {business_id: $business_id})
                    MERGE (b)-[:LOCATED_IN]->(l)
                """, {
                    'business_id': business['business_id'],
                    'city': business['city'],
                    'state': business['state'],
                    'postal_code': business.get('postal_code', ''),
                    'latitude': business.get('latitude'),
                    'longitude': business.get('longitude')
                })
            
            # Process categories
            if 'categories' in business and business['categories']:
                categories = [c.strip() for c in business['categories'].split(',')]
                for category in categories:
                    if category:
                        session.run("""
                            MERGE (c:Category {name: $category})
                            WITH c
                            MATCH (b:Business {business_id: $business_id})
                            MERGE (b)-[:IN_CATEGORY]->(c)
                        """, {
                            'business_id': business['business_id'],
                            'category': category
                        })
        
        return True
    except Exception as e:
        print(f"Error loading business batch into Neo4j: {e}")
        return False

def load_users(session, file_path, batch_size=100):
    """Load users from Yelp dataset file into Neo4j"""
    if os.path.exists(file_path):
        print(f"Loading users from {file_path}...")
        batch_size = 100  # Smaller batch for Neo4j
        loaded_count = 0
        
        with open(file_path, 'r', encoding='utf-8') as f:
            batch = []
            for line in f:
                if not line.strip():
                    continue
                
                user = json.loads(line)
                batch.append(user)
                
                if len(batch) >= batch_size:
                    _load_user_batch_neo4j(session, batch)
                    loaded_count += len(batch)
                    print(f"Loaded {loaded_count} users...")
                    batch = []
            
            # Load any remaining users
            if batch:
                _load_user_batch_neo4j(session, batch)
                loaded_count += len(batch)
                print(f"Loaded {loaded_count} total users")
    else:
        print(f"User file not found: {file_path}")

def _load_user_batch_neo4j(session, users):
    """Load a batch of users into Neo4j"""
    try:
        for user in users:
            if 'user_id' not in user:
                continue
                
            # Parse yelping_since date
            yelping_since = None
            if 'yelping_since' in user:
                try:
                    yelping_since = user['yelping_since']
                except:
                    pass
            
            # Create User node
            session.run("""
                CREATE (u:User {
                    user_id: $user_id,
                    name: $name,
                    yelping_since: date($yelping_since),
                    review_count: $review_count,
                    fans: $fans,
                    average_stars: $avg_stars
                })
            """, {
                'user_id': user['user_id'],
                'name': user.get('name', 'Unknown'),
                'yelping_since': yelping_since,
                'review_count': user.get('review_count', 0),
                'fans': user.get('fans', 0),
                'avg_stars': user.get('average_stars', 0)
            })
        
        return True
    except Exception as e:
        print(f"Error loading user batch into Neo4j: {e}")
        return False

def load_reviews(session, file_path, batch_size=100):
    """Load reviews from Yelp dataset file into Neo4j"""
    if os.path.exists(file_path):
        print(f"Loading reviews from {file_path}...")
        batch_size = 100  # Smaller batch for Neo4j
        loaded_count = 0
        
        with open(file_path, 'r', encoding='utf-8') as f:
            batch = []
            for line in f:
                if not line.strip():
                    continue
                
                review = json.loads(line)
                batch.append(review)
                
                if len(batch) >= batch_size:
                    _load_review_batch_neo4j(session, batch)
                    loaded_count += len(batch)
                    print(f"Loaded {loaded_count} reviews...")
                    batch = []
            
            # Load any remaining reviews
            if batch:
                _load_review_batch_neo4j(session, batch)
                loaded_count += len(batch)
                print(f"Loaded {loaded_count} total reviews")
    else:
        print(f"Review file not found: {file_path}")

def _load_review_batch_neo4j(session, reviews):
    """Load a batch of reviews into Neo4j"""
    try:
        for review in reviews:
            if not all(k in review for k in ['review_id', 'business_id', 'user_id', 'stars', 'date']):
                continue
                
            # Create review and connect to Business, User, and Time
            session.run("""
                MATCH (b:Business {business_id: $business_id})
                MATCH (u:User {user_id: $user_id})
                MATCH (t:Time) 
                WHERE t.date = date($date) OR 
                      (t.year = $year AND t.month = $month)
                WITH b, u, t
                LIMIT 1
                CREATE (r:Review {
                    review_id: $review_id,
                    stars: $stars,
                    useful: $useful,
                    funny: $funny,
                    cool: $cool,
                    text: $text,
                    date: date($date)
                })
                CREATE (u)-[:WROTE]->(r)
                CREATE (r)-[:REVIEWS]->(b)
                CREATE (r)-[:ON_DATE]->(t)
            """, {
                'review_id': review['review_id'],
                'business_id': review['business_id'],
                'user_id': review['user_id'],
                'stars': review.get('stars', 0),
                'useful': review.get('useful', 0),
                'funny': review.get('funny', 0),
                'cool': review.get('cool', 0),
                'text': review.get('text', ''),
                'date': review['date'],
                'year': int(review['date'].split('-')[0]),
                'month': int(review['date'].split('-')[1])
            })
        
        return True
    except Exception as e:
        print(f"Error loading review batch into Neo4j: {e}")
        return False

def load_checkins(session, file_path):
    """Load checkins from Yelp dataset file into Neo4j"""
    if os.path.exists(file_path):
        print(f"Loading checkins from {file_path}...")
        loaded_count = 0
        
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                if not line.strip():
                    continue
                
                checkin = json.loads(line)
                
                # Process checkin 
                success = _load_checkin_neo4j(session, checkin)
                if success:
                    loaded_count += 1
                    
                if loaded_count % 100 == 0:
                    print(f"Loaded {loaded_count} checkins...")
            
            print(f"Loaded {loaded_count} total checkins")
    else:
        print(f"Checkin file not found: {file_path}")

def _load_checkin_neo4j(session, checkin):
    """Load a checkin into Neo4j"""
    try:
        if 'business_id' not in checkin or 'date' not in checkin:
            return False
            
        business_id = checkin['business_id']
        
        # Process dates
        dates = []
        if isinstance(checkin['date'], str):
            # Old format: string of date-times
            dates = [d.split(' ')[0] for d in checkin['date'].split(', ')]
        elif isinstance(checkin['date'], dict):
            # New format: dict with date as key and times as values
            dates = list(checkin['date'].keys())
        
        # Group by date to get counts
        date_counts = {}
        for date_str in dates:
            date_counts[date_str] = date_counts.get(date_str, 0) + 1
        
        # Create checkin relationships
        for date_str, count in date_counts.items():
            try:
                year = int(date_str.split('-')[0])
                month = int(date_str.split('-')[1])
                
                session.run("""
                    MATCH (b:Business {business_id: $business_id})
                    MATCH (t:Time) 
                    WHERE t.date = date($date) OR 
                          (t.year = $year AND t.month = $month)
                    WITH b, t
                    LIMIT 1
                    MERGE (b)-[c:HAD_CHECKIN]->(t)
                    ON CREATE SET c.count = $count
                    ON MATCH SET c.count = c.count + $count
                """, {
                    'business_id': business_id,
                    'date': date_str,
                    'year': year,
                    'month': month,
                    'count': count
                })
            except Exception as e:
                print(f"Error processing checkin date {date_str}: {e}")
        
        return True
    except Exception as e:
        print(f"Error loading checkin into Neo4j: {e}")
        return False

def load_tips(session, file_path, batch_size=100):
    """Load tips from Yelp dataset file into Neo4j"""
    if os.path.exists(file_path):
        print(f"Loading tips from {file_path}...")
        batch_size = 100  # Smaller batch for Neo4j
        loaded_count = 0
        
        with open(file_path, 'r', encoding='utf-8') as f:
            batch = []
            for line in f:
                if not line.strip():
                    continue
                
                tip = json.loads(line)
                batch.append(tip)
                
                if len(batch) >= batch_size:
                    _load_tip_batch_neo4j(session, batch)
                    loaded_count += len(batch)
                    print(f"Loaded {loaded_count} tips...")
                    batch = []
            
            # Load any remaining tips
            if batch:
                _load_tip_batch_neo4j(session, batch)
                loaded_count += len(batch)
                print(f"Loaded {loaded_count} total tips")
    else:
        print(f"Tip file not found: {file_path}")

def _load_tip_batch_neo4j(session, tips):
    """Load a batch of tips into Neo4j"""
    try:
        for tip in tips:
            if not all(k in tip for k in ['business_id', 'user_id', 'text', 'date']):
                continue
                
            # Create tip and connect to Business, User, and Time
            session.run("""
                MATCH (b:Business {business_id: $business_id})
                MATCH (u:User {user_id: $user_id})
                MATCH (t:Time) 
                WHERE t.date = date($date) OR 
                      (t.year = $year AND t.month = $month)
                WITH b, u, t
                LIMIT 1
                CREATE (tip:Tip {
                    text: $text,
                    date: date($date),
                    compliment_count: $compliment_count
                })
                CREATE (u)-[:WROTE_TIP]->(tip)
                CREATE (tip)-[:ABOUT]->(b)
                CREATE (tip)-[:ON_DATE]->(t)
            """, {
                'business_id': tip['business_id'],
                'user_id': tip['user_id'],
                'text': tip['text'],
                'date': tip['date'],
                'year': int(tip['date'].split('-')[0]),
                'month': int(tip['date'].split('-')[1]),
                'compliment_count': tip.get('compliment_count', 0)
            })
        
        return True
    except Exception as e:
        print(f"Error loading tip batch into Neo4j: {e}")
        return False

def update_business_summaries(session):
    """Update business summary statistics in Neo4j"""
    print("Updating business summary statistics...")
    session.run("""
        MATCH (b:Business)
        OPTIONAL MATCH (b)<-[r:REVIEWS]-(:Review)
        WITH b, count(r) as review_count, avg(r.stars) as avg_stars
        
        OPTIONAL MATCH (b)-[c:HAD_CHECKIN]->(:Time)
        WITH b, review_count, avg_stars, sum(c.count) as checkin_count
        
        SET b.summary_review_count = review_count,
            b.summary_avg_stars = avg_stars,
            b.summary_checkin_count = checkin_count
    """)
    print("Business summary statistics updated successfully.")