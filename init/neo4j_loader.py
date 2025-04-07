"""
Neo4j data loading functions for Yelp Data Warehouse
"""

import os
import json
from datetime import datetime
from neo4j import GraphDatabase
from utils import get_config

def check_and_initialize_neo4j(data_limits=None):
    """Check if Neo4j has data, initialize if needed"""
    print("\nChecking Neo4j data...")
    
    config = get_config()
    neo4j_uri = config['neo4j']['uri']
    neo4j_user = config['neo4j']['user']
    neo4j_password = config['neo4j']['password']
    data_config = config['data']
    
    # Set default limits if none provided
    if data_limits is None:
        data_limits = {
            'business_limit': float('inf'),
            'review_limit': float('inf'),
            'user_limit': float('inf'),
            'filter_by_business': False
        }
    
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
            loaded_business_ids = load_businesses(session, data_config['business_file'], 
                                                limit=data_limits['business_limit'])
            
            # Load users with batch processing
            load_users(session, data_config['user_file'], 
                      limit=data_limits['user_limit'])
            
            # Filter related data by loaded businesses if requested
            filter_business_ids = loaded_business_ids if data_limits['filter_by_business'] else None
            
            # Load reviews with batch processing
            load_reviews(session, data_config['review_file'], 
                        limit=data_limits['review_limit'], 
                        filter_business_ids=filter_business_ids)
            
            # Load checkins with batch processing
            load_checkins(session, data_config['checkin_file'], 
                         filter_business_ids=filter_business_ids)
            
            # Load tips with batch processing
            load_tips(session, data_config['tip_file'], 
                     filter_business_ids=filter_business_ids)
            
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
    """Create constraints and indexes for Neo4j (Community Edition compatible)"""
    print("Creating constraints and indexes...")
    
    # Basic constraints compatible with Community Edition
    constraints = [
        "CREATE CONSTRAINT IF NOT EXISTS ON (b:Business) ASSERT b.business_id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS ON (u:User) ASSERT u.user_id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS ON (r:Review) ASSERT r.review_id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS ON (c:Category) ASSERT c.name IS UNIQUE"
    ]
    
    # Create a composite index for locations instead of a node key constraint
    indexes = [
        "CREATE INDEX location_city_state IF NOT EXISTS FOR (l:Location) ON (l.city, l.state)"
    ]
    
    # Apply constraints
    for constraint in constraints:
        try:
            session.run(constraint)
        except Exception as e:
            print(f"Warning: Error creating constraint: {e}")
            # Try alternative syntax for older Neo4j versions
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
            except Exception as e2:
                print(f"Error with alternate constraint syntax: {e2}")
                # Continue anyway - constraints aren't critical for a demo
    
    # Apply indexes
    for index in indexes:
        try:
            session.run(index)
        except Exception as e:
            print(f"Warning: Error creating index: {e}")
            try:
                # Try older Neo4j syntax if needed
                if "location_city_state" in index:
                    session.run("CREATE INDEX ON :Location(city, state)")
            except Exception as e2:
                print(f"Error with alternate index syntax: {e2}")
                # Continue anyway

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

def load_businesses(session, file_path, batch_size=100, limit=float('inf')):
    """
    Load businesses from Yelp dataset file into Neo4j
    
    Returns:
        set: Set of loaded business IDs
    """
    loaded_business_ids = set()
    
    if os.path.exists(file_path):
        print(f"Loading businesses from {file_path} (limit: {limit if limit != float('inf') else 'none'})...")
        batch_size = 100  # Smaller batch for Neo4j
        loaded_count = 0
        
        with open(file_path, 'r', encoding='utf-8') as f:
            batch = []
            for line in f:
                if not line.strip():
                    continue
                
                if loaded_count >= limit:
                    break
                    
                business = json.loads(line)
                batch.append(business)
                loaded_business_ids.add(business['business_id'])
                
                if len(batch) >= batch_size:
                    success = _load_business_batch_neo4j(session, batch)
                    if not success:
                        # Remove failed business IDs from tracking
                        for b in batch:
                            loaded_business_ids.discard(b['business_id'])
                            
                    loaded_count += len(batch)
                    print(f"Loaded {loaded_count} businesses...")
                    batch = []
            
            # Load any remaining businesses
            if batch:
                success = _load_business_batch_neo4j(session, batch)
                if not success:
                    # Remove failed business IDs from tracking
                    for b in batch:
                        loaded_business_ids.discard(b['business_id'])
                        
                loaded_count += len(batch)
                print(f"Loaded {loaded_count} total businesses")
    else:
        print(f"Business file not found: {file_path}")
    
    return loaded_business_ids

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

def load_users(session, file_path, batch_size=100, limit=float('inf')):
    """Load users from Yelp dataset file into Neo4j"""
    if os.path.exists(file_path):
        print(f"Loading users from {file_path} (limit: {limit if limit != float('inf') else 'none'})...")
        batch_size = 100  # Smaller batch for Neo4j
        loaded_count = 0
        
        with open(file_path, 'r', encoding='utf-8') as f:
            batch = []
            for line in f:
                if not line.strip():
                    continue
                
                if loaded_count >= limit:
                    break
                    
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
    """Load a batch of users into Neo4j with proper date handling"""
    try:
        for user in users:
            if 'user_id' not in user:
                continue
                
            # Parse yelping_since date - extract just the date part
            yelping_since = None
            if 'yelping_since' in user:
                try:
                    # Extract just the date part (YYYY-MM-DD) from the timestamp
                    if isinstance(user['yelping_since'], str):
                        # Split by space and take the first part (the date)
                        yelping_since = user['yelping_since'].split()[0]
                except Exception as date_error:
                    print(f"Error parsing date: {date_error}, using NULL instead")
                    yelping_since = None
            
            # Create User node - handle the date inside Cypher if possible
            query = """
                CREATE (u:User {
                    user_id: $user_id,
                    name: $name,
                    review_count: $review_count,
                    fans: $fans,
                    average_stars: $avg_stars
                })
            """
            
            params = {
                'user_id': user['user_id'],
                'name': user.get('name', 'Unknown'),
                'review_count': user.get('review_count', 0),
                'fans': user.get('fans', 0),
                'avg_stars': user.get('average_stars', 0)
            }
            
            session.run(query, params)
            
            # Set the date separately only if a valid date was parsed
            if yelping_since:
                try:
                    session.run("""
                        MATCH (u:User {user_id: $user_id})
                        SET u.yelping_since = date($yelping_since)
                    """, {
                        'user_id': user['user_id'],
                        'yelping_since': yelping_since
                    })
                except Exception as date_set_error:
                    print(f"Error setting date for user {user['user_id']}: {date_set_error}")
        
        return True
    except Exception as e:
        print(f"Error loading user batch into Neo4j: {e}")
        return False

def load_reviews(session, file_path, batch_size=100, limit=float('inf'), filter_business_ids=None):
    """Load reviews from Yelp dataset file into Neo4j"""
    if os.path.exists(file_path):
        print(f"Loading reviews from {file_path} (limit: {limit if limit != float('inf') else 'none'})...")
        batch_size = 100  # Smaller batch for Neo4j
        loaded_count = 0
        skipped_count = 0
        
        # Prepare a set of valid business IDs if filtering is enabled
        valid_businesses = None
        if filter_business_ids:
            valid_businesses = set(filter_business_ids)
            print(f"Filtering reviews to {len(valid_businesses)} businesses")
        
        with open(file_path, 'r', encoding='utf-8') as f:
            batch = []
            for line in f:
                if not line.strip():
                    continue
                
                if loaded_count >= limit:
                    break
                    
                review = json.loads(line)
                
                # Skip reviews for filtered-out businesses
                if valid_businesses and review['business_id'] not in valid_businesses:
                    skipped_count += 1
                    continue
                    
                batch.append(review)
                
                if len(batch) >= batch_size:
                    _load_review_batch_neo4j(session, batch)
                    loaded_count += len(batch)
                    print(f"Loaded {loaded_count} reviews, skipped {skipped_count}...")
                    batch = []
            
            # Load any remaining reviews
            if batch:
                _load_review_batch_neo4j(session, batch)
                loaded_count += len(batch)
                print(f"Loaded {loaded_count} total reviews, skipped {skipped_count}")
    else:
        print(f"Review file not found: {file_path}")

def _load_review_batch_neo4j(session, reviews):
    """Load a batch of reviews into Neo4j with proper date handling"""
    try:
        for review in reviews:
            if not all(k in review for k in ['review_id', 'business_id', 'user_id', 'stars']):
                continue
            
            # Make sure we have a date field
            if 'date' not in review:
                continue
                
            # Extract just the date part
            try:
                date_str = review['date'].split()[0]  # Split by space and take first part
                year = int(date_str.split('-')[0])
                month = int(date_str.split('-')[1])
            except Exception as e:
                print(f"Error parsing review date: {e}, skipping this review")
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
                    text: $text
                })
                SET r.date = date($date)
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
                'date': date_str,
                'year': year,
                'month': month
            })
        
        return True
    except Exception as e:
        print(f"Error loading review batch into Neo4j: {e}")
        return False

def load_checkins(session, file_path, filter_business_ids=None):
    """Load checkins from Yelp dataset file into Neo4j"""
    if os.path.exists(file_path):
        print(f"Loading checkins from {file_path}...")
        loaded_count = 0
        skipped_count = 0
        
        # Prepare a set of valid business IDs if filtering is enabled
        valid_businesses = None
        if filter_business_ids:
            valid_businesses = set(filter_business_ids)
            print(f"Filtering checkins to {len(valid_businesses)} businesses")
        
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                if not line.strip():
                    continue
                
                checkin = json.loads(line)
                
                # Skip checkins for filtered-out businesses
                if valid_businesses and checkin['business_id'] not in valid_businesses:
                    skipped_count += 1
                    continue
                
                # Process checkin 
                success = _load_checkin_neo4j(session, checkin)
                if success:
                    loaded_count += 1
                else:
                    skipped_count += 1
                    
                if loaded_count % 100 == 0:
                    print(f"Loaded {loaded_count} checkins, skipped {skipped_count}...")
            
            print(f"Loaded {loaded_count} total checkins, skipped {skipped_count}")
    else:
        print(f"Checkin file not found: {file_path}")

def _load_checkin_neo4j(session, checkin):
    """Load a checkin into Neo4j with proper date handling"""
    try:
        if 'business_id' not in checkin or 'date' not in checkin:
            return False
            
        business_id = checkin['business_id']
        
        # Process dates
        dates = []
        if isinstance(checkin['date'], str):
            # Old format: string of date-times
            for dt in checkin['date'].split(', '):
                try:
                    # Extract just the date part
                    dates.append(dt.split()[0])
                except:
                    pass
        elif isinstance(checkin['date'], dict):
            # New format: dict with date as key and times as values
            dates = list(checkin['date'].keys())
        else:
            return False
        
        # Group by date to get counts
        date_counts = {}
        for date_str in dates:
            date_counts[date_str] = date_counts.get(date_str, 0) + 1
        
        # Create checkin relationships
        for date_str, count in date_counts.items():
            try:
                # Make sure date_str is just YYYY-MM-DD
                if ' ' in date_str:
                    date_str = date_str.split()[0]
                    
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

def load_tips(session, file_path, batch_size=100, filter_business_ids=None):
    """Load tips from Yelp dataset file into Neo4j"""
    if os.path.exists(file_path):
        print(f"Loading tips from {file_path}...")
        batch_size = 100  # Smaller batch for Neo4j
        loaded_count = 0
        skipped_count = 0
        
        # Prepare a set of valid business IDs if filtering is enabled
        valid_businesses = None
        if filter_business_ids:
            valid_businesses = set(filter_business_ids)
            print(f"Filtering tips to {len(valid_businesses)} businesses")
        
        with open(file_path, 'r', encoding='utf-8') as f:
            batch = []
            for line in f:
                if not line.strip():
                    continue
                
                tip = json.loads(line)
                
                # Skip tips for filtered-out businesses
                if valid_businesses and tip['business_id'] not in valid_businesses:
                    skipped_count += 1
                    continue
                    
                batch.append(tip)
                
                if len(batch) >= batch_size:
                    _load_tip_batch_neo4j(session, batch)
                    loaded_count += len(batch)
                    print(f"Loaded {loaded_count} tips, skipped {skipped_count}...")
                    batch = []
            
            # Load any remaining tips
            if batch:
                _load_tip_batch_neo4j(session, batch)
                loaded_count += len(batch)
                print(f"Loaded {loaded_count} total tips, skipped {skipped_count}")
    else:
        print(f"Tip file not found: {file_path}")

def _load_tip_batch_neo4j(session, tips):
    """Load a batch of tips into Neo4j with proper date handling"""
    try:
        for tip in tips:
            if not all(k in tip for k in ['business_id', 'user_id', 'text', 'date']):
                continue
                
            # Extract just the date part
            try:
                date_str = tip['date'].split()[0]  # Split by space and take first part
                year = int(date_str.split('-')[0])
                month = int(date_str.split('-')[1])
            except Exception as e:
                print(f"Error parsing tip date: {e}, skipping this tip")
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
                    compliment_count: $compliment_count
                })
                SET tip.date = date($date)
                CREATE (u)-[:WROTE_TIP]->(tip)
                CREATE (tip)-[:ABOUT]->(b)
                CREATE (tip)-[:ON_DATE]->(t)
            """, {
                'business_id': tip['business_id'],
                'user_id': tip['user_id'],
                'text': tip['text'],
                'date': date_str,
                'year': year,
                'month': month,
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