import json
from neo4j import GraphDatabase
from tqdm import tqdm
from datetime import datetime

# Connect to Neo4j
def get_neo4j_driver():
    driver = GraphDatabase.driver("bolt://neo4j:7687", auth=("neo4j", "user"))
    return driver

def run_query(driver, query, parameters=None):
    with driver.session() as session:
        result = session.run(query, parameters)
        return list(result)

def clear_database(driver):
    print("Clearing existing data...")
    run_query(driver, "MATCH (n) DETACH DELETE n")

def create_constraints(driver):
    print("Creating constraints and indexes...")
    
    # Create constraints
    constraints = [
        "CREATE CONSTRAINT IF NOT EXISTS ON (b:Business) ASSERT b.business_id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS ON (u:User) ASSERT u.user_id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS ON (r:Review) ASSERT r.review_id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS ON (c:Category) ASSERT c.name IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS ON (l:Location) ASSERT (l.city, l.state) IS NODE KEY",
        "CREATE CONSTRAINT IF NOT EXISTS ON (t:Time) ASSERT t.date IS UNIQUE"
    ]
    
    # Create indexes
    indexes = [
        "CREATE INDEX IF NOT EXISTS FOR (b:Business) ON (b.name)",
        "CREATE INDEX IF NOT EXISTS FOR (b:Business) ON (b.stars)",
        "CREATE INDEX IF NOT EXISTS FOR (l:Location) ON (l.city)",
        "CREATE INDEX IF NOT EXISTS FOR (l:Location) ON (l.state)",
        "CREATE INDEX IF NOT EXISTS FOR (r:Review) ON (r.stars)",
        "CREATE INDEX IF NOT EXISTS FOR (u:User) ON (u.name)",
        "CREATE INDEX IF NOT EXISTS FOR (t:Time) ON (t.year)",
        "CREATE INDEX IF NOT EXISTS FOR (t:Time) ON (t.month)"
    ]
    
    for constraint in constraints:
        try:
            run_query(driver, constraint)
        except Exception as e:
            print(f"Error creating constraint: {e}")
    
    for index in indexes:
        try:
            run_query(driver, index)
        except Exception as e:
            print(f"Error creating index: {e}")

def load_time_nodes(driver):
    """
    Create time nodes for all dates from 2004 to 2023
    """
    print("Creating time nodes...")
    
    # Generate time nodes in batches using Cypher
    years = range(2004, 2024)
    months = range(1, 13)
    
    for year in tqdm(years):
        for month in months:
            query = """
            UNWIND range(1, CASE 
                WHEN $month IN [4, 6, 9, 11] THEN 30
                WHEN $month = 2 THEN CASE
                    WHEN $year % 4 = 0 AND ($year % 100 <> 0 OR $year % 400 = 0) THEN 29
                    ELSE 28
                END
                ELSE 31
            END) AS day
            
            MERGE (t:Time {date: date($year + '-' + $month + '-' + day)})
            SET t.year = $year,
                t.month = $month,
                t.day = day,
                t.quarter = CASE
                    WHEN $month <= 3 THEN 1
                    WHEN $month <= 6 THEN 2
                    WHEN $month <= 9 THEN 3
                    ELSE 4
                END
            """
            
            run_query(driver, query, {"year": year, "month": month})
    
    print("Time nodes created successfully")

def load_businesses(driver, batch_size=100):
    """
    Load businesses from JSON into Neo4j
    """
    print("Loading businesses into Neo4j...")
    
    with open('data/yelp_academic_dataset_business.json', 'r', encoding='utf-8') as f:
        batch = []
        
        for i, line in enumerate(tqdm(f)):
            business = json.loads(line)
            batch.append(business)
            
            if len(batch) >= batch_size:
                load_business_batch(driver, batch)
                batch = []
            
            # For testing, limit to a smaller number
            if i >= 10000:  # Comment this out for full dataset
                break
        
        # Process remaining businesses
        if batch:
            load_business_batch(driver, batch)
    
    print("Businesses loaded successfully")

def load_business_batch(driver, businesses):
    """
    Process a batch of businesses using efficient Cypher queries
    """
    # First, create all business nodes in one query
    business_data = []
    
    for business in businesses:
        # Build a clean business object
        business_obj = {
            'business_id': business['business_id'],
            'name': business.get('name', ''),
            'stars': business.get('stars'),
            'review_count': business.get('review_count', 0),
            'is_open': business.get('is_open', 0) == 1,
            'city': business.get('city', ''),
            'state': business.get('state', ''),
            'postal_code': business.get('postal_code', ''),
            'latitude': business.get('latitude'),
            'longitude': business.get('longitude'),
            'categories': business.get('categories', '').split(', ') if business.get('categories') else []
        }
        
        business_data.append(business_obj)
    
    # Create businesses and locations in one batch
    query = """
    UNWIND $businesses AS business
    
    // Create Business node
    MERGE (b:Business {business_id: business.business_id})
    SET b.name = business.name,
        b.stars = business.stars,
        b.review_count = business.review_count,
        b.is_open = business.is_open
    
    // Create Location node and relationship
    WITH b, business
    MERGE (l:Location {city: business.city, state: business.state})
    SET l.postal_code = business.postal_code,
        l.latitude = business.latitude,
        l.longitude = business.longitude
    MERGE (b)-[:LOCATED_IN]->(l)
    
    // Create Category nodes and relationships
    WITH b, business
    UNWIND business.categories AS category
    WHERE category <> ''
    MERGE (c:Category {name: trim(category)})
    MERGE (b)-[:IN_CATEGORY]->(c)
    """
    
    run_query(driver, query, {"businesses": business_data})

def load_users(driver, batch_size=100):
    """
    Load users from JSON into Neo4j
    """
    print("Loading users into Neo4j...")
    
    with open('data/yelp_academic_dataset_user.json', 'r', encoding='utf-8') as f:
        batch = []
        
        for i, line in enumerate(tqdm(f)):
            user = json.loads(line)
            batch.append(user)
            
            if len(batch) >= batch_size:
                load_user_batch(driver, batch)
                batch = []
            
            # For testing, limit to a smaller number
            if i >= 10000:  # Comment this out for full dataset
                break
        
        # Process remaining users
        if batch:
            load_user_batch(driver, batch)
    
    print("Users loaded successfully")

def load_user_batch(driver, users):
    """
    Process a batch of users using efficient Cypher queries
    """
    # Prepare user data
    user_data = []
    
    for user in users:
        # Parse yelping_since date
        try:
            yelping_since = user.get('yelping_since', '2000-01-01')
        except:
            yelping_since = '2000-01-01'
        
        # Parse friends list
        if user.get('friends') and user['friends'] != 'None':
            friends = user['friends'].split(', ')
        else:
            friends = []
        
        # Build user object
        user_obj = {
            'user_id': user['user_id'],
            'name': user.get('name', ''),
            'review_count': user.get('review_count', 0),
            'yelping_since': yelping_since,
            'fans': user.get('fans', 0),
            'average_stars': user.get('average_stars', 0),
            'friends': friends
        }
        
        user_data.append(user_obj)
    
    # Create user nodes
    query = """
    UNWIND $users AS user
    
    // Create User node
    MERGE (u:User {user_id: user.user_id})
    SET u.name = user.name,
        u.review_count = user.review_count,
        u.yelping_since = date(user.yelping_since),
        u.fans = user.fans,
        u.average_stars = user.average_stars
    """
    
    run_query(driver, query, {"users": user_data})
    
    # Create friendship relationships (in smaller batches to avoid memory issues)
    for user_obj in user_data:
        if user_obj['friends']:
            # Only create batches of 50 friendships at a time
            for i in range(0, len(user_obj['friends']), 50):
                friend_batch = user_obj['friends'][i:i+50]
                
                query = """
                MATCH (u:User {user_id: $user_id})
                UNWIND $friends AS friend_id
                MATCH (friend:User {user_id: friend_id})
                MERGE (u)-[:FRIENDS_WITH]->(friend)
                """
                
                run_query(driver, query, {"user_id": user_obj['user_id'], "friends": friend_batch})

def load_reviews(driver, batch_size=100):
    """
    Load reviews from JSON into Neo4j
    """
    print("Loading reviews into Neo4j...")
    
    with open('data/yelp_academic_dataset_review.json', 'r', encoding='utf-8') as f:
        batch = []
        
        for i, line in enumerate(tqdm(f)):
            review = json.loads(line)
            batch.append(review)
            
            if len(batch) >= batch_size:
                load_review_batch(driver, batch)
                batch = []
            
            # For testing, limit to a smaller number
            if i >= 10000:  # Comment this out for full dataset
                break
        
        # Process remaining reviews
        if batch:
            load_review_batch(driver, batch)
    
    print("Reviews loaded successfully")

def load_review_batch(driver, reviews):
    """
    Process a batch of reviews using efficient Cypher queries
    """
    # Prepare review data
    review_data = []
    
    for review in reviews:
        # Parse date
        try:
            date_str = review.get('date', '2000-01-01')
        except:
            date_str = '2000-01-01'
        
        # Build review object
        review_obj = {
            'review_id': review['review_id'],
            'business_id': review['business_id'],
            'user_id': review['user_id'],
            'stars': review.get('stars', 0),
            'useful': review.get('useful', 0),
            'funny': review.get('funny', 0),
            'cool': review.get('cool', 0),
            'text': review.get('text', ''),
            'date': date_str
        }
        
        review_data.append(review_obj)
    
    # Create review nodes and relationships
    query = """
    UNWIND $reviews AS review
    
    // Create Review node and connect to Business, User and Time
    MATCH (b:Business {business_id: review.business_id})
    MATCH (u:User {user_id: review.user_id})
    MATCH (t:Time {date: date(review.date)})
    
    CREATE (r:Review {review_id: review.review_id})
    SET r.stars = review.stars,
        r.useful = review.useful,
        r.funny = review.funny,
        r.cool = review.cool,
        r.text = review.text,
        r.date = date(review.date)
    
    CREATE (u)-[:WROTE]->(r)
    CREATE (r)-[:REVIEWS]->(b)
    CREATE (r)-[:ON_DATE]->(t)
    """
    
    run_query(driver, query, {"reviews": review_data})

def load_checkins(driver, batch_size=100):
    """
    Load checkins from JSON into Neo4j
    """
    print("Loading checkins into Neo4j...")
    
    with open('data/yelp_academic_dataset_checkin.json', 'r', encoding='utf-8') as f:
        batch = []
        
        for i, line in enumerate(tqdm(f)):
            checkin = json.loads(line)
            batch.append(checkin)
            
            if len(batch) >= batch_size:
                load_checkin_batch(driver, batch)
                batch = []
            
            # For testing, limit to a smaller number
            if i >= 10000:  # Comment this out for full dataset
                break
        
        # Process remaining checkins
        if batch:
            load_checkin_batch(driver, batch)
    
    print("Checkins loaded successfully")

def load_checkin_batch(driver, checkins):
    """
    Process a batch of checkins using efficient Cypher queries
    """
    # Process each checkin
    for checkin in checkins:
        business_id = checkin['business_id']
        
        # Process date strings
        if 'date' in checkin:
            # First, parse the dates into a list
            dates = []
            for date_str in checkin['date'].split(', '):
                try:
                    # Extract just the date part
                    date_only = date_str.split(' ')[0]
                    dates.append(date_only)
                except:
                    continue
            
            # Count occurrences of each date
            date_counts = {}
            for date in dates:
                if date in date_counts:
                    date_counts[date] += 1
                else:
                    date_counts[date] = 1
            
            # Create checkin relationships in batches
            date_data = []
            for date, count in date_counts.items():
                date_data.append({"date": date, "count": count})
            
            if date_data:
                query = """
                MATCH (b:Business {business_id: $business_id})
                UNWIND $date_data AS check
                MATCH (t:Time {date: date(check.date)})
                MERGE (b)-[c:HAD_CHECKIN]->(t)
                ON CREATE SET c.count = check.count
                ON MATCH SET c.count = c.count + check.count
                """
                
                run_query(driver, query, {"business_id": business_id, "date_data": date_data})

def create_summary_views(driver):
    """
    Create summary views in Neo4j
    """
    print("Creating summary views...")
    
    # Business summary with review statistics
    query = """
    MATCH (b:Business)
    OPTIONAL MATCH (b)<-[r:REVIEWS]-(:Review)
    WITH b, count(r) as review_count, avg(r.stars) as avg_stars
    
    OPTIONAL MATCH (b)-[c:HAD_CHECKIN]->(:Time)
    WITH b, review_count, avg_stars, sum(c.count) as checkin_count
    
    SET b.summary_review_count = review_count,
        b.summary_avg_stars = avg_stars,
        b.summary_checkin_count = checkin_count
    """
    
    run_query(driver, query)
    
    # Category popularity summary
    query = """
    MATCH (c:Category)<-[:IN_CATEGORY]-(b:Business)
    WITH c, count(b) as business_count
    
    OPTIONAL MATCH (c)<-[:IN_CATEGORY]-(b:Business)<-[:REVIEWS]-(r:Review)
    WITH c, business_count, count(r) as review_count
    
    SET c.business_count = business_count,
        c.review_count = review_count
    """
    
    run_query(driver, query)
    
    print("Summary views created successfully")

if __name__ == "__main__":
    driver = get_neo4j_driver()
    
    try:
        clear_database(driver)
        create_constraints(driver)
        load_time_nodes(driver)
        load_businesses(driver)
        load_users(driver)
        load_reviews(driver)
        load_checkins(driver)
        create_summary_views(driver)
        print("Neo4j ETL process completed successfully!")
    finally:
        driver.close()