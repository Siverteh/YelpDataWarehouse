import json
from neo4j import GraphDatabase
from tqdm import tqdm

# Connect to Neo4j
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "your_password"))

def run_query(query, parameters=None):
    with driver.session() as session:
        return session.run(query, parameters)

def clear_database():
    print("Clearing existing data...")
    run_query("MATCH (n) DETACH DELETE n")

def create_constraints():
    print("Creating constraints and indexes...")
    run_query("CREATE CONSTRAINT IF NOT EXISTS ON (b:Business) ASSERT b.business_id IS UNIQUE")
    run_query("CREATE CONSTRAINT IF NOT EXISTS ON (u:User) ASSERT u.user_id IS UNIQUE")
    run_query("CREATE CONSTRAINT IF NOT EXISTS ON (r:Review) ASSERT r.review_id IS UNIQUE")
    run_query("CREATE CONSTRAINT IF NOT EXISTS ON (c:Category) ASSERT c.name IS UNIQUE")
    run_query("CREATE INDEX IF NOT EXISTS FOR (b:Business) ON (b.name)")

def load_businesses():
    print("Loading businesses into Neo4j...")
    batch_size = 100
    businesses_processed = 0
    
    with open('data/yelp_academic_dataset_business.json', 'r') as f:
        batch = []
        
        for line in tqdm(f):
            business = json.loads(line)
            batch.append(business)
            
            if len(batch) >= batch_size:
                load_business_batch(batch)
                businesses_processed += len(batch)
                batch = []
        
        # Process remaining businesses
        if batch:
            load_business_batch(batch)
            businesses_processed += len(batch)
    
    print(f"Loaded {businesses_processed} businesses")

def load_business_batch(batch):
    # This is a simplified version - in a real implementation, you'd use 
    # UNWIND for better performance
    for business in batch:
        # Create business node
        run_query("""
            MERGE (b:Business {business_id: $business_id})
            SET b.name = $name,
                b.stars = $stars,
                b.review_count = $review_count,
                b.is_open = $is_open
        """, {
            'business_id': business['business_id'],
            'name': business.get('name', ''),
            'stars': business.get('stars'),
            'review_count': business.get('review_count', 0),
            'is_open': business.get('is_open', 0) == 1
        })
        
        # Create location node and relationship
        run_query("""
            MERGE (l:Location {city: $city, state: $state})
            SET l.postal_code = $postal_code,
                l.latitude = $latitude,
                l.longitude = $longitude
            WITH l
            MATCH (b:Business {business_id: $business_id})
            MERGE (b)-[:LOCATED_IN]->(l)
        """, {
            'business_id': business['business_id'],
            'city': business.get('city', ''),
            'state': business.get('state', ''),
            'postal_code': business.get('postal_code', ''),
            'latitude': business.get('latitude'),
            'longitude': business.get('longitude')
        })
        
        # Create category nodes and relationships
        if business.get('categories'):
            categories = [c.strip() for c in business['categories'].split(',')]
            for category in categories:
                if category:
                    run_query("""
                        MERGE (c:Category {name: $category})
                        WITH c
                        MATCH (b:Business {business_id: $business_id})
                        MERGE (b)-[:IN_CATEGORY]->(c)
                    """, {
                        'business_id': business['business_id'],
                        'category': category
                    })

if __name__ == "__main__":
    clear_database()
    create_constraints()
    load_businesses()
    # Add functions for users, reviews, etc.