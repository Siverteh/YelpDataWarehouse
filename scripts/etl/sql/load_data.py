import json
import pymysql
import pandas as pd
from datetime import datetime
import os
from tqdm import tqdm

# Database connection
DB_CONFIG = {
    'host': 'mysql',  # Use container name in Docker network
    'port': 3306,
    'user': 'root',
    'password': 'user',  # Match with your .env file
    'db': 'yelp_dw',
    'charset': 'utf8mb4'
}

def load_time_dimension():
    """
    Load time dimension table with dates from 2004 to 2023
    """
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    print("Loading time dimension...")
    # Generate dates from 2004 to 2023
    dates = pd.date_range('2004-01-01', '2023-12-31')
    
    for date in tqdm(dates):
        cursor.execute("""
            INSERT IGNORE INTO dim_time (date_actual, day_of_week, day_of_month, 
                                  month_actual, month_name, quarter_actual, year_actual)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            date.strftime('%Y-%m-%d'),
            date.day_name(),
            date.day,
            date.month,
            date.month_name(),
            (date.month - 1) // 3 + 1,
            date.year
        ))
    
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Loaded {len(dates)} dates into time dimension")

def load_businesses(chunk_size=1000):
    """
    Load businesses from JSON into dimension tables
    """
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    print("Loading businesses...")
    # Process in chunks to avoid memory issues
    with open('data/yelp_academic_dataset_business.json', 'r', encoding='utf-8') as f:
        chunk = []
        for i, line in enumerate(tqdm(f)):
            business = json.loads(line)
            chunk.append(business)
            
            if len(chunk) >= chunk_size:
                # Process chunk
                load_businesses_chunk(cursor, chunk)
                chunk = []
                conn.commit()
                print(f"Processed {i+1} businesses")
            
            # For testing, limit to a smaller number
            if i >= 50000:  # Comment this out for full dataset
                break
        
        # Process remaining businesses
        if chunk:
            load_businesses_chunk(cursor, chunk)
            conn.commit()
    
    cursor.close()
    conn.close()
    print("Finished loading businesses")

def load_businesses_chunk(cursor, businesses):
    """
    Process a chunk of businesses, loading into location, business, and category tables
    """
    for business in businesses:
        # Handle location
        city = business.get('city', '')
        state = business.get('state', '')
        postal_code = business.get('postal_code', '')
        
        # Insert location if new
        cursor.execute("""
            INSERT IGNORE INTO dim_location (city, state, postal_code, latitude, longitude)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            city, state, postal_code,
            business.get('latitude'),
            business.get('longitude')
        ))
        
        # Get location ID
        cursor.execute("""
            SELECT location_id FROM dim_location
            WHERE city = %s AND state = %s AND postal_code = %s
        """, (city, state, postal_code))
        location_id = cursor.fetchone()[0]
        
        # Insert business
        cursor.execute("""
            INSERT IGNORE INTO dim_business 
            (business_id, business_name, location_id, stars, review_count, is_open)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            business['business_id'],
            business.get('name', ''),
            location_id,
            business.get('stars'),
            business.get('review_count', 0),
            business.get('is_open', 0) == 1
        ))
        
        # Process categories
        if business.get('categories'):
            for category in business['categories'].split(','):
                category = category.strip()
                if not category:
                    continue
                    
                # Insert category if new
                cursor.execute("""
                    INSERT IGNORE INTO dim_category (category_name)
                    VALUES (%s)
                """, (category,))
                
                # Get category ID
                cursor.execute("""
                    SELECT category_id FROM dim_category
                    WHERE category_name = %s
                """, (category,))
                category_id = cursor.fetchone()[0]
                
                # Link business to category
                cursor.execute("""
                    INSERT IGNORE INTO business_category (business_id, category_id)
                    VALUES (%s, %s)
                """, (business['business_id'], category_id))

def load_users(chunk_size=1000):
    """
    Load users from JSON into user dimension table
    """
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    print("Loading users...")
    # Process in chunks to avoid memory issues
    with open('data/yelp_academic_dataset_user.json', 'r', encoding='utf-8') as f:
        chunk = []
        for i, line in enumerate(tqdm(f)):
            user = json.loads(line)
            chunk.append(user)
            
            if len(chunk) >= chunk_size:
                # Process chunk
                load_users_chunk(cursor, chunk)
                chunk = []
                conn.commit()
                print(f"Processed {i+1} users")
            
            # For testing, limit to a smaller number
            if i >= 50000:  # Comment this out for full dataset
                break
        
        # Process remaining users
        if chunk:
            load_users_chunk(cursor, chunk)
            conn.commit()
    
    cursor.close()
    conn.close()
    print("Finished loading users")

def load_users_chunk(cursor, users):
    """
    Process a chunk of users, loading into user dimension table
    """
    for user in users:
        # Parse yelping_since date
        try:
            yelping_since = datetime.fromisoformat(user.get('yelping_since', '2000-01-01')).date()
        except:
            yelping_since = '2000-01-01'  # Default date
        
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

def load_reviews(chunk_size=1000):
    """
    Load reviews from JSON into fact table
    """
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    # Create lookup dictionary for date_actual to time_id
    cursor.execute("SELECT time_id, date_actual FROM dim_time")
    time_lookup = {row[1].strftime('%Y-%m-%d'): row[0] for row in cursor.fetchall()}
    
    print("Loading reviews...")
    # Process in chunks to avoid memory issues
    with open('data/yelp_academic_dataset_review.json', 'r', encoding='utf-8') as f:
        chunk = []
        for i, line in enumerate(tqdm(f)):
            review = json.loads(line)
            chunk.append(review)
            
            if len(chunk) >= chunk_size:
                # Process chunk
                load_reviews_chunk(cursor, chunk, time_lookup)
                chunk = []
                conn.commit()
                print(f"Processed {i+1} reviews")
            
            # For testing, limit to a smaller number
            if i >= 50000:  # Comment this out for full dataset
                break
        
        # Process remaining reviews
        if chunk:
            load_reviews_chunk(cursor, chunk, time_lookup)
            conn.commit()
    
    cursor.close()
    conn.close()
    print("Finished loading reviews")

def load_reviews_chunk(cursor, reviews, time_lookup):
    """
    Process a chunk of reviews, loading into fact_review table
    """
    for review in reviews:
        # Get date and lookup time_id
        try:
            date_str = datetime.fromisoformat(review.get('date', '2000-01-01')).strftime('%Y-%m-%d')
            time_id = time_lookup.get(date_str)
            
            if not time_id:
                continue  # Skip if date not in time dimension
            
            # Insert review
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
        except Exception as e:
            print(f"Error loading review: {e}")
            continue

def load_checkins(chunk_size=1000):
    """
    Load checkins from JSON into fact_checkin table
    """
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    # Create lookup dictionary for date_actual to time_id
    cursor.execute("SELECT time_id, date_actual FROM dim_time")
    time_lookup = {row[1].strftime('%Y-%m-%d'): row[0] for row in cursor.fetchall()}
    
    print("Loading checkins...")
    # Process in chunks to avoid memory issues
    with open('data/yelp_academic_dataset_checkin.json', 'r', encoding='utf-8') as f:
        chunk = []
        for i, line in enumerate(tqdm(f)):
            checkin = json.loads(line)
            chunk.append(checkin)
            
            if len(chunk) >= chunk_size:
                # Process chunk
                load_checkins_chunk(cursor, chunk, time_lookup)
                chunk = []
                conn.commit()
                print(f"Processed {i+1} checkins")
            
            # For testing, limit to a smaller number
            if i >= 50000:  # Comment this out for full dataset
                break
        
        # Process remaining checkins
        if chunk:
            load_checkins_chunk(cursor, chunk, time_lookup)
            conn.commit()
    
    cursor.close()
    conn.close()
    print("Finished loading checkins")

def load_checkins_chunk(cursor, checkins, time_lookup):
    """
    Process a chunk of checkins, loading into fact_checkin table
    """
    for checkin in checkins:
        business_id = checkin['business_id']
        
        # Process date strings
        if 'date' in checkin:
            date_counts = {}
            for date_str in checkin['date'].split(', '):
                try:
                    # Extract just the date part (not time)
                    date_only = date_str.split(' ')[0]
                    if date_only in date_counts:
                        date_counts[date_only] += 1
                    else:
                        date_counts[date_only] = 1
                except:
                    continue
            
            # Insert each unique date as a separate checkin record
            for date_str, count in date_counts.items():
                time_id = time_lookup.get(date_str)
                if not time_id:
                    continue  # Skip if date not in time dimension
                
                # Insert checkin
                cursor.execute("""
                    INSERT INTO fact_checkin 
                    (business_id, time_id, checkin_count)
                    VALUES (%s, %s, %s)
                """, (
                    business_id,
                    time_id,
                    count
                ))

def create_summary_tables():
    """
    Create and populate summary tables with pre-aggregated data
    """
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    print("Creating summary tables...")
    
    # Business performance summary
    cursor.execute("""
        INSERT INTO summary_business_performance (business_id, total_reviews, avg_rating, total_checkins)
        SELECT 
            b.business_id,
            COUNT(r.review_id) as total_reviews,
            AVG(r.stars) as avg_rating,
            COALESCE(SUM(c.checkin_count), 0) as total_checkins
        FROM 
            dim_business b
            LEFT JOIN fact_review r ON b.business_id = r.business_id
            LEFT JOIN fact_checkin c ON b.business_id = c.business_id
        GROUP BY 
            b.business_id
        ON DUPLICATE KEY UPDATE
            total_reviews = VALUES(total_reviews),
            avg_rating = VALUES(avg_rating),
            total_checkins = VALUES(total_checkins)
    """)
    
    conn.commit()
    cursor.close()
    conn.close()
    print("Finished creating summary tables")

if __name__ == "__main__":
    load_time_dimension()
    load_businesses()
    load_users()
    load_reviews()
    load_checkins()
    create_summary_tables()
    print("ETL process completed successfully!")