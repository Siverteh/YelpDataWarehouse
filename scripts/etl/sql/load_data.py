import json
import pymysql
import pandas as pd
from datetime import datetime
import os

# Database connection
DB_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'your_password',
    'db': 'yelp_dw',
    'charset': 'utf8mb4'
}

def load_time_dimension():
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    # Generate dates from 2004 to 2021
    dates = pd.date_range('2004-01-01', '2021-12-31')
    
    for date in dates:
        cursor.execute("""
            INSERT INTO dim_time (date_actual, day_of_week, day_of_month, 
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

def process_businesses(chunk_size=1000):
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    # Process in chunks to avoid memory issues
    with open('data/yelp_academic_dataset_business.json', 'r') as f:
        chunk = []
        for i, line in enumerate(f):
            business = json.loads(line)
            chunk.append(business)
            
            if len(chunk) >= chunk_size:
                # Process chunk
                load_businesses_chunk(cursor, chunk)
                chunk = []
                print(f"Processed {i+1} businesses")
        
        # Process remaining businesses
        if chunk:
            load_businesses_chunk(cursor, chunk)
    
    conn.commit()
    cursor.close()
    conn.close()

def load_businesses_chunk(cursor, businesses):
    # Track locations and categories
    locations = {}
    categories = {}
    
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

if __name__ == "__main__":
    print("Loading time dimension...")
    load_time_dimension()
    print("Processing businesses...")
    process_businesses()