"""
MySQL data loading functions for Yelp Data Warehouse
"""

import os
import time
import json
import pymysql
from datetime import datetime, timedelta
from utils import get_config

def check_and_initialize_mysql():
    """Check if MySQL has data, initialize if needed"""
    print("\nChecking MySQL data...")
    
    config = get_config()
    mysql_config = config['mysql']
    data_config = config['data']
    
    conn = None
    try:
        conn = pymysql.connect(**mysql_config)
        cursor = conn.cursor()
        
        # Check if tables exist
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        tables = [t[0] for t in tables]
        
        # If tables exist, check if they have data
        if tables:
            print(f"MySQL tables found: {tables}")
            
            # Check dim_business table
            if 'dim_business' in tables:
                cursor.execute("SELECT COUNT(*) FROM dim_business")
                count = cursor.fetchone()[0]
                print(f"Business count: {count}")
                
                if count > 0:
                    print("MySQL has data. Skipping initialization.")
                    return False  # No need to initialize
        
        # Initialize MySQL schema
        print("Initializing MySQL schema...")
        
        # Create schema
        create_mysql_tables(cursor)
        conn.commit()
        
        # Create time dimension
        create_time_dimension(cursor)
        conn.commit()
        
        # Load data from Yelp dataset files
        # A. Check file paths 
        file_paths_valid = True
        
        if not os.path.exists(data_config['business_file']):
            print(f"Warning: Business file not found: {data_config['business_file']}")
            file_paths_valid = False
        
        if not os.path.exists(data_config['user_file']):
            print(f"Warning: User file not found: {data_config['user_file']}")
            file_paths_valid = False
        
        if not os.path.exists(data_config['review_file']):
            print(f"Warning: Review file not found: {data_config['review_file']}")
            file_paths_valid = False
        
        if not os.path.exists(data_config['checkin_file']):
            print(f"Warning: Checkin file not found: {data_config['checkin_file']}")
            file_paths_valid = False
        
        if not os.path.exists(data_config['tip_file']):
            print(f"Warning: Tip file not found: {data_config['tip_file']}")
            file_paths_valid = False
        
        if not file_paths_valid:
            print("Some dataset files are missing. Please check your data directory.")
        
        # B. Load data in the correct order (businesses/users first, then related facts)
        load_businesses(cursor, data_config['business_file'])
        load_users(cursor, data_config['user_file'])
        load_reviews(cursor, data_config['review_file'])
        load_checkins(cursor, data_config['checkin_file'])
        load_tips(cursor, data_config['tip_file'])
        
        # C. Create summary tables
        create_summary_tables(cursor)
        
        # Final commit
        conn.commit()
        print("MySQL initialized with data from Yelp dataset.")
        return True  # Initialized
        
    except Exception as e:
        print(f"Error initializing MySQL: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()

def create_mysql_tables(cursor):
    """Create MySQL tables if they don't exist"""
    # Create dimension tables
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_time (
            time_id INT PRIMARY KEY AUTO_INCREMENT,
            date_actual DATE NOT NULL,
            day_of_week VARCHAR(10) NOT NULL,
            day_of_month INT NOT NULL,
            month_actual INT NOT NULL,
            month_name VARCHAR(10) NOT NULL,
            quarter_actual INT NOT NULL,
            year_actual INT NOT NULL,
            UNIQUE KEY uk_date (date_actual)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_location (
            location_id INT PRIMARY KEY AUTO_INCREMENT,
            city VARCHAR(100) NOT NULL,
            state VARCHAR(50) NOT NULL,
            postal_code VARCHAR(20) NOT NULL,
            latitude DECIMAL(9,6),
            longitude DECIMAL(9,6),
            UNIQUE KEY uk_location (city, state, postal_code)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_business (
            business_id VARCHAR(100) PRIMARY KEY,
            business_name VARCHAR(255) NOT NULL,
            location_id INT NOT NULL,
            stars DECIMAL(2,1),
            review_count INT,
            is_open BOOLEAN,
            FOREIGN KEY (location_id) REFERENCES dim_location(location_id)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_category (
            category_id INT PRIMARY KEY AUTO_INCREMENT,
            category_name VARCHAR(100) NOT NULL,
            UNIQUE KEY uk_category (category_name)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS business_category (
            business_id VARCHAR(100) NOT NULL,
            category_id INT NOT NULL,
            PRIMARY KEY (business_id, category_id),
            FOREIGN KEY (business_id) REFERENCES dim_business(business_id),
            FOREIGN KEY (category_id) REFERENCES dim_category(category_id)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_user (
            user_id VARCHAR(100) PRIMARY KEY,
            name VARCHAR(255),
            review_count INT,
            yelping_since DATE,
            fans INT,
            average_stars DECIMAL(2,1)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fact_review (
            review_id VARCHAR(100) PRIMARY KEY,
            business_id VARCHAR(100) NOT NULL,
            user_id VARCHAR(100) NOT NULL,
            time_id INT NOT NULL,
            stars INT NOT NULL,
            useful_votes INT NOT NULL,
            funny_votes INT NOT NULL,
            cool_votes INT NOT NULL,
            text TEXT,
            FOREIGN KEY (business_id) REFERENCES dim_business(business_id),
            FOREIGN KEY (user_id) REFERENCES dim_user(user_id),
            FOREIGN KEY (time_id) REFERENCES dim_time(time_id)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fact_checkin (
            checkin_id INT PRIMARY KEY AUTO_INCREMENT,
            business_id VARCHAR(100) NOT NULL,
            time_id INT NOT NULL,
            checkin_count INT NOT NULL,
            FOREIGN KEY (business_id) REFERENCES dim_business(business_id),
            FOREIGN KEY (time_id) REFERENCES dim_time(time_id)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fact_tip (
            tip_id INT PRIMARY KEY AUTO_INCREMENT,
            business_id VARCHAR(100) NOT NULL,
            user_id VARCHAR(100) NOT NULL,
            time_id INT NOT NULL,
            text TEXT NOT NULL,
            compliment_count INT NOT NULL,
            FOREIGN KEY (business_id) REFERENCES dim_business(business_id),
            FOREIGN KEY (user_id) REFERENCES dim_user(user_id),
            FOREIGN KEY (time_id) REFERENCES dim_time(time_id)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS summary_business_performance (
            business_id VARCHAR(100) PRIMARY KEY,
            total_reviews INT,
            avg_rating DECIMAL(2,1),
            total_checkins INT,
            total_tips INT,
            FOREIGN KEY (business_id) REFERENCES dim_business(business_id)
        )
    """)

def create_time_dimension(cursor):
    """Create time dimension with dates from 2004 to 2023"""
    print("Creating time dimension...")
    start_date = datetime(2004, 1, 1)  # Yelp was founded in 2004
    end_date = datetime(2023, 12, 31)  # Include data up to 2023
    current_date = start_date
    
    # Use batch processing for better performance
    batch_size = 1000
    batch_values = []
    
    while current_date <= end_date:
        day_name = current_date.strftime('%A')
        month_name = current_date.strftime('%B')
        quarter = ((current_date.month - 1) // 3) + 1
        
        batch_values.append((
            current_date.strftime('%Y-%m-%d'),
            day_name,
            current_date.day,
            current_date.month,
            month_name,
            quarter,
            current_date.year
        ))
        
        if len(batch_values) >= batch_size:
            # Insert batch
            cursor.executemany("""
                INSERT IGNORE INTO dim_time 
                (date_actual, day_of_week, day_of_month, month_actual, month_name, quarter_actual, year_actual)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, batch_values)
            batch_values = []
        
        current_date += timedelta(days=1)
    
    # Insert any remaining dates
    if batch_values:
        cursor.executemany("""
            INSERT IGNORE INTO dim_time 
            (date_actual, day_of_week, day_of_month, month_actual, month_name, quarter_actual, year_actual)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, batch_values)
    
    print("Time dimension created.")

def process_categories(cursor, categories_data):
    """Process categories data and link to businesses"""
    if not categories_data:
        return
    
    # Get unique categories
    unique_categories = set(category for _, category in categories_data)
    
    # Insert categories
    category_values = [(category,) for category in unique_categories]
    cursor.executemany("""
        INSERT IGNORE INTO dim_category (category_name)
        VALUES (%s)
    """, category_values)
    
    # Create a dict mapping category names to IDs
    cursor.execute("SELECT category_id, category_name FROM dim_category WHERE category_name IN (%s)" % 
                  ','.join(['%s'] * len(unique_categories)), tuple(unique_categories))
    category_ids = {row[1]: row[0] for row in cursor.fetchall()}
    
    # Link businesses to categories
    business_category_values = []
    for business_id, category in categories_data:
        if category in category_ids:
            business_category_values.append((business_id, category_ids[category]))
    
    if business_category_values:
        cursor.executemany("""
            INSERT IGNORE INTO business_category (business_id, category_id)
            VALUES (%s, %s)
        """, business_category_values)

def load_businesses(cursor, file_path, batch_size=1000):
    """Load businesses from Yelp dataset file in batches"""
    print(f"Loading businesses from {file_path}...")
    
    if not os.path.exists(file_path):
        print(f"Error: File {file_path} not found.")
        return 0
    
    locations = {}  # Cache location IDs to avoid duplicate queries
    total_loaded = 0
    batch_count = 0
    
    # First ensure table is empty or exists
    cursor.execute("SELECT COUNT(*) FROM dim_business")
    if cursor.fetchone()[0] > 0:
        print("Business table already has data. Skipping business load.")
        return 0
    
    with open(file_path, 'r', encoding='utf-8') as f:
        businesses_batch = []
        categories_data = []  # To store business_id and category data
        
        for line in f:
            if not line.strip():
                continue
                
            business = json.loads(line)
            
            # Skip businesses with missing required fields
            if not all(k in business for k in ['business_id', 'name', 'city', 'state']):
                continue
                
            # Process location
            location_key = f"{business.get('city')}-{business.get('state')}-{business.get('postal_code', '')}"
            if location_key not in locations:
                # Add location
                try:
                    cursor.execute("""
                        INSERT IGNORE INTO dim_location (city, state, postal_code, latitude, longitude)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (
                        business.get('city', ''),
                        business.get('state', ''),
                        business.get('postal_code', ''),
                        business.get('latitude'),
                        business.get('longitude')
                    ))
                    
                    # Get location_id
                    cursor.execute("""
                        SELECT location_id FROM dim_location 
                        WHERE city = %s AND state = %s AND postal_code = %s
                    """, (
                        business.get('city', ''),
                        business.get('state', ''),
                        business.get('postal_code', '')
                    ))
                    
                    row = cursor.fetchone()
                    if row:
                        location_id = row[0]
                        locations[location_key] = location_id
                except Exception as e:
                    print(f"Error inserting location: {e}")
                    continue
            
            # Skip if location wasn't added
            if location_key not in locations:
                continue
                
            location_id = locations[location_key]
            
            # Prepare business for batch insert
            businesses_batch.append((
                business['business_id'],
                business['name'],
                location_id,
                business.get('stars', 0),
                business.get('review_count', 0),
                business.get('is_open', 0)
            ))
            
            # Process categories
            if 'categories' in business and business['categories']:
                categories = [c.strip() for c in business['categories'].split(',')]
                for category in categories:
                    if category:
                        categories_data.append((business['business_id'], category))
            
            # Process batch when it reaches the batch size
            if len(businesses_batch) >= batch_size:
                try:
                    # Insert businesses
                    cursor.executemany("""
                        INSERT IGNORE INTO dim_business 
                        (business_id, business_name, location_id, stars, review_count, is_open)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, businesses_batch)
                    
                    # Process categories
                    process_categories(cursor, categories_data)
                    
                    # Commit transaction
                    cursor.connection.commit()
                    
                    total_loaded += len(businesses_batch)
                    batch_count += 1
                    print(f"Processed batch {batch_count}: {total_loaded} businesses loaded")
                except Exception as e:
                    print(f"Error processing batch: {e}")
                    cursor.connection.rollback()
                
                # Clear batches
                businesses_batch = []
                categories_data = []
        
        # Process any remaining businesses
        if businesses_batch:
            try:
                # Insert businesses
                cursor.executemany("""
                    INSERT IGNORE INTO dim_business 
                    (business_id, business_name, location_id, stars, review_count, is_open)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, businesses_batch)
                
                # Process categories
                process_categories(cursor, categories_data)
                
                # Commit transaction
                cursor.connection.commit()
                
                total_loaded += len(businesses_batch)
                batch_count += 1
                print(f"Processed final batch {batch_count}: {total_loaded} total businesses loaded")
            except Exception as e:
                print(f"Error processing batch: {e}")
                cursor.connection.rollback()
    
    print(f"Finished loading businesses: {total_loaded} total loaded")
    return total_loaded

def load_users(cursor, file_path, batch_size=1000):
    """Load users from Yelp dataset file in batches"""
    print(f"Loading users from {file_path}...")
    
    if not os.path.exists(file_path):
        print(f"Error: File {file_path} not found.")
        return 0
    
    # First ensure table is empty or exists
    cursor.execute("SELECT COUNT(*) FROM dim_user")
    if cursor.fetchone()[0] > 0:
        print("User table already has data. Skipping user load.")
        return 0
    
    total_loaded = 0
    batch_count = 0
    
    with open(file_path, 'r', encoding='utf-8') as f:
        users_batch = []
        
        for line in f:
            if not line.strip():
                continue
                
            user = json.loads(line)
            
            # Skip users with missing required fields
            if 'user_id' not in user:
                continue
                
            # Parse yelping_since date
            yelping_since = None
            if 'yelping_since' in user:
                try:
                    yelping_since = datetime.strptime(user['yelping_since'], '%Y-%m-%d')
                except:
                    pass
            
            # Prepare user for batch insert
            users_batch.append((
                user['user_id'],
                user.get('name', 'Unknown'),
                user.get('review_count', 0),
                yelping_since,
                user.get('fans', 0),
                user.get('average_stars', 0)
            ))
            
            # Process batch when it reaches the batch size
            if len(users_batch) >= batch_size:
                try:
                    cursor.executemany("""
                        INSERT IGNORE INTO dim_user
                        (user_id, name, review_count, yelping_since, fans, average_stars)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, users_batch)
                    
                    cursor.connection.commit()
                    total_loaded += len(users_batch)
                    batch_count += 1
                    print(f"Processed batch {batch_count}: {total_loaded} users loaded")
                except Exception as e:
                    print(f"Error processing user batch: {e}")
                    cursor.connection.rollback()
                
                users_batch = []
        
        # Process any remaining users
        if users_batch:
            try:
                cursor.executemany("""
                    INSERT IGNORE INTO dim_user
                    (user_id, name, review_count, yelping_since, fans, average_stars)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, users_batch)
                
                cursor.connection.commit()
                total_loaded += len(users_batch)
                batch_count += 1
                print(f"Processed final batch {batch_count}: {total_loaded} total users loaded")
            except Exception as e:
                print(f"Error processing user batch: {e}")
                cursor.connection.rollback()
    
    print(f"Finished loading users: {total_loaded} total loaded")
    return total_loaded

def load_reviews(cursor, file_path, batch_size=1000):
    """Load reviews from Yelp dataset file in batches"""
    print(f"Loading reviews from {file_path}...")
    
    if not os.path.exists(file_path):
        print(f"Error: File {file_path} not found.")
        return 0
    
    # First ensure table is empty or exists
    cursor.execute("SELECT COUNT(*) FROM fact_review")
    if cursor.fetchone()[0] > 0:
        print("Review table already has data. Skipping review load.")
        return 0
    
    # Create a lookup dictionary for date_actual to time_id
    print("Loading time dimension lookup...")
    cursor.execute("SELECT time_id, date_actual FROM dim_time")
    time_lookup = {row[1].strftime('%Y-%m-%d'): row[0] for row in cursor.fetchall()}
    
    # Get valid business_ids and user_ids
    print("Loading valid business and user IDs...")
    cursor.execute("SELECT business_id FROM dim_business")
    valid_businesses = {row[0] for row in cursor.fetchall()}
    
    cursor.execute("SELECT user_id FROM dim_user")
    valid_users = {row[0] for row in cursor.fetchall()}
    
    print(f"Found {len(valid_businesses)} valid businesses and {len(valid_users)} valid users")
    
    total_loaded = 0
    batch_count = 0
    skipped_count = 0
    
    with open(file_path, 'r', encoding='utf-8') as f:
        reviews_batch = []
        
        for line_num, line in enumerate(f, 1):
            if not line.strip():
                continue
                
            try:
                review = json.loads(line)
                
                # Skip reviews with missing required fields or invalid references
                if not all(k in review for k in ['review_id', 'business_id', 'user_id', 'stars', 'date']):
                    skipped_count += 1
                    continue
                
                # Skip if business or user doesn't exist
                if (review['business_id'] not in valid_businesses or 
                    review['user_id'] not in valid_users):
                    skipped_count += 1
                    continue
                
                # Parse date and get time_id
                try:
                    date_str = review['date'].split()[0]  # Split by whitespace and take first part
                    review_date = datetime.strptime(date_str, '%Y-%m-%d').strftime('%Y-%m-%d')
                    if review_date not in time_lookup:
                        # Date not in time dimension, try to add it
                        date_obj = datetime.strptime(review_date, '%Y-%m-%d')
                        cursor.execute("""
                            INSERT IGNORE INTO dim_time 
                            (date_actual, day_of_week, day_of_month, month_actual, month_name, quarter_actual, year_actual)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """, (
                            review_date,
                            date_obj.strftime('%A'),
                            date_obj.day,
                            date_obj.month,
                            date_obj.strftime('%B'),
                            ((date_obj.month - 1) // 3) + 1,
                            date_obj.year
                        ))
                        
                        # Update time_lookup
                        cursor.execute("SELECT time_id FROM dim_time WHERE date_actual = %s", (review_date,))
                        result = cursor.fetchone()
                        if result:
                            time_lookup[review_date] = result[0]
                        else:
                            # Still can't find time_id, skip this review
                            skipped_count += 1
                            continue
                    
                    time_id = time_lookup[review_date]
                except Exception as e:
                    print(f"Error processing review date at line {line_num}: {e}")
                    skipped_count += 1
                    continue
                
                # Prepare review for batch insert
                reviews_batch.append((
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
                
                # Process batch when it reaches the batch size
                if len(reviews_batch) >= batch_size:
                    try:
                        cursor.executemany("""
                            INSERT IGNORE INTO fact_review
                            (review_id, business_id, user_id, time_id, stars, useful_votes, funny_votes, cool_votes, text)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, reviews_batch)
                        
                        cursor.connection.commit()
                        total_loaded += len(reviews_batch)
                        batch_count += 1
                        print(f"Processed batch {batch_count}: {total_loaded} reviews loaded, {skipped_count} skipped")
                    except Exception as e:
                        print(f"Error processing review batch: {e}")
                        cursor.connection.rollback()
                    
                    reviews_batch = []
                
                # Progress reporting for large files
                if line_num % 100000 == 0:
                    print(f"Processed {line_num} lines, {total_loaded} reviews loaded, {skipped_count} skipped")
            
            except Exception as e:
                print(f"Error processing line {line_num}: {e}")
                skipped_count += 1
        
        # Process any remaining reviews
        if reviews_batch:
            try:
                cursor.executemany("""
                    INSERT IGNORE INTO fact_review
                    (review_id, business_id, user_id, time_id, stars, useful_votes, funny_votes, cool_votes, text)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, reviews_batch)
                
                cursor.connection.commit()
                total_loaded += len(reviews_batch)
                batch_count += 1
                print(f"Processed final batch {batch_count}: {total_loaded} total reviews loaded, {skipped_count} skipped")
            except Exception as e:
                print(f"Error processing review batch: {e}")
                cursor.connection.rollback()
    
    print(f"Finished loading reviews: {total_loaded} total loaded, {skipped_count} skipped")
    return total_loaded

def load_checkins(cursor, file_path, batch_size=1000):
    """Load checkins from Yelp dataset file in batches"""
    print(f"Loading checkins from {file_path}...")
    
    if not os.path.exists(file_path):
        print(f"Error: File {file_path} not found.")
        return 0
    
    # First ensure table is empty or exists
    cursor.execute("SELECT COUNT(*) FROM fact_checkin")
    if cursor.fetchone()[0] > 0:
        print("Checkin table already has data. Skipping checkin load.")
        return 0
    
    # Create a lookup dictionary for date_actual to time_id
    print("Loading time dimension lookup...")
    cursor.execute("SELECT time_id, date_actual FROM dim_time")
    time_lookup = {row[1].strftime('%Y-%m-%d'): row[0] for row in cursor.fetchall()}
    
    # Get valid business_ids
    print("Loading valid business IDs...")
    cursor.execute("SELECT business_id FROM dim_business")
    valid_businesses = {row[0] for row in cursor.fetchall()}
    
    total_loaded = 0
    batch_count = 0
    skipped_count = 0
    
    with open(file_path, 'r', encoding='utf-8') as f:
        checkins_batch = []
        
        for line in f:
            if not line.strip():
                continue
                
            try:
                checkin = json.loads(line)
                
                # Skip checkins with missing required fields or invalid references
                if not all(k in checkin for k in ['business_id', 'date']):
                    skipped_count += 1
                    continue
                
                # Skip if business doesn't exist
                if checkin['business_id'] not in valid_businesses:
                    skipped_count += 1
                    continue
                
                business_id = checkin['business_id']
                
                # Parse dates
                dates = []
                if isinstance(checkin['date'], str):
                    # Old format where date is a string of timestamps
                    dates = checkin['date'].split(', ')
                elif isinstance(checkin['date'], dict):
                    # New format where date is a dictionary
                    for day, times in checkin['date'].items():
                        dates.extend([day] * len(times.split(', ')))
                else:
                    skipped_count += 1
                    continue
                
                # Group by date to get counts
                date_counts = {}
                for date_str in dates:
                    try:
                        # Extract just the date part (YYYY-MM-DD)
                        date_only = date_str.split(' ')[0]
                        date_counts[date_only] = date_counts.get(date_only, 0) + 1
                    except:
                        continue
                
                # Prepare checkins for batch insert
                for date_str, count in date_counts.items():
                    # Get time_id for this date
                    if date_str not in time_lookup:
                        # Try to add missing date
                        try:
                            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                            cursor.execute("""
                                INSERT IGNORE INTO dim_time 
                                (date_actual, day_of_week, day_of_month, month_actual, month_name, quarter_actual, year_actual)
                                VALUES (%s, %s, %s, %s, %s, %s, %s)
                            """, (
                                date_str,
                                date_obj.strftime('%A'),
                                date_obj.day,
                                date_obj.month,
                                date_obj.strftime('%B'),
                                ((date_obj.month - 1) // 3) + 1,
                                date_obj.year
                            ))
                            
                            # Update time_lookup
                            cursor.execute("SELECT time_id FROM dim_time WHERE date_actual = %s", (date_str,))
                            result = cursor.fetchone()
                            if result:
                                time_lookup[date_str] = result[0]
                            else:
                                continue
                        except:
                            continue
                    
                    if date_str in time_lookup:
                        time_id = time_lookup[date_str]
                        checkins_batch.append((business_id, time_id, count))
                
                # Process batch when it reaches the batch size
                if len(checkins_batch) >= batch_size:
                    try:
                        cursor.executemany("""
                            INSERT IGNORE INTO fact_checkin
                            (business_id, time_id, checkin_count)
                            VALUES (%s, %s, %s)
                        """, checkins_batch)
                        
                        cursor.connection.commit()
                        total_loaded += len(checkins_batch)
                        batch_count += 1
                        print(f"Processed batch {batch_count}: {total_loaded} checkins loaded, {skipped_count} skipped")
                    except Exception as e:
                        print(f"Error processing checkin batch: {e}")
                        cursor.connection.rollback()
                    
                    checkins_batch = []
                
            except Exception as e:
                print(f"Error processing checkin: {e}")
                skipped_count += 1
        
        # Process any remaining checkins
        if checkins_batch:
            try:
                cursor.executemany("""
                    INSERT IGNORE INTO fact_checkin
                    (business_id, time_id, checkin_count)
                    VALUES (%s, %s, %s)
                """, checkins_batch)
                
                cursor.connection.commit()
                total_loaded += len(checkins_batch)
                batch_count += 1
                print(f"Processed final batch {batch_count}: {total_loaded} total checkins loaded, {skipped_count} skipped")
            except Exception as e:
                print(f"Error processing checkin batch: {e}")
                cursor.connection.rollback()
    
    print(f"Finished loading checkins: {total_loaded} total loaded, {skipped_count} skipped")
    return total_loaded

def load_tips(cursor, file_path, batch_size=1000):
    """Load tips from Yelp dataset file in batches"""
    print(f"Loading tips from {file_path}...")
    
    if not os.path.exists(file_path):
        print(f"Error: File {file_path} not found.")
        return 0
    
    # First ensure table is empty or exists
    cursor.execute("SELECT COUNT(*) FROM fact_tip")
    if cursor.fetchone()[0] > 0:
        print("Tip table already has data. Skipping tip load.")
        return 0
    
    # Create a lookup dictionary for date_actual to time_id
    print("Loading time dimension lookup...")
    cursor.execute("SELECT time_id, date_actual FROM dim_time")
    time_lookup = {row[1].strftime('%Y-%m-%d'): row[0] for row in cursor.fetchall()}
    
    # Get valid business_ids and user_ids
    print("Loading valid business and user IDs...")
    cursor.execute("SELECT business_id FROM dim_business")
    valid_businesses = {row[0] for row in cursor.fetchall()}
    
    cursor.execute("SELECT user_id FROM dim_user")
    valid_users = {row[0] for row in cursor.fetchall()}
    
    total_loaded = 0
    batch_count = 0
    skipped_count = 0
    
    with open(file_path, 'r', encoding='utf-8') as f:
        tips_batch = []
        
        for line in f:
            if not line.strip():
                continue
                
            try:
                tip = json.loads(line)
                
                # Skip tips with missing required fields or invalid references
                if not all(k in tip for k in ['business_id', 'user_id', 'text', 'date']):
                    skipped_count += 1
                    continue
                
                # Skip if business or user doesn't exist
                if (tip['business_id'] not in valid_businesses or 
                    tip['user_id'] not in valid_users):
                    skipped_count += 1
                    continue
                
                # Parse date and get time_id
                try:
                    date_str = tip['date'].split()[0]  # Split by whitespace and take first part
                    tip_date = datetime.strptime(date_str, '%Y-%m-%d').strftime('%Y-%m-%d')
                    if tip_date not in time_lookup:
                        # Date not in time dimension, try to add it
                        date_obj = datetime.strptime(tip_date, '%Y-%m-%d')
                        cursor.execute("""
                            INSERT IGNORE INTO dim_time 
                            (date_actual, day_of_week, day_of_month, month_actual, month_name, quarter_actual, year_actual)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """, (
                            tip_date,
                            date_obj.strftime('%A'),
                            date_obj.day,
                            date_obj.month,
                            date_obj.strftime('%B'),
                            ((date_obj.month - 1) // 3) + 1,
                            date_obj.year
                        ))
                        
                        # Update time_lookup
                        cursor.execute("SELECT time_id FROM dim_time WHERE date_actual = %s", (tip_date,))
                        result = cursor.fetchone()
                        if result:
                            time_lookup[tip_date] = result[0]
                        else:
                            # Still can't find time_id, skip this tip
                            skipped_count += 1
                            continue
                    
                    time_id = time_lookup[tip_date]
                except Exception as e:
                    print(f"Error processing tip date: {e}")
                    skipped_count += 1
                    continue
                
                # Prepare tip for batch insert
                tips_batch.append((
                    tip['business_id'],
                    tip['user_id'],
                    time_id,
                    tip['text'],
                    tip.get('compliment_count', 0)
                ))
                
                # Process batch when it reaches the batch size
                if len(tips_batch) >= batch_size:
                    try:
                        cursor.executemany("""
                            INSERT INTO fact_tip
                            (business_id, user_id, time_id, text, compliment_count)
                            VALUES (%s, %s, %s, %s, %s)
                        """, tips_batch)
                        
                        cursor.connection.commit()
                        total_loaded += len(tips_batch)
                        batch_count += 1
                        print(f"Processed batch {batch_count}: {total_loaded} tips loaded, {skipped_count} skipped")
                    except Exception as e:
                        print(f"Error processing tip batch: {e}")
                        cursor.connection.rollback()
                    
                    tips_batch = []
                
            except Exception as e:
                print(f"Error processing tip: {e}")
                skipped_count += 1
        
        # Process any remaining tips
        if tips_batch:
            try:
                cursor.executemany("""
                    INSERT INTO fact_tip
                    (business_id, user_id, time_id, text, compliment_count)
                    VALUES (%s, %s, %s, %s, %s)
                """, tips_batch)
                
                cursor.connection.commit()
                total_loaded += len(tips_batch)
                batch_count += 1
                print(f"Processed final batch {batch_count}: {total_loaded} total tips loaded, {skipped_count} skipped")
            except Exception as e:
                print(f"Error processing tip batch: {e}")
                cursor.connection.rollback()
    
    print(f"Finished loading tips: {total_loaded} total loaded, {skipped_count} skipped")
    return total_loaded

def create_summary_tables(cursor):
    """Create summary tables for better performance"""
    print("Creating summary tables...")
    
    try:
        cursor.execute("""
            INSERT INTO summary_business_performance 
                (business_id, total_reviews, avg_rating, total_checkins, total_tips)
            SELECT 
                b.business_id,
                COUNT(DISTINCT r.review_id) as total_reviews,
                IFNULL(AVG(r.stars), b.stars) as avg_rating,
                IFNULL(SUM(c.checkin_count), 0) as total_checkins,
                COUNT(DISTINCT t.tip_id) as total_tips
            FROM 
                dim_business b
                LEFT JOIN fact_review r ON b.business_id = r.business_id
                LEFT JOIN fact_checkin c ON b.business_id = c.business_id
                LEFT JOIN fact_tip t ON b.business_id = t.business_id
            GROUP BY 
                b.business_id
            ON DUPLICATE KEY UPDATE
                total_reviews = VALUES(total_reviews),
                avg_rating = VALUES(avg_rating),
                total_checkins = VALUES(total_checkins),
                total_tips = VALUES(total_tips)
        """)
        
        cursor.connection.commit()
        print("Summary tables created successfully.")
        return True
    except Exception as e:
        print(f"Error creating summary tables: {e}")
        cursor.connection.rollback()
        return False