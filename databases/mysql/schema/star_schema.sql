-- Create schema for Yelp Data Warehouse
USE yelp_dw;

-- Dimension tables
CREATE TABLE dim_time (
    time_id INT PRIMARY KEY AUTO_INCREMENT,
    date_actual DATE NOT NULL,
    day_of_week VARCHAR(10) NOT NULL,
    day_of_month INT NOT NULL,
    month_actual INT NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    quarter_actual INT NOT NULL,
    year_actual INT NOT NULL,
    UNIQUE KEY uk_date (date_actual)
);

CREATE TABLE dim_location (
    location_id INT PRIMARY KEY AUTO_INCREMENT,
    city VARCHAR(100) NOT NULL,
    state VARCHAR(50) NOT NULL,
    postal_code VARCHAR(20) NOT NULL,
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    UNIQUE KEY uk_location (city, state, postal_code)
);

CREATE TABLE dim_business (
    business_id VARCHAR(100) PRIMARY KEY,
    business_name VARCHAR(255) NOT NULL,
    location_id INT NOT NULL,
    stars DECIMAL(2,1),
    review_count INT,
    is_open BOOLEAN,
    FOREIGN KEY (location_id) REFERENCES dim_location(location_id)
);

CREATE TABLE dim_user (
    user_id VARCHAR(100) PRIMARY KEY,
    name VARCHAR(255),
    review_count INT,
    yelping_since DATE,
    fans INT,
    average_stars DECIMAL(2,1)
);

CREATE TABLE dim_category (
    category_id INT PRIMARY KEY AUTO_INCREMENT,
    category_name VARCHAR(100) NOT NULL,
    UNIQUE KEY uk_category (category_name)
);

CREATE TABLE business_category (
    business_id VARCHAR(100) NOT NULL,
    category_id INT NOT NULL,
    PRIMARY KEY (business_id, category_id),
    FOREIGN KEY (business_id) REFERENCES dim_business(business_id),
    FOREIGN KEY (category_id) REFERENCES dim_category(category_id)
);

-- Fact tables
CREATE TABLE fact_review (
    review_id VARCHAR(100) PRIMARY KEY,
    business_id VARCHAR(100) NOT NULL,
    user_id VARCHAR(100) NOT NULL,
    time_id INT NOT NULL,
    stars INT NOT NULL,
    useful_votes INT NOT NULL,
    funny_votes INT NOT NULL,
    cool_votes INT NOT NULL,
    text TEXT,  <!-- Add this line -->
    FOREIGN KEY (business_id) REFERENCES dim_business(business_id),
    FOREIGN KEY (user_id) REFERENCES dim_user(user_id),
    FOREIGN KEY (time_id) REFERENCES dim_time(time_id)
);

CREATE TABLE fact_checkin (
    checkin_id INT PRIMARY KEY AUTO_INCREMENT,
    business_id VARCHAR(100) NOT NULL,
    time_id INT NOT NULL,
    checkin_count INT NOT NULL,
    FOREIGN KEY (business_id) REFERENCES dim_business(business_id),
    FOREIGN KEY (time_id) REFERENCES dim_time(time_id)
);

-- Pre-aggregated summary tables
CREATE TABLE summary_business_performance (
    business_id VARCHAR(100) PRIMARY KEY,
    total_reviews INT,
    avg_rating DECIMAL(2,1),
    total_checkins INT,
    FOREIGN KEY (business_id) REFERENCES dim_business(business_id)
);