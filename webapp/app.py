"""
Main Flask application for the Yelp Data Warehouse
Modular implementation with separate files for each database backend
"""
from flask import Flask, render_template
import logging
import time

# Import database module
from db_utils import logger

# Import routes from blueprints
from mysql_routes import mysql_bp
from mongodb_routes import mongodb_bp
from neo4j_routes import neo4j_bp
from utility_routes import utility_bp

# Initialize Flask app
app = Flask(__name__)

# Register blueprints
app.register_blueprint(mysql_bp)
app.register_blueprint(mongodb_bp)
app.register_blueprint(neo4j_bp)
app.register_blueprint(utility_bp)

# Main routes
@app.route('/')
def index():
    """Render the main dashboard page"""
    return render_template('index.html')

# Run the application
if __name__ == '__main__':
    # Add a short delay to ensure databases are ready
    logger.info("Starting Flask application on 0.0.0.0:8080...")
    time.sleep(1)  # Small delay to ensure logging is set up
    app.run(host='0.0.0.0', port=8080, debug=True)