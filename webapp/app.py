from flask import Flask, render_template
from flask_socketio import SocketIO, emit
import logging
import time
import json

# Import database module
from db_utils import logger

# Import routes from blueprints
from mysql_routes import mysql_bp
from mongodb_routes import mongodb_bp
from neo4j_routes import neo4j_bp
from utility_routes import utility_bp

# Initialize Flask app
app = Flask(__name__)
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0  # Disable caching of static files
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet')

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

# Socket.IO event handlers
@socketio.on('connect')
def handle_connect():
    logger.info("Client connected")
    emit('connection_response', {'status': 'connected'})

@socketio.on('disconnect')
def handle_disconnect():
    logger.info("Client disconnected")

# Create a function to broadcast updates to all connected clients
def broadcast_update(database, update_type, data):
    """
    Broadcast data updates to all connected clients
    
    Args:
        database (str): The database that was updated ('mysql', 'mongodb', or 'neo4j')
        update_type (str): The type of update ('review', 'checkin', etc.)
        data (dict): The data to broadcast
    """
    socketio.emit('data_update', {
        'database': database,
        'update_type': update_type,
        'data': data,
        'timestamp': time.time()
    })

# Run the application
if __name__ == '__main__':
    # Add a short delay to ensure databases are ready
    logger.info("Starting Flask application on 0.0.0.0:8080...")
    time.sleep(1)  # Small delay to ensure logging is set up
    socketio.run(app, host='0.0.0.0', port=8080, debug=True)