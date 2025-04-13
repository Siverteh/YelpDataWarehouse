/**
 * WebSocket functionality for real-time updates in the Yelp Data Warehouse Dashboard
 */

let socket = null;
let reconnectAttempts = 0;
const maxReconnectAttempts = 5;

// Initialize WebSocket connection
function initWebSocket() {
    // Close existing connection if any
    if (socket) {
        socket.close();
    }
    
    try {
        // Create socket.io connection without explicitly constructing the URL
        socket = io();
        
        // Connection opened
        socket.on('connect', function() {
            console.log('WebSocket connected');
            reconnectAttempts = 0;
            
            // Show connection status
            const statusBadge = document.getElementById('websocketStatus');
            if (statusBadge) {
                statusBadge.className = 'badge bg-success';
                statusBadge.textContent = 'Real-time Updates Active';
            }
        });
        
        // Connection closed or error
        socket.on('disconnect', function() {
            console.log('WebSocket disconnected');
            
            // Show disconnected status
            const statusBadge = document.getElementById('websocketStatus');
            if (statusBadge) {
                statusBadge.className = 'badge bg-warning';
                statusBadge.textContent = 'Updates Disconnected';
            }
            
            // Try to reconnect
            if (reconnectAttempts < maxReconnectAttempts) {
                reconnectAttempts++;
                setTimeout(initWebSocket, 3000);
            }
        });
        
        // Handle data updates
        socket.on('data_update', function(data) {
            console.log('Received data update:', data);
            handleDataUpdate(data);
        });
        
        // Handle connection response
        socket.on('connection_response', function(data) {
            console.log('Connection response:', data);
        });
        
        // Handle errors
        socket.on('error', function(error) {
            console.error('WebSocket error:', error);
        });
    } catch (error) {
        console.error('Error initializing WebSocket:', error);
    }
}

// Handle data updates based on the database and update type
function handleDataUpdate(data) {
    const { database, update_type, data: updateData } = data;
    
    // Show notification
    showUpdateNotification(database, update_type, updateData);
    
    // Refresh data based on the active tab and update type
    const activeTab = document.querySelector('#mainTabs button.active');
    if (!activeTab) return;
    
    const activeTabId = activeTab.id;
    
    if (activeTabId === 'mysql-tab' && database === 'mysql') {
        refreshMySQLData(update_type, updateData);
    } else if (activeTabId === 'mongodb-tab' && database === 'mongodb') {
        refreshMongoDBData(update_type, updateData);
    } else if (activeTabId === 'neo4j-tab' && database === 'neo4j') {
        refreshNeo4jData(update_type, updateData);
    }
}

// Refresh MySQL data
function refreshMySQLData(updateType, data) {
    // Determine what to refresh based on update type
    if (updateType === 'review' || updateType === 'checkin') {
        // Only refresh if overview tab is active
        const overviewTab = document.getElementById('mysql-overview-tab');
        if (overviewTab && overviewTab.classList.contains('active')) {
            loadMySQLOverviewStats();
        }
        
        // If business details are shown and match the updated business, refresh
        const businessDetails = document.getElementById('mysqlBusinessDetails');
        if (!businessDetails.classList.contains('d-none')) {
            const businessId = data.business_id;
            if (businessId) {
                const currentBusinessId = businessDetails.getAttribute('data-business-id');
                if (currentBusinessId === businessId) {
                    showMySQLBusinessDetails(businessId, document.getElementById('mysqlBusinessDetailsName').textContent);
                }
            }
        }
    }
}

// Refresh MongoDB data
function refreshMongoDBData(updateType, data) {
    // Similar to MySQL refresh, adapt for MongoDB components
    if (updateType === 'review' || updateType === 'checkin') {
        const overviewTab = document.getElementById('mongodb-overview-tab');
        if (overviewTab && overviewTab.classList.contains('active')) {
            loadMongoDBOverviewStats();
        }
        
        const businessDetails = document.getElementById('mongodbBusinessDetails');
        if (!businessDetails.classList.contains('d-none')) {
            const businessId = data.business_id;
            if (businessId) {
                const currentBusinessId = businessDetails.getAttribute('data-business-id');
                if (currentBusinessId === businessId) {
                    showMongoDBBusinessDetails(businessId, document.getElementById('mongodbBusinessDetailsName').textContent);
                }
            }
        }
    }
}

// Refresh Neo4j data
function refreshNeo4jData(updateType, data) {
    // Similar to MySQL refresh, adapt for Neo4j components
    if (updateType === 'review' || updateType === 'checkin') {
        const overviewTab = document.getElementById('neo4j-overview-tab');
        if (overviewTab && overviewTab.classList.contains('active')) {
            loadNeo4jOverviewStats();
        }
        
        const businessDetails = document.getElementById('neo4jBusinessDetails');
        if (!businessDetails.classList.contains('d-none')) {
            const businessId = data.business_id;
            if (businessId) {
                const currentBusinessId = businessDetails.getAttribute('data-business-id');
                if (currentBusinessId === businessId) {
                    showNeo4jBusinessDetails(businessId, document.getElementById('neo4jBusinessDetailsName').textContent);
                }
            }
        }
    }
}

// Show notification when new data arrives
function showUpdateNotification(database, updateType, data) {
    // Create notification element if it doesn't exist
    let notificationContainer = document.getElementById('updateNotifications');
    if (!notificationContainer) {
        notificationContainer = document.createElement('div');
        notificationContainer.id = 'updateNotifications';
        notificationContainer.className = 'update-notification-container';
        document.body.appendChild(notificationContainer);
    }
    
    // Create notification
    const notification = document.createElement('div');
    notification.className = 'update-notification';
    
    // Set notification content based on update type
    let title = 'New Data';
    let content = '';
    
    if (updateType === 'review') {
        title = 'New Review';
        content = `New ${data.stars}★ review for business ${data.business_id.substring(0, 10)}...`;
    } else if (updateType === 'checkin') {
        title = 'New Check-in';
        content = `${data.count} new check-in(s) for business ${data.business_id.substring(0, 10)}...`;
    }
    
    // Set database badge
    let badgeClass = 'badge bg-primary';
    if (database === 'mysql') {
        badgeClass = 'badge bg-danger';
    } else if (database === 'mongodb') {
        badgeClass = 'badge bg-success';
    } else if (database === 'neo4j') {
        badgeClass = 'badge bg-info text-dark';
    }
    
    notification.innerHTML = `
        <div class="notification-header">
            <span class="${badgeClass}">${database.toUpperCase()}</span>
            <strong>${title}</strong>
            <button type="button" class="btn-close" onclick="this.parentElement.parentElement.remove()"></button>
        </div>
        <div class="notification-content">
            ${content}
        </div>
    `;
    
    // Add notification to container
    notificationContainer.appendChild(notification);
    
    // Remove notification after 5 seconds
    setTimeout(() => {
        notification.classList.add('notification-fade-out');
        setTimeout(() => {
            notification.remove();
        }, 500);
    }, 5000);
}

// Initialize WebSocket when the DOM is loaded
document.addEventListener('DOMContentLoaded', function() {
    // Add WebSocket status badge to header
    const headerDiv = document.querySelector('.header-bar .container');
    if (headerDiv) {
        const statusBadge = document.createElement('span');
        statusBadge.id = 'websocketStatus';
        statusBadge.className = 'badge bg-secondary ms-2';
        statusBadge.textContent = 'Connecting...';
        headerDiv.appendChild(statusBadge);
    }
    
    // Initialize WebSocket connection
    initWebSocket();
});

// Create notification styling
document.addEventListener('DOMContentLoaded', function() {
    const style = document.createElement('style');
    style.textContent = `
        .update-notification-container {
            position: fixed;
            bottom: 20px;
            right: 20px;
            z-index: 9999;
            max-width: 300px;
        }
        
        .update-notification {
            background-color: white;
            border-radius: 4px;
            box-shadow: 0 2px 10px rgba(0, 0, 0, 0.2);
            margin-top: 10px;
            overflow: hidden;
            transition: opacity 0.5s;
        }
        
        .notification-header {
            background-color: #f5f5f5;
            padding: 8px 12px;
            display: flex;
            align-items: center;
            justify-content: space-between;
        }
        
        .notification-content {
            padding: 12px;
        }
        
        .notification-fade-out {
            opacity: 0;
        }
        
        .btn-close {
            font-size: 0.8rem;
            background: transparent;
            border: none;
            cursor: pointer;
        }
    `;
    document.head.appendChild(style);
});