/**
 * Main JavaScript for Yelp Data Warehouse Dashboard
 * Contains common functionality used across all database implementations
 */

// Check database status
async function checkDatabaseStatus() {
    try {
        const response = await fetch('/api/health');
        const data = await response.json();
        
        const statusBadge = document.getElementById('databaseStatus');
        if (data.status === 'healthy') {
            statusBadge.className = 'badge bg-success';
            statusBadge.textContent = 'All Connections OK';
        } else {
            statusBadge.className = 'badge bg-warning';
            statusBadge.textContent = 'Some Connections Failed';
        }
        
        // Show individual statuses
        console.log('Database Status:', data.services);
    } catch (error) {
        console.error('Error checking database status:', error);
        const statusBadge = document.getElementById('databaseStatus');
        statusBadge.className = 'badge bg-danger';
        statusBadge.textContent = 'Connection Error';
    }
}

// Initialize tab listeners
function initializeTabs() {
    // Fix any bootstrap v5 tab event listener issues
    const triggerTabList = [].slice.call(document.querySelectorAll(
        '#mainTabs button, #mysqlDashboardTabs button, ' +
        '#mongodbDashboardTabs button, #neo4jDashboardTabs button'
    ));
    
    triggerTabList.forEach(function(triggerEl) {
        const tabTrigger = new bootstrap.Tab(triggerEl);
        
        triggerEl.addEventListener('click', function (event) {
            event.preventDefault();
            tabTrigger.show();
        });
    });
}

// Generic function to create a stars rating display
function formatStarRating(stars) {
    if (stars === undefined || stars === null) return 'N/A';
    
    const fullStars = Math.floor(stars);
    const halfStar = stars % 1 >= 0.5;
    let html = '';
    
    // Add full stars
    for (let i = 0; i < fullStars; i++) {
        html += '<i class="bi bi-star-fill text-warning"></i>';
    }
    
    // Add half star if needed
    if (halfStar) {
        html += '<i class="bi bi-star-half text-warning"></i>';
    }
    
    // Add empty stars to make 5 total
    const emptyStars = 5 - fullStars - (halfStar ? 1 : 0);
    for (let i = 0; i < emptyStars; i++) {
        html += '<i class="bi bi-star text-warning"></i>';
    }
    
    return html + ` (${parseFloat(stars).toFixed(1)})`;
}

// Format numbers with commas for better readability
function formatNumber(num) {
    if (num === undefined || num === null) return 'N/A';
    return num.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

// Initialize event listeners when the DOM is loaded
document.addEventListener('DOMContentLoaded', function() {
    // Check database status on page load
    checkDatabaseStatus();
    
    // Setup tab functionality
    initializeTabs();
});