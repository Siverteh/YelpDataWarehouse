/**
 * Neo4j-specific functionality for Yelp Data Warehouse Dashboard
 */

// Global variables for Neo4j charts
let neo4jCategoriesChart = null;
let neo4jRelationshipsChart = null;
let neo4jBusinessDetailsChart = null;

// Load Neo4j Overview Stats
async function loadNeo4jOverviewStats() {
    try {
        const response = await fetch('/api/neo4j/overview_stats');
        const data = await response.json();
        
        // Update the stats counts
        document.getElementById('neo4j-business-count').textContent = formatNumber(data.business_count);
        document.getElementById('neo4j-review-count').textContent = formatNumber(data.review_count);
        document.getElementById('neo4j-user-count').textContent = formatNumber(data.user_count);
        document.getElementById('neo4j-relationship-count').textContent = formatNumber(data.relationship_count);
        
        // Create or update category chart
        const categoryLabels = data.category_stats.map(item => item.category);
        const categoryCounts = data.category_stats.map(item => item.count);
        
        const categoryCtx = document.getElementById('neo4jCategoriesChart').getContext('2d');
        if (neo4jCategoriesChart) {
            neo4jCategoriesChart.destroy();
        }
        
        neo4jCategoriesChart = new Chart(categoryCtx, {
            type: 'bar',
            data: {
                labels: categoryLabels,
                datasets: [{
                    label: 'Businesses per Category',
                    data: categoryCounts,
                    backgroundColor: 'rgba(46, 204, 113, 0.7)'
                }]
            },
            options: {
                responsive: true,
                scales: {
                    y: {
                        beginAtZero: true
                    }
                }
            }
        });
        
        // Create or update relationships chart
        const relationshipLabels = data.relationship_stats.map(item => item.type);
        const relationshipCounts = data.relationship_stats.map(item => item.count);
        
        const relationshipsCtx = document.getElementById('neo4jRelationshipsChart').getContext('2d');
        if (neo4jRelationshipsChart) {
            neo4jRelationshipsChart.destroy();
        }
        
        neo4jRelationshipsChart = new Chart(relationshipsCtx, {
            type: 'pie',
            data: {
                labels: relationshipLabels,
                datasets: [{
                    data: relationshipCounts,
                    backgroundColor: [
                        'rgba(52, 152, 219, 0.8)',
                        'rgba(46, 204, 113, 0.8)',
                        'rgba(241, 196, 15, 0.8)',
                        'rgba(230, 126, 34, 0.8)',
                        'rgba(211, 35, 35, 0.8)',
                        'rgba(155, 89, 182, 0.8)',
                        'rgba(26, 188, 156, 0.8)'
                    ]
                }]
            },
            options: {
                responsive: true
            }
        });
        
    } catch (error) {
        console.error('Error loading Neo4j overview stats:', error);
    }
}

// Load Neo4j Top Businesses
async function loadNeo4jBusinesses() {
    const category = document.getElementById('neo4jCategorySelect').value;
    const limit = document.getElementById('neo4jLimitSelect').value;
    
    // Show loader
    document.getElementById('neo4jBusinessesLoader').classList.remove('d-none');
    document.getElementById('neo4jBusinessesTable').innerHTML = '';
    
    try {
        const response = await fetch(`/api/neo4j/top_businesses?category=${encodeURIComponent(category)}&limit=${limit}`);
        const businesses = await response.json();
        
        // Create table
        let tableHtml = `
            <table class="table table-hover business-table">
                <thead>
                    <tr>
                        <th>Business Name</th>
                        <th>City</th>
                        <th>State</th>
                        <th>Stars</th>
                        <th>Reviews</th>
                    </tr>
                </thead>
                <tbody>
        `;
        
        businesses.forEach(business => {
            tableHtml += `
                <tr data-business-id="${business.business_id}" onclick="showNeo4jBusinessDetails('${business.business_id}', '${business.business_name}')">
                    <td>${business.business_name}</td>
                    <td>${business.city || 'N/A'}</td>
                    <td>${business.state || 'N/A'}</td>
                    <td>${business.stars || 'N/A'}</td>
                    <td>${business.review_count || 'N/A'}</td>
                </tr>
            `;
        });
        
        tableHtml += `
                </tbody>
            </table>
        `;
        
        document.getElementById('neo4jBusinessesTable').innerHTML = tableHtml;
    } catch (error) {
        console.error('Error loading businesses:', error);
        document.getElementById('neo4jBusinessesTable').innerHTML = '<div class="alert alert-danger">Error loading businesses. Please try again.</div>';
    } finally {
        // Hide loader
        document.getElementById('neo4jBusinessesLoader').classList.add('d-none');
    }
}

// Show Neo4j Business Details
async function showNeo4jBusinessDetails(businessId, businessName) {
    document.getElementById('neo4jBusinessDetails').classList.remove('d-none');
    document.getElementById('neo4jBusinessDetailsName').textContent = businessName;
    document.getElementById('neo4jBusinessDetails').setAttribute('data-business-id', businessId);
    
    try {
        const response = await fetch(`/api/neo4j/business_performance?business_id=${encodeURIComponent(businessId)}`);
        const data = await response.json();
        
        // Update business details
        const business = data.business;
        document.getElementById('neo4jBusinessDetailsLocation').textContent = `${business.city || 'N/A'}, ${business.state || 'N/A'}`;
        document.getElementById('neo4jBusinessDetailsRating').textContent = `${business.stars || 'N/A'} stars (${business.avg_stars ? business.avg_stars.toFixed(1) : 'N/A'} avg)`;
        document.getElementById('neo4jBusinessDetailsReviews').textContent = business.review_count || business.total_reviews || 'N/A';
        document.getElementById('neo4jBusinessDetailsCheckins').textContent = business.checkin_count || 'N/A';
        
        // Create chart for reviews and checkins by month
        const reviewsData = data.reviews_by_month || [];
        const checkinsData = data.checkins_by_month || [];
        
        // Prepare chart data
        const labels = [];
        const reviewCounts = [];
        const checkinCounts = [];
        
        // Combine and sort data by year and month
        const combinedData = {};
        
        reviewsData.forEach(item => {
            const key = `${item.year}-${item.month}`;
            if (!combinedData[key]) {
                combinedData[key] = { year: item.year, month: item.month, reviews: 0, checkins: 0 };
            }
            combinedData[key].reviews = item.review_count;
        });
        
        checkinsData.forEach(item => {
            const key = `${item.year}-${item.month}`;
            if (!combinedData[key]) {
                combinedData[key] = { year: item.year, month: item.month, reviews: 0, checkins: 0 };
            }
            combinedData[key].checkins = item.checkin_count;
        });
        
        // Convert to sorted array
        const sortedData = Object.values(combinedData).sort((a, b) => {
            if (a.year !== b.year) return a.year - b.year;
            return a.month - b.month;
        });
        
        // Extract data for chart
        sortedData.forEach(item => {
            labels.push(`${item.year}-${item.month}`);
            reviewCounts.push(item.reviews);
            checkinCounts.push(item.checkins);
        });
        
        // Create chart
        const ctx = document.getElementById('neo4jBusinessDetailsChart').getContext('2d');
        if (neo4jBusinessDetailsChart) {
            neo4jBusinessDetailsChart.destroy();
        }
        
        neo4jBusinessDetailsChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: labels,
                datasets: [
                    {
                        label: 'Reviews',
                        data: reviewCounts,
                        borderColor: '#d32323',
                        backgroundColor: 'rgba(211, 35, 35, 0.1)',
                        fill: true,
                        tension: 0.1
                    },
                    {
                        label: 'Checkins',
                        data: checkinCounts,
                        borderColor: '#4682B4',
                        backgroundColor: 'rgba(70, 130, 180, 0.1)',
                        fill: true,
                        tension: 0.1
                    }
                ]
            },
            options: {
                responsive: true,
                plugins: {
                    title: {
                        display: true,
                        text: 'Monthly Activity'
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true
                    }
                }
            }
        });
        
    } catch (error) {
        console.error('Error loading business details:', error);
    }
}

// Load Neo4j Network Analysis
async function loadNeo4jNetwork() {
    const businessId = document.getElementById('neo4jBusinessIdInput').value.trim();
    
    if (!businessId) {
        alert('Please enter a business ID');
        return;
    }
    
    // Show loader
    document.getElementById('neo4jNetworkLoader').classList.remove('d-none');
    document.getElementById('neo4jNetworkResults').classList.add('d-none');
    
    try {
        const response = await fetch(`/api/neo4j/business_network?business_id=${encodeURIComponent(businessId)}`);
        const data = await response.json();
        
        const categoryRelated = data.category_related || [];
        const userRelated = data.user_related || [];
        
        // Create tables
        let categoryTableHtml = `
            <table class="table table-hover">
                <thead>
                    <tr>
                        <th>Business Name</th>
                        <th>City</th>
                        <th>Stars</th>
                        <th>Common Categories</th>
                    </tr>
                </thead>
                <tbody>
        `;
        
        categoryRelated.forEach(business => {
            categoryTableHtml += `
                <tr>
                    <td>${business.business_name}</td>
                    <td>${business.city || 'N/A'}, ${business.state || 'N/A'}</td>
                    <td>${business.stars || 'N/A'}</td>
                    <td>${business.common_categories || 'N/A'}</td>
                </tr>
            `;
        });
        
        categoryTableHtml += `
                </tbody>
            </table>
        `;
        
        let userTableHtml = `
            <table class="table table-hover">
                <thead>
                    <tr>
                        <th>Business Name</th>
                        <th>City</th>
                        <th>Stars</th>
                        <th>Common Users</th>
                    </tr>
                </thead>
                <tbody>
        `;
        
        userRelated.forEach(business => {
            userTableHtml += `
                <tr>
                    <td>${business.business_name}</td>
                    <td>${business.city || 'N/A'}, ${business.state || 'N/A'}</td>
                    <td>${business.stars || 'N/A'}</td>
                    <td>${business.common_users || 'N/A'}</td>
                </tr>
            `;
        });
        
        userTableHtml += `
                </tbody>
            </table>
        `;
        
        document.getElementById('neo4jCategoryRelatedTable').innerHTML = categoryTableHtml;
        document.getElementById('neo4jUserRelatedTable').innerHTML = userTableHtml;
        document.getElementById('neo4jNetworkResults').classList.remove('d-none');
    } catch (error) {
        console.error('Error loading network analysis:', error);
        alert('Error loading network analysis. Please check the business ID and try again.');
    } finally {
        // Hide loader
        document.getElementById('neo4jNetworkLoader').classList.add('d-none');
    }
}

// Initialize Neo4j event listeners
function initializeNeo4jEventListeners() {
    // Business search
    document.getElementById('loadNeo4jBusinesses').addEventListener('click', loadNeo4jBusinesses);
    
    // Network analysis
    document.getElementById('loadNeo4jNetwork').addEventListener('click', loadNeo4jNetwork);
    
    // Tab change events
    document.getElementById('neo4j-overview-tab').addEventListener('shown.bs.tab', function (e) {
        loadNeo4jOverviewStats();
    });
    
    // Main tab selection
    document.getElementById('neo4j-tab').addEventListener('shown.bs.tab', function (e) {
        // Check if the overview tab is active
        if (document.getElementById('neo4j-overview-tab').classList.contains('active')) {
            loadNeo4jOverviewStats();
        }
    });
}

// Initialize when the DOM is loaded
document.addEventListener('DOMContentLoaded', function() {
    initializeNeo4jEventListeners();
});