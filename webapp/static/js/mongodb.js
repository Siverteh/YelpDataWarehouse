/**
 * MongoDB-specific functionality for Yelp Data Warehouse Dashboard
 */

// Global variables for MongoDB charts
let mongodbCategoriesChart = null;
let mongodbStarsChart = null;
let mongodbBusinessDetailsChart = null;
let mongodbDocumentSizeChart = null;
let mongodbAttributesChart = null;

// Load MongoDB Overview Stats
async function loadMongoDBOverviewStats() {
    try {
        const response = await fetch('/api/mongodb/overview_stats');
        const data = await response.json();
        
        // Update the stats counts
        document.getElementById('mongodb-business-count').textContent = formatNumber(data.business_count);
        document.getElementById('mongodb-review-count').textContent = formatNumber(data.review_count);
        document.getElementById('mongodb-user-count').textContent = formatNumber(data.user_count);
        
        // Create or update category chart
        const categoryLabels = data.category_stats.map(item => item.category);
        const categoryCounts = data.category_stats.map(item => item.count);
        
        const categoryCtx = document.getElementById('mongodbCategoriesChart').getContext('2d');
        if (mongodbCategoriesChart) {
            mongodbCategoriesChart.destroy();
        }
        
        mongodbCategoriesChart = new Chart(categoryCtx, {
            type: 'bar',
            data: {
                labels: categoryLabels,
                datasets: [{
                    label: 'Businesses per Category',
                    data: categoryCounts,
                    backgroundColor: 'rgba(52, 152, 219, 0.7)'
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
        
        // Create or update stars chart
        const starLabels = data.star_distribution.map(item => `${item.stars} Stars`);
        const starCounts = data.star_distribution.map(item => item.count);
        
        const starsCtx = document.getElementById('mongodbStarsChart').getContext('2d');
        if (mongodbStarsChart) {
            mongodbStarsChart.destroy();
        }
        
        mongodbStarsChart = new Chart(starsCtx, {
            type: 'pie',
            data: {
                labels: starLabels,
                datasets: [{
                    data: starCounts,
                    backgroundColor: [
                        'rgba(52, 152, 219, 0.8)', // 1 star
                        'rgba(46, 204, 113, 0.8)', // 2 stars
                        'rgba(241, 196, 15, 0.8)', // 3 stars
                        'rgba(230, 126, 34, 0.8)', // 4 stars
                        'rgba(211, 35, 35, 0.8)'   // 5 stars
                    ]
                }]
            },
            options: {
                responsive: true
            }
        });
        
    } catch (error) {
        console.error('Error loading MongoDB overview stats:', error);
    }
}

// Load MongoDB Top Businesses
async function loadMongoDBBusinesses() {
    const category = document.getElementById('mongodbCategorySelect').value;
    const limit = document.getElementById('mongodbLimitSelect').value;
    
    // Show loader
    document.getElementById('mongodbBusinessesLoader').classList.remove('d-none');
    document.getElementById('mongodbBusinessesTable').innerHTML = '';
    
    try {
        const response = await fetch(`/api/mongodb/top_businesses?category=${encodeURIComponent(category)}&limit=${limit}`);
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
                <tr data-business-id="${business.business_id}" onclick="showMongoDBBusinessDetails('${business.business_id}', '${business.name}')">
                    <td>${business.name}</td>
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
        
        document.getElementById('mongodbBusinessesTable').innerHTML = tableHtml;
    } catch (error) {
        console.error('Error loading businesses:', error);
        document.getElementById('mongodbBusinessesTable').innerHTML = '<div class="alert alert-danger">Error loading businesses. Please try again.</div>';
    } finally {
        // Hide loader
        document.getElementById('mongodbBusinessesLoader').classList.add('d-none');
    }
}

// Show MongoDB Business Details
async function showMongoDBBusinessDetails(businessId, businessName) {
    document.getElementById('mongodbBusinessDetails').classList.remove('d-none');
    document.getElementById('mongodbBusinessDetailsName').textContent = businessName;
    document.getElementById('mongodbBusinessDetails').setAttribute('data-business-id', businessId);
    
    try {
        const response = await fetch(`/api/mongodb/business_performance?business_id=${encodeURIComponent(businessId)}`);
        const data = await response.json();
        
        // Update business details
        const business = data.business;
        document.getElementById('mongodbBusinessDetailsLocation').textContent = `${business.city || 'N/A'}, ${business.state || 'N/A'}`;
        document.getElementById('mongodbBusinessDetailsRating').textContent = `${business.stars || 'N/A'} stars`;
        document.getElementById('mongodbBusinessDetailsReviews').textContent = business.review_count || (business.review_stats ? business.review_stats.total_reviews : 'N/A');
        document.getElementById('mongodbBusinessDetailsCheckins').textContent = (business.checkins ? business.checkins.total_checkins : 'N/A');
        
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
        const ctx = document.getElementById('mongodbBusinessDetailsChart').getContext('2d');
        if (mongodbBusinessDetailsChart) {
            mongodbBusinessDetailsChart.destroy();
        }
        
        mongodbBusinessDetailsChart = new Chart(ctx, {
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

// Load MongoDB Analytics
async function loadMongoDBAnalytics() {
    try {
        // Document size distribution
        const docSizeResponse = await fetch('/api/mongodb/document_size_stats');
        const docSizeData = await docSizeResponse.json();
        
        const docSizeCtx = document.getElementById('mongodbDocumentSizeChart').getContext('2d');
        if (mongodbDocumentSizeChart) {
            mongodbDocumentSizeChart.destroy();
        }
        
        mongodbDocumentSizeChart = new Chart(docSizeCtx, {
            type: 'bar',
            data: {
                labels: docSizeData.size_ranges,
                datasets: [{
                    label: 'Number of Documents',
                    data: docSizeData.document_counts,
                    backgroundColor: 'rgba(52, 152, 219, 0.7)'
                }]
            },
            options: {
                responsive: true,
                plugins: {
                    title: {
                        display: true,
                        text: 'Document Size Distribution'
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true
                    }
                }
            }
        });
        
        // Business attributes analysis
        const attributesResponse = await fetch('/api/mongodb/business_attributes');
        const attributesData = await attributesResponse.json();
        
        const attributesCtx = document.getElementById('mongodbAttributesChart').getContext('2d');
        if (mongodbAttributesChart) {
            mongodbAttributesChart.destroy();
        }
        
        mongodbAttributesChart = new Chart(attributesCtx, {
            type: 'radar',
            data: {
                labels: attributesData.attribute_names,
                datasets: [{
                    label: 'Business Attributes',
                    data: attributesData.attribute_counts,
                    backgroundColor: 'rgba(46, 204, 113, 0.2)',
                    borderColor: 'rgba(46, 204, 113, 1)',
                    pointBackgroundColor: 'rgba(46, 204, 113, 1)',
                    pointBorderColor: '#fff',
                    pointHoverBackgroundColor: '#fff',
                    pointHoverBorderColor: 'rgba(46, 204, 113, 1)'
                }]
            },
            options: {
                responsive: true,
                plugins: {
                    title: {
                        display: true,
                        text: 'Business Attributes Distribution'
                    }
                }
            }
        });
        
    } catch (error) {
        console.error('Error loading MongoDB analytics:', error);
    }
}

// Initialize MongoDB event listeners
function initializeMongoDBEventListeners() {
    // Business search
    document.getElementById('loadMongoDBBusinesses').addEventListener('click', loadMongoDBBusinesses);
    
    // Tab change events
    document.getElementById('mongodb-overview-tab').addEventListener('shown.bs.tab', function (e) {
        loadMongoDBOverviewStats();
    });
    
    document.getElementById('mongodb-analytics-tab').addEventListener('shown.bs.tab', function (e) {
        loadMongoDBAnalytics();
    });
    
    // Main tab selection
    document.getElementById('mongodb-tab').addEventListener('shown.bs.tab', function (e) {
        // Check if the overview tab is active
        if (document.getElementById('mongodb-overview-tab').classList.contains('active')) {
            loadMongoDBOverviewStats();
        }
    });
}

// Initialize when the DOM is loaded
document.addEventListener('DOMContentLoaded', function() {
    initializeMongoDBEventListeners();
});