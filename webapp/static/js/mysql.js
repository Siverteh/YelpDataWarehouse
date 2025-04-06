/**
 * MySQL-specific functionality for Yelp Data Warehouse Dashboard
 */

// Global variables for MySQL charts
let mysqlCategoriesChart = null;
let mysqlStarsChart = null;
let mysqlBusinessDetailsChart = null;
let mysqlReviewTrendsChart = null;
let mysqlCategoryRatingsChart = null;

// Load MySQL Overview Stats
async function loadMySQLOverviewStats() {
    try {
        const response = await fetch('/api/mysql/overview_stats');
        const data = await response.json();
        
        // Update the stats counts
        document.getElementById('mysql-business-count').textContent = formatNumber(data.business_count);
        document.getElementById('mysql-review-count').textContent = formatNumber(data.review_count);
        document.getElementById('mysql-user-count').textContent = formatNumber(data.user_count);
        
        // Create or update category chart
        const categoryLabels = data.category_stats.map(item => item.category_name);
        const categoryCounts = data.category_stats.map(item => item.count);
        
        const categoryCtx = document.getElementById('mysqlCategoriesChart').getContext('2d');
        if (mysqlCategoriesChart) {
            mysqlCategoriesChart.destroy();
        }
        
        mysqlCategoriesChart = new Chart(categoryCtx, {
            type: 'bar',
            data: {
                labels: categoryLabels,
                datasets: [{
                    label: 'Businesses per Category',
                    data: categoryCounts,
                    backgroundColor: 'rgba(211, 35, 35, 0.7)'
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
        
        const starsCtx = document.getElementById('mysqlStarsChart').getContext('2d');
        if (mysqlStarsChart) {
            mysqlStarsChart.destroy();
        }
        
        mysqlStarsChart = new Chart(starsCtx, {
            type: 'pie',
            data: {
                labels: starLabels,
                datasets: [{
                    data: starCounts,
                    backgroundColor: [
                        'rgba(52, 152, 219, 0.8)',  // 1 star
                        'rgba(46, 204, 113, 0.8)',  // 2 stars
                        'rgba(241, 196, 15, 0.8)',  // 3 stars
                        'rgba(230, 126, 34, 0.8)',  // 4 stars
                        'rgba(211, 35, 35, 0.8)'    // 5 stars
                    ]
                }]
            },
            options: {
                responsive: true
            }
        });
        
    } catch (error) {
        console.error('Error loading MySQL overview stats:', error);
    }
}

// Load MySQL Top Businesses
async function loadMySQLBusinesses() {
    const category = document.getElementById('mysqlCategorySelect').value;
    const limit = document.getElementById('mysqlLimitSelect').value;
    
    // Show loader
    document.getElementById('mysqlBusinessesLoader').classList.remove('d-none');
    document.getElementById('mysqlBusinessesTable').innerHTML = '';
    
    try {
        const response = await fetch(`/api/mysql/top_businesses?category=${encodeURIComponent(category)}&limit=${limit}`);
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
                <tr data-business-id="${business.business_id}" onclick="showMySQLBusinessDetails('${business.business_id}', '${business.business_name}')">
                    <td>${business.business_name}</td>
                    <td>${business.city}</td>
                    <td>${business.state}</td>
                    <td>${business.stars}</td>
                    <td>${business.review_count}</td>
                </tr>
            `;
        });
        
        tableHtml += `
                </tbody>
            </table>
        `;
        
        document.getElementById('mysqlBusinessesTable').innerHTML = tableHtml;
    } catch (error) {
        console.error('Error loading businesses:', error);
        document.getElementById('mysqlBusinessesTable').innerHTML = '<div class="alert alert-danger">Error loading businesses. Please try again.</div>';
    } finally {
        // Hide loader
        document.getElementById('mysqlBusinessesLoader').classList.add('d-none');
    }
}

// Show MySQL Business Details
async function showMySQLBusinessDetails(businessId, businessName) {
    document.getElementById('mysqlBusinessDetails').classList.remove('d-none');
    document.getElementById('mysqlBusinessDetailsName').textContent = businessName;
    
    try {
        const response = await fetch(`/api/mysql/business_performance?business_id=${encodeURIComponent(businessId)}`);
        const data = await response.json();
        
        // Update business details
        const business = data.business;
        document.getElementById('mysqlBusinessDetailsLocation').textContent = `${business.city}, ${business.state}`;
        document.getElementById('mysqlBusinessDetailsRating').textContent = `${business.stars} stars (${business.avg_rating ? business.avg_rating.toFixed(1) : 'N/A'} avg)`;
        document.getElementById('mysqlBusinessDetailsReviews').textContent = business.review_count || business.total_reviews || 'N/A';
        document.getElementById('mysqlBusinessDetailsCheckins').textContent = business.total_checkins || 'N/A';
        
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
        const ctx = document.getElementById('mysqlBusinessDetailsChart').getContext('2d');
        if (mysqlBusinessDetailsChart) {
            mysqlBusinessDetailsChart.destroy();
        }
        
        mysqlBusinessDetailsChart = new Chart(ctx, {
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

// Load MySQL analytics charts
async function loadMySQLAnalytics() {
    try {
        const reviewTrendsResponse = await fetch('/api/mysql/review_trends');
        const reviewTrendsData = await reviewTrendsResponse.json();
        
        // Create review trends chart
        const reviewTrendsCtx = document.getElementById('mysqlReviewTrendsChart').getContext('2d');
        if (mysqlReviewTrendsChart) {
            mysqlReviewTrendsChart.destroy();
        }
        
        mysqlReviewTrendsChart = new Chart(reviewTrendsCtx, {
            type: 'line',
            data: {
                labels: reviewTrendsData.dates,
                datasets: [
                    {
                        label: 'Reviews',
                        data: reviewTrendsData.review_counts,
                        borderColor: '#d32323',
                        backgroundColor: 'rgba(211, 35, 35, 0.1)',
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
                        text: 'Review Trends Over Time'
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true
                    }
                }
            }
        });
        
        // Load category ratings data
        const categoryRatingsResponse = await fetch('/api/mysql/category_ratings');
        const categoryRatingsData = await categoryRatingsResponse.json();
        
        // Create category ratings chart
        const categoryRatingsCtx = document.getElementById('mysqlCategoryRatingsChart').getContext('2d');
        if (mysqlCategoryRatingsChart) {
            mysqlCategoryRatingsChart.destroy();
        }
        
        mysqlCategoryRatingsChart = new Chart(categoryRatingsCtx, {
            type: 'bar',
            data: {
                labels: categoryRatingsData.categories,
                datasets: [
                    {
                        label: 'Average Rating',
                        data: categoryRatingsData.avg_ratings,
                        backgroundColor: 'rgba(46, 204, 113, 0.7)'
                    }
                ]
            },
            options: {
                responsive: true,
                scales: {
                    y: {
                        beginAtZero: true,
                        max: 5
                    }
                }
            }
        });
        
    } catch (error) {
        console.error('Error loading MySQL analytics:', error);
    }
}

// Initialize MySQL event listeners
function initializeMySQLEventListeners() {
    // Business search
    document.getElementById('loadMySQLBusinesses').addEventListener('click', loadMySQLBusinesses);
    
    // Tab change events
    document.getElementById('mysql-overview-tab').addEventListener('shown.bs.tab', function (e) {
        loadMySQLOverviewStats();
    });
    
    document.getElementById('mysql-analytics-tab').addEventListener('shown.bs.tab', function (e) {
        loadMySQLAnalytics();
    });
    
    // Main tab selection
    document.getElementById('mysql-tab').addEventListener('shown.bs.tab', function (e) {
        // Check if the overview tab is active
        if (document.getElementById('mysql-overview-tab').classList.contains('active')) {
            loadMySQLOverviewStats();
        }
    });
}

// Initialize when the DOM is loaded
document.addEventListener('DOMContentLoaded', function() {
    initializeMySQLEventListeners();
    
    // Load initial data for MySQL tab
    loadMySQLBusinesses();
    loadMySQLOverviewStats();
});