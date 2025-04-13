/**
 * Enhanced MySQL-specific functionality for Yelp Data Warehouse Dashboard
 */

// Global variables for MySQL charts
let mysqlCategoriesChart = null;
let mysqlStarsChart = null;
let mysqlBusinessDetailsChart = null;
let mysqlReviewTrendsChart = null;
let mysqlCategoryRatingsChart = null;
let mysqlMonthlyDistributionChart = null;
let mysqlYearComparisonChart = null;
let mysqlCategoryVolumeChart = null;
let mysqlCategoryTrendChart = null;
let mysqlStateCountChart = null;
let mysqlStateRatingChart = null;
let checkinsByDayChart = null;
let checkinsByMonthChart = null;

// Format number with commas
function formatNumber(number) {
    return number.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

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

// Search businesses
async function searchMySQLBusinesses(page = 1) {
    // Get search parameters
    const query = document.getElementById('mysqlSearchQuery').value;
    const location = document.getElementById('mysqlLocationFilter').value;
    const category = document.getElementById('mysqlCategoryFilter').value;
    const minRating = document.getElementById('mysqlRatingFilter').value;
    const minReviews = document.getElementById('mysqlReviewCountFilter').value;
    const sortBy = document.getElementById('mysqlSortBy').value;
    const limit = 10; // Fixed limit per page
    
    // Show loader
    document.getElementById('mysqlSearchLoader').classList.remove('d-none');
    document.getElementById('mysqlSearchResults').innerHTML = '';
    document.getElementById('mysqlSearchPagination').classList.add('d-none');
    
    try {
        // Build query parameters
        const params = new URLSearchParams();
        if (query) params.append('query', query);
        if (location) params.append('location', location);
        if (category) params.append('category', category);
        if (minRating) params.append('min_rating', minRating);
        if (minReviews) params.append('min_reviews', minReviews);
        if (sortBy) params.append('sort_by', sortBy);
        params.append('page', page);
        params.append('limit', limit);
        
        const response = await fetch(`/api/mysql/search_businesses?${params.toString()}`);
        const data = await response.json();
        
        // Extract businesses array from response, handling both direct array and nested object formats
        const businesses = Array.isArray(data) ? data : data.businesses || [];
        const pagination = data.pagination || { total: businesses.length, page: 1, limit: limit, pages: 1 };
        
        // Display total count at top of results
        const totalResultsDiv = document.createElement('div');
        totalResultsDiv.className = 'mb-3';
        totalResultsDiv.innerHTML = `<strong>Found ${formatNumber(pagination.total)} businesses matching your criteria.</strong>`;
        document.getElementById('mysqlSearchResults').appendChild(totalResultsDiv);
        
        // Create table
        if (businesses.length === 0) {
            document.getElementById('mysqlSearchResults').innerHTML = '<div class="alert alert-info">No businesses found matching your criteria.</div>';
        } else {
            let tableHtml = `
                <div class="table-responsive">
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
                    <tr data-business-id="${business.business_id}" onclick="showMySQLBusinessDetails('${business.business_id}', '${business.business_name || business.name}')">
                        <td>${business.business_name || business.name}</td>
                        <td>${business.city || 'N/A'}</td>
                        <td>${business.state || 'N/A'}</td>
                        <td>${formatStarRating(business.stars)}</td>
                        <td>${business.review_count || 'N/A'}</td>
                    </tr>
                `;
            });
            
            tableHtml += `
                    </tbody>
                </table>
            </div>
            `;
            
            document.getElementById('mysqlSearchResults').innerHTML = tableHtml;
            
            // Generate pagination
            if (pagination.pages > 1) {
                let paginationHtml = '';
                
                // Previous button
                paginationHtml += `
                    <li class="page-item ${pagination.page <= 1 ? 'disabled' : ''}">
                        <a class="page-link" href="#" onclick="event.preventDefault(); ${pagination.page > 1 ? 'searchMySQLBusinesses(' + (pagination.page - 1) + ')' : ''}">Previous</a>
                    </li>
                `;
                
                // Page numbers
                const startPage = Math.max(1, pagination.page - 2);
                const endPage = Math.min(pagination.pages, pagination.page + 2);
                
                for (let i = startPage; i <= endPage; i++) {
                    paginationHtml += `
                        <li class="page-item ${i === pagination.page ? 'active' : ''}">
                            <a class="page-link" href="#" onclick="event.preventDefault(); searchMySQLBusinesses(${i})">${i}</a>
                        </li>
                    `;
                }
                
                // Next button
                paginationHtml += `
                    <li class="page-item ${pagination.page >= pagination.pages ? 'disabled' : ''}">
                        <a class="page-link" href="#" onclick="event.preventDefault(); ${pagination.page < pagination.pages ? 'searchMySQLBusinesses(' + (pagination.page + 1) + ')' : ''}">Next</a>
                    </li>
                `;
                
                document.getElementById('mysqlSearchPagination').innerHTML = `
                    <nav>
                        <ul class="pagination justify-content-center">
                            ${paginationHtml}
                        </ul>
                    </nav>
                `;
                document.getElementById('mysqlSearchPagination').classList.remove('d-none');
            }
        }
    } catch (error) {
        console.error('Error searching businesses:', error);
        document.getElementById('mysqlSearchResults').innerHTML = '<div class="alert alert-danger">Error searching businesses. Please try again.</div>';
    } finally {
        // Hide loader
        document.getElementById('mysqlSearchLoader').classList.add('d-none');
    }
}

// Format star rating display for tables
function formatStarRating(stars) {
    if (stars === undefined || stars === null) return 'N/A';
    
    // Convert to number if it's a string
    stars = parseFloat(stars);
    
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
    
    // Add empty stars
    const emptyStars = 5 - fullStars - (halfStar ? 1 : 0);
    for (let i = 0; i < emptyStars; i++) {
        html += '<i class="bi bi-star text-warning"></i>';
    }
    
    return html;
}

// Clear search form
function clearMySQLSearch() {
    document.getElementById('mysqlSearchQuery').value = '';
    document.getElementById('mysqlLocationFilter').value = '';
    document.getElementById('mysqlCategoryFilter').value = '';
    document.getElementById('mysqlRatingFilter').value = '';
    document.getElementById('mysqlReviewCountFilter').value = '';
    document.getElementById('mysqlSortBy').value = 'stars';
    
    // Clear results
    document.getElementById('mysqlSearchResults').innerHTML = '';
    document.getElementById('mysqlSearchPagination').classList.add('d-none');
}

// Load business reviews
async function loadBusinessReviews(businessId, page = 1, sort = 'date_desc') {
    const limit = 5; // Reviews per page
    
    document.getElementById('reviewsLoader').classList.remove('d-none');
    document.getElementById('reviewsList').innerHTML = '';
    
    try {
        const response = await fetch(`/api/mysql/business_reviews?business_id=${encodeURIComponent(businessId)}&page=${page}&limit=${limit}&sort=${sort}`);
        const data = await response.json();
        
        const reviews = data.reviews || [];
        const pagination = data.pagination || { total: reviews.length, page: 1, limit: limit, pages: 1 };
        
        if (reviews.length === 0) {
            document.getElementById('reviewsList').innerHTML = '<div class="alert alert-info">No reviews found for this business.</div>';
        } else {
            let reviewsHtml = '';
            
            reviews.forEach(review => {
                // Format date
                const reviewDate = new Date(review.review_date);
                const formattedDate = reviewDate.toLocaleDateString('en-US', { year: 'numeric', month: 'long', day: 'numeric' });
                
                // Generate stars display
                const starsHtml = generateStarsHtml(review.stars);
                
                reviewsHtml += `
                    <div class="card mb-3">
                        <div class="card-body">
                            <div class="d-flex justify-content-between align-items-center mb-2">
                                <div>
                                    <h6 class="mb-0">${review.user_name || 'Anonymous'}</h6>
                                    <div class="text-muted small">${formattedDate}</div>
                                </div>
                                <div>
                                    ${starsHtml}
                                </div>
                            </div>
                            <p class="mb-1">${review.text || 'No review text available.'}</p>
                            <div class="d-flex mt-2 text-muted small">
                                <div class="me-3">
                                    <i class="bi bi-hand-thumbs-up"></i> ${review.useful_votes || 0} Useful
                                </div>
                                <div class="me-3">
                                    <i class="bi bi-emoji-smile"></i> ${review.funny_votes || 0} Funny
                                </div>
                                <div>
                                    <i class="bi bi-star"></i> ${review.cool_votes || 0} Cool
                                </div>
                            </div>
                        </div>
                    </div>
                `;
            });
            
            document.getElementById('reviewsList').innerHTML = reviewsHtml;
            
            // Generate pagination
            if (pagination.pages > 1) {
                let paginationHtml = '';
                
                // Previous button
                paginationHtml += `
                    <li class="page-item ${pagination.page <= 1 ? 'disabled' : ''}">
                        <a class="page-link" href="#" onclick="event.preventDefault(); ${pagination.page > 1 ? 'loadBusinessReviews(\'' + businessId + '\', ' + (pagination.page - 1) + ', \'' + sort + '\')' : ''}">Previous</a>
                    </li>
                `;
                
                // Page numbers
                const startPage = Math.max(1, pagination.page - 2);
                const endPage = Math.min(pagination.pages, pagination.page + 2);
                
                for (let i = startPage; i <= endPage; i++) {
                    paginationHtml += `
                        <li class="page-item ${i === pagination.page ? 'active' : ''}">
                            <a class="page-link" href="#" onclick="event.preventDefault(); loadBusinessReviews('${businessId}', ${i}, '${sort}')">${i}</a>
                        </li>
                    `;
                }
                
                // Next button
                paginationHtml += `
                    <li class="page-item ${pagination.page >= pagination.pages ? 'disabled' : ''}">
                        <a class="page-link" href="#" onclick="event.preventDefault(); ${pagination.page < pagination.pages ? 'loadBusinessReviews(\'' + businessId + '\', ' + (pagination.page + 1) + ', \'' + sort + '\')' : ''}">Next</a>
                    </li>
                `;
                
                document.getElementById('reviewsPagination').innerHTML = `
                    <nav>
                        <ul class="pagination justify-content-center">
                            ${paginationHtml}
                        </ul>
                    </nav>
                `;
            } else {
                document.getElementById('reviewsPagination').innerHTML = '';
            }
        }
    } catch (error) {
        console.error('Error loading reviews:', error);
        document.getElementById('reviewsList').innerHTML = '<div class="alert alert-danger">Error loading reviews. Please try again.</div>';
    } finally {
        document.getElementById('reviewsLoader').classList.add('d-none');
    }
}

// Helper function to generate stars HTML
function generateStarsHtml(stars) {
    let html = '';
    const fullStars = Math.floor(stars);
    const halfStar = stars % 1 >= 0.5;
    
    // Add full stars
    for (let i = 0; i < fullStars; i++) {
        html += '<i class="bi bi-star-fill text-warning"></i>';
    }
    
    // Add half star if needed
    if (halfStar) {
        html += '<i class="bi bi-star-half text-warning"></i>';
    }
    
    // Add empty stars
    const emptyStars = 5 - fullStars - (halfStar ? 1 : 0);
    for (let i = 0; i < emptyStars; i++) {
        html += '<i class="bi bi-star text-warning"></i>';
    }
    
    return html;
}

// Load checkin analysis data
async function loadCheckinAnalysis(businessId) {
    try {
        const response = await fetch(`/api/mysql/business_checkins?business_id=${encodeURIComponent(businessId)}`);
        const data = await response.json();
        
        // Checkins by day of week
        const dayLabels = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
        const dayData = data.day_distribution || Array(7).fill(0);
        
        const dayCtx = document.getElementById('checkinsByDayChart').getContext('2d');
        if (checkinsByDayChart) {
            checkinsByDayChart.destroy();
        }
        
        checkinsByDayChart = new Chart(dayCtx, {
            type: 'bar',
            data: {
                labels: dayLabels,
                datasets: [{
                    label: 'Checkins',
                    data: dayData,
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
        
        // Checkins by month
        const monthLabels = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
        const monthData = data.month_distribution || Array(12).fill(0);
        
        const monthCtx = document.getElementById('checkinsByMonthChart').getContext('2d');
        if (checkinsByMonthChart) {
            checkinsByMonthChart.destroy();
        }
        
        checkinsByMonthChart = new Chart(monthCtx, {
            type: 'line',
            data: {
                labels: monthLabels,
                datasets: [{
                    label: 'Checkins',
                    data: monthData,
                    borderColor: '#4682B4',
                    backgroundColor: 'rgba(70, 130, 180, 0.1)',
                    fill: true,
                    tension: 0.1
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
        
    } catch (error) {
        console.error('Error loading checkin analysis:', error);
    }
}

// Enhanced business details
async function showMySQLBusinessDetails(businessId, businessName) {
    const businessDetails = document.getElementById('mysqlBusinessDetails');
    businessDetails.classList.remove('d-none');
    businessDetails.setAttribute('data-business-id', businessId);
    document.getElementById('mysqlBusinessDetailsName').textContent = businessName;
    
    try {
        const response = await fetch(`/api/mysql/business_performance?business_id=${encodeURIComponent(businessId)}`);
        const data = await response.json();
        
        // Update business details
        const business = data.business;
        document.getElementById('mysqlBusinessDetailsLocation').textContent = `${business.city}, ${business.state}`;
        document.getElementById('mysqlBusinessDetailsRating').innerHTML = `${formatStarRating(business.stars)} (${business.avg_rating ? business.avg_rating.toFixed(1) : 'N/A'} avg)`;
        document.getElementById('mysqlBusinessDetailsReviews').textContent = business.review_count || business.total_reviews || 'N/A';
        
        // Improved checkins count calculation
        // Calculate total checkins from the checkins_by_month data if available
        let totalCheckins = 0;
        
        if (data.checkins_by_month && data.checkins_by_month.length > 0) {
            // Sum up all checkin counts from the monthly data
            totalCheckins = data.checkins_by_month.reduce((sum, month) => sum + (month.checkin_count || 0), 0);
        } else {
            // Fall back to the business summary data
            totalCheckins = business.total_checkins || business.checkin_count || 0;
        }
        
        document.getElementById('mysqlBusinessDetailsCheckins').textContent = 
            totalCheckins > 0 ? totalCheckins : 'No checkins recorded';
        
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
            // Format month names
            const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
            const monthName = monthNames[(item.month - 1) % 12];
            labels.push(`${monthName} ${item.year}`);
            
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
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        position: 'top',
                        labels: {
                            boxWidth: 12,
                            font: {
                                size: 11
                            }
                        }
                    },
                    tooltip: {
                        mode: 'index',
                        intersect: false
                    }
                },
                scales: {
                    x: {
                        ticks: {
                            autoSkip: true,
                            maxTicksLimit: 15,
                            maxRotation: 45,
                            minRotation: 0
                        }
                    },
                    y: {
                        beginAtZero: true,
                        ticks: {
                            precision: 0
                        }
                    }
                }
            }
        });
        
        // Load reviews (will be shown when reviews tab is clicked)
        loadBusinessReviews(businessId);
        
        // Load checkin analysis
        loadCheckinAnalysis(businessId);
        
    } catch (error) {
        console.error('Error loading business details:', error);
    }
}

// Enhanced MySQL analytics with multiple chart types
async function loadMySQLAnalytics() {
    try {
        // Load review trends data
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
                        display: false
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true
                    }
                }
            }
        });
        
        // Monthly distribution chart (use dummy data for now)
        const monthlyDistributionCtx = document.getElementById('mysqlMonthlyDistributionChart').getContext('2d');
        if (mysqlMonthlyDistributionChart) {
            mysqlMonthlyDistributionChart.destroy();
        }
        
        const monthLabels = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
        const monthlyData = [65, 72, 85, 93, 105, 110, 95, 92, 88, 79, 82, 90]; // Dummy data
        
        mysqlMonthlyDistributionChart = new Chart(monthlyDistributionCtx, {
            type: 'bar',
            data: {
                labels: monthLabels,
                datasets: [{
                    label: 'Average Monthly Reviews',
                    data: monthlyData,
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
        
        // Year comparison chart (dummy data for now)
        const yearComparisonCtx = document.getElementById('mysqlYearComparisonChart').getContext('2d');
        if (mysqlYearComparisonChart) {
            mysqlYearComparisonChart.destroy();
        }
        
        mysqlYearComparisonChart = new Chart(yearComparisonCtx, {
            type: 'line',
            data: {
                labels: monthLabels,
                datasets: [
                    {
                        label: '2022',
                        data: [65, 72, 85, 93, 105, 110, 95, 92, 88, 79, 82, 90], // Dummy data
                        borderColor: '#d32323',
                        backgroundColor: 'rgba(211, 35, 35, 0.1)',
                        fill: false
                    },
                    {
                        label: '2023',
                        data: [70, 78, 92, 98, 110, 115, 100, 96, 94, 85, 88, 95], // Dummy data
                        borderColor: '#3498db',
                        backgroundColor: 'rgba(52, 152, 219, 0.1)',
                        fill: false
                    }
                ]
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
        
        // Category volume chart (dummy data for now)
        const categoryVolumeCtx = document.getElementById('mysqlCategoryVolumeChart').getContext('2d');
        if (mysqlCategoryVolumeChart) {
            mysqlCategoryVolumeChart.destroy();
        }
        
        mysqlCategoryVolumeChart = new Chart(categoryVolumeCtx, {
            type: 'bar',
            data: {
                labels: categoryRatingsData.categories,
                datasets: [
                    {
                        label: 'Review Count',
                        data: [1200, 950, 800, 650, 500, 450, 400, 350, 300, 250], // Dummy data
                        backgroundColor: 'rgba(52, 152, 219, 0.7)'
                    }
                ]
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
        
        // State data charts (dummy data)
        const stateCountCtx = document.getElementById('mysqlStateCountChart').getContext('2d');
        if (mysqlStateCountChart) {
            mysqlStateCountChart.destroy();
        }
        
        const stateLabels = ['PA', 'NV', 'AZ', 'CA', 'OH', 'NC', 'FL', 'TX', 'IL', 'NY'];
        const stateCounts = [1250, 950, 875, 780, 650, 540, 490, 425, 380, 350]; // Dummy data
        
        mysqlStateCountChart = new Chart(stateCountCtx, {
            type: 'bar',
            data: {
                labels: stateLabels,
                datasets: [{
                    label: 'Number of Businesses',
                    data: stateCounts,
                    backgroundColor: 'rgba(155, 89, 182, 0.7)'
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
        
        const stateRatingCtx = document.getElementById('mysqlStateRatingChart').getContext('2d');
        if (mysqlStateRatingChart) {
            mysqlStateRatingChart.destroy();
        }
        
        const stateRatings = [3.8, 4.2, 3.9, 4.1, 3.7, 4.0, 3.9, 4.0, 3.8, 3.6]; // Dummy data
        
        mysqlStateRatingChart = new Chart(stateRatingCtx, {
            type: 'bar',
            data: {
                labels: stateLabels,
                datasets: [{
                    label: 'Average Rating',
                    data: stateRatings,
                    backgroundColor: 'rgba(230, 126, 34, 0.7)'
                }]
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
        
        // Load dummy top cities data
        loadTopCitiesTable();
        
    } catch (error) {
        console.error('Error loading MySQL analytics:', error);
    }
}

// Load top cities table with dummy data
function loadTopCitiesTable() {
    const tableBody = document.getElementById('mysqlTopCitiesTable').querySelector('tbody');
    
    // Clear existing rows
    tableBody.innerHTML = '';
    
    // Dummy data
    const dummyCities = [
        { city: 'Philadelphia', state: 'PA', count: 450, rating: 4.1, reviews: 15200 },
        { city: 'Pittsburgh', state: 'PA', count: 380, rating: 3.9, reviews: 12800 },
        { city: 'Las Vegas', state: 'NV', count: 920, rating: 4.3, reviews: 48000 },
        { city: 'Phoenix', state: 'AZ', count: 680, rating: 4.0, reviews: 25600 },
        { city: 'Scottsdale', state: 'AZ', count: 420, rating: 4.2, reviews: 16800 },
        { city: 'Los Angeles', state: 'CA', count: 580, rating: 4.1, reviews: 35000 },
        { city: 'San Francisco', state: 'CA', count: 520, rating: 4.2, reviews: 32000 },
        { city: 'Cleveland', state: 'OH', count: 340, rating: 3.8, reviews: 11500 },
        { city: 'Columbus', state: 'OH', count: 320, rating: 3.9, reviews: 10800 },
        { city: 'Cincinnati', state: 'OH', count: 290, rating: 3.7, reviews: 9800 }
    ];
    
    // Get selected state filter
    const stateFilter = document.getElementById('stateFilterSelect').value;
    
    // Filter cities by state if a state is selected
    const filteredCities = stateFilter ? 
        dummyCities.filter(city => city.state === stateFilter) : 
        dummyCities;
    
    // Add rows to table
    filteredCities.forEach(city => {
        const row = document.createElement('tr');
        row.innerHTML = `
            <td>${city.city}</td>
            <td>${city.state}</td>
            <td>${city.count}</td>
            <td>${city.rating.toFixed(1)}</td>
            <td>${city.reviews.toLocaleString()}</td>
        `;
        tableBody.appendChild(row);
    });
}

// Handle SQL query selection
function handleSqlQuerySelection() {
    const querySelect = document.getElementById('sqlQuerySelect');
    const queryDisplay = document.getElementById('sqlQueryDisplay');
    
    const queries = {
        'top_businesses_by_category': `-- Find top businesses by category with rating and review count
SELECT 
    c.category_name,
    b.business_name,
    b.stars,
    b.review_count,
    l.city,
    l.state
FROM 
    dim_business b
    JOIN dim_location l ON b.location_id = l.location_id
    JOIN business_category bc ON b.business_id = bc.business_id
    JOIN dim_category c ON bc.category_id = c.category_id
WHERE 
    c.category_name = 'Restaurants'
ORDER BY 
    b.stars DESC, b.review_count DESC
LIMIT 10;`,
        
        'rating_distribution_by_city': `-- Get rating distribution by city
SELECT 
    l.city,
    l.state,
    COUNT(DISTINCT b.business_id) as business_count,
    AVG(b.stars) as avg_rating,
    SUM(b.review_count) as total_reviews,
    COUNT(CASE WHEN b.stars >= 4.5 THEN 1 END) as five_star_count,
    COUNT(CASE WHEN b.stars >= 3.5 AND b.stars < 4.5 THEN 1 END) as four_star_count,
    COUNT(CASE WHEN b.stars >= 2.5 AND b.stars < 3.5 THEN 1 END) as three_star_count,
    COUNT(CASE WHEN b.stars >= 1.5 AND b.stars < 2.5 THEN 1 END) as two_star_count,
    COUNT(CASE WHEN b.stars < 1.5 THEN 1 END) as one_star_count
FROM 
    dim_business b
    JOIN dim_location l ON b.location_id = l.location_id
GROUP BY 
    l.city, l.state
HAVING 
    COUNT(DISTINCT b.business_id) > 10
ORDER BY 
    business_count DESC
LIMIT 10;`,
        
        'review_volume_trend': `-- Review volume trend over time
SELECT 
    t.year_actual,
    t.month_actual,
    t.month_name,
    COUNT(*) as review_count,
    AVG(r.stars) as avg_stars
FROM 
    fact_review r
    JOIN dim_time t ON r.time_id = t.time_id
WHERE 
    t.year_actual >= 2020
GROUP BY 
    t.year_actual, t.month_actual, t.month_name
ORDER BY 
    t.year_actual, t.month_actual;`,
        
        'business_category_popularity': `-- Business category popularity over time
SELECT 
    c.category_name,
    t.year_actual,
    COUNT(DISTINCT b.business_id) as business_count,
    COUNT(DISTINCT r.review_id) as review_count,
    AVG(r.stars) as avg_rating
FROM 
    fact_review r
    JOIN dim_business b ON r.business_id = b.business_id
    JOIN business_category bc ON b.business_id = bc.business_id
    JOIN dim_category c ON bc.category_id = c.category_id
    JOIN dim_time t ON r.time_id = t.time_id
WHERE 
    t.year_actual >= 2018
GROUP BY 
    c.category_name, t.year_actual
HAVING 
    review_count >= 100
ORDER BY 
    t.year_actual, review_count DESC;`,
        
        'user_review_patterns': `-- User review patterns and preferences
SELECT 
    u.user_id,
    u.name as user_name,
    COUNT(r.review_id) as review_count,
    AVG(r.stars) as avg_rating,
    MIN(t.date_actual) as first_review_date,
    MAX(t.date_actual) as last_review_date,
    COUNT(DISTINCT b.business_id) as unique_businesses,
    COUNT(DISTINCT c.category_id) as unique_categories,
    GROUP_CONCAT(DISTINCT c.category_name ORDER BY COUNT(r.review_id) DESC SEPARATOR ', ') as top_categories
FROM 
    fact_review r
    JOIN dim_user u ON r.user_id = u.user_id
    JOIN dim_business b ON r.business_id = b.business_id
    JOIN business_category bc ON b.business_id = bc.business_id
    JOIN dim_category c ON bc.category_id = c.category_id
    JOIN dim_time t ON r.time_id = t.time_id
GROUP BY 
    u.user_id, u.name
HAVING 
    review_count >= 10
ORDER BY 
    review_count DESC
LIMIT 10;`
    };
    
    const selectedQuery = querySelect.value;
    if (selectedQuery && queries[selectedQuery]) {
        queryDisplay.textContent = queries[selectedQuery];
    } else {
        queryDisplay.textContent = '-- Select a query from the dropdown above';
    }
}

// Run SQL query and display results
function runSqlQuery() {
    const querySelect = document.getElementById('sqlQuerySelect');
    const selectedQuery = querySelect.value;
    
    if (!selectedQuery) {
        alert('Please select a query to run');
        return;
    }
    
    // Show loader
    const resultsLoader = document.getElementById('sqlResultsLoader');
    const resultsContainer = document.getElementById('sqlResultsContainer');
    resultsLoader.classList.remove('d-none');
    resultsContainer.innerHTML = '';
    
    // Start timer
    const startTime = performance.now();
    
    // Fetch results (using fixed URLs for demonstration)
    const endpointMap = {
        'top_businesses_by_category': '/api/mysql/top_businesses?category=Restaurants&limit=10',
        'rating_distribution_by_city': '/api/mysql/city_ratings',
        'review_volume_trend': '/api/mysql/review_trends',
        'business_category_popularity': '/api/mysql/category_trends',
        'user_review_patterns': '/api/mysql/top_users'
    };
    
    const endpoint = endpointMap[selectedQuery] || '/api/mysql/top_businesses?category=Restaurants&limit=10';
    
    fetch(endpoint)
        .then(response => response.json())
        .then(data => {
            // End timer
            const endTime = performance.now();
            const queryTime = ((endTime - startTime) / 1000).toFixed(2);
            document.getElementById('queryTimeDisplay').textContent = `Query executed in ${queryTime} seconds`;
            
            // Process data for display
            let resultHtml = '';
            
            // Handle different response formats
            if (Array.isArray(data)) {
                resultHtml = createTableFromArray(data);
            } else if (data.businesses && Array.isArray(data.businesses)) {
                resultHtml = createTableFromArray(data.businesses);
            } else {
                // Try to extract data from properties
                const possibleArrayKeys = Object.keys(data).filter(key => Array.isArray(data[key]));
                if (possibleArrayKeys.length > 0) {
                    resultHtml = createTableFromArray(data[possibleArrayKeys[0]]);
                } else {
                    // No array found, display as JSON
                    resultHtml = `<pre>${JSON.stringify(data, null, 2)}</pre>`;
                }
            }
            
            resultsContainer.innerHTML = resultHtml;
        })
        .catch(error => {
            console.error('Error running query:', error);
            resultsContainer.innerHTML = `<div class="alert alert-danger">Error executing query: ${error.message}</div>`;
        })
        .finally(() => {
            resultsLoader.classList.add('d-none');
        });
}

// Helper to create HTML table from array of objects
function createTableFromArray(data) {
    if (!data || data.length === 0) {
        return '<div class="alert alert-info">No results returned</div>';
    }
    
    // Get column headers from first object
    const columns = Object.keys(data[0]);
    
    let tableHtml = `
        <div class="table-responsive">
            <table class="table table-striped table-hover">
                <thead>
                    <tr>
                        ${columns.map(col => `<th>${formatColumnName(col)}</th>`).join('')}
                    </tr>
                </thead>
                <tbody>
    `;
    
    // Add data rows
    data.forEach(row => {
        tableHtml += '<tr>';
        columns.forEach(col => {
            tableHtml += `<td>${formatCellValue(row[col])}</td>`;
        });
        tableHtml += '</tr>';
    });
    
    tableHtml += `
                </tbody>
            </table>
        </div>
        <div class="mt-2 text-muted small">Showing ${data.length} results</div>
    `;
    
    return tableHtml;
}

// Format column name for display
function formatColumnName(columnName) {
    return columnName
        .replace(/_/g, ' ')
        .split(' ')
        .map(word => word.charAt(0).toUpperCase() + word.slice(1))
        .join(' ');
}

// Format cell value for display
function formatCellValue(value) {
    if (value === null || value === undefined) {
        return 'NULL';
    }
    
    if (typeof value === 'number') {
        // Format decimal numbers
        return Number.isInteger(value) ? value : value.toFixed(2);
    }
    
    if (typeof value === 'boolean') {
        return value ? 'Yes' : 'No';
    }
    
    return value;
}

// Initialize MySQL event listeners
function initializeMySQLEventListeners() {
    // Business search listeners
    document.getElementById('mysqlSearchButton')?.addEventListener('click', () => searchMySQLBusinesses(1));
    document.getElementById('mysqlClearButton')?.addEventListener('click', clearMySQLSearch);
    
    // Tab change events
    document.getElementById('mysql-overview-tab')?.addEventListener('shown.bs.tab', function (e) {
        loadMySQLOverviewStats();
    });
    
    document.getElementById('mysql-analytics-tab')?.addEventListener('shown.bs.tab', function (e) {
        loadMySQLAnalytics();
    });
    
    // Review sort change
    document.getElementById('reviewSortSelect')?.addEventListener('change', function() {
        const businessDetails = document.getElementById('mysqlBusinessDetails');
        const businessId = businessDetails.getAttribute('data-business-id');
        if (businessId) {
            loadBusinessReviews(businessId, 1, this.value);
        }
    });
    
    // Business details tabs
    document.getElementById('reviews-tab')?.addEventListener('click', function() {
        const businessDetails = document.getElementById('mysqlBusinessDetails');
        const businessId = businessDetails.getAttribute('data-business-id');
        if (businessId) {
            loadBusinessReviews(businessId);
        }
    });
    
    document.getElementById('checkins-tab')?.addEventListener('click', function() {
        const businessDetails = document.getElementById('mysqlBusinessDetails');
        const businessId = businessDetails.getAttribute('data-business-id');
        if (businessId) {
            loadCheckinAnalysis(businessId);
        }
    });
    
    // Analytics tab listeners
    document.getElementById('loadTrendsBtn')?.addEventListener('click', loadMySQLAnalytics);
    
    document.getElementById('stateFilterSelect')?.addEventListener('change', loadTopCitiesTable);
    
    document.getElementById('sqlQuerySelect')?.addEventListener('change', handleSqlQuerySelection);
    
    document.getElementById('runSqlQuery')?.addEventListener('click', runSqlQuery);
    
    // Main tab selection
    document.getElementById('mysql-tab')?.addEventListener('shown.bs.tab', function (e) {
        // Check if the overview tab is active
        if (document.getElementById('mysql-overview-tab').classList.contains('active')) {
            loadMySQLOverviewStats();
        }
    });
}

// Initialize when the DOM is loaded
document.addEventListener('DOMContentLoaded', function() {
    // Initialize all event listeners
    initializeMySQLEventListeners();
    
    // Load initial data if elements exist
    if (document.getElementById('mysqlCategoriesChart')) {
        loadMySQLOverviewStats();
    }
    
    // Initialize SQL query display
    if (document.getElementById('sqlQuerySelect')) {
        handleSqlQuerySelection();
    }
});


document.getElementById('mysql-businesses-tab').addEventListener('shown.bs.tab', function() {
    searchMySQLBusinesses(1);
});
