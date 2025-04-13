/**
 * MongoDB-specific functionality for Yelp Data Warehouse Dashboard
 */

// Global variables for MongoDB charts
let mongodbCategoriesChart = null;
let mongodbStarsChart = null;
let mongodbBusinessDetailsChart = null;
let mongodbDocumentSizeChart = null;
let mongodbAttributesChart = null;
let mongodbReviewsTable = null;
let mongodbCheckinsChart = null;
let mongodbSchemaStatsChart = null;
let mongodbArrayDistributionChart = null;

// Load MongoDB Overview Stats
async function loadMongoDBOverviewStats() {
    try {
        const response = await fetch('/api/mongodb/overview_stats');
        const data = await response.json();
        
        // Update the stats counts with proper formatting
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

// Format number with commas for better readability
function formatNumber(num) {
    if (num === undefined || num === null) return 'N/A';
    return num.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
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
    
    return html + ` (${parseFloat(stars).toFixed(1)})`;
}

// Search businesses in MongoDB
async function searchMongoDBBusinesses(page = 1) {
    // Get search parameters
    const searchQuery = document.getElementById('mongodbSearchQuery').value;
    const location = document.getElementById('mongodbLocationFilter').value;
    const category = document.getElementById('mongodbCategoryFilter').value;
    const minRating = document.getElementById('mongodbRatingFilter').value;
    const attributeKey = document.getElementById('mongodbAttributeKey').value;
    const attributeValue = document.getElementById('mongodbAttributeValue').value;
    const sortBy = document.getElementById('mongodbSortBy').value;
    const limit = 10; // Fixed limit of 10 items per page
    
    // Show loader
    document.getElementById('mongodbSearchLoader').classList.remove('d-none');
    document.getElementById('mongodbSearchResults').innerHTML = '';
    document.getElementById('mongodbSearchPagination').classList.add('d-none');
    
    try {
        const params = new URLSearchParams();
        if (searchQuery) params.append('query', searchQuery);
        if (location) params.append('location', location);
        if (category) params.append('category', category);
        if (minRating) params.append('min_rating', minRating);
        if (attributeKey && attributeValue) {
            params.append('attribute_key', attributeKey);
            params.append('attribute_value', attributeValue);
        }
        params.append('sort_by', sortBy);
        params.append('page', page);
        params.append('limit', limit);
        
        const response = await fetch(`/api/mongodb/top_businesses?${params.toString()}`);
        const data = await response.json();
        
        // Handle both array and object with pagination formats
        let businesses = [];
        let pagination = { total: 0, page: page, limit: limit, pages: 1 };
        
        if (Array.isArray(data)) {
            businesses = data;
            pagination.total = data.length;
        } else if (data.businesses) {
            businesses = data.businesses;
            pagination = data.pagination || pagination;
        } else if (data.reviews) {
            // This is an unexpected format but we'll handle it anyway
            businesses = [];
            pagination = data.pagination || pagination;
        } else {
            businesses = data;
            pagination.total = businesses.length;
        }
        
        // Calculate total pages
        pagination.pages = Math.ceil(pagination.total / limit);
        
        // Create table
        if (businesses.length === 0) {
            document.getElementById('mongodbSearchResults').innerHTML = '<div class="alert alert-info">No businesses found matching your criteria.</div>';
        } else {
            // Show total count
            const totalResultsDiv = document.createElement('div');
            totalResultsDiv.className = 'mb-3';
            totalResultsDiv.innerHTML = `<strong>Found ${formatNumber(pagination.total)} businesses matching your criteria.</strong>`;
            document.getElementById('mongodbSearchResults').appendChild(totalResultsDiv);
            
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
                // Extract city and state with improved handling of location structures
                let city = 'N/A';
                let state = 'N/A';
                
                // Handle different location structures
                if (business.location) {
                    if (typeof business.location === 'object') {
                        city = business.location.city || business.location.address?.city || 'N/A';
                        state = business.location.state || business.location.address?.state || 'N/A';
                    } else if (typeof business.location === 'string') {
                        const parts = business.location.split(',');
                        if (parts.length >= 2) {
                            city = parts[0].trim();
                            state = parts[1].trim();
                        }
                    }
                } else {
                    // Try direct properties
                    city = business.city || 'N/A';
                    state = business.state || 'N/A';
                }
                
                // Extract business name safely
                const businessName = business.name || business.business_name || 'Unnamed Business';
                
                // Escape any special characters in the business name to avoid JS errors when clicked
                const escapedBusinessName = businessName.replace(/"/g, '&quot;').replace(/'/g, '&#39;');
                
                // Extract business ID safely
                const businessId = business.business_id || business._id || '';
                
                tableHtml += `
                    <tr data-business-id="${businessId}" onclick="showMongoDBBusinessDetails('${businessId}', '${escapedBusinessName}')">
                        <td>${businessName}</td>
                        <td>${city}</td>
                        <td>${state}</td>
                        <td>${formatStarRating(business.stars)}</td>
                        <td>${formatNumber(business.review_count)}</td>
                    </tr>
                `;
            });
            
            tableHtml += `
                    </tbody>
                </table>
            </div>
            `;
            
            document.getElementById('mongodbSearchResults').innerHTML = tableHtml;
            
            // Add pagination if needed
            if (pagination.pages > 1) {
                let paginationHtml = '';
                
                // Previous button
                paginationHtml += `
                    <li class="page-item ${page <= 1 ? 'disabled' : ''}">
                        <a class="page-link" href="#" onclick="event.preventDefault(); ${page > 1 ? 'searchMongoDBBusinesses(' + (page - 1) + ')' : ''}">Previous</a>
                    </li>
                `;
                
                // Page numbers
                const startPage = Math.max(1, page - 2);
                const endPage = Math.min(pagination.pages, page + 2);
                
                for (let i = startPage; i <= endPage; i++) {
                    paginationHtml += `
                        <li class="page-item ${i === page ? 'active' : ''}">
                            <a class="page-link" href="#" onclick="event.preventDefault(); searchMongoDBBusinesses(${i})">${i}</a>
                        </li>
                    `;
                }
                
                // Next button
                paginationHtml += `
                    <li class="page-item ${page >= pagination.pages ? 'disabled' : ''}">
                        <a class="page-link" href="#" onclick="event.preventDefault(); ${page < pagination.pages ? 'searchMongoDBBusinesses(' + (page + 1) + ')' : ''}">Next</a>
                    </li>
                `;
                
                document.getElementById('mongodbSearchPagination').innerHTML = `
                    <nav>
                        <ul class="pagination justify-content-center">
                            ${paginationHtml}
                        </ul>
                    </nav>
                `;
                document.getElementById('mongodbSearchPagination').classList.remove('d-none');
            }
        }
    } catch (error) {
        console.error('Error searching businesses:', error);
        document.getElementById('mongodbSearchResults').innerHTML = '<div class="alert alert-danger">Error searching businesses. Please try again.</div>';
    } finally {
        // Hide loader
        document.getElementById('mongodbSearchLoader').classList.add('d-none');
    }
}

// Clear search form
function clearMongoDBSearch() {
    document.getElementById('mongodbSearchQuery').value = '';
    document.getElementById('mongodbLocationFilter').value = '';
    document.getElementById('mongodbCategoryFilter').value = '';
    document.getElementById('mongodbRatingFilter').value = '';
    document.getElementById('mongodbAttributeKey').value = '';
    document.getElementById('mongodbAttributeValue').value = '';
    document.getElementById('mongodbSortBy').value = 'stars';
    
    // Clear results
    document.getElementById('mongodbSearchResults').innerHTML = '';
    document.getElementById('mongodbSearchPagination').classList.add('d-none');
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
        
        // Handle nested location correctly with improved robustness
        let city = 'N/A';
        let state = 'N/A';
        
        // Check all possible location formats
        if (business.location) {
            if (typeof business.location === 'object') {
                city = business.location.city || business.location.address?.city || 'N/A';
                state = business.location.state || business.location.address?.state || 'N/A';
            } else if (typeof business.location === 'string') {
                const parts = business.location.split(',');
                if (parts.length >= 2) {
                    city = parts[0].trim();
                    state = parts[1].trim();
                }
            }
        } else {
            // Try direct properties
            city = business.city || 'N/A';
            state = business.state || 'N/A';
        }
        
        document.getElementById('mongodbBusinessDetailsLocation').textContent = `${city}, ${state}`;
        document.getElementById('mongodbBusinessDetailsRating').innerHTML = formatStarRating(business.stars);
        
        // Handle review counts from different possible structures
        let reviewCount = 0;
        if (business.review_count) {
            reviewCount = business.review_count;
        } else if (business.review_stats && business.review_stats.review_count) {
            reviewCount = business.review_stats.review_count;
        }
        document.getElementById('mongodbBusinessDetailsReviews').textContent = formatNumber(reviewCount);
        
        // Handle checkin counts from different possible structures
        let checkinCount = 0;
        
        // Try all possible checkin data structures
        if (business.checkins && business.checkins.total_checkins) {
            checkinCount = business.checkins.total_checkins;
        } else if (data.checkins_by_month && data.checkins_by_month.length > 0) {
            // Sum up all checkin counts from the monthly data
            checkinCount = data.checkins_by_month.reduce((sum, month) => sum + (month.checkin_count || 0), 0);
        }
        
        document.getElementById('mongodbBusinessDetailsCheckins').textContent = formatNumber(checkinCount);
        
        // Display business attributes if they exist
        const attributesContainer = document.getElementById('mongodbBusinessAttributes');
        if (attributesContainer && business.attributes) {
            let attributesHtml = '<div class="row">';
            
            // Convert attributes object to a list of key-value pairs
            const attributes = Object.entries(business.attributes).sort((a, b) => a[0].localeCompare(b[0]));
            
            // Display attributes in a responsive grid
            attributes.forEach(([key, value]) => {
                let displayValue = value;
                
                // Handle nested attributes or boolean values
                if (typeof value === 'object') {
                    displayValue = JSON.stringify(value);
                } else if (typeof value === 'boolean') {
                    displayValue = value ? 'Yes' : 'No';
                }
                
                attributesHtml += `
                    <div class="col-md-4 mb-2">
                        <strong>${key.replace(/_/g, ' ')}:</strong> ${displayValue}
                    </div>
                `;
            });
            
            attributesHtml += '</div>';
            attributesContainer.innerHTML = attributesHtml;
        } else if (attributesContainer) {
            attributesContainer.innerHTML = '<div class="alert alert-info">No attributes available for this business.</div>';
        }
        
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
        
        // Load reviews for the reviews tab
        loadMongoDBBusinessReviews(businessId);
        
        // Load checkin data for the checkins tab
        loadMongoDBBusinessCheckins(businessId);
        
    } catch (error) {
        console.error('Error loading business details:', error);
    }
}

// Load business reviews from MongoDB
async function loadMongoDBBusinessReviews(businessId, page = 1, sort = 'date_desc') {
    const limit = 5; // Reviews per page
    
    document.getElementById('mongodbReviewsLoader').classList.remove('d-none');
    document.getElementById('mongodbReviewsList').innerHTML = '';
    
    try {
        // Try to fetch reviews from our MongoDB API endpoint
        const response = await fetch(`/api/mongodb/business_reviews?business_id=${encodeURIComponent(businessId)}&page=${page}&limit=${limit}&sort=${sort}`);
        let data = await response.json();
        
        // Extract reviews from different possible response formats
        let reviews = [];
        let pagination = { total: 0, page: page, limit: limit, pages: 1 };
        
        if (Array.isArray(data)) {
            reviews = data;
            pagination.total = data.length;
        } else if (data.reviews && Array.isArray(data.reviews)) {
            reviews = data.reviews;
            pagination = data.pagination || pagination;
        }
        
        if (reviews.length === 0) {
            document.getElementById('mongodbReviewsList').innerHTML = '<div class="alert alert-info">No reviews found for this business.</div>';
        } else {
            let reviewsHtml = '';
            
            reviews.forEach(review => {
                // Format date
                let reviewDate = 'N/A';
                if (review.review_date) {
                    const date = new Date(review.review_date);
                    reviewDate = date.toLocaleDateString('en-US', { year: 'numeric', month: 'long', day: 'numeric' });
                } else if (review.date) {
                    const date = new Date(review.date);
                    reviewDate = date.toLocaleDateString('en-US', { year: 'numeric', month: 'long', day: 'numeric' });
                }
                
                reviewsHtml += `
                    <div class="card mb-3">
                        <div class="card-body">
                            <div class="d-flex justify-content-between align-items-center mb-2">
                                <div>
                                    <h6 class="mb-0">${review.user_name || 'Anonymous'}</h6>
                                    <div class="text-muted small">${reviewDate}</div>
                                </div>
                                <div>
                                    ${formatStarRating(review.stars)}
                                </div>
                            </div>
                            <p class="mb-1">${review.text || 'No review text available.'}</p>
                            <div class="d-flex mt-2 text-muted small">
                                <div class="me-3">
                                    <i class="bi bi-hand-thumbs-up"></i> ${review.useful_votes || review.useful || 0} Useful
                                </div>
                                <div class="me-3">
                                    <i class="bi bi-emoji-smile"></i> ${review.funny_votes || review.funny || 0} Funny
                                </div>
                                <div>
                                    <i class="bi bi-star"></i> ${review.cool_votes || review.cool || 0} Cool
                                </div>
                            </div>
                        </div>
                    </div>
                `;
            });
            
            document.getElementById('mongodbReviewsList').innerHTML = reviewsHtml;
            
            // Generate pagination
            if (pagination.pages > 1) {
                let paginationHtml = '';
                
                // Previous button
                paginationHtml += `
                    <li class="page-item ${pagination.page <= 1 ? 'disabled' : ''}">
                        <a class="page-link" href="#" onclick="event.preventDefault(); ${pagination.page > 1 ? 'loadMongoDBBusinessReviews(\'' + businessId + '\', ' + (pagination.page - 1) + ', \'' + sort + '\')' : ''}">Previous</a>
                    </li>
                `;
                
                // Page numbers
                const startPage = Math.max(1, pagination.page - 2);
                const endPage = Math.min(pagination.pages, pagination.page + 2);
                
                for (let i = startPage; i <= endPage; i++) {
                    paginationHtml += `
                        <li class="page-item ${i === pagination.page ? 'active' : ''}">
                            <a class="page-link" href="#" onclick="event.preventDefault(); loadMongoDBBusinessReviews('${businessId}', ${i}, '${sort}')">${i}</a>
                        </li>
                    `;
                }
                
                // Next button
                paginationHtml += `
                    <li class="page-item ${pagination.page >= pagination.pages ? 'disabled' : ''}">
                        <a class="page-link" href="#" onclick="event.preventDefault(); ${pagination.page < pagination.pages ? 'loadMongoDBBusinessReviews(\'' + businessId + '\', ' + (pagination.page + 1) + ', \'' + sort + '\')' : ''}">Next</a>
                    </li>
                `;
                
                document.getElementById('mongodbReviewsPagination').innerHTML = `
                    <nav>
                        <ul class="pagination justify-content-center">
                            ${paginationHtml}
                        </ul>
                    </nav>
                `;
            } else {
                document.getElementById('mongodbReviewsPagination').innerHTML = '';
            }
        }
    } catch (error) {
        console.error('Error loading reviews:', error);
        document.getElementById('mongodbReviewsList').innerHTML = '<div class="alert alert-danger">Error loading reviews. Please try again.</div>';
    } finally {
        document.getElementById('mongodbReviewsLoader').classList.add('d-none');
    }
}

// Load business checkins from MongoDB
async function loadMongoDBBusinessCheckins(businessId) {
    document.getElementById('mongodbCheckinsLoader').classList.remove('d-none');
    
    try {
        const response = await fetch(`/api/mongodb/business_checkins?business_id=${encodeURIComponent(businessId)}`);
        const data = await response.json();
        
        // Process checkin data for charts
        // Day of week chart
        const dayLabels = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
        const dayData = data.day_distribution || Array(7).fill(0);
        
        const dayCtx = document.getElementById('mongodbCheckinsByDayChart').getContext('2d');
        if (mongodbCheckinsChart) {
            mongodbCheckinsChart.destroy();
        }
        
        mongodbCheckinsChart = new Chart(dayCtx, {
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
        
        // For the hour distribution display a heatmap or another visualization
        if (data.hour_distribution) {
            const hourContainer = document.getElementById('mongodbHourDistribution');
            hourContainer.innerHTML = '';
            
            // Create a heat map-like visualization
            const hours = Array.from({ length: 24 }, (_, i) => i);
            const hourData = data.hour_distribution;
            const maxValue = Math.max(...hourData);
            
            let html = '<div class="hour-heatmap">';
            hours.forEach(hour => {
                const value = hourData[hour] || 0;
                const intensity = maxValue > 0 ? (value / maxValue) * 100 : 0;
                const ampm = hour < 12 ? 'AM' : 'PM';
                const hour12 = hour % 12 || 12;
                
                html += `
                    <div class="hour-block" title="${hour12}${ampm}: ${value} checkins">
                        <div class="hour-label">${hour12}${ampm}</div>
                        <div class="hour-bar" style="height: ${intensity}%"></div>
                        <div class="hour-value">${value}</div>
                    </div>
                `;
            });
            html += '</div>';
            
            hourContainer.innerHTML = html;
            
            // Add some styles
            const style = document.createElement('style');
            style.textContent = `
                .hour-heatmap {
                    display: flex;
                    justify-content: space-between;
                    height: 200px;
                    align-items: flex-end;
                    margin-top: 20px;
                }
                .hour-block {
                    flex: 1;
                    display: flex;
                    flex-direction: column;
                    align-items: center;
                    height: 100%;
                }
                .hour-label {
                    font-size: 10px;
                    transform: rotate(-90deg);
                    white-space: nowrap;
                    margin-bottom: 5px;
                }
                .hour-bar {
                    width: 80%;
                    background-color: rgba(52, 152, 219, 0.7);
                    margin-top: auto;
                }
                .hour-value {
                    font-size: 10px;
                    margin-top: 5px;
                }
            `;
            document.head.appendChild(style);
        }
        
    } catch (error) {
        console.error('Error loading checkin data:', error);
        document.getElementById('mongodbCheckinsContent').innerHTML = '<div class="alert alert-danger">Error loading checkin data. Please try again.</div>';
    } finally {
        document.getElementById('mongodbCheckinsLoader').classList.add('d-none');
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
        
        // Load schema statistics
        loadSchemaStatistics();
        
        // Load document structure visualization
        visualizeDocumentStructure();
        
    } catch (error) {
        console.error('Error loading MongoDB analytics:', error);
    }
}

// Load schema statistics for MongoDB
async function loadSchemaStatistics() {
    try {
        const response = await fetch('/api/mongodb/schema_analysis');
        const data = await response.json();
        
        const schemaStatsCtx = document.getElementById('mongodbSchemaStatsChart').getContext('2d');
        if (mongodbSchemaStatsChart) {
            mongodbSchemaStatsChart.destroy();
        }
        
        // Extract data for chart
        const collections = data.map(item => item.collection);
        const fieldCounts = data.map(item => item.avg_field_count);
        const variationPercentages = data.map(item => item.schema_variation);
        
        mongodbSchemaStatsChart = new Chart(schemaStatsCtx, {
            type: 'bar',
            data: {
                labels: collections,
                datasets: [
                    {
                        label: 'Average Field Count',
                        data: fieldCounts,
                        backgroundColor: 'rgba(52, 152, 219, 0.7)',
                        yAxisID: 'y'
                    },
                    {
                        label: 'Schema Variation (%)',
                        data: variationPercentages,
                        backgroundColor: 'rgba(255, 99, 132, 0.7)',
                        yAxisID: 'y1'
                    }
                ]
            },
            options: {
                responsive: true,
                plugins: {
                    title: {
                        display: true,
                        text: 'MongoDB Collection Schema Statistics'
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        title: {
                            display: true,
                            text: 'Average Field Count'
                        }
                    },
                    y1: {
                        beginAtZero: true,
                        position: 'right',
                        title: {
                            display: true,
                            text: 'Schema Variation (%)'
                        },
                        max: 100,
                        grid: {
                            drawOnChartArea: false
                        }
                    }
                }
            }
        });
        
        // Load array field analysis
        const arrayResponse = await fetch('/api/mongodb/array_field_analysis');
        const arrayData = await arrayResponse.json();
        
        const arrayFields = arrayData.array_fields.map(item => item.field);
        const avgArrayLengths = arrayData.array_fields.map(item => item.avg_length);
        
        const arrayDistributionCtx = document.getElementById('mongodbArrayDistributionChart').getContext('2d');
        if (mongodbArrayDistributionChart) {
            mongodbArrayDistributionChart.destroy();
        }
        
        mongodbArrayDistributionChart = new Chart(arrayDistributionCtx, {
            type: 'bar',
            data: {
                labels: arrayFields,
                datasets: [{
                    label: 'Average Array Length',
                    data: avgArrayLengths,
                    backgroundColor: 'rgba(46, 204, 113, 0.7)'
                }]
            },
            options: {
                indexAxis: 'y',
                responsive: true,
                plugins: {
                    title: {
                        display: true,
                        text: 'Array Field Length Distribution'
                    }
                }
            }
        });
    } catch (error) {
        console.error('Error loading schema statistics:', error);
    }
}

// Visualize document structure
async function visualizeDocumentStructure() {
    const container = document.getElementById('documentStructureViz');
    if (!container) return;
    
    try {
        const response = await fetch('/api/mongodb/document_structure');
        const structure = await response.json();
        
        let html = '<div class="document-structure">';
        html += '<h6>Business Document Structure</h6>';
        html += '<div class="structure-description">MongoDB\'s flexible schema allows storing complex, nested data in a single document.</div>';
        html += '<div class="structure-container">';
        
        // Function to recursively render the structure
        function renderStructure(obj, indent = 0) {
            let result = '';
            
            if (obj.type === 'Object' && obj.fields) {
                for (const [key, value] of Object.entries(obj.fields)) {
                    const indentStr = ' '.repeat(indent * 2);
                    
                    if (value.type === 'Object') {
                        result += `<div class="structure-item nested-item">`;
                        result += `<div class="structure-key">${indentStr}${key}:</div>`;
                        result += `<div class="structure-value">`;
                        result += renderStructure(value, indent + 1);
                        result += `</div></div>`;
                    } else if (value.type.startsWith('Array')) {
                        result += `<div class="structure-item">`;
                        result += `<span class="structure-key">${indentStr}${key}:</span> `;
                        result += `<span class="structure-value array-type">${value.type}</span>`;
                        
                        if (value.items) {
                            result += `<div class="structure-value" style="margin-left: ${(indent + 1) * 20}px;">`;
                            result += renderStructure(value.items, indent + 2);
                            result += `</div>`;
                        }
                        
                        result += `</div>`;
                    } else {
                        let typeClass = '';
                        if (value.type === 'str' || value.type === 'string') typeClass = 'string-type';
                        else if (value.type === 'int' || value.type === 'float' || value.type === 'number') typeClass = 'number-type';
                        
                        result += `<div class="structure-item">`;
                        result += `<span class="structure-key">${indentStr}${key}:</span> `;
                        result += `<span class="structure-value ${typeClass}">${value.type}</span>`;
                        if (value.example) {
                            result += ` <span class="text-muted">(example: ${value.example})</span>`;
                        }
                        result += `</div>`;
                    }
                }
            } else {
                result += `<div class="structure-item">`;
                result += `<span class="structure-value">${obj.type || 'Unknown type'}</span>`;
                if (obj.example) {
                    result += ` <span class="text-muted">(example: ${obj.example})</span>`;
                }
                result += `</div>`;
            }
            
            return result;
        }
        
        html += renderStructure(structure);
        html += '</div></div>';
        
        container.innerHTML = html;
        
        // Add some styles
        const style = document.createElement('style');
        style.textContent = `
            .document-structure {
                border: 1px solid #ddd;
                border-radius: 4px;
                padding: 15px;
                background-color: #f9f9f9;
            }
            .structure-description {
                margin-bottom: 15px;
                color: #666;
            }
            .structure-container {
                font-family: monospace;
                font-size: 14px;
            }
            .structure-item {
                padding: 4px 0;
                border-bottom: 1px dotted #eee;
            }
            .structure-key {
                font-weight: bold;
                color: #333;
            }
            .string-type {
                color: #2e86c1;
            }
            .number-type {
                color: #27ae60;
            }
            .array-type {
                color: #8e44ad;
            }
            .nested-item {
                margin-bottom: 10px;
            }
            .structure-subitem {
                padding-left: 20px;
                margin: 2px 0;
            }
            .structure-subkey {
                color: #666;
            }
        `;
        document.head.appendChild(style);
    } catch (error) {
        console.error('Error visualizing document structure:', error);
        container.innerHTML = '<div class="alert alert-danger">Error loading document structure. Please try again.</div>';
    }
}

// Helper function to populate attribute values based on the selected key
function populateAttributeValues(key) {
    const valueDropdown = document.getElementById('mongodbAttributeValue');
    if (!valueDropdown) return;
    
    // Clear existing options
    valueDropdown.innerHTML = '<option value="">Any value</option>';
    
    // Set values based on key
    let values = [];
    
    switch (key) {
        case 'RestaurantsPriceRange2':
            values = ['1', '2', '3', '4'];
            break;
        case 'WiFi':
            values = ['free', 'paid', 'no'];
            break;
        case 'Alcohol':
            values = ['full_bar', 'beer_and_wine', 'none'];
            break;
        case 'NoiseLevel':
            values = ['quiet', 'average', 'loud', 'very_loud'];
            break;
        case 'RestaurantsAttire':
            values = ['casual', 'dressy', 'formal'];
            break;
        case 'OutdoorSeating':
            values = ['true', 'false'];
            break;
        case 'GoodForKids':
            values = ['true', 'false'];
            break;
    }
    
    // Add options
    values.forEach(value => {
        const option = document.createElement('option');
        option.value = value;
        option.textContent = value;
        valueDropdown.appendChild(option);
    });
}

// Initialize MongoDB event listeners
function initializeMongoDBEventListeners() {
    // Business search listeners
    document.getElementById('mongodbSearchButton')?.addEventListener('click', () => searchMongoDBBusinesses(1));
    document.getElementById('mongodbClearButton')?.addEventListener('click', clearMongoDBSearch);
    
    // Review sort change
    document.getElementById('mongodbReviewSortSelect')?.addEventListener('change', function() {
        const businessDetails = document.getElementById('mongodbBusinessDetails');
        const businessId = businessDetails.getAttribute('data-business-id');
        if (businessId) {
            loadMongoDBBusinessReviews(businessId, 1, this.value);
        }
    });
    
    // Tab change events
    document.getElementById('mongodb-overview-tab')?.addEventListener('shown.bs.tab', function (e) {
        loadMongoDBOverviewStats();
    });
    
    document.getElementById('mongodb-analytics-tab')?.addEventListener('shown.bs.tab', function (e) {
        loadMongoDBAnalytics();
    });
    
    // Business details tab events
    document.getElementById('mongodb-reviews-tab')?.addEventListener('click', function() {
        const businessDetails = document.getElementById('mongodbBusinessDetails');
        const businessId = businessDetails.getAttribute('data-business-id');
        if (businessId) {
            loadMongoDBBusinessReviews(businessId);
        }
    });
    
    document.getElementById('mongodb-checkins-tab')?.addEventListener('click', function() {
        const businessDetails = document.getElementById('mongodbBusinessDetails');
        const businessId = businessDetails.getAttribute('data-business-id');
        if (businessId) {
            loadMongoDBBusinessCheckins(businessId);
        }
    });
    
    // Main tab selection
    document.getElementById('mongodb-tab')?.addEventListener('shown.bs.tab', function (e) {
        // Check if the overview tab is active
        if (document.getElementById('mongodb-overview-tab').classList.contains('active')) {
            loadMongoDBOverviewStats();
        }
    });
    
    // Attribute dropdown updates
    document.getElementById('mongodbAttributeKey')?.addEventListener('change', function() {
        populateAttributeValues(this.value);
    });
}

// Initialize when the DOM is loaded
document.addEventListener('DOMContentLoaded', function() {
    initializeMongoDBEventListeners();
    
    // When clicking on the search tab, auto-load the businesses
    document.getElementById('mongodb-businesses-tab')?.addEventListener('shown.bs.tab', function (e) {
        searchMongoDBBusinesses(1);
    });
});