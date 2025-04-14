/**
 * Enhanced Neo4j-specific functionality for Yelp Data Warehouse Dashboard
 */

// Global variables for Neo4j charts
let neo4jCategoriesChart = null;
let neo4jRelationshipsChart = null;
let neo4jBusinessDetailsChart = null;
let neo4jTopCategoriesChart = null;

// Format number with commas for better readability
function formatNumber(num) {
    if (num === undefined || num === null) return '0';
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
        
        // Update density metrics
        const densityMetricsContainer = document.getElementById('neo4j-density-metrics');
        if (densityMetricsContainer && data.density_metrics) {
            const metrics = data.density_metrics;
            
            densityMetricsContainer.innerHTML = `
                <div class="col-md-6">
                    <div class="card">
                        <div class="card-body text-center">
                            <h3>${metrics.avg_categories_per_business.toFixed(2)}</h3>
                            <p>Average Categories per Business</p>
                        </div>
                    </div>
                </div>
                <div class="col-md-6">
                    <div class="card">
                        <div class="card-body text-center">
                            <h3>${metrics.max_categories_per_business}</h3>
                            <p>Maximum Categories per Business</p>
                        </div>
                    </div>
                </div>
            `;
        }
        
        // Update top users
        const topUsersContainer = document.getElementById('neo4j-top-users');
        if (topUsersContainer && data.top_users) {
            topUsersContainer.innerHTML = '';
            
            data.top_users.forEach(user => {
                const row = document.createElement('tr');
                row.innerHTML = `
                    <td>${user.name}</td>
                    <td>${user.review_count}</td>
                `;
                topUsersContainer.appendChild(row);
            });
        }
        
    } catch (error) {
        console.error('Error loading Neo4j overview stats:', error);
    }
}

// Search Neo4j Businesses
async function searchNeo4jBusinesses(page = 1) {
    // Get search parameters
    const name = document.getElementById('neo4jSearchName').value;
    const location = document.getElementById('neo4jLocationFilter').value;
    const category = document.getElementById('neo4jCategoryFilter').value;
    const minRating = document.getElementById('neo4jRatingFilter').value;
    const connectedTo = document.getElementById('neo4jConnectedFilter').value;
    const sortBy = document.getElementById('neo4jSortBy').value;
    const limit = 10; // Fixed limit of 10 items per page
    
    // Show loader
    document.getElementById('neo4jSearchLoader').classList.remove('d-none');
    document.getElementById('neo4jSearchResults').innerHTML = '';
    document.getElementById('neo4jSearchPagination').classList.add('d-none');
    
    try {
        // Build query parameters
        const params = new URLSearchParams();
        if (name) params.append('name', name);
        if (location) params.append('location', location);
        if (category) params.append('category', category);
        if (minRating) params.append('min_rating', minRating);
        if (connectedTo) params.append('connected_to', connectedTo);
        params.append('sort_by', sortBy);
        params.append('page', page);
        params.append('limit', limit);
        
        const response = await fetch(`/api/neo4j/search_businesses?${params.toString()}`);
        const data = await response.json();
        
        // Get businesses from response
        const businesses = data.businesses || [];
        const pagination = data.pagination || {
            total: businesses.length,
            page: page,
            limit: limit,
            pages: Math.ceil(businesses.length / limit)
        };
        
        // Display total count
        const totalResultsDiv = document.createElement('div');
        totalResultsDiv.className = 'mb-3';
        totalResultsDiv.innerHTML = `<strong>Found ${formatNumber(pagination.total)} businesses matching your criteria.</strong>`;
        document.getElementById('neo4jSearchResults').appendChild(totalResultsDiv);
        
        if (businesses.length === 0) {
            document.getElementById('neo4jSearchResults').innerHTML = '<div class="alert alert-info">No businesses found matching your criteria.</div>';
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
                // Safely escape business name for use in onclick attribute
                const escapedBusinessName = business.business_name
                    ? business.business_name.replace(/'/g, "\\'").replace(/"/g, '&quot;')
                    : 'Unnamed Business';
                
                tableHtml += `
                    <tr data-business-id="${business.business_id}" onclick="showNeo4jBusinessDetails('${business.business_id}', '${escapedBusinessName}')">
                        <td>${business.business_name || 'Unnamed Business'}</td>
                        <td>${business.city || 'N/A'}</td>
                        <td>${business.state || 'N/A'}</td>
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
            
            document.getElementById('neo4jSearchResults').innerHTML = tableHtml;
            
            // Add pagination if needed
            if (pagination.pages > 1) {
                let paginationHtml = '';
                
                // Previous button
                paginationHtml += `
                    <li class="page-item ${page <= 1 ? 'disabled' : ''}">
                        <a class="page-link" href="#" onclick="event.preventDefault(); ${page > 1 ? 'searchNeo4jBusinesses(' + (page - 1) + ')' : ''}">Previous</a>
                    </li>
                `;
                
                // Page numbers
                const startPage = Math.max(1, page - 2);
                const endPage = Math.min(pagination.pages, page + 2);
                
                for (let i = startPage; i <= endPage; i++) {
                    paginationHtml += `
                        <li class="page-item ${i === page ? 'active' : ''}">
                            <a class="page-link" href="#" onclick="event.preventDefault(); searchNeo4jBusinesses(${i})">${i}</a>
                        </li>
                    `;
                }
                
                // Next button
                paginationHtml += `
                    <li class="page-item ${page >= pagination.pages ? 'disabled' : ''}">
                        <a class="page-link" href="#" onclick="event.preventDefault(); ${page < pagination.pages ? 'searchNeo4jBusinesses(' + (page + 1) + ')' : ''}">Next</a>
                    </li>
                `;
                
                document.getElementById('neo4jSearchPagination').innerHTML = `
                    <nav>
                        <ul class="pagination justify-content-center">
                            ${paginationHtml}
                        </ul>
                    </nav>
                `;
                document.getElementById('neo4jSearchPagination').classList.remove('d-none');
            }
        }
    } catch (error) {
        console.error('Error searching businesses:', error);
        document.getElementById('neo4jSearchResults').innerHTML = '<div class="alert alert-danger">Error searching businesses. Please try again.</div>';
    } finally {
        // Hide loader
        document.getElementById('neo4jSearchLoader').classList.add('d-none');
    }
}

// Clear search form
function clearNeo4jSearch() {
    document.getElementById('neo4jSearchName').value = '';
    document.getElementById('neo4jLocationFilter').value = '';
    document.getElementById('neo4jCategoryFilter').value = '';
    document.getElementById('neo4jRatingFilter').value = '';
    document.getElementById('neo4jConnectedFilter').value = '';
    document.getElementById('neo4jSortBy').value = 'stars';
    
    // Clear results
    document.getElementById('neo4jSearchResults').innerHTML = '';
    document.getElementById('neo4jSearchPagination').classList.add('d-none');
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
            const escapedBusinessName = business.business_name
                ? business.business_name.replace(/'/g, "\\'").replace(/"/g, '&quot;')
                : 'Unnamed Business';
                
            tableHtml += `
                <tr data-business-id="${business.business_id}" onclick="showNeo4jBusinessDetails('${business.business_id}', '${escapedBusinessName}')">
                    <td>${business.business_name || 'Unnamed Business'}</td>
                    <td>${business.city || 'N/A'}</td>
                    <td>${business.state || 'N/A'}</td>
                    <td>${formatStarRating(business.stars)}</td>
                    <td>${formatNumber(business.review_count)}</td>
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
        document.getElementById('neo4jBusinessDetailsRating').innerHTML = `${formatStarRating(business.stars)} (${business.avg_stars ? parseFloat(business.avg_stars).toFixed(1) : 'N/A'} avg)`;
        document.getElementById('neo4jBusinessDetailsReviews').textContent = formatNumber(business.review_count || 0);
        document.getElementById('neo4jBusinessDetailsCheckins').textContent = formatNumber(business.checkin_count || 0);
        document.getElementById('neo4jBusinessDetailsId').textContent = businessId;
        
        // Display categories
        if (data.categories && data.categories.length > 0) {
            document.getElementById('neo4jBusinessDetailsCategories').textContent = data.categories.join(', ');
        } else {
            document.getElementById('neo4jBusinessDetailsCategories').textContent = 'No categories available';
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
        
        // Display recent reviews if available
        if (data.recent_reviews && data.recent_reviews.length > 0) {
            let reviewsHtml = '';
            
            data.recent_reviews.forEach(review => {
                // Format date
                const reviewDate = new Date(review.date);
                const formattedDate = reviewDate.toLocaleDateString('en-US', { 
                    year: 'numeric', 
                    month: 'long', 
                    day: 'numeric'
                });
                
                reviewsHtml += `
                    <div class="card mb-3">
                        <div class="card-body">
                            <div class="d-flex justify-content-between align-items-center mb-2">
                                <div>
                                    <h6 class="mb-0">${review.user_name || 'Anonymous'}</h6>
                                    <div class="text-muted small">${formattedDate}</div>
                                </div>
                                <div>
                                    ${formatStarRating(review.stars)}
                                </div>
                            </div>
                            <p class="mb-1">${review.text || 'No review text available.'}</p>
                        </div>
                    </div>
                `;
            });
            
            const reviewsTab = document.getElementById('neo4j-reviews-content');
            if (reviewsTab) {
                reviewsTab.querySelector('#neo4jReviewsList').innerHTML = reviewsHtml;
            }
        }
        
        // Load business network details
        loadBusinessNetworkDetails(businessId);
        
        // Load business recommendations
        loadBusinessRecommendations(businessId, 'details');
        
        // Load complete reviews
        loadBusinessReviews(businessId);
        
    } catch (error) {
        console.error('Error loading business details:', error);
    }
}

// Load business reviews
async function loadBusinessReviews(businessId, page = 1, sort = 'date_desc') {
    const limit = 5; // Reviews per page
    
    document.getElementById('neo4jReviewsLoader').classList.remove('d-none');
    
    try {
        const response = await fetch(`/api/neo4j/business_reviews?business_id=${encodeURIComponent(businessId)}&page=${page}&limit=${limit}&sort=${sort}`);
        const data = await response.json();
        
        const reviews = data.reviews || [];
        const pagination = data.pagination || {
            total: reviews.length,
            page: 1,
            limit: limit,
            pages: 1
        };
        
        if (reviews.length === 0) {
            document.getElementById('neo4jReviewsList').innerHTML = '<div class="alert alert-info">No reviews found for this business.</div>';
        } else {
            let reviewsHtml = '';
            
            reviews.forEach(review => {
                // Format date
                const reviewDate = new Date(review.review_date);
                const formattedDate = reviewDate.toLocaleDateString('en-US', { 
                    year: 'numeric', 
                    month: 'long', 
                    day: 'numeric'
                });
                
                reviewsHtml += `
                    <div class="card mb-3">
                        <div class="card-body">
                            <div class="d-flex justify-content-between align-items-center mb-2">
                                <div>
                                    <h6 class="mb-0">${review.user_name || 'Anonymous'}</h6>
                                    <div class="text-muted small">${formattedDate}</div>
                                </div>
                                <div>
                                    ${formatStarRating(review.stars)}
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
            
            document.getElementById('neo4jReviewsList').innerHTML = reviewsHtml;
            
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
                            <a class="page-link" href="#" onclick="event.preventDefault(); loadBusinessReviews('${businessId}', ${i}, '${sort}');">${i}</a>
                        </li>
                    `;
                }
                
                // Next button
                paginationHtml += `
                    <li class="page-item ${pagination.page >= pagination.pages ? 'disabled' : ''}">
                        <a class="page-link" href="#" onclick="event.preventDefault(); ${pagination.page < pagination.pages ? 'loadBusinessReviews(\'' + businessId + '\', ' + (pagination.page + 1) + ', \'' + sort + '\')' : ''}">Next</a>
                    </li>
                `;
                
                document.getElementById('neo4jReviewsPagination').innerHTML = `
                    <nav>
                        <ul class="pagination justify-content-center">
                            ${paginationHtml}
                        </ul>
                    </nav>
                `;
            } else {
                document.getElementById('neo4jReviewsPagination').innerHTML = '';
            }
        }
    } catch (error) {
        console.error('Error loading reviews:', error);
        document.getElementById('neo4jReviewsList').innerHTML = '<div class="alert alert-danger">Error loading reviews. Please try again.</div>';
    } finally {
        document.getElementById('neo4jReviewsLoader').classList.add('d-none');
    }
}

// Load business network details
async function loadBusinessNetworkDetails(businessId) {
    document.getElementById('neo4jNetworkDetailsLoader').classList.remove('d-none');
    
    try {
        const response = await fetch(`/api/neo4j/business_network?business_id=${encodeURIComponent(businessId)}`);
        const data = await response.json();
        
        // Category-related businesses
        const categoryRelatedContainer = document.querySelector('#neo4j-network-details-content #neo4jCategoryRelatedTable');
        if (categoryRelatedContainer) {
            if (data.category_related && data.category_related.length > 0) {
                let tableHtml = `
                    <table class="table table-hover">
                        <thead>
                            <tr>
                                <th>Business Name</th>
                                <th>Location</th>
                                <th>Rating</th>
                                <th>Common Categories</th>
                            </tr>
                        </thead>
                        <tbody>
                `;
                
                data.category_related.forEach(business => {
                    const escapedName = business.business_name.replace(/'/g, "\\'").replace(/"/g, '&quot;');
                    
                    tableHtml += `
                        <tr onclick="showNeo4jBusinessDetails('${business.business_id}', '${escapedName}')">
                            <td>${business.business_name}</td>
                            <td>${business.city}, ${business.state}</td>
                            <td>${formatStarRating(business.stars)}</td>
                            <td>${business.common_categories}</td>
                        </tr>
                    `;
                });
                
                tableHtml += `
                        </tbody>
                    </table>
                `;
                
                categoryRelatedContainer.innerHTML = tableHtml;
            } else {
                categoryRelatedContainer.innerHTML = '<div class="alert alert-info">No category-related businesses found.</div>';
            }
        }
        
        // User-related businesses
        const userRelatedContainer = document.querySelector('#neo4j-network-details-content #neo4jUserRelatedTable');
        if (userRelatedContainer) {
            if (data.user_related && data.user_related.length > 0) {
                let tableHtml = `
                    <table class="table table-hover">
                        <thead>
                            <tr>
                                <th>Business Name</th>
                                <th>Location</th>
                                <th>Rating</th>
                                <th>Common Users</th>
                            </tr>
                        </thead>
                        <tbody>
                `;
                
                data.user_related.forEach(business => {
                    const escapedName = business.business_name.replace(/'/g, "\\'").replace(/"/g, '&quot;');
                    
                    tableHtml += `
                        <tr onclick="showNeo4jBusinessDetails('${business.business_id}', '${escapedName}')">
                            <td>${business.business_name}</td>
                            <td>${business.city}, ${business.state}</td>
                            <td>${formatStarRating(business.stars)}</td>
                            <td>${business.common_users}</td>
                        </tr>
                    `;
                });
                
                tableHtml += `
                        </tbody>
                    </table>
                `;
                
                userRelatedContainer.innerHTML = tableHtml;
            } else {
                userRelatedContainer.innerHTML = '<div class="alert alert-info">No user-related businesses found.</div>';
            }
        }
        
        // Shared categories
        const sharedCategoriesContainer = document.querySelector('#neo4j-network-details-content #neo4jSharedCategoriesTable');
        if (sharedCategoriesContainer) {
            if (data.shared_categories && data.shared_categories.length > 0) {
                let tableHtml = `
                    <table class="table">
                        <thead>
                            <tr>
                                <th>Category</th>
                                <th>Shared Businesses</th>
                            </tr>
                        </thead>
                        <tbody>
                `;
                
                data.shared_categories.forEach(category => {
                    tableHtml += `
                        <tr>
                            <td>${category.category}</td>
                            <td>${category.business_count}</td>
                        </tr>
                    `;
                });
                
                tableHtml += `
                        </tbody>
                    </table>
                `;
                
                sharedCategoriesContainer.innerHTML = tableHtml;
            } else {
                sharedCategoriesContainer.innerHTML = '<div class="alert alert-info">No shared categories data available.</div>';
            }
        }
        
        // Connected users
        const connectedUsersContainer = document.querySelector('#neo4j-network-details-content #neo4jConnectedUsersTable');
        if (connectedUsersContainer) {
            if (data.connected_users && data.connected_users.length > 0) {
                let tableHtml = `
                    <table class="table">
                        <thead>
                            <tr>
                                <th>User Name</th>
                                <th>Reviewed Businesses</th>
                            </tr>
                        </thead>
                        <tbody>
                `;
                
                data.connected_users.forEach(user => {
                    tableHtml += `
                        <tr>
                            <td>${user.user_name}</td>
                            <td>${user.business_count}</td>
                        </tr>
                    `;
                });
                
                tableHtml += `
                        </tbody>
                    </table>
                `;
                
                connectedUsersContainer.innerHTML = tableHtml;
            } else {
                connectedUsersContainer.innerHTML = '<div class="alert alert-info">No connected users data available.</div>';
            }
        }
        
    } catch (error) {
        console.error('Error loading network details:', error);
        
        // Set error messages in all containers
        const containers = [
            '#neo4j-network-details-content #neo4jCategoryRelatedTable',
            '#neo4j-network-details-content #neo4jUserRelatedTable',
            '#neo4j-network-details-content #neo4jSharedCategoriesTable',
            '#neo4j-network-details-content #neo4jConnectedUsersTable'
        ];
        
        containers.forEach(selector => {
            const container = document.querySelector(selector);
            if (container) {
                container.innerHTML = '<div class="alert alert-danger">Error loading network data.</div>';
            }
        });
    } finally {
        document.getElementById('neo4jNetworkDetailsLoader').classList.add('d-none');
    }
}

// Load business recommendations
async function loadBusinessRecommendations(businessId, targetId = 'main') {
    // Determine which container to use based on the target ID
    const loaderSelector = targetId === 'details' 
        ? '#neo4j-recommend-content #neo4jRecommendationsLoader' 
        : '#businessRecommendationsLoader';
        
    const tablesSelectors = {
        similar: targetId === 'details' ? '#neo4j-recommend-content #neo4jSimilarBusinessesTable' : '#similarBusinessesTable',
        complementary: targetId === 'details' ? '#neo4j-recommend-content #neo4jComplementaryTable' : '#complementaryBusinessesTable',
        alsoReviewed: targetId === 'details' ? '#neo4j-recommend-content #neo4jAlsoReviewedTable' : '#alsoReviewedTable'
    };
    
    const resultsSelector = targetId === 'details' ? '' : '#businessRecommendationsResults';
    
    // Show loader
    document.querySelector(loaderSelector)?.classList.remove('d-none');
    
    try {
        const response = await fetch(`/api/neo4j/business_recommendations?business_id=${encodeURIComponent(businessId)}`);
        const data = await response.json();
        
        // Show results container if not in details view
        if (targetId !== 'details') {
            document.querySelector(resultsSelector)?.classList.remove('d-none');
        }
        
        // Similar businesses
        const similarContainer = document.querySelector(tablesSelectors.similar);
        if (similarContainer) {
            if (data.similar_businesses && data.similar_businesses.length > 0) {
                let tableHtml = `
                    <table class="table table-hover">
                        <thead>
                            <tr>
                                <th>Business Name</th>
                                <th>Location</th>
                                <th>Rating</th>
                                <th>Categories</th>
                            </tr>
                        </thead>
                        <tbody>
                `;
                
                data.similar_businesses.forEach(business => {
                    const escapedName = business.business_name.replace(/'/g, "\\'").replace(/"/g, '&quot;');
                    const categoriesText = business.shared_categories.join(', ');
                    
                    tableHtml += `
                        <tr onclick="showNeo4jBusinessDetails('${business.business_id}', '${escapedName}')">
                            <td>${business.business_name}</td>
                            <td>${business.city}, ${business.state}</td>
                            <td>${formatStarRating(business.stars)}</td>
                            <td>${categoriesText}</td>
                        </tr>
                    `;
                });
                
                tableHtml += `
                        </tbody>
                    </table>
                `;
                
                similarContainer.innerHTML = tableHtml;
            } else {
                similarContainer.innerHTML = '<div class="alert alert-info">No similar businesses found.</div>';
            }
        }
        
        // Complementary businesses
        const complementaryContainer = document.querySelector(tablesSelectors.complementary);
        if (complementaryContainer) {
            if (data.complementary_businesses && data.complementary_businesses.length > 0) {
                let tableHtml = `
                    <table class="table table-hover">
                        <thead>
                            <tr>
                                <th>Business Name</th>
                                <th>Location</th>
                                <th>Categories</th>
                            </tr>
                        </thead>
                        <tbody>
                `;
                
                data.complementary_businesses.forEach(business => {
                    const escapedName = business.business_name.replace(/'/g, "\\'").replace(/"/g, '&quot;');
                    const categoriesText = business.categories.join(', ');
                    
                    tableHtml += `
                        <tr onclick="showNeo4jBusinessDetails('${business.business_id}', '${escapedName}')">
                            <td>${business.business_name}</td>
                            <td>${business.city}, ${business.state}</td>
                            <td>${categoriesText}</td>
                        </tr>
                    `;
                });
                
                tableHtml += `
                        </tbody>
                    </table>
                `;
                
                complementaryContainer.innerHTML = tableHtml;
            } else {
                complementaryContainer.innerHTML = '<div class="alert alert-info">No complementary businesses found.</div>';
            }
        }
        
        // Also reviewed businesses
        const alsoReviewedContainer = document.querySelector(tablesSelectors.alsoReviewed);
        if (alsoReviewedContainer) {
            if (data.also_reviewed && data.also_reviewed.length > 0) {
                let tableHtml = `
                    <table class="table table-hover">
                        <thead>
                            <tr>
                                <th>Business Name</th>
                                <th>Location</th>
                                <th>Rating</th>
                                <th>Common Reviewers</th>
                            </tr>
                        </thead>
                        <tbody>
                `;
                
                data.also_reviewed.forEach(business => {
                    const escapedName = business.business_name.replace(/'/g, "\\'").replace(/"/g, '&quot;');
                    
                    tableHtml += `
                        <tr onclick="showNeo4jBusinessDetails('${business.business_id}', '${escapedName}')">
                            <td>${business.business_name}</td>
                            <td>${business.city}, ${business.state}</td>
                            <td>${formatStarRating(business.stars)}</td>
                            <td>${business.common_reviewers}</td>
                        </tr>
                    `;
                });
                
                tableHtml += `
                        </tbody>
                    </table>
                `;
                
                alsoReviewedContainer.innerHTML = tableHtml;
            } else {
                alsoReviewedContainer.innerHTML = '<div class="alert alert-info">No "also reviewed" data available.</div>';
            }
        }
        
    } catch (error) {
        console.error('Error loading business recommendations:', error);
        
        // Set error messages in all containers
        Object.values(tablesSelectors).forEach(selector => {
            const container = document.querySelector(selector);
            if (container) {
                container.innerHTML = '<div class="alert alert-danger">Error loading recommendations.</div>';
            }
        });
    } finally {
        document.querySelector(loaderSelector)?.classList.add('d-none');
    }
}

// Load Neo4j Network Analysis
// Fixed loadNeo4jNetwork function
async function loadNeo4jNetwork() {
    const businessId = document.getElementById('neo4jBusinessIdInput').value.trim();
    
    if (!businessId) {
        alert('Please enter a business ID');
        return;
    }
    
    // Show loader and hide previous results
    document.getElementById('neo4jNetworkLoader').classList.remove('d-none');
    document.getElementById('neo4jNetworkResults').classList.add('d-none');
    document.getElementById('neo4jNetworkError').classList.add('d-none');
    
    try {
        console.log("Fetching network data for business ID:", businessId);
        const response = await fetch(`/api/neo4j/business_network?business_id=${encodeURIComponent(businessId)}`);
        
        // Check if response is ok
        if (!response.ok) {
            throw new Error(`Server returned ${response.status}: ${response.statusText}`);
        }
        
        const data = await response.json();
        console.log("Received network data:", data);
        
        if (data.error) {
            throw new Error(data.error);
        }
        
        // Get data with proper defaults
        const categoryRelated = data.category_related || [];
        const userRelated = data.user_related || [];
        const sharedCategories = data.shared_categories || [];
        const connectedUsers = data.connected_users || [];
        
        // Create category-related table
        let categoryTableHtml = '';
        
        if (categoryRelated.length === 0) {
            categoryTableHtml = '<div class="alert alert-info">No businesses found with common categories</div>';
        } else {
            categoryTableHtml = `
                <table class="table table-hover">
                    <thead>
                        <tr>
                            <th>Business Name</th>
                            <th>Location</th>
                            <th>Rating</th>
                            <th>Common Categories</th>
                        </tr>
                    </thead>
                    <tbody>
            `;
            
            categoryRelated.forEach(business => {
                const escapedName = business.business_name.replace(/'/g, "\\'").replace(/"/g, '&quot;');
                
                categoryTableHtml += `
                    <tr onclick="showNeo4jBusinessDetails('${business.business_id}', '${escapedName}')">
                        <td>${business.business_name}</td>
                        <td>${business.city || 'N/A'}, ${business.state || 'N/A'}</td>
                        <td>${formatStarRating(business.stars)}</td>
                        <td>${business.common_categories}</td>
                    </tr>
                `;
            });
            
            categoryTableHtml += `
                    </tbody>
                </table>
            `;
        }
        
        // Create user-related table
        let userTableHtml = '';
        
        if (userRelated.length === 0) {
            userTableHtml = '<div class="alert alert-info">No businesses found with common users</div>';
        } else {
            userTableHtml = `
                <table class="table table-hover">
                    <thead>
                        <tr>
                            <th>Business Name</th>
                            <th>Location</th>
                            <th>Rating</th>
                            <th>Common Users</th>
                        </tr>
                    </thead>
                    <tbody>
            `;
            
            userRelated.forEach(business => {
                const escapedName = business.business_name.replace(/'/g, "\\'").replace(/"/g, '&quot;');
                
                userTableHtml += `
                    <tr onclick="showNeo4jBusinessDetails('${business.business_id}', '${escapedName}')">
                        <td>${business.business_name}</td>
                        <td>${business.city || 'N/A'}, ${business.state || 'N/A'}</td>
                        <td>${formatStarRating(business.stars)}</td>
                        <td>${business.common_users}</td>
                    </tr>
                `;
            });
            
            userTableHtml += `
                    </tbody>
                </table>
            `;
        }
        
        // Create shared categories table
        let sharedCategoriesHtml = '';
        
        if (sharedCategories.length === 0) {
            sharedCategoriesHtml = '<div class="alert alert-info">No shared categories data available</div>';
        } else {
            sharedCategoriesHtml = `
                <table class="table">
                    <thead>
                        <tr>
                            <th>Category</th>
                            <th>Business Count</th>
                        </tr>
                    </thead>
                    <tbody>
            `;
            
            sharedCategories.forEach(category => {
                sharedCategoriesHtml += `
                    <tr>
                        <td>${category.category}</td>
                        <td>${category.business_count}</td>
                    </tr>
                `;
            });
            
            sharedCategoriesHtml += `
                    </tbody>
                </table>
            `;
        }
        
        // Create connected users table
        let connectedUsersHtml = '';
        
        if (connectedUsers.length === 0) {
            connectedUsersHtml = '<div class="alert alert-info">No connected users data available</div>';
        } else {
            connectedUsersHtml = `
                <table class="table">
                    <thead>
                        <tr>
                            <th>User Name</th>
                            <th>Businesses Count</th>
                        </tr>
                    </thead>
                    <tbody>
            `;
            
            connectedUsers.forEach(user => {
                connectedUsersHtml += `
                    <tr>
                        <td>${user.user_name}</td>
                        <td>${user.business_count}</td>
                    </tr>
                `;
            });
            
            connectedUsersHtml += `
                    </tbody>
                </table>
            `;
        }
        
        // Update the DOM - Note the different IDs for the network tab
        document.getElementById('neo4jNetworkCategoryRelatedTable').innerHTML = categoryTableHtml;
        document.getElementById('neo4jNetworkUserRelatedTable').innerHTML = userTableHtml;
        document.getElementById('neo4jNetworkSharedCategoriesTable').innerHTML = sharedCategoriesHtml;
        document.getElementById('neo4jNetworkConnectedUsersTable').innerHTML = connectedUsersHtml;
        document.getElementById('neo4jNetworkResults').classList.remove('d-none');
    } catch (error) {
        console.error('Error loading network analysis:', error);
        document.getElementById('neo4jNetworkError').innerHTML = `
            <div class="alert alert-danger">
                <h5>Error loading network analysis</h5>
                <p>${error.message}</p>
                <p>Please check that the business ID is valid. You can find business IDs in the business details section when searching for businesses.</p>
                <h6 class="mt-3">Try these sample business IDs:</h6>
                <div class="d-flex flex-wrap gap-2">
                    <button class="btn btn-sm btn-outline-secondary sample-id" onclick="document.getElementById('neo4jBusinessIdInput').value='4JNXUYY8wbaaDmk3BPzlWw'">4JNXUYY8wbaaDmk3BPzlWw</button>
                    <button class="btn btn-sm btn-outline-secondary sample-id" onclick="document.getElementById('neo4jBusinessIdInput').value='RESDUcs7fIiihp38-d6_6g'">RESDUcs7fIiihp38-d6_6g</button>
                    <button class="btn btn-sm btn-outline-secondary sample-id" onclick="document.getElementById('neo4jBusinessIdInput').value='K7lWdNUhCbcnEvI0NhGewg'">K7lWdNUhCbcnEvI0NhGewg</button>
                </div>
            </div>
        `;
        document.getElementById('neo4jNetworkError').classList.remove('d-none');
    } finally {
        // Hide loader
        document.getElementById('neo4jNetworkLoader').classList.add('d-none');
    }
}

// Fixed findBusinessPaths function
async function findBusinessPaths() {
    const businessId1 = document.getElementById('neo4jBusinessId1').value.trim();
    const businessId2 = document.getElementById('neo4jBusinessId2').value.trim();
    const pathType = document.querySelector('input[name="pathType"]:checked').value;
    
    if (!businessId1 || !businessId2) {
        alert('Please enter both business IDs');
        return;
    }
    
    if (businessId1 === businessId2) {
        alert('Please enter different business IDs');
        return;
    }
    
    // Show loader and hide results/errors
    document.getElementById('pathFinderLoader').classList.remove('d-none');
    document.getElementById('pathFinderResults').classList.add('d-none');
    document.getElementById('pathFinderError').classList.add('d-none');
    
    try {
        console.log(`Finding ${pathType} paths between ${businessId1} and ${businessId2}`);
        const response = await fetch(`/api/neo4j/connection_path?business_id1=${encodeURIComponent(businessId1)}&business_id2=${encodeURIComponent(businessId2)}&path_type=${pathType}`);
        
        // Check if response is ok
        if (!response.ok) {
            throw new Error(`Server returned ${response.status}: ${response.statusText}`);
        }
        
        const data = await response.json();
        console.log("Received path data:", data);
        
        if (data.error) {
            throw new Error(data.error);
        }
        
        // Display summary
        const summaryContainer = document.getElementById('pathFinderSummary');
        summaryContainer.innerHTML = `
            <p>Paths between <strong>${data.business1.name}</strong> and <strong>${data.business2.name}</strong></p>
        `;
        
        // Display paths
        const pathsContainer = document.getElementById('pathFinderTable');
        
        if (!data.paths || data.paths.length === 0) {
            pathsContainer.innerHTML = '<div class="alert alert-info">No paths found between these businesses.</div>';
        } else {
            let pathsHtml = '';
            
            if (pathType === 'user') {
                pathsHtml = `
                    <table class="table">
                        <thead>
                            <tr>
                                <th>Connection Type</th>
                                <th>User Name</th>
                            </tr>
                        </thead>
                        <tbody>
                `;
                
                data.paths.forEach(path => {
                    pathsHtml += `
                        <tr>
                            <td>User connection</td>
                            <td>${path.connection}</td>
                        </tr>
                    `;
                });
                
                pathsHtml += `
                        </tbody>
                    </table>
                `;
            } else {
                pathsHtml = `
                    <table class="table">
                        <thead>
                            <tr>
                                <th>Connection Type</th>
                                <th>Category</th>
                            </tr>
                        </thead>
                        <tbody>
                `;
                
                data.paths.forEach(path => {
                    pathsHtml += `
                        <tr>
                            <td>Category connection</td>
                            <td>${path.connection}</td>
                        </tr>
                    `;
                });
                
                pathsHtml += `
                        </tbody>
                    </table>
                `;
            }
            
            pathsContainer.innerHTML = pathsHtml;
        }
        
        // Show results
        document.getElementById('pathFinderResults').classList.remove('d-none');
    } catch (error) {
        console.error('Error finding paths:', error);
        document.getElementById('pathFinderError').innerHTML = `
            <div class="alert alert-danger">
                <h5>Error finding paths</h5>
                <p>${error.message}</p>
                <p>Please check that both business IDs are valid. You can find business IDs in the business details section when searching for businesses.</p>
                <h6 class="mt-3">Try these sample business ID pairs:</h6>
                <div class="d-flex flex-wrap gap-2 mb-2">
                    <button class="btn btn-sm btn-outline-secondary" 
                            onclick="document.getElementById('neo4jBusinessId1').value='4JNXUYY8wbaaDmk3BPzlWw';
                                     document.getElementById('neo4jBusinessId2').value='RESDUcs7fIiihp38-d6_6g';">
                        Sample Pair 1
                    </button>
                    <button class="btn btn-sm btn-outline-secondary" 
                            onclick="document.getElementById('neo4jBusinessId1').value='K7lWdNUhCbcnEvI0NhGewg';
                                     document.getElementById('neo4jBusinessId2').value='RESDUcs7fIiihp38-d6_6g';">
                        Sample Pair 2
                    </button>
                </div>
            </div>
        `;
        document.getElementById('pathFinderError').classList.remove('d-none');
    } finally {
        // Hide loader
        document.getElementById('pathFinderLoader').classList.add('d-none');
    }
}

// Find paths between businesses
async function findBusinessPaths() {
    const businessId1 = document.getElementById('neo4jBusinessId1').value.trim();
    const businessId2 = document.getElementById('neo4jBusinessId2').value.trim();
    const pathType = document.querySelector('input[name="pathType"]:checked').value;
    
    if (!businessId1 || !businessId2) {
        alert('Please enter both business IDs');
        return;
    }
    
    if (businessId1 === businessId2) {
        alert('Please enter different business IDs');
        return;
    }
    
    // Show loader and hide results/errors
    document.getElementById('pathFinderLoader').classList.remove('d-none');
    document.getElementById('pathFinderResults').classList.add('d-none');
    document.getElementById('pathFinderError').classList.add('d-none');
    
    try {
        console.log(`Finding ${pathType} paths between ${businessId1} and ${businessId2}`);
        const response = await fetch(`/api/neo4j/connection_path?business_id1=${encodeURIComponent(businessId1)}&business_id2=${encodeURIComponent(businessId2)}&path_type=${pathType}`);
        
        // Check if response is ok
        if (!response.ok) {
            throw new Error(`Server returned ${response.status}: ${response.statusText}`);
        }
        
        const data = await response.json();
        console.log("Received path data:", data);
        
        if (data.error) {
            throw new Error(data.error);
        }
        
        // Display summary
        const summaryContainer = document.getElementById('pathFinderSummary');
        summaryContainer.innerHTML = `
            <p>Paths between <strong>${data.business1.name}</strong> and <strong>${data.business2.name}</strong></p>
        `;
        
        // Display paths
        const pathsContainer = document.getElementById('pathFinderTable');
        
        if (data.paths.length === 0) {
            pathsContainer.innerHTML = '<div class="alert alert-info">No paths found between these businesses.</div>';
        } else {
            // Existing code to show paths
            // ...
        }
        
        // Show results
        document.getElementById('pathFinderResults').classList.remove('d-none');
    } catch (error) {
        console.error('Error finding paths:', error);
        document.getElementById('pathFinderError').innerHTML = `
            <div class="alert alert-danger">
                <h5>Error finding paths</h5>
                <p>${error.message}</p>
                <p>Please check that both business IDs are valid. You can find business IDs in the business details section when searching for businesses.</p>
                <h6 class="mt-3">Try these sample business ID pairs:</h6>
                <div class="d-flex flex-wrap gap-2 mb-2">
                    <button class="btn btn-sm btn-outline-secondary" 
                            onclick="document.getElementById('neo4jBusinessId1').value='4JNXUYY8wbaaDmk3BPzlWw';
                                     document.getElementById('neo4jBusinessId2').value='RESDUcs7fIiihp38-d6_6g';">
                        Sample Pair 1
                    </button>
                    <button class="btn btn-sm btn-outline-secondary" 
                            onclick="document.getElementById('neo4jBusinessId1').value='K7lWdNUhCbcnEvI0NhGewg';
                                     document.getElementById('neo4jBusinessId2').value='RESDUcs7fIiihp38-d6_6g';">
                        Sample Pair 2
                    </button>
                </div>
            </div>
        `;
        document.getElementById('pathFinderError').classList.remove('d-none');
    } finally {
        // Hide loader
        document.getElementById('pathFinderLoader').classList.add('d-none');
    }
}

// Load user recommendations
async function loadUserRecommendations() {
    const userId = document.getElementById('neo4jUserIdInput').value.trim();
    
    if (!userId) {
        alert('Please enter a user ID');
        return;
    }
    
    // Show loader
    document.getElementById('userRecommendationsLoader').classList.remove('d-none');
    document.getElementById('userRecommendationsResults').classList.add('d-none');
    
    try {
        const response = await fetch(`/api/neo4j/user_recommendations?user_id=${encodeURIComponent(userId)}`);
        const data = await response.json();
        
        // Preferred categories
        const preferredCategoriesTable = document.getElementById('preferredCategoriesTable');
        
        if (data.preferred_categories && data.preferred_categories.length > 0) {
            let tableHtml = `
                <table class="table">
                    <thead>
                        <tr>
                            <th>Category</th>
                            <th>Reviewed Businesses</th>
                        </tr>
                    </thead>
                    <tbody>
            `;
            
            data.preferred_categories.forEach(category => {
                tableHtml += `
                    <tr>
                        <td>${category.category}</td>
                        <td>${category.category_count}</td>
                    </tr>
                `;
            });
            
            tableHtml += `
                    </tbody>
                </table>
            `;
            
            preferredCategoriesTable.innerHTML = tableHtml;
        } else {
            preferredCategoriesTable.innerHTML = '<div class="alert alert-info">No preferred categories found for this user.</div>';
        }
        
        // Category recommendations
        const categoryRecommendationsTable = document.getElementById('categoryRecommendationsTable');
        
        if (data.category_recommendations && data.category_recommendations.length > 0) {
            let tableHtml = `
                <table class="table table-hover">
                    <thead>
                        <tr>
                            <th>Business Name</th>
                            <th>Location</th>
                            <th>Rating</th>
                        </tr>
                    </thead>
                    <tbody>
            `;
            
            data.category_recommendations.forEach(business => {
                const escapedName = business.business_name.replace(/'/g, "\\'").replace(/"/g, '&quot;');
                
                tableHtml += `
                    <tr onclick="showNeo4jBusinessDetails('${business.business_id}', '${escapedName}')">
                        <td>${business.business_name}</td>
                        <td>${business.city}, ${business.state}</td>
                        <td>${formatStarRating(business.stars)}</td>
                    </tr>
                `;
            });
            
            tableHtml += `
                    </tbody>
                </table>
            `;
            
            categoryRecommendationsTable.innerHTML = tableHtml;
        } else {
            categoryRecommendationsTable.innerHTML = '<div class="alert alert-info">No category recommendations available.</div>';
        }
        
        // Collaborative recommendations
        const collaborativeRecommendationsTable = document.getElementById('collaborativeRecommendationsTable');
        
        if (data.collaborative_recommendations && data.collaborative_recommendations.length > 0) {
            let tableHtml = `
                <table class="table table-hover">
                    <thead>
                        <tr>
                            <th>Business Name</th>
                            <th>Location</th>
                            <th>Rating</th>
                            <th>Similar Users</th>
                        </tr>
                    </thead>
                    <tbody>
            `;
            
            data.collaborative_recommendations.forEach(business => {
                const escapedName = business.business_name.replace(/'/g, "\\'").replace(/"/g, '&quot;');
                
                tableHtml += `
                    <tr onclick="showNeo4jBusinessDetails('${business.business_id}', '${escapedName}')">
                        <td>${business.business_name}</td>
                        <td>${business.city}, ${business.state}</td>
                        <td>${formatStarRating(business.stars)}</td>
                        <td>${business.common_users}</td>
                    </tr>
                `;
            });
            
            tableHtml += `
                    </tbody>
                </table>
            `;
            
            collaborativeRecommendationsTable.innerHTML = tableHtml;
        } else {
            collaborativeRecommendationsTable.innerHTML = '<div class="alert alert-info">No collaborative recommendations available.</div>';
        }
        
        // Show results
        document.getElementById('userRecommendationsResults').classList.remove('d-none');
    } catch (error) {
        console.error('Error loading user recommendations:', error);
        
        // Set error messages in all containers
        const containers = [
            'preferredCategoriesTable',
            'categoryRecommendationsTable',
            'collaborativeRecommendationsTable'
        ];
        
        containers.forEach(containerId => {
            const container = document.getElementById(containerId);
            if (container) {
                container.innerHTML = '<div class="alert alert-danger">Error loading recommendations.</div>';
            }
        });
        
        document.getElementById('userRecommendationsResults').classList.remove('d-none');
    } finally {
        // Hide loader
        document.getElementById('userRecommendationsLoader').classList.add('d-none');
    }
}

// Load graph analytics
async function loadGraphAnalytics() {
    // Show loader
    document.getElementById('graphAnalyticsLoader').classList.remove('d-none');
    document.getElementById('graphAnalyticsResults').classList.add('d-none');
    
    try {
        const response = await fetch('/api/neo4j/graph_analytics');
        const data = await response.json();
        
        // Most central businesses
        const centralBusinessesTable = document.getElementById('centralBusinessesTable');
        
        if (data.central_businesses && data.central_businesses.length > 0) {
            let tableHtml = `
                <table class="table table-hover">
                    <thead>
                        <tr>
                            <th>Business Name</th>
                            <th>Location</th>
                            <th>Rating</th>
                            <th>Connections</th>
                        </tr>
                    </thead>
                    <tbody>
            `;
            
            data.central_businesses.forEach(business => {
                const escapedName = business.business_name.replace(/'/g, "\\'").replace(/"/g, '&quot;');
                
                tableHtml += `
                    <tr onclick="showNeo4jBusinessDetails('${business.business_id}', '${escapedName}')">
                        <td>${business.business_name}</td>
                        <td>${business.city}, ${business.state}</td>
                        <td>${formatStarRating(business.stars)}</td>
                        <td>${business.connection_count}</td>
                    </tr>
                `;
            });
            
            tableHtml += `
                    </tbody>
                </table>
            `;
            
            centralBusinessesTable.innerHTML = tableHtml;
        } else {
            centralBusinessesTable.innerHTML = '<div class="alert alert-info">No central businesses data available.</div>';
        }
        
        // Most influential users
        const influentialUsersTable = document.getElementById('influentialUsersTable');
        
        if (data.influential_users && data.influential_users.length > 0) {
            let tableHtml = `
                <table class="table">
                    <thead>
                        <tr>
                            <th>User Name</th>
                            <th>Connection Count</th>
                        </tr>
                    </thead>
                    <tbody>
            `;
            
            data.influential_users.forEach(user => {
                tableHtml += `
                    <tr>
                        <td>${user.user_name}</td>
                        <td>${user.connection_count}</td>
                    </tr>
                `;
            });
            
            tableHtml += `
                    </tbody>
                </table>
            `;
            
            influentialUsersTable.innerHTML = tableHtml;
        } else {
            influentialUsersTable.innerHTML = '<div class="alert alert-info">No influential users data available.</div>';
        }
        
        // Most popular categories chart
        const topCategoriesContainer = document.getElementById('topCategoriesContainer');
        
        if (data.top_categories && data.top_categories.length > 0) {
            const categories = data.top_categories.map(cat => cat.category);
            const counts = data.top_categories.map(cat => cat.business_count);
            
            const ctx = document.getElementById('topCategoriesChart').getContext('2d');
            if (neo4jTopCategoriesChart) {
                neo4jTopCategoriesChart.destroy();
            }
            
            neo4jTopCategoriesChart = new Chart(ctx, {
                type: 'bar',
                data: {
                    labels: categories,
                    datasets: [{
                        label: 'Businesses per Category',
                        data: counts,
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
        } else {
            topCategoriesContainer.innerHTML = '<div class="alert alert-info">No category data available.</div>';
        }
        
        // Category co-occurrence
        const categoryPairsTable = document.getElementById('categoryPairsTable');
        
        if (data.category_pairs && data.category_pairs.length > 0) {
            let tableHtml = `
                <table class="table">
                    <thead>
                        <tr>
                            <th>Category Pair</th>
                            <th>Co-occurrence Count</th>
                        </tr>
                    </thead>
                    <tbody>
            `;
            
            data.category_pairs.forEach(pair => {
                tableHtml += `
                    <tr>
                        <td>${pair.category1} + ${pair.category2}</td>
                        <td>${pair.business_count}</td>
                    </tr>
                `;
            });
            
            tableHtml += `
                    </tbody>
                </table>
            `;
            
            categoryPairsTable.innerHTML = tableHtml;
        } else {
            categoryPairsTable.innerHTML = '<div class="alert alert-info">No category co-occurrence data available.</div>';
        }
        
        // Show results
        document.getElementById('graphAnalyticsResults').classList.remove('d-none');
    } catch (error) {
        console.error('Error loading graph analytics:', error);
        
        // Set error messages in all containers
        const containers = [
            'centralBusinessesTable',
            'influentialUsersTable',
            'topCategoriesContainer',
            'categoryPairsTable'
        ];
        
        containers.forEach(containerId => {
            const container = document.getElementById(containerId);
            if (container) {
                container.innerHTML = '<div class="alert alert-danger">Error loading graph analytics.</div>';
            }
        });
        
        document.getElementById('graphAnalyticsResults').classList.remove('d-none');
    } finally {
        // Hide loader
        document.getElementById('graphAnalyticsLoader').classList.add('d-none');
    }
}

async function searchBusinessForId(searchQuery, limit = 5) {
    try {
        // Build query parameters for a simple search
        const params = new URLSearchParams();
        if (searchQuery) params.append('name', searchQuery);
        params.append('limit', limit);
        
        const response = await fetch(`/api/neo4j/search_businesses?${params.toString()}`);
        const data = await response.json();
        
        // Extract businesses from the response
        const businesses = data.businesses || [];
        
        return businesses.map(b => ({
            id: b.business_id,
            name: b.business_name || 'Unnamed Business',
            location: `${b.city || 'N/A'}, ${b.state || 'N/A'}`,
            stars: b.stars
        }));
    } catch (error) {
        console.error('Error searching businesses for ID:', error);
        return [];
    }
}

async function showBusinessIdFinder(targetInputId) {
    const searchQuery = prompt("Enter business name to search");
    if (!searchQuery) return;
    
    try {
        const businesses = await searchBusinessForId(searchQuery);
        
        if (businesses.length === 0) {
            alert("No businesses found matching that name");
            return;
        }
        
        // Create a simple selection dialog
        let html = '<div class="list-group">';
        businesses.forEach(b => {
            html += `
                <button type="button" class="list-group-item list-group-item-action" 
                  onclick="document.getElementById('${targetInputId}').value='${b.id}'; this.closest('.modal').remove()">
                    <div class="d-flex w-100 justify-content-between">
                        <h5 class="mb-1">${b.name}</h5>
                        <small>${formatStarRating(b.stars)}</small>
                    </div>
                    <p class="mb-1">${b.location}</p>
                    <small class="text-muted">ID: ${b.id}</small>
                </button>
            `;
        });
        html += '</div>';
        
        // Create modal
        const modal = document.createElement('div');
        modal.className = 'modal fade show';
        modal.style.display = 'block';
        modal.style.backgroundColor = 'rgba(0,0,0,0.5)';
        modal.tabIndex = -1;
        
        modal.innerHTML = `
            <div class="modal-dialog">
                <div class="modal-content">
                    <div class="modal-header">
                        <h5 class="modal-title">Select a Business</h5>
                        <button type="button" class="btn-close" onclick="this.closest('.modal').remove()"></button>
                    </div>
                    <div class="modal-body">
                        ${html}
                    </div>
                </div>
            </div>
        `;
        
        document.body.appendChild(modal);
    } catch (error) {
        console.error('Error showing business finder:', error);
        alert('Error searching for businesses');
    }
}

// Initialize Neo4j event listeners
function initializeNeo4jEventListeners() {
    // Overview tab
    document.getElementById('neo4j-overview-tab').addEventListener('shown.bs.tab', function (e) {
        loadNeo4jOverviewStats();
    });

    document.getElementById('neo4j-analytics-tab')?.addEventListener('shown.bs.tab', function (e) {
        loadGraphAnalytics();
    });
    
    // Business search
    document.getElementById('neo4jSearchButton')?.addEventListener('click', () => searchNeo4jBusinesses(1));
    document.getElementById('neo4jClearButton')?.addEventListener('click', clearNeo4jSearch);
    
    // Top businesses
    document.getElementById('loadNeo4jBusinesses')?.addEventListener('click', loadNeo4jBusinesses);
    
    // Network analysis
    document.getElementById('loadNeo4jNetwork')?.addEventListener('click', loadNeo4jNetwork);
    
    // Path finder
    document.getElementById('findPathButton')?.addEventListener('click', findBusinessPaths);
    
    // Business recommendations
    document.getElementById('loadBusinessRecommendations')?.addEventListener('click', function() {
        const businessId = document.getElementById('neo4jRecommendBusinessId').value.trim();
        if (businessId) {
            loadBusinessRecommendations(businessId, 'main');
        } else {
            alert('Please enter a business ID');
        }
    });
    
    // User recommendations
    document.getElementById('loadUserRecommendations')?.addEventListener('click', loadUserRecommendations);
    
    // Graph analytics
    document.getElementById('loadGraphAnalytics')?.addEventListener('click', loadGraphAnalytics);
    
    // Review sort change
    document.getElementById('neo4jReviewSortSelect')?.addEventListener('change', function() {
        const businessDetails = document.getElementById('neo4jBusinessDetails');
        const businessId = businessDetails.getAttribute('data-business-id');
        if (businessId) {
            loadBusinessReviews(businessId, 1, this.value);
        }
    });
    
    // Main tab selection
    document.getElementById('neo4j-tab')?.addEventListener('shown.bs.tab', function (e) {
        // Check if the overview tab is active
        if (document.getElementById('neo4j-overview-tab').classList.contains('active')) {
            loadNeo4jOverviewStats();
        }
    });
    
    // Business details tabs
    document.getElementById('neo4j-reviews-tab')?.addEventListener('click', function() {
        const businessDetails = document.getElementById('neo4jBusinessDetails');
        const businessId = businessDetails.getAttribute('data-business-id');
        if (businessId) {
            loadBusinessReviews(businessId);
        }
    });
    
    document.getElementById('neo4j-network-details-tab')?.addEventListener('click', function() {
        const businessDetails = document.getElementById('neo4jBusinessDetails');
        const businessId = businessDetails.getAttribute('data-business-id');
        if (businessId) {
            loadBusinessNetworkDetails(businessId);
        }
    });
    
    document.getElementById('neo4j-recommend-tab')?.addEventListener('click', function() {
        const businessDetails = document.getElementById('neo4jBusinessDetails');
        const businessId = businessDetails.getAttribute('data-business-id');
        if (businessId) {
            loadBusinessRecommendations(businessId, 'details');
        }
    });
}

// Initialize when the DOM is loaded
document.addEventListener('DOMContentLoaded', function() {
    initializeNeo4jEventListeners();
    
    // When clicking on the businesses tab, auto-load the businesses
    document.getElementById('neo4j-businesses-tab')?.addEventListener('shown.bs.tab', function (e) {
        loadNeo4jBusinesses();
        searchNeo4jBusinesses(1);
    });
});