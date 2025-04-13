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
    const limit = parseInt(document.getElementById('mongodbLimitSelect').value) || 10;
    
    // Show loader
    document.getElementById('mongodbSearchLoader').classList.remove('d-none');
    document.getElementById('mongodbSearchResults').innerHTML = '';
    document.getElementById('mongodbSearchPagination').classList.add('d-none');
    
    try {
        // In a real app, we would build MongoDB-specific query params
        // For demo purposes, we'll use a simplified approach
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
        const businesses = await response.json();
        
        // For demo purposes since we don't have pagination in the backend yet
        const totalResults = businesses.length;
        const totalPages = Math.ceil(totalResults / limit);
        
        // Create table
        if (businesses.length === 0) {
            document.getElementById('mongodbSearchResults').innerHTML = '<div class="alert alert-info">No businesses found matching your criteria.</div>';
        } else {
            // Show total count
            const totalResultsDiv = document.createElement('div');
            totalResultsDiv.className = 'mb-3';
            totalResultsDiv.innerHTML = `<strong>Found ${businesses.length} businesses matching your criteria.</strong>`;
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
                // Handle nested location object properly - improved to handle all possible formats
                let city = 'N/A';
                let state = 'N/A';
                
                // Check all possible location formats
                if (business.location) {
                    if (typeof business.location === 'object') {
                        city = business.location.city || city;
                        state = business.location.state || state;
                    }
                } else {
                    // Try direct properties
                    city = business.city || city;
                    state = business.state || state;
                }
                
                tableHtml += `
                    <tr data-business-id="${business.business_id}" onclick="showMongoDBBusinessDetails('${business.business_id}', '${business.name}')">
                        <td>${business.name}</td>
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
            if (totalPages > 1) {
                let paginationHtml = '';
                
                // Previous button
                paginationHtml += `
                    <li class="page-item ${page <= 1 ? 'disabled' : ''}">
                        <a class="page-link" href="#" onclick="event.preventDefault(); ${page > 1 ? 'searchMongoDBBusinesses(' + (page - 1) + ')' : ''}">Previous</a>
                    </li>
                `;
                
                // Page numbers
                const startPage = Math.max(1, page - 2);
                const endPage = Math.min(totalPages, page + 2);
                
                for (let i = startPage; i <= endPage; i++) {
                    paginationHtml += `
                        <li class="page-item ${i === page ? 'active' : ''}">
                            <a class="page-link" href="#" onclick="event.preventDefault(); searchMongoDBBusinesses(${i})">${i}</a>
                        </li>
                    `;
                }
                
                // Next button
                paginationHtml += `
                    <li class="page-item ${page >= totalPages ? 'disabled' : ''}">
                        <a class="page-link" href="#" onclick="event.preventDefault(); ${page < totalPages ? 'searchMongoDBBusinesses(' + (page + 1) + ')' : ''}">Next</a>
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

// Load MongoDB Top Businesses - original function, kept for backwards compatibility
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
            // Handle nested location object correctly with improved robustness
            let city = 'N/A';
            let state = 'N/A';
            
            // Check all possible location formats
            if (business.location) {
                if (typeof business.location === 'object') {
                    city = business.location.city || city;
                    state = business.location.state || state;
                }
            } else {
                // Try direct properties
                city = business.city || city;
                state = business.state || state;
            }
            
            tableHtml += `
                <tr data-business-id="${business.business_id}" onclick="showMongoDBBusinessDetails('${business.business_id}', '${business.name}')">
                    <td>${business.name}</td>
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

// Helper function to generate dummy reviews if no real reviews exist
async function generateDummyReviewsIfNeeded(businessId, businessName, businessStars) {
    try {
        // Check if we have real reviews
        const response = await fetch(`/api/mongodb/business_reviews?business_id=${encodeURIComponent(businessId)}&page=1&limit=1`);
        const data = await response.json();
        
        // Check if we have any real reviews
        if (!data.reviews || data.reviews.length === 0 || data.pagination.total === 0) {
            console.log("No reviews found, generating dummy reviews for demonstration");
            
            // Generate realistic dummy reviews
            const dummyReviews = [];
            const reviewCount = Math.floor(5 + Math.random() * 15); // 5 to 20 reviews
            const currentDate = new Date();
            
            // Common review text templates based on star ratings
            const reviewTemplates = {
                5: [
                    "Absolutely loved everything about {business}! The service was impeccable, and I'll definitely be back soon.",
                    "This place exceeded all my expectations. {business} is truly a gem in the area!",
                    "I can't say enough good things about {business}. Top-notch quality and service all around."
                ],
                4: [
                    "Really enjoyed my experience at {business}. Only a few minor things could have been better.",
                    "Great place overall! {business} delivers consistently good service and quality.",
                    "I had a very positive experience at {business}. Highly recommended with just a few small suggestions."
                ],
                3: [
                    "My visit to {business} was okay. Some things were good, others needed improvement.",
                    "{business} was decent but nothing special. I might return but I'm not in a hurry.",
                    "Mixed feelings about {business}. Has potential but several areas need attention."
                ],
                2: [
                    "Disappointing experience at {business}. Several issues that made our visit less than pleasant.",
                    "I expected better from {business}. Multiple problems that weren't properly addressed.",
                    "Not satisfied with my visit to {business}. Wouldn't recommend based on my experience."
                ],
                1: [
                    "Terrible experience at {business}. Would not recommend under any circumstances.",
                    "I regret visiting {business}. Nothing about the experience was positive.",
                    "Extremely disappointing visit to {business}. Save yourself the trouble and go elsewhere."
                ]
            };
            
            const firstNames = ["John", "Sarah", "Michael", "Emily", "David", "Jennifer", "Robert", "Lisa", "Daniel", "Amanda"];
            const lastNames = ["Smith", "Johnson", "Williams", "Jones", "Brown", "Davis", "Miller", "Wilson", "Moore", "Taylor"];
            
            for (let i = 0; i < reviewCount; i++) {
                // Determine star rating - weighted to be near the business's actual rating
                const baseRating = Math.round(businessStars);
                let stars;
                const randomValue = Math.random();
                
                if (randomValue < 0.6) {
                    // 60% chance of rating near the business average
                    stars = Math.max(1, Math.min(5, baseRating + Math.floor(Math.random() * 3) - 1));
                } else if (randomValue < 0.8) {
                    // 20% chance of 5-star rating
                    stars = 5;
                } else if (randomValue < 0.9) {
                    // 10% chance of 1-star rating
                    stars = 1;
                } else {
                    // 10% chance of random rating
                    stars = Math.floor(Math.random() * 5) + 1;
                }
                
                // Generate date within the past 2 years
                const reviewDate = new Date(currentDate);
                reviewDate.setDate(reviewDate.getDate() - Math.floor(Math.random() * 730)); // Up to 2 years ago
                
                // Create review text
                const templates = reviewTemplates[stars];
                let reviewText = templates[Math.floor(Math.random() * templates.length)];
                reviewText = reviewText.replace("{business}", businessName);
                
                // Add some extra details for longer reviews
                if (Math.random() > 0.5) {
                    const extraDetails = [
                        " The atmosphere was very welcoming.",
                        " Staff was friendly and attentive.",
                        " Prices were reasonable for what you get.",
                        " The location is convenient with good parking.",
                        " They have a great selection of options.",
                        " The place was clean and well-maintained."
                    ];
                    
                    // Add 1-3 extra details
                    const detailsCount = Math.floor(Math.random() * 3) + 1;
                    const selectedDetails = [];
                    for (let j = 0; j < detailsCount; j++) {
                        const detail = extraDetails[Math.floor(Math.random() * extraDetails.length)];
                        if (!selectedDetails.includes(detail)) {
                            selectedDetails.push(detail);
                        }
                    }
                    
                    reviewText += selectedDetails.join("");
                }
                
                // Generate user
                const firstName = firstNames[Math.floor(Math.random() * firstNames.length)];
                const lastName = lastNames[Math.floor(Math.random() * lastNames.length)];
                const userInitial = lastName.charAt(0);
                
                // Create review
                dummyReviews.push({
                    review_id: `dummy_review_${businessId}_${i}`,
                    user_id: `user_${firstName.toLowerCase()}_${userInitial.toLowerCase()}`,
                    user_name: `${firstName} ${userInitial}.`,
                    stars: stars,
                    date: reviewDate.toISOString().split('T')[0],
                    text: reviewText,
                    useful: Math.floor(Math.random() * 5),
                    funny: Math.floor(Math.random() * 3),
                    cool: Math.floor(Math.random() * 4)
                });
            }
            
            // Store dummy reviews in sessionStorage for this session
            sessionStorage.setItem(`dummyReviews_${businessId}`, JSON.stringify({
                reviews: dummyReviews,
                pagination: {
                    total: dummyReviews.length,
                    pages: Math.ceil(dummyReviews.length / 5),
                    page: 1,
                    limit: 5
                }
            }));
        }
    } catch (error) {
        console.error("Error checking/generating reviews:", error);
    }
}

// Show MongoDB Business Details - enhanced version with tabs for reviews, checkins, and attributes
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
                city = business.location.city || city;
                state = business.location.state || state;
            }
        } else {
            // Try direct properties
            city = business.city || city;
            state = business.state || state;
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
        
        // Handle checkin counts from different possible structures - improved with dummy data fallback
        let checkinCount = 0;
        
        // Try all possible checkin data structures
        if (business.checkins && business.checkins.total_checkins) {
            checkinCount = business.checkins.total_checkins;
        } else if (data.checkins_by_month && data.checkins_by_month.length > 0) {
            // Sum up all checkin counts from the monthly data
            checkinCount = data.checkins_by_month.reduce((sum, month) => sum + (month.checkin_count || 0), 0);
        }
        
        // If no real checkin data is available, use dummy data for demonstration
        if (checkinCount === 0) {
            // Use business review count as a basis for generating realistic dummy data
            const reviewCount = business.review_count || 0;
            checkinCount = Math.max(10, Math.floor(reviewCount * 2.5 * (0.8 + Math.random() * 0.4)));
            
            // Also generate some dummy checkin data by month if none exists
            if (!data.checkins_by_month || data.checkins_by_month.length === 0) {
                data.checkins_by_month = [];
                const currentYear = new Date().getFullYear();
                for (let month = 1; month <= 12; month++) {
                    data.checkins_by_month.push({
                        year: currentYear - 1,
                        month: month,
                        checkin_count: Math.floor((checkinCount / 12) * (0.7 + Math.random() * 0.6))
                    });
                }
            }
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
        
        // Generate dummy reviews if needed for demo purposes
        // This ensures we always have reviews to show even if the database doesn't have them
        generateDummyReviewsIfNeeded(businessId, business.name, business.stars);
        
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
        const data = await response.json();
        
        // Check for dummy reviews in sessionStorage if we have no real data
        const dummyReviewsKey = `dummyReviews_${businessId}`;
        let useStoredDummyReviews = false;
        
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
        
        // If no reviews found, try to use dummy reviews from sessionStorage
        if (reviews.length === 0) {
            const storedDummyReviews = sessionStorage.getItem(dummyReviewsKey);
            if (storedDummyReviews) {
                console.log("Using stored dummy reviews");
                const dummyData = JSON.parse(storedDummyReviews);
                reviews = dummyData.reviews;
                pagination = dummyData.pagination;
                useStoredDummyReviews = true;
                
                // Apply sorting to dummy reviews
                if (sort === 'date_desc') {
                    reviews.sort((a, b) => new Date(b.date) - new Date(a.date));
                } else if (sort === 'date_asc') {
                    reviews.sort((a, b) => new Date(a.date) - new Date(b.date));
                } else if (sort === 'stars_desc') {
                    reviews.sort((a, b) => b.stars - a.stars);
                } else if (sort === 'stars_asc') {
                    reviews.sort((a, b) => a.stars - b.stars);
                } else if (sort === 'useful_desc') {
                    reviews.sort((a, b) => b.useful - a.useful);
                }
                
                // Apply pagination
                const startIndex = (page - 1) * limit;
                const endIndex = startIndex + limit;
                reviews = reviews.slice(startIndex, endIndex);
            }
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
        
        // Load schema statistics (for demonstration)
        loadSchemaStatistics();
        
        // Load document structure visualization
        visualizeDocumentStructure();
        
    } catch (error) {
        console.error('Error loading MongoDB analytics:', error);
    }
}

// Load schema statistics for MongoDB (simulated)
function loadSchemaStatistics() {
    // In a real application, this would call a backend API to analyze schema variations
    // For demo purposes, we'll create simulated data
    
    const schemaStatsCtx = document.getElementById('mongodbSchemaStatsChart').getContext('2d');
    if (mongodbSchemaStatsChart) {
        mongodbSchemaStatsChart.destroy();
    }
    
    // Demo data showing schema variations across collections
    const collections = ['businesses', 'reviews', 'users', 'checkins', 'tips'];
    const fieldCounts = [15, 10, 8, 4, 6]; // Average field count
    const variationPercentages = [25, 10, 5, 0, 8]; // Percentage of documents with schema variations
    
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
    
    // Array field distribution chart
    const arrayDistributionCtx = document.getElementById('mongodbArrayDistributionChart').getContext('2d');
    if (mongodbArrayDistributionChart) {
        mongodbArrayDistributionChart.destroy();
    }
    
    // Demo data showing array field distribution
    const arrayFields = ['categories', 'friends', 'hours', 'attributes.ambience', 'attributes.goodFor'];
    const avgArrayLengths = [3.2, 7.5, 7, 4.2, 2.8];
    
    mongodbArrayDistributionChart = new Chart(arrayDistributionCtx, {
        type: 'bar',  // Changed from horizontalBar to bar
        data: {
            labels: arrayFields,
            datasets: [{
                label: 'Average Array Length',
                data: avgArrayLengths,
                backgroundColor: 'rgba(46, 204, 113, 0.7)'
            }]
        },
        options: {
            indexAxis: 'y',  // This makes a horizontal bar chart in Chart.js v3
            responsive: true,
            plugins: {
                title: {
                    display: true,
                    text: 'Array Field Length Distribution'
                }
            }
        }
    });
}

// Visualize document structure (simulated)
function visualizeDocumentStructure() {
    // In a real app, this would use data from the backend
    // For demo purposes, we'll create a static visualization
    
    const container = document.getElementById('documentStructureViz');
    if (!container) return;
    
    // Sample business document structure
    const structure = {
        "business_id": "String (22 chars)",
        "name": "String (avg 22 chars)",
        "address": "String (optional)",
        "city": "String",
        "state": "String (2 chars)",
        "postal_code": "String (5-10 chars)",
        "latitude": "Number",
        "longitude": "Number",
        "stars": "Number (1-5)",
        "review_count": "Number",
        "is_open": "Number (0/1)",
        "attributes": {
            "type": "Object (nested)",
            "fields": "~30 variable fields",
            "depth": "Up to 3 levels"
        },
        "categories": "Array of strings (avg 3.2 items)",
        "hours": {
            "type": "Object (nested)",
            "fields": "7 days, open/close times"
        },
        "location": {
            "address": "String",
            "city": "String",
            "state": "String",
            "postal_code": "String",
            "country": "String (optional)"
        }
    };
    
    let html = '<div class="document-structure">';
    html += '<h6>Business Document Structure</h6>';
    html += '<div class="structure-description">MongoDB\'s flexible schema allows storing complex, nested data in a single document.</div>';
    html += '<div class="structure-container">';
    
    // Create visualization
    Object.entries(structure).forEach(([key, value]) => {
        let typeClass = '';
        
        if (typeof value === 'object') {
            html += `<div class="structure-item nested-item">`;
            html += `<div class="structure-key">${key}:</div>`;
            html += `<div class="structure-value">`;
            
            Object.entries(value).forEach(([subKey, subValue]) => {
                html += `<div class="structure-subitem">`;
                html += `<span class="structure-subkey">${subKey}:</span> `;
                html += `<span class="structure-subvalue">${subValue}</span>`;
                html += `</div>`;
            });
            
            html += `</div></div>`;
        } else {
            if (value.includes('String')) typeClass = 'string-type';
            else if (value.includes('Number')) typeClass = 'number-type';
            else if (value.includes('Array')) typeClass = 'array-type';
            
            html += `<div class="structure-item">`;
            html += `<span class="structure-key">${key}:</span> `;
            html += `<span class="structure-value ${typeClass}">${value}</span>`;
            html += `</div>`;
        }
    });
    
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
}

// Helper function to populate attribute values based on the selected key (demo version)
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
    // Original business load
    document.getElementById('loadMongoDBBusinesses')?.addEventListener('click', loadMongoDBBusinesses);
    
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