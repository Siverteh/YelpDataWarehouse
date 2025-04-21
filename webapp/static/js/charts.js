/**
 * Common chart utilities for Yelp Data Warehouse Dashboard
 */

// Default chart colors
const chartColors = {
    red: 'rgba(211, 35, 35, 0.7)',
    blue: 'rgba(52, 152, 219, 0.7)',
    green: 'rgba(46, 204, 113, 0.7)',
    yellow: 'rgba(241, 196, 15, 0.7)',
    orange: 'rgba(230, 126, 34, 0.7)',
    purple: 'rgba(155, 89, 182, 0.7)',
    teal: 'rgba(26, 188, 156, 0.7)'
};

// Color arrays for different chart types
const barChartColors = [
    chartColors.red,
    chartColors.blue,
    chartColors.green,
    chartColors.yellow,
    chartColors.orange
];

const pieChartColors = [
    chartColors.blue,
    chartColors.green,
    chartColors.yellow,
    chartColors.orange,
    chartColors.red,
    chartColors.purple,
    chartColors.teal
];

const lineChartColors = [
    {
        borderColor: '#d32323',
        backgroundColor: 'rgba(211, 35, 35, 0.1)'
    },
    {
        borderColor: '#4682B4',
        backgroundColor: 'rgba(70, 130, 180, 0.1)'
    },
    {
        borderColor: '#2ecc71',
        backgroundColor: 'rgba(46, 204, 113, 0.1)'
    }
];

// Chart creation utilities
function createBarChart(ctx, labels, data, title = null, yAxisMax = null) {
    return new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                data: data,
                backgroundColor: barChartColors
            }]
        },
        options: {
            animation: false,
            responsiveAnimationDuration: 0,
            responsive: true,
            plugins: {
                title: {
                    display: title !== null,
                    text: title
                },
                legend: {
                    display: false
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    max: yAxisMax
                }
            }
        }
    });
}

function createPieChart(ctx, labels, data, title = null) {
    return new Chart(ctx, {
        type: 'pie',
        data: {
            labels: labels,
            datasets: [{
                data: data,
                backgroundColor: pieChartColors.slice(0, data.length)
            }]
        },
        options: {
            animation: false,
            responsiveAnimationDuration: 0,
            responsive: true,
            plugins: {
                title: {
                    display: title !== null,
                    text: title
                }
            }
        }
    });
}

function createLineChart(ctx, labels, datasets, title = null) {
    // Format datasets with colors if not provided
    const formattedDatasets = datasets.map((dataset, index) => {
        const colorIndex = index % lineChartColors.length;
        return {
            ...dataset,
            borderColor: dataset.borderColor || lineChartColors[colorIndex].borderColor,
            backgroundColor: dataset.backgroundColor || lineChartColors[colorIndex].backgroundColor,
            fill: dataset.fill !== undefined ? dataset.fill : true,
            tension: 0 // Set tension to 0 to remove curve animation
        };
    });
    
    return new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: formattedDatasets
        },
        options: {
            animation: false,
            responsiveAnimationDuration: 0,
            responsive: true,
            plugins: {
                title: {
                    display: title !== null,
                    text: title
                }
            },
            scales: {
                y: {
                    beginAtZero: true
                }
            }
        }
    });
}

// Data format utilities
function formatMonthYearData(data) {
    // Convert array of objects with year and month properties into a sorted array
    // and generate appropriate labels
    const sortedData = [...data].sort((a, b) => {
        if (a.year !== b.year) return a.year - b.year;
        return a.month - b.month;
    });
    
    const labels = sortedData.map(item => {
        // Convert month number to name
        const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
        const monthName = monthNames[(item.month - 1) % 12];
        return `${monthName} ${item.year}`;
    });
    
    return {
        labels,
        sortedData
    };
}

// Chart clearing utility
function clearCharts(charts) {
    for (const chart of charts) {
        if (chart) {
            chart.destroy();
        }
    }
}