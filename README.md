# Yelp Data Warehouse

A comprehensive multi-model data warehouse implementation for the Yelp dataset using relational (MySQL), document-based (MongoDB), and graph-based (Neo4j) database systems, with data streaming via Kafka.

## Project Overview

This project creates a data warehouse for Yelp business, user, and review data, implementing it across three different database paradigms:

1. **Relational Data Warehouse (MySQL)**: Traditional star schema implementation
2. **Document Database (MongoDB)**: JSON document-based implementation
3. **Graph Database (Neo4j)**: Graph-based implementation focusing on relationships

Additionally, the project includes a real-time data streaming component using Kafka, and a unified web dashboard for visualizing and analyzing data across all three database systems.

## Architecture

![Architecture Diagram](docs/architecture_diagram.png)

The architecture consists of the following components:

- **Data Sources**: Yelp Academic Dataset (5 JSON files)
- **ETL Pipeline**: Python scripts for data extraction, transformation, and loading
- **Data Storage**:
  - MySQL: Star schema with dimension and fact tables
  - MongoDB: Document collections with embedded documents
  - Neo4j: Graph database with nodes and relationships
- **Data Streaming**: Kafka producer and consumer for real-time data updates
- **Web Dashboard**: Flask-based web application with unified interface

## Prerequisites

- Docker and Docker Compose
- Python 3.9+
- Yelp dataset files (see below)

## Yelp Dataset

This project uses the [Yelp Academic Dataset](https://www.yelp.com/dataset), which consists of five JSON files:

1. `yelp_academic_dataset_business.json`: Information about businesses
2. `yelp_academic_dataset_user.json`: Information about users
3. `yelp_academic_dataset_review.json`: Reviews by users for businesses
4. `yelp_academic_dataset_checkin.json`: Check-ins at businesses
5. `yelp_academic_dataset_tip.json`: Tips by users for businesses

Place these files in the `data/` directory at the root of the project.

## Setup and Installation

1. **Clone the repository**:
   ```bash
   git clone https://github.com/yourusername/yelp-data-warehouse.git
   cd yelp-data-warehouse
   ```

2. **Create a data directory and add Yelp dataset files**:
   ```bash
   mkdir -p data
   # Copy or download the Yelp dataset files to the data directory
   ```

3. **Build and start the Docker containers**:
   ```bash
   sudo docker-compose up -d
   ```

4. **Load data into the databases**:
   ```bash
   # Load data into MySQL
   python scripts/etl/sql/load_data.py
   
   # Load data into MongoDB
   python scripts/etl/mongodb/load_data.py
   
   # Load data into Neo4j
   python scripts/etl/neo4j/load_data.py
   ```

5. **Access the web dashboard**:
   Open your browser and navigate to `http://localhost:8080`

## Data Streaming with Kafka

To start the real-time data streaming simulation:

```bash
# Make the script executable
chmod +x kafka_starter.sh

# Run the starter script
./kafka_starter.sh
```

This will:
1. Create necessary Kafka topics
2. Start a producer that simulates real-time Yelp data events
3. Start a consumer that processes these events and updates all three databases

## Project Structure

```
yelp-data-warehouse/
├── data/                    # Yelp dataset files
├── docs/                    # Documentation and diagrams
├── scripts/
│   └── etl/                 # ETL scripts for each database
│       ├── sql/             # MySQL ETL scripts
│       ├── mongodb/         # MongoDB ETL scripts
│       └── neo4j/           # Neo4j ETL scripts
├── webapp/                  # Flask web application
│   ├── templates/           # HTML templates
│   ├── app.py               # Flask application
│   ├── Dockerfile           # Dockerfile for webapp
│   └── requirements.txt     # Python dependencies
├── kafka_producer.py        # Kafka producer for data streaming
├── kafka_consumer.py        # Kafka consumer for data processing
├── kafka_starter.sh         # Script to start Kafka streaming
├── docker-compose.yml       # Docker Compose configuration
├── .env                     # Environment variables
└── README.md                # This README file
```

## Database Schema Design

### MySQL Star Schema

The MySQL implementation follows a traditional star schema design with:

- **Dimension Tables**:
  - `dim_time`: Time dimension with date hierarchy
  - `dim_location`: Location dimension with city, state, etc.
  - `dim_business`: Business dimension
  - `dim_user`: User dimension
  - `dim_category`: Category dimension

- **Fact Tables**:
  - `fact_review`: Reviews with foreign keys to dimensions
  - `fact_checkin`: Check-ins with foreign keys to dimensions

- **Summary Tables**:
  - `summary_business_performance`: Pre-aggregated business metrics

### MongoDB Document Schema

The MongoDB implementation uses the following collections:

- `businesses`: Business documents with embedded location and categories
- `users`: User documents with embedded user information
- `reviews`: Review documents with references to businesses and users
- `checkins`: Check-in documents with references to businesses
- `business_summaries`: Pre-aggregated business performance summaries

### Neo4j Graph Schema

The Neo4j implementation uses the following node and relationship types:

- **Nodes**:
  - `Business`: Business nodes
  - `User`: User nodes
  - `Review`: Review nodes
  - `Category`: Category nodes
  - `Location`: Location nodes
  - `Time`: Time nodes

- **Relationships**:
  - `LOCATED_IN`: Business to Location
  - `IN_CATEGORY`: Business to Category
  - `WROTE`: User to Review
  - `REVIEWS`: Review to Business
  - `ON_DATE`: Review to Time
  - `HAD_CHECKIN`: Business to Time (with count property)
  - `FRIENDS_WITH`: User to User

## Dashboard Features

The web dashboard provides a unified interface for analyzing data across all three database systems:

- Overview stats and charts for each database
- Business search and filtering
- Detailed business performance analysis
- Category and rating distribution visualizations
- Network analysis (Neo4j specific)
- Comparison of database implementation approaches

## Conclusion

This project demonstrates the implementation of a comprehensive data warehouse using three different database paradigms, highlighting the strengths and trade-offs of each approach. The relational model provides structured query capabilities, the document model offers flexibility for hierarchical data, and the graph model excels at relationship-based analysis.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- Yelp for providing the academic dataset
- The MySQL, MongoDB, and Neo4j communities for their excellent documentation