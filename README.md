# Query Execution Service

This application allows users to submit read-only SQL queries for
asynchronous execution and fetch results once completed.

------------------------------------------------------------------------

## How the App Works

1.  Submit a SQL query
2.  Query runs asynchronously
3.  Poll for status
4.  Fetch results when ready
5.  Optionally cancel a running query

------------------------------------------------------------------------

## Setup

### Prerequisites

-   Java 17+
-   Maven
-   Docker
-   Bash

### Start Database

``` bash
docker compose up -d
```

### Build Application

``` bash
mvn clean package
```

### Run Application

``` bash
java -jar target/grepr-application.jar server config.yml
```

App runs at: http://localhost:8080

------------------------------------------------------------------------

## Authentication

All requests require:

    Authorization: Bearer user:user1

------------------------------------------------------------------------

## API Endpoints

### Health Check

    GET /ping

### Submit Query

    POST /queries

Body:

``` json
{ "sql": "select * from orders limit 10" }
```

### Get Query Status

    GET /queries/{id}

### Get Results

    GET /queries/{id}/results

### Cancel Query

    POST /queries/{id}/cancel

------------------------------------------------------------------------

## Testing

Run integration tests:

``` bash
chmod +x test.sh
./test.sh
```

------------------------------------------------------------------------
## Business Schema

The application ships with a sample e-commerce business schema (customers, products, orders, order_items) that represents realistic transactional data users can query.
These tables are read-only and exist to demonstrate analytics style SELECT queries over relational business data.

## Notes

-   Only SELECT queries are allowed
-   Requests are rate limited per user
-   Users can only access their own queries
