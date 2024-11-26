# METRO Shopping Store Near-Real-Time Data Warehouse

This project implements a near-real-time data warehouse for METRO Shopping Store using the MESHJOIN algorithm to process streaming transaction data and enrich it with master data.

## Project Structure
```
project/
├── src/main/java/com/metro/warehouse/
│   ├── model/
│   │   ├── Transaction.java
│   │   ├── Customer.java
│   │   ├── Product.java
│   │   └── WarehouseRecord.java
│   ├── meshjoin/
│   │   └── MeshJoin.java
│   └── Main.java
├── create_master_tables.sql
├── create_transactions_table.sql
├── create_warehouse_table.sql
├── olap_queries.sql
├── pom.xml
└── README.md
```

## Prerequisites
1. Java JDK 8 or higher
2. Maven
3. MySQL 8.0 or higher
4. MySQL Connector/J

## Setup Instructions

### 1. Database Setup
1. Start MySQL server and login:
   ```bash
   mysql -u root -p
   ```

2. Create the database:
   ```sql
   CREATE DATABASE metro_dw;
   USE metro_dw;
   ```

3. Enable local infile loading:
   ```sql
   SET GLOBAL local_infile = 1;
   ```

4. Create and load tables (run in order):
   ```bash
   mysql -u root -p metro_dw < create_master_tables.sql
   mysql -u root -p metro_dw < create_transactions_table.sql
   mysql -u root -p metro_dw < create_warehouse_table.sql
   ```

### 2. Java Project Setup
1. Build the project using Maven:
   ```bash
   cd /path/to/project
   mvn clean compile assembly:single
   ```

2. The build will create a JAR file with dependencies in the target directory:
   ```
   target/warehouse-etl-1.0-SNAPSHOT-jar-with-dependencies.jar
   ```

## Eclipse IDE Setup

### Prerequisites for Eclipse
1. Eclipse IDE for Java Developers (2021-12 or newer recommended)
2. Make sure you have Java JDK 11 installed (required by the project)
3. Maven Integration for Eclipse (m2e) plugin - usually comes pre-installed

### Importing the Project
1. Open Eclipse IDE
2. Go to `File` > `Import`
3. Expand `Maven` folder and select `Existing Maven Projects`
4. Click `Next`
5. Click `Browse` and navigate to your project directory:
   ```
   /path/to/metro-dw/project
   ```
6. The `pom.xml` file should be automatically selected
7. Click `Finish` and wait for the project to be imported and dependencies to be downloaded

### Configuring Database Connection
1. Open `src/main/resources/application.properties` (if it doesn't exist, create it)
2. Add your database configuration:
   ```properties
   db.url=jdbc:mysql://localhost:3306/metro_dw
   db.username=your_username
   db.password=your_password
   ```

### Running the Project
1. In the Project Explorer, expand the project
2. Navigate to `src/main/java/com/metro/warehouse/Main.java`
3. Right-click on `Main.java`
4. Select `Run As` > `Java Application`

### Troubleshooting Common Issues
1. **Build Path Errors**
   - Right-click on project > `Maven` > `Update Project`
   - Check `Force Update of Snapshots/Releases`
   - Click `OK`

2. **Java Version Mismatch**
   - Right-click on project > `Properties`
   - Go to `Java Compiler`
   - Ensure Compiler compliance level is set to 11
   - Click `Apply and Close`

3. **Database Connection Issues**
   - Verify MySQL is running: `systemctl status mysql`
   - Check if database exists: `mysql -u root -p -e "SHOW DATABASES;"`
   - Ensure database credentials in `application.properties` are correct

4. **Runtime Errors**
   - Check Console view for error messages
   - Verify all SQL scripts have been executed in correct order
   - Ensure MySQL Connector/J is properly included (check `pom.xml`)

### Running Tests
1. Right-click on project
2. Select `Run As` > `JUnit Test`
3. Test results will appear in the JUnit view

### Debugging
1. Double-click on the left margin of `Main.java` to set breakpoints
2. Right-click on `Main.java`
3. Select `Debug As` > `Java Application`
4. Use the Debug perspective to step through code

## Running the ETL Process

1. Execute the MESHJOIN ETL process:
   ```bash
   java -jar target/warehouse-etl-1.0-SNAPSHOT-jar-with-dependencies.jar
   ```

2. The process will:
   - Read transactions in chunks
   - Join with master data (customers and products)
   - Calculate sales amounts
   - Load enriched data into the warehouse

3. Monitor progress in the console output

## Verifying Data Loading

1. Check record counts:
   ```sql
   SELECT COUNT(*) FROM WAREHOUSE_SALES;
   ```
   Expected: 30,247 records

2. Verify data enrichment:
   ```sql
   SELECT * FROM WAREHOUSE_SALES LIMIT 5;
   ```

## Running OLAP Queries

1. Create helper functions and execute queries:
   ```sql
   source olap_queries.sql
   ```

2. Available analyses:
   - Q1: Top Revenue-Generating Products (Weekday vs Weekend)
   - Q2: Store Revenue Growth Rate
   - Q3: Supplier Sales Contribution
   - Q4: Seasonal Analysis
   - Q5: Revenue Volatility
   - Q6: Product Affinity Analysis
   - Q7: ROLLUP Analysis
   - Q8: H1/H2 Analysis
   - Q9: Sales Spike Analysis
   - Q10: Regional Quarterly Sales View

## Implementation Details

### MESHJOIN Algorithm Components
1. Stream Processing:
   - Chunks of 100 transactions
   - Concurrent queue for thread safety

2. Master Data Management:
   - In-memory hash tables for customers and products
   - Concurrent hash maps for thread safety

3. Join Processing:
   - Multi-threaded implementation
   - Enrichment with calculated sales

### Data Quality Measures
- Primary key constraints prevent duplicates
- Foreign key relationships ensure data integrity
- Data validation in Java code
- Error handling and logging

## Troubleshooting

1. If you encounter "Table doesn't exist" errors:
   - Verify table names match exactly (case-sensitive)
   - Ensure all SQL scripts were executed

2. If you see connection issues:
   - Verify MySQL is running
   - Check credentials in Java code
   - Ensure database name is correct

3. For performance issues:
   - Check available system memory
   - Verify MySQL buffer pool size
   - Consider adjusting chunk size in MeshJoin.java

## Contact
For any questions or issues, please contact:
[Your Name]
[Your Email/Contact Information]
