# Credit Card Fraud Detection

This project focuses on creating a database for credit card fraud detection and creating a python script for analyzing
various datasets generated synthetically at the beginning with respect to certain operations required to be performed on
the designed Neo4j database.

The final goal is the analysis of the execution times of the requested operations with
respect to the database with a related discussion and final report on the possible improvement that can be implemented
in order to improve the final performance.

## Data Structure

The domain is composed of three main elements:

1. **Customer Profile**: Contains information about customers, including geographic location, spending frequency, and
   spending amounts.
2. **Terminal Profile**: Contains data about payment terminals, including geographic location.
3. **Transactions**: Record a customer transaction from a terminal with amount and date. Some transactions are labeled
   as fraudulent.

## Requirements

- **Programming Language**: [Python](https://www.python.org/)
- **Database**: [Neo4j](https://neo4j.com/)

## Setup

1. **Install Dependencies**  
   Install the required dependencies by running:
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure the Neo4j Database**
    - Create a new database for the project from a Neo4j DBMS.
    - Enable CSV file imports by modifying the DBMS settings with this following line:
      ```text
      dbms.security.allow_csv_import_from_file_urls=true
      ```
    - Start the chosen DBMS


3. **Set Environment Variables**  
   Create a `.env` file with the following variables:
   ```env
   DMBS_URI=[DBMS uri]
   DMBS_USER=[DBMS authentication username]
   DMBS_PASSWORD=[DBMS authentication password]
   DATABASE_NAME=[Database name]
   ```

4. **Install Dependencies**  
   Start the main script:
   ```bash
   python main.py
   ```

## Implemented Features

### Dataset Generation

The datasets that will be processed are generated using the provided
simulator [documents](https://fraud-detection-handbook.github.io/fraud-detection-handbook/Chapter_3_GettingStarted/SimulatedDataset.html)
with the following characteristics:

- Three datasets of increasing sizes: `50MB`, `100MB` and `200MB`.
- A fixed number of customers and terminals of 5000 and 3000 for each dataset.
  Transactions are scaled to meet the required dataset size.

### Operations

1. Identifying customers with similar spending profiles.
2. Detecting possible fraudulent transactions for each terminal.
3. Computing `co-customer-relationships` (relationships between customers of degree `k`).
4. Extending the logical model with:
    1. The period of day when all the transactions occurred, the category of the purchased product and the user's
       expressed security feeling.
    2. Identifying `buying friends` based on the similarity of security feelings.
5. Analyzing transactions by period of day.

### Included Scripts

- Generating datasets `.csv` files.
- Loading datasets into the database.
- Query implementation for the required operations.
- Execution time report for each dataset.

## Output

- All the final results data will be saved in the `./.output` directory, including:
    - The generated datasets.
    - A logging message report file.
    - A final plot represents the execution times for the generated datasets with respect to the requested operations.

## Documentation

The project includes a technical documentation PDF file (in Italian) containing:

- UML class diagram and explanations.
- Logical data model and design motivations.
- Description of the data loading scripts.
- Description of the operation implementation scripts.
- Discussion of the performance results and possible improvement.
