# Finance Complaint Data Pipeline

This project is designed to build a robust data pipeline for the Consumer Financial Protection Bureau (CFPB) finance complaint API. The pipeline automates the daily download of finance complaint data, stores it in an Amazon S3 bucket, performs an ETL (Extract, Transform, Load) job using AWS Glue, and loads the processed data into an AWS DynamoDB table.

## Project Overview
The primary aim of this project is to create an efficient and scalable data pipeline that:

- Downloads data from the CFPB finance complaint API daily.
- Stores the data in an S3 bucket.
- Performs ETL operations using AWS Glue.
- Loads the transformed data into DynamoDB for further analysis.

## Architecture

[Logo](images/pipeline.png)


- **Data Ingestion:**
     
     **API Source:** Consumer Financial Protection Bureau (CFPB) finance complaint API.

    **Lambda Function:** A scheduled AWS Lambda function triggers daily to fetch data based on a date range retrieved from a MongoDB collection and stores it in an S3 bucket.

- **Data Storage:**

    **Amazon S3:** Stores raw data fetched from the API.
- **Data Processing:**

    **AWS Glue:** Executes custom ETL jobs to transform the data.

- **Data Storage:**

    **DynamoDB:** Stores the processed data for efficient querying and analysis.

## Setup and Deployment

### Prerequisites
- AWS Account
- AWS CLI configured.
- MongoDB instance.
- Python 3.9+

## Step-by-Step Guide
- **Clone the Repository:**

    ```
    git clone https://github.com/yourusername/finance-complaint-data-pipeline.git
    ```

- **AWS Lambda Function:**

    - Create a Lambda function with the provided lambda_function.py script.

    - Configure the Lambda trigger to run daily.

    - Set up the necessary IAM roles and policies for S3 and MongoDB access.

    - **Instead of uploading a zip file of libraries as a layer in Lambda, you can directly add the ARN of the required libraries from this link:**
        
        https://api.klayers.cloud/api/v2/p3.11/layers/latest/us-east-1/html



- **AWS S3 Bucket:**

    - Create an S3 bucket to store raw data. Update the bucket name in the Lambda function configuration.

- **AWS Glue ETL Job:**

    - Create an AWS Glue job using the provided glue_job.py script in the repository.Configure the job to read from the S3 bucket and write to DynamoDB.

- **DynamoDB Table:**

    - Create a DynamoDB table to store the processed data. Update the table name in the Glue job script.

## Configuration

- **Lambda Environment Variables:**

    - **MONGO_URI:** URI of the MongoDB instance.
    - **DATABASE_NAME:** Database name of the MongoDB instance.
    - **COLLECTION_NAME:** Collection name of the Database
    - **S3_BUCKET_NAME:** Name of the S3 bucket.

- **Glue Job Script:**
    
    - Create IAM Role with the following policies like S3FullAccess, GlueServiceRole, DynamoDBFullAccess attached to it.

## Code
- **lambda_function.py:** Script for the AWS Lambda function.
- **glue_job.py:** Custom ETL job script for AWS Glue.