# Project: Spark and Human Balance

## Table of Contents

+ [Project Overview](#Project-Overview)
+ [Project Introduction](#Project-Introduction)
+ [Project Details](#Project-Details)
+ [Project Summary](#Project-Summary)

---

## Project Overview

In this project, we delve into the practical applications of Spark and AWS Glue in processing and curating data from multiple sources. The focus is on using these technologies to build a data lakehouse solution that aids in training a machine learning model for human balance analytics.

---

## Project Introduction: STEDI Human Balance Analytics

The STEDI team is developing a cutting-edge STEDI Step Trainer, integrated with sensors to collect data for a machine-learning algorithm. This project involves building a data lakehouse on AWS to manage and process this sensor data efficiently.

---

## Project Details

The STEDI Step Trainer is designed to:

- Train users in STEDI balance exercises.
- Collect data via sensors to train machine learning algorithms for step detection.
- Work in tandem with a mobile app to gather customer data and interact with device sensors.

The goal is to use motion sensor data from the Step Trainer and accelerometer data from the mobile app to train a machine learning model that can accurately detect steps in real-time. Data privacy is a top priority, and only data from customers who consented to share their information will be utilized.

---

## Project Summary

As a Data Engineer on this project, the primary task is to extract and curate data from the STEDI Step Trainer sensors and the accompanying mobile app into a data lakehouse solution on AWS. This solution will enable Data Scientists to effectively train machine learning models for human balance analysis.

---

## Implementation

## Data Ingestion and Storage
- **S3 Directories Setup**: Created and configured `customer/landing`, `step_trainer/landing`, and `accelerometer/landing` directories in AWS S3 for structured data storage.
- **Data Transfer**: Successfully populated these S3 directories with relevant data, simulating real-world data sources.

## Data Processing with AWS Glue and Spark
- **AWS Glue Tables**: Established two AWS Glue tables for the landing zones, utilizing `customer_landing.sql` and `accelerometer_landing.sql` scripts.
- **Data Transformation Glue Jobs**: 
  - `customer_landing_to_trusted.py`: Efficiently transformed customer data from the landing to the trusted zone.
  - `accelerometer_landing_to_trusted.py`: Seamlessly transferred accelerometer data to the trusted zone.

## Data Querying and Verification
- **Querying with AWS Athena**: Conducted queries on the Glue tables using AWS Athena, ensuring data integrity.
  - Captured query results in `customer_landing.png` and `accelerometer_landing.png`.

## Addressing Data Quality Issues
- **Serial Number Duplication Resolution**: Implemented a strategy to resolve duplicate serial numbers in customer data.
- **Curated Data Glue Job**: Developed `customer_trusted_to_curated.py`, creating a refined `customers_curated` table.

## Final Data Processing Steps
- **Step Trainer Data Handling**: Processed Step Trainer IoT data, populating the `step_trainer_trusted` table.
- **Preparation for Machine Learning**: Compiled and readied data in the `machine_learning_curated` table for machine learning purposes.

## Validation and Review
- For validation and visual review of these processes, refer to the screenshots and files located in the `/imgs` folder.

---


*Note: Detailed scripts and configurations for each step are available in the respective directories within this repository.*

