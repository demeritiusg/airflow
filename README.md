# AWS MWAA Data Pipeline
## Overview
This repository contains a production-ready example of an end-to-end data pipeline orchestrated using **AWS Managed Workflows for Apache Airflow (MWAA)**. The pipeline demonstrates best practices in AWS integration, Airflow orchestration, data ingestion, transformation, and loading into an analytics data store.

The pipeline uses a modular Airflow DAG to extract data from AWS S3, perform transformation via Python scripts, and load the processed data into Amazon Redshift. It also incorporates operational components such as monitoring, alerting, logging, and security via IAM roles.

## Architecture
The data pipeline architecture includes:
- **MWAA**: Apache Airflow managed service on AWS for workflow orchestration.
- **Amazon S3**: Source and staging data storage.
- **Python scripts**: Data transformation logic executed within Airflow tasks.
- **Amazon Redshift**: Target data warehouse for loading processed data.
- **IAM Roles & Policies**: Secure access management.
- **Amazon CloudWatch**: Monitoring and alerting of pipeline health.

## Features
- Modular Airflow DAG with clear task dependencies and retry mechanisms.
- Use of AWS-specific Airflow operators and sensors.
- Parameterized DAG configuration via Airflow Variables.
- Secure and scalable AWS resource integration.
- Monitoring via CloudWatch logs and alerts.
- Infrastructure as Code (IaC) templates for environment provisioning.
