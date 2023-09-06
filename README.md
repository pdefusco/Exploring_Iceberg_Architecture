# Exploring Iceberg Architecture

## Objective

Apache Iceberg is a new open table format targeted for petabyte-scale analytic datasets.  It  has been designed and developed as an open community standard to ensure compatibility across languages and implementations.  Apache Iceberg is open source, and is developed through the Apache Software Foundation.  Companies such as Adobe, Expedia, LinkedIn, Tencent, and Netflix have published blogs about their Apache Iceberg adoption for processing their large scale analytics datasets.  

This project allows you to dive deeper into Iceberg Architecture by observing file updates in the Iceberg Metadata and File Layers. You will issue a small set of Spark SQL queries to an Iceberg table and you will validate your own understanding of Iceberg Metadata by exploring the Iceberg Catalog and Files as you go.

## What is Apache Iceberg

Iceberg is a high-performance format for huge analytic tables. Iceberg brings the reliability and simplicity of SQL tables to big data, while making it possible for engines like Spark, Trino, Flink, Presto, Hive and Impala to safely work with the same tables, at the same time.

To satisfy multi-function analytics over large datasets with the flexibility offered by hybrid and multi-cloud deployments, we integrated Apache Iceberg with CDP to provide a unique solution that future-proofs the data architecture for our customers. By  optimizing the various CDP Data Services, including CDW, CDE, and Cloudera Machine Learning (CML) with Iceberg, Cloudera customers can define and manipulate datasets with SQL commands, build complex data pipelines using  features like Time Travel operations, and deploy machine learning models built from Iceberg tables.  Along with CDP’s enterprise features such as Shared Data Experience (SDX), unified management and deployment across hybrid cloud and multi-cloud, customers can benefit from Cloudera’s contribution to Apache Iceberg, the next generation table format for large scale analytic datasets.

To learn more about the Apache Iceberg Open Source project and its role in the Cloudera Data Platform please visit this in-depth [Cloudera Blog article](https://blog.cloudera.com/introducing-apache-iceberg-in-cloudera-data-platform/).

![alt text](img/iceberg-metadata.png)

## Relevant Iceberg Concepts

##### Iceberg Catalog

##### Iceberg Metadata Layer

##### Iceberg Data Layer

##### Iceberg Snapshots

##### Iceberg and Spark


## Requirements

The notebooks are designed to run in CML. However, the same code is also compatible with CDE. A future version of this project will include instructions for CDE.

## Project Setup

Create a CML Project by cloning this Git repository from the CML UI.

Launch a CML Session with the following configurations:

```
Editor: JupyterLab
Kernel: Python 3.8 or above
Edition: Standard
Version: any version ok
Enable Spark: Spark 3.2.0 and above ok
Resource Profile: 2vCPU / 4GiB Memory - 0 GPU
```

The notebooks are named and are meant to be run in numerical order. The CatalogUtil module was created to allow you to interact with the Iceberg Metadata and Data Layers more easily. No code changes are required. Further instructions are included in the notebooks. Please run them all and follow instructions provided as you go.

### Conclusions

CDE is the Cloudera Data Engineering Service, a containerized managed service for Spark and Airflow.

CDP Data Engineering is the only cloud-native service purpose-built for enterprise data engineering teams. Building on Apache Spark, Data Engineering is an all-inclusive data engineering toolset that enables orchestration automation with Apache Airflow, advanced pipeline monitoring, visual troubleshooting, and comprehensive management tools to streamline ETL processes across enterprise analytics teams.

Data Engineering is fully integrated with Cloudera Data Platform, enabling end-to-end visibility and security with SDX as well as seamless integrations with CDP services such as Data Warehouse and Machine Learning. Data Engineering on CDP powers consistent, repeatable, and automated data engineering workflows on a hybrid cloud platform anywhere.

### CDE Relevant Projects

If you are exploring or using CDE today you may find the following tutorials relevant:

* [Spark 3 & Iceberg](https://github.com/pdefusco/Spark3_Iceberg_CML): A quick intro of Time Travel Capabilities with Spark 3.

* [Simple Intro to the CDE CLI](https://github.com/pdefusco/CDE_CLI_Simple): An introduction to the CDE CLI for the CDE beginner.

* [CDE CLI Demo](https://github.com/pdefusco/CDE_CLI_demo): A more advanced CDE CLI reference with additional details for the CDE user who wants to move beyond the basics.

* [CDE Resource 2 ADLS](https://github.com/pdefusco/CDEResource2ADLS): An example integration between ADLS and CDE Resource. This pattern is applicable to AWS S3 as well and can be used to pass execution scripts, dependencies, and virtually any file from CDE to 3rd party systems and viceversa.

* [Using CDE Airflow](https://github.com/pdefusco/Using_CDE_Airflow): A guide to Airflow in CDE including examples to integrate with 3rd party systems via Airflow Operators such as BashOperator, HttpOperator, PythonOperator, and more.

* [GitLab2CDE](https://github.com/pdefusco/Gitlab2CDE): a CI/CD pipeline to orchestrate Cross-Cluster Workflows for Hybrid/Multicloud Data Engineering.

* [CML2CDE](https://github.com/pdefusco/cml2cde_api_example): an API to create and orchestrate CDE Jobs from any Python based environment including CML. Relevant for ML Ops or any Python Users who want to leverage the power of Spark in CDE via Python requests.

* [Postman2CDE](https://github.com/pdefusco/Postman2CDE): An example of the Postman API to bootstrap CDE Services with the CDE API.

* [Oozie2CDEAirflow API](https://github.com/pdefusco/Oozie2CDE_Migration): An API to programmatically convert Oozie workflows and dependencies into CDE Airflow and CDE Jobs. This API is designed to easily migrate from Oozie to CDE Airflow and not just Open Source Airflow.

For more information on the Cloudera Data Platform and its form factors please visit [this site](https://docs.cloudera.com/).

For more information on migrating Spark jobs to CDE, please reference [this guide](https://docs.cloudera.com/cdp-private-cloud-upgrade/latest/cdppvc-data-migration-spark/topics/cdp-migration-spark-cdp-cde.html).

If you have any questions about CML or would like to see a demo, please reach out to your Cloudera Account Team or send a message [through this portal](https://www.cloudera.com/contact-sales.html) and we will be in contact with you soon.

![alt text](../../img/cde_thankyou.png)
