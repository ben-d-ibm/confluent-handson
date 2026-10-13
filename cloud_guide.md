Home
Cloud
Get Started with Confluent Cloud

Ask

Quick Start for Confluent Cloud
Confluent Cloud is a fully managed, cloud-native data streaming platform powered by Apache Kafka®. This quick start guide helps you begin streaming data by showing you how to create a cluster, add a topic, and produce data.

Why use Confluent Cloud instead of managing Kafka yourself?
Confluent Cloud eliminates the operational complexity of Kafka with:

Automatic scaling and reliability
Built-in security, monitoring, and schema management
Lower total cost of ownership and faster development
Compare Confluent Cloud and Apache Kafka or sign up for Confluent Cloud to get started.
The Confluent Cloud Console Home page
When you sign in to Confluent Cloud, the Home page displays widgets for returning to the resources you use most and for monitoring your environments:

Widget

Description

Your resource overview
Shows how many environments, clusters, topics, connectors, cluster links, compute pools, and SQL workspaces you have. Select a resource type to go to its list.
Action needed
Lists resources that need your attention, such as idle resources. Select View all to see the full list.
Favorites
Lists resources you bookmarked, such as environments, clusters, topics, or connectors, so you can return to them directly from the Home page. To add a favorite, select Add favorite, then search for and select the resource. To remove a favorite, select Edit, then select Remove from favorites next to it.
Recently visited resources
Lists the resources you navigated to most recently, with how long ago you visited each one. Select View all to search your full history of recently visited resources.
Discover data
Shows recently created topics for a selected environment. Select Explore more to browse your data in Data Portal.
Data health score
Shows a score based on how many of your topics have a registered schema, and lists topics that are missing one.
Blogs
Shows recent posts from the Confluent blog.
Considerations before you start
This quick start gets you up and running with Confluent Cloud using a Basic Kafka cluster.
The first section shows how to use Confluent Cloud to create topics, and produce and consume data to and from the cluster.
The second section walks you through how to use Confluent Cloud for Apache Flink® to run queries on the data using SQL syntax.
The quick start workflows assume you already have a working Confluent Cloud environment, which includes a Stream Governance package. To learn more about Stream Governance packages, features, and environment setup workflows, see Manage Stream Governance Packages in Confluent Cloud.
Confluent Cloud has a web interface, a local command line interface, and REST APIs for managing your environments and resources.
Prerequisites
Access to Confluent Cloud. To get started for free, see Confluent Cloud Free Trial.
Internet connectivity
Stream Governance package
Section 1: Create a cluster and add a topic
This section sets up a Kafka cluster on Confluent Cloud and shows how to produce data to a topic using the Cloud Console, Confluent CLI, or REST APIs.

Step 1: Create a Kafka cluster in Confluent Cloud
In this step, you create and launch a basic Kafka cluster inside your default environment.

Cloud Console
Sign in to Confluent Cloud at https://confluent.cloud.
Click + Add cluster.
On the Create cluster page, specify a cluster name.
Under Cluster type, select Basic.

Create cluster page in Confluent Cloud.
This example creates a Basic Kafka cluster, which supports single-zone availability. For information about other cluster types, see Kafka Cluster Types in Confluent Cloud.

Under Provider and region, select a cloud provider tile and a region.
Review the cost estimate and summary, then select Launch cluster.

If you haven’t set up a payment method, add one or enter a promotional code before you can launch the cluster.

After your cluster provisions, the Cluster Overview page displays. Provisioning time varies by cloud provider and other settings, and might take a few minutes.

Cluster Overview page in Confluent Cloud.
Now you can configure apps and data on your new cluster.
Confluent CLI
Confluent Cloud APIs
Step 2: Create a Kafka topic
In this step, you create a users Kafka topic by using the Cloud Console. A topic is a unit of organization for a cluster, and is essentially an append-only log. For more information about topics, see What is Apache Kafka.

Cloud Console
From the navigation menu, click Topics, and then click Create topic.
In the Topic name field, type “users” and then select Create with defaults.

Topics page in Confluent Cloud with the new topic.
The users topic is created on the Kafka cluster and is available for use by producers and consumers.

The success message might prompt you to take an action, but continue with Step 3: Create a sample producer.
Confluent CLI
Confluent Cloud APIs
Step 3: Create a sample producer
A connector is a pluggable component that streams data into or out of Kafka. This step uses the hosted Datagen Source Connector for Confluent Cloud, which generates mock data, to produce example data to your Kafka cluster.

Tip
Watch an interactive demo of how to configure a connector.

Storylane interactive demo for configuring a Datagen Source connector.
Cloud Console
From the navigation menu, select Connectors.

To open Confluent Cloud at Connectors: https://confluent.cloud/go/connectors.

In the Search box, type “datagen”.
From the search results, select the Datagen Source connector.

Searching for the Datagen connector in Confluent Cloud.
Tip
If you see the Launch Sample Data box, select Additional configuration.
On the Launch Sample Data dialog, select the Users template, and click Additional configuration.
On the Topic selection dialog, select the “users” topic you created in the previous section and click Continue.
In the Kafka credentials dialog, select My account, and click Generate API key & download, then click Continue.

This creates an API key and secret that allows the connector to access your cluster, and downloads the key and secret to your computer. The key and secret are required for the connector and also for the Confluent CLI and ksqlDB CLI to access your cluster.

Note
An API key and associated secret apply to the active Kafka cluster. If you add a new cluster, you must create a new API key for producers and consumers on the new Kafka cluster. For more information, see Use API Keys to Authenticate to Confluent Cloud.
On the Configuration page, select JSON_SR for the output record value format, Users for the template, and click Continue.
For Connector sizing, leave the slider at the default of 1 task and click Continue.
On the Review and launch page, select the text in the Connector name box and replace it with “DatagenSourceConnector_users”.
Click Continue to start the connector.

The status of your new connector should read Provisioning, which lasts for a few seconds. When the status changes to Running, your connector is producing data to the users topic.

Running Datagen Source connector in Confluent Cloud.
Confluent CLI
Confluent Cloud APIs
Step 4: View messages
Your new users topic is now receiving messages. Use Confluent Cloud Console to see the data.

From the navigation menu, select Topics to show the list of topics in your cluster.
Select the users topic.
In the users topic detail page, select Messages to view the messages being produced to the topic.

Messages page in Confluent Cloud.
Step 5: View the topic schema
When the connector from Step 3 produces data to the users topic, it registers a schema for the topic in Schema Registry, the foundation of Stream Governance. The schema keeps the topic’s data consistent and compatible as applications evolve.

From the navigation menu, select Topics, then select the users topic.
In the users topic detail page, click Schema to view the schema registered for the topic.

The schema lists each field and its type, defining the structure that producers and consumers agree on.

To learn more about managing schemas in Confluent Cloud, see Manage Schemas in Confluent Cloud.

Step 6: Inspect the data stream
Use Stream Lineage to track data movement through your cluster.

Click Lineage in the navigation menu.
Click the DatagenSourceConnector_users node, which is the connector that you created in Step 3. The details overview shows graphs for total production and other data.

Source connector details in Confluent Cloud.
Dismiss this view and select the topic labeled users. Click Overview to show graphs for total throughput and other data. Click the other tabs to view more details for the topic:

Schema, if one is defined
Metadata
Messages
Topic details in Confluent Cloud.
Click the arrow on the canvas border to re-open the navigation menu.
Step 7: Delete resources (optional)
If you plan to continue to Section 2: Query streaming data with Flink SQL and use Flink SQL statements to query your data, skip this step. Otherwise, delete the resources you created to avoid unexpected charges to your account.

Cloud Console
Delete the connector:
From the navigation menu, select Connectors.
Click DatagenSourceConnector_users and choose the Settings tab.
Click Delete connector, enter the connector name (DatagenSourceConnector_users), and click Confirm.
Delete the topic:
From the navigation menu, click Topics, select the users topic, and then choose the Settings tab.
Click Delete topic, enter the topic name (users), and select Continue.
Delete the cluster:
From the navigation menu, select Cluster, and then choose the Settings tab.
Click Delete cluster, enter the cluster name, and click Continue.
Confluent CLI
Confluent Cloud APIs
Section 2: Query streaming data with Flink SQL
In this section, you create a Flink workspace and write queries against the users topic and other streaming data. In Section 1, you installed a Datagen connector that produced this data to the users topic in your Confluent Cloud cluster.

Step 1: Create a Flink workspace
A Flink workspace is an interactive SQL editor for writing and running queries against streaming data. Create a new workspace to get started.

Navigate to the SQL Workspaces page, and in the navigation menu, click Stream processing.
In the dropdown, select the environment where you created the users topic and the Datagen Source connector.
Click Create workspace.

Tip
To experiment with example data streams provided by Confluent Cloud for Apache Flink, click Try example to open a curated Flink workspace that has example Flink SQL queries you can run immediately.
A new workspace opens with an example query in the code editor, or cell.

Flink workspace in Confluent Cloud
In the background, Confluent Cloud for Apache Flink creates a compute pool, which represents the compute resources that are used to run your SQL statements.

It might take a minute or two for the compute pool to be provisioned. The status is displayed in the upper-right section of the workspace.

Step 2: Run Flink SQL statements
The compute pool is ready to run queries after its status changes from Provisioning to Running.

Click Run to submit the example query.

The example statement is submitted, and information about the statement is displayed, including its status and a unique identifier. Click the Statement name link to open the statement details view, which displays the statement status and other information. Click X to dismiss the details view.

After an initialization period, the query results display beneath the cell.

Your output should resemble:

EXPR$0
0
1
2
Clear the previous query from the cell and run the following query to inspect the users stream.

Confluent Cloud for Apache Flink registers tables automatically on your Kafka topics, and your query runs against the users table with the streaming data from the underlying topic.

SELECT * FROM users;
Your output should resemble:

key             registertime  userid regionid gender
x'557365725f34' 1502088104187 User_4 Region_1 MALE
x'557365725f32' 1500243991207 User_2 Region_9 FEMALE
x'557365725f32' 1497969328414 User_2 Region_9 OTHER
...
Click Stop to end the query.

Data continues to flow from the Datagen connector into the users table even though the SELECT query is stopped.

Step 3: Mask a field
With a Flink compute pool running, you can run SQL queries on your streaming data.

In this step, you run a Flink SQL statement to hide personal information in the users stream and publish the scrubbed data to a new Kafka topic, named users_mask.

Run the following statement to create a new Flink table based on the users table.

CREATE TABLE users_mask LIKE users;
When the status of the previous statement is Completed, run the following statement to inspect the schema of the users_mask table.

DESCRIBE users_mask;
Your output should resemble:

+--------------+-----------+----------+------------+
| Column Name  | Data Type | Nullable |   Extras   |
+--------------+-----------+----------+------------+
| key          | BYTES     | NULL     | BUCKET KEY |
| registertime | BIGINT    | NULL     |            |
| userid       | STRING    | NULL     |            |
| regionid     | STRING    | NULL     |            |
| gender       | STRING    | NULL     |            |
+--------------+-----------+----------+------------+
Run the following statement to start a persistent query that uses the Flink REGEXP_REPLACE function to mask the value of the gender field and stream the results to the users_mask table.

INSERT INTO users_mask SELECT
  `key`,
  registertime,
  userid,
  regionid,
  REGEXP_REPLACE(gender, '(\w)', '*') as gender
FROM users;
The INSERT INTO FROM SELECT statement runs continuously until you stop it manually.

Click Add new cell icon to create a new cell, and run the following statement to inspect the rows in the users_mask table.

SELECT * FROM users_mask;
Your output should resemble:

key             registertime  userid regionid gender
x'557365725f34' 1488737391835 User_4 Region_5 *****
x'557365725f34' 1499070045309 User_4 Region_5 *****
x'557365725f32' 1505447077187 User_2 Region_7 *****
x'557365725f34' 1505592707164 User_4 Region_2 *****
...
Click Stop to end the SELECT statement.

The INSERT INTO statement that you started previously continues streaming data into the users_mask topic.

Step 4: View the Stream Lineage
Your Flink SQL statements are resources in Confluent Cloud, like topics and connectors, so you can view them in Stream Lineage.

In your workspace, open the catalog browser and expand your environment, cluster, and Tables to find the users_mask topic.
Hover over the users_mask topic and click View topic details.

Navigation view in a Flink workspace in Confluent Cloud
In the topic details pane, under Lineage, click View full lineage.

Stream Lineage view of a Flink pipeline in Confluent Cloud
Hover over the nodes in the Stream Lineage diagram to see details of the data flow.
Step 5: Delete resources
When you finish the Quick Start, delete the resources you created to avoid unexpected charges to your account.

Cloud Console
Delete the persistent query
Navigate to your environment’s details page and click Flink.
In the statements list, find the statement that has a status of Running.
Select the checkbox for the statement, click Actions, and select Delete statement.
In the Confirm statement deletion dialog, type DELETE and click Confirm.
Delete the connector:
From the navigation menu, select Connectors.
Click DatagenSourceConnector_users and choose the Settings tab.
Click Delete connector, enter the connector name (DatagenSourceConnector_users), and click Confirm.
Delete the topics:
From the navigation menu, click Topics, select the users topic, and choose the Settings tab.
Click Delete topic, enter the topic name (users), and click Continue.
Repeat these steps with the users_mask topic.
Confluent CLI
Confluent Cloud APIs
Related content
Stream Processing with Confluent Cloud for Apache Flink
Connect External Systems to Confluent Cloud
Quick Start for Schema Management on Confluent Cloud
Connect Clients to Confluent Cloud
Configure Multi-Node Environment
Try out the Confluent Cloud Demos and Examples
Confluent Developer: Cloud-Native Apache Kafka: Designing Cloud Systems for Speed and Scale
Ask the community
Confluent Support Portal
Last published: Oct 01, 2026
Terms & Conditions
|
Privacy Policy
|
Do Not Sell My Information
|
Modern Slavery Policy
|
Cookie Settings
|
Feedback
Copyright © Confluent, Inc. 2014- 2026 Apache®, Apache Kafka®, Kafka®, Apache Flink®, Flink®, Apache Iceberg®, Iceberg® and associated open source project names are trademarks of the Apache Software Foundation

Table of contents
Overview
Get Started
Overview
Free Trial
Regions and Availability
Quick Start: Confluent Cloud
Quick Start: REST API
Manage Schemas
Tutorials and Examples
Overview
Tutorial: Use Replicator to Copy Kafka Data to Cloud
Example: Create Fully Managed Services
Use Docs with AI Tools
Use Kafka Clusters
Overview
Resilience
Cluster Deletion Protection
Multi-Tenancy and Client Quotas
Manage Clusters
Migrate Clusters
Copy and Share Data
Connect Confluent Platform and Cloud Environments
Automate with Infrastructure as Code
FAQ
Build Client Applications
Build Kafka Streams Applications
AI and ML
Manage Topics
Manage and Monitor Resources with USM
Govern Data Streams
Connect to External Services
Integrate with Confluent Cloud
Process Data with Flink
Manage Networking
Log and Monitor
Manage Security
Manage Billing
Manage Service Quotas
APIs
Confluent CLI
Release Notes & FAQ
Support
Glossary
Give feedback about this page
ON THIS PAGE:
The Confluent Cloud Console Home page
Considerations before you start
Prerequisites
Section 1: Create a cluster and add a topic
Step 1: Create a Kafka cluster in Confluent Cloud
Step 2: Create a Kafka topic
Step 3: Create a sample producer
Step 4: View messages
Step 5: View the topic schema
Step 6: Inspect the data stream
Step 7: Delete resources (optional)
Section 2: Query streaming data with Flink SQL
Step 1: Create a Flink workspace
Step 2: Run Flink SQL statements
Step 3: Mask a field
Step 4: View the Stream Lineage
Step 5: Delete resources
Related content