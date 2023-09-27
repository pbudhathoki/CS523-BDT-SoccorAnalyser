# CS523-BDT-SoccorAnalyser

Analyzing a dataset  "International Football Results from 1872 to 2023" using big data technologies like  Apache Spark Streaming and Apache Hive can provide valuable insights into the world of international football. Below, I'll outline a high-level description of this project:

**Data Preparation:**

Data Source: We obtain the data from popular website kaggle (https://www.kaggle.com/datasets/martj42/international-football-results-from-1872-to-2017?datasetId=4305) in CSV format containing international football match results (up to date dataset till September 2023). This dataset typically includes information such as match date, home team, away team, home team goals, away team goals, tournament type, and more.

Data Splitting: The results.csv contains all the information of different international matches. To verify the Apache Data Streaming on the smaller files in HDFS, we split the dataset into smaller files so that we can apply data streaming and processing operations as needed to test our technology.

Data feed to Apache Spark Data Streaming: The pipeline in shellscript is developed which fetch data from local directory in cloudera VM to the Apache Data Streaming.

**Real-time Analysis with Apache Spark Streaming:**
Data Streaming Setup: Configured Apache Spark Streaming to ingest smaller real time dataset from local directory in cloudera VM to the Apache Data Streaming in every 20 seconds.
Text file streaming is used to load data. 
Different stream function like filter, maptoPair, Union, reducebykey etc are used. complex data structure with multiple Tuple are used to get the aggregate data from input data.

**Data Processing with Apache Hive:**
Schema Definition: Defined a Hive schema that matches the structure of the output result from apache data streaming. This includes specifying the data types and columns that correspond to the dataset's attributes.

Data Loading: Load the CSV data into a Hive table using Hive's LOAD DATA command or other methods for bulk data loading.

Data Transformation: Use Hive's SQL-like language, HiveQL, to perform various data transformations and aggregations. we calculated statistics, filter data by date, tournament, or teams, and perform other necessary preprocessing.

Querying: We ran Hive queries to extract insights from the data. For example, we found the countries with the most international goes, city and country organizing most games, the pattern of football match in international tournament, and more.


**Output and Visualization:**
We planned to do visualization of some statistics. However, due to time constraints, we were not able to do it.
In future, We plan to visualize the real-time data and insights using data visualization tools like Apache Zeppelin, Jupyter Notebook, or custom dashboards. This can help us provide live updates to football enthusiasts.
