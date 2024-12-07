# AppleSalesAnalysis
Big Data processing provides meaningful insights for businesses in decision making, and this project focuses on analyzing Apple Product transactions data
by establishing an end-to-end ETL (Extract, Transform, Load) pipeline. The goal is to leverage scalable data processing tools to extract insights on product
trends, customer purchase behaviors, and time-based patterns. Raw data from various sources, including CSV, Parquet, and data lakes, is processed in a distributed computing environment using PySpark. Utilizing the Factory Method design pattern for reader objects ensures flexibility for different data sources.
The transformation phase involves data cleansing and business logic procedures,such as removing inaccurate data, determining product linkages, and analyzing
consumer preferences, with advanced PySpark features like bucketing, partitioning, and broadcast joining to maximize performance. The transformed data
is stored as Parquet files for efficient querying and visualization, and imported into Data Lake and Lake House tables.
