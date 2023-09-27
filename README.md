# Case Study

This case study involves extracting data from multiple sources, transforming it, and loading it into a database. Below are the details of the functions in the Python code:

1. **drop_schemas()**
   - Drops existing schemas (databases) from a MySQL server based on the configuration provided in 'config.json'.

2. **create_schemas()**
   - Creates new schemas (databases) on a MySQL server based on the configuration provided in 'config.json'.

3. **sales_data_read()**
   - Reads sales data from a CSV file specified in 'config.json' and returns it as a Pandas DataFrame.

4. **users_data_read()**
   - Retrieves user data from a specified API endpoint, processes it, and returns relevant columns as a Pandas DataFrame.

5. **weather_data_read()**
   - Uses latitude and longitude information from the user data to fetch weather data from an API, processes it, and returns relevant columns as a Pandas DataFrame.

6. **merge_dfs()**
   - Combines sales, user, and weather data based on common columns like customer ID and location.

7. **create_merge_table_ddl()**
   - Creates a MySQL table with specific columns to store the merged data.

8. **insert_merge_data()**
   - Inserts the merged data into the MySQL table.

9. **calculate_agg_df()**
   - Calculates various aggregated metrics from the merged data.

10. **display_agg_data()**
    - Generates and displays visualizations based on the aggregated metrics.

This case study demonstrates the process of data extraction, transformation, and loading, along with the creation of visualizations for analysis.
