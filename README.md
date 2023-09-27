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

*Here are the visual representations derived from the aggregated data, providing valuable insights into the dataset.*


It helps identify the highest-spending customers, allowing for targeted marketing strategies or special offers to retain valuable customers.
![sales amount](https://github.com/shaharyar0312/case_study/assets/18499963/66a129a5-abe7-4a02-bb1d-da7d6d13cbef)

It provides information on which products have the highest average order quantities, which can be useful for inventory management and demand forecasting.
![Avg order Qty](https://github.com/shaharyar0312/case_study/assets/18499963/e4072d4f-717f-4296-ad9d-7f3eaf04ea58)

It offers a comprehensive view of both top-selling products and high-value customers, providing insights for targeted marketing and sales strategies.
![Top Selling](https://github.com/shaharyar0312/case_study/assets/18499963/20127371-50f3-43d2-af7c-6ab1f4172361)

It helps identify trends and seasonality in sales, which can inform inventory planning, marketing campaigns, and resource allocation.
![monthly trend](https://github.com/shaharyar0312/case_study/assets/18499963/b032fd9b-0691-4e13-a719-8517e3317fe1)

It provides a higher-level view of sales trends, useful for long-term planning, budgeting, and forecasting.
![Quarterly Sales Trend](https://github.com/shaharyar0312/case_study/assets/18499963/603f8b4c-420d-47d0-84d5-aa26375cc970)

It helps understand how sales are influenced by temperature, which can inform marketing strategies, product offerings, and seasonal promotions.
![Sales by temp](https://github.com/shaharyar0312/case_study/assets/18499963/4ccf8f5e-beb0-41c5-be95-65379cf2c688)



This case study demonstrates the process of data extraction, transformation, and loading, along with the creation of visualizations for analysis.
