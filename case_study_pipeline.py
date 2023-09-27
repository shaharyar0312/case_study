
import os
import pandas as pd
import mysql.connector
import json
import requests
import matplotlib.pyplot as plt

def config_parameters():
    with open('config.json', 'r') as config_file:
        config = json.load(config_file)
    return(config)

def drop_schemas():
    config=config_parameters()
    host = config['mysql']['host']
    user = config['mysql']['user']
    password = config['mysql']['password']
    conn = mysql.connector.connect(host=host, user=user, password=password)
    cursor = conn.cursor()
    schema_list=[config['mysql']['database1'], config['mysql']['database2']]
    for schema_name in schema_list:
        drop_schema_query = f"DROP SCHEMA IF EXISTS {schema_name}"
        cursor.execute(drop_schema_query)
        conn.commit()

def create_schemas():
    drop_schemas()
    config=config_parameters()
    host = config['mysql']['host']
    user = config['mysql']['user']
    password = config['mysql']['password']
    conn = mysql.connector.connect(host=host, user=user, password=password)
    cursor = conn.cursor()
    schema_list=[config['mysql']['database1'], config['mysql']['database2']]
    for schema_name in schema_list:
        create_schema_query = f"CREATE SCHEMA IF NOT EXISTS {schema_name}"
        cursor.execute(create_schema_query)
        conn.commit()
def sales_data_read():
    config=config_parameters()
    filename=config['filename']
    sales_df = pd.read_csv(filename)
    return(sales_df)

def users_data_read():
    config=config_parameters()
    user_api=config['user_api']
    response = requests.get(user_api)

    # Convert the response to JSON format
    data = response.json()

    # Create a DataFrame
    df = pd.DataFrame(data)

    extract_lat = lambda x: x['geo']['lat']
    extract_lng = lambda x: x['geo']['lng']

    # Apply the lambda functions to create new columns
    df['lat'] = df['address'].apply(extract_lat)
    df['lng'] = df['address'].apply(extract_lng)


    # Extracting specific columns
    users_df = df[["id","name","username", "email", "lat", "lng"]]
    users_df['lat'] = users_df['lat'].astype(float)
    users_df['lng'] = users_df['lng'].astype(float)
    return (users_df)
def weather_data_read():
    users_df=users_data_read()
    config=config_parameters()
    weather_api=config['weather_api'] 
    api_key=config['api_key']
    json_responses = []
    user_lat_lon = users_df[["lat", "lng"]]
    extract_weather_info = lambda x: {'main': x[0]['main'], 'description': x[0]['description']}
    for index, row in user_lat_lon.iterrows():
        weather_response = requests.get(weather_api+"lat="+str(row['lat'])+"&lon="+str(row['lng'])+"&appid="+str(api_key))
        weather_data = weather_response.json()
        json_responses.append(weather_data)
    weather_df = pd.concat([pd.json_normalize(response) for response in json_responses], ignore_index=True)
    weather_df[['lat', 'lng']]=weather_df[['coord.lat', 'coord.lon']]
    weather_df['temperature']=weather_df['main.temp']
    weather_df[['weather_type', 'weather_description']] = weather_df['weather'].apply(extract_weather_info).apply(pd.Series)
    weather_final_df=weather_df[['lat', 'lng','temperature', 'weather_type', 'weather_description']]
    return(weather_final_df)
def merge_dfs():
    merged_df = pd.merge(sales_data_read(), users_data_read(), left_on='customer_id', right_on='id', how='inner')

    final_merged_df = pd.merge(merged_df, weather_data_read(), on=['lat', 'lng'], how='inner')
    final_merged_df = final_merged_df.drop(columns=['id'])
    final_merged_df['customer_name'] = final_merged_df['name']
    final_merged_df = final_merged_df.drop(columns=['name'])
#     final_merged_df['order_date'] = pd.to_datetime(final_merged_df['order_date'])
    return(final_merged_df)

def create_merge_table_ddl():
    create_schemas()
    config=config_parameters()
    host = config['mysql']['host']
    user = config['mysql']['user']
    password = config['mysql']['password']
    table_name = 'merged_data'
    database = config['mysql']['database1']
    create_query=f"""CREATE TABLE IF NOT EXISTS {database}.{table_name}
                     (
                         order_id            INT          NULL,
                         customer_id         INT          NULL,
                         product_id          INT          NULL,
                         quantity            INT          NULL,
                         price               FLOAT        NULL,
                         order_date          DATETIME     NULL,
                         customer_name       VARCHAR(60)  NULL,
                         username            VARCHAR(60)  NULL,
                         email               VARCHAR(60)  NULL,
                         lat                 FLOAT        NULL,
                         lng                 FLOAT        NULL,
                         temperature         FLOAT        NULL,
                         weather_type        VARCHAR(60)  NULL,
                         weather_description VARCHAR(100) NULL
                     )"""
    conn = mysql.connector.connect(host=host, user=user, password=password)
    cursor = conn.cursor()
    cursor.execute(create_query)
    conn.commit()
    conn.close()

    
def insert_merge_data():
    config=config_parameters()
    host = config['mysql']['host']
    user = config['mysql']['user']
    password = config['mysql']['password']
    database = config['mysql']['database1']
    table_name = 'merged_data'
    create_merge_table_ddl()
    final_merged_df = merge_dfs()
    columns = ', '.join(final_merged_df.columns)
    placeholders = ', '.join(['%s'] * len(final_merged_df.columns))
    insert_query = f"INSERT INTO {database}.{table_name} ({columns}) VALUES ({placeholders})"
    conn = mysql.connector.connect(host=host, user=user, password=password)
    cursor = conn.cursor()

    data = [tuple(row) for row in final_merged_df.values]
    cursor.executemany(insert_query, data)
    conn.commit()
    conn.close()
    
def calculate_agg_df():
    config=config_parameters()
    host = config['mysql']['host']
    user = config['mysql']['user']
    password = config['mysql']['password']
    database = config['mysql']['database2']
    final_merged_df = merge_dfs()
    insert_merge_data()
    final_merged_df['order_date'] = pd.to_datetime(final_merged_df['order_date'])
    # 1. Calculate sales amount per customer
    sales_per_customer = final_merged_df.groupby('customer_name')['price'].sum().round(2).reset_index()

    # 2. Calculate average order quantity per product
    avg_order_qty_per_product = final_merged_df.groupby('product_id')['quantity'].mean().round(2).reset_index()

    # 3. Identify top-selling products and customers
    top_5_selling_products = final_merged_df['product_id'].value_counts().nlargest(5).reset_index()
    top_5_selling_products.columns = ['value', 'category']

    top_5_selling_customers = final_merged_df['customer_name'].value_counts().nlargest(5).reset_index()
    top_5_selling_customers.columns = ['value', 'category']

    top_selling_combined = pd.concat([top_5_selling_products, top_5_selling_customers])

    # 4. Analyze sales trends over time (monthly)
    monthly_sales_trends = final_merged_df.groupby(final_merged_df['order_date'].dt.month)['price'].sum().round(2).reset_index()
    monthly_sales_trends.columns = ['month', 'total_sales']

    # 5. Analyze sales trends over time (quarterly)
    quarterly_sales_trends = final_merged_df.groupby(final_merged_df['order_date'].dt.quarter)['price'].sum().round(2).reset_index()
    quarterly_sales_trends.columns = ['quarter', 'total_sales']

    # 6. Include temperature in the analysis
    sales_by_temperature = final_merged_df.groupby('temperature')['price'].sum().round(2).reset_index()

    # 7. Include weather data in the analysis
    sales_by_weather = final_merged_df.groupby(['weather_description'])['price'].sum().round(2).reset_index()
    dataframes = {
        'sales_per_customer': sales_per_customer,
        'avg_order_qty_per_product': avg_order_qty_per_product,
        'top_selling_combined': top_selling_combined,
        'monthly_sales_trends': monthly_sales_trends,
        'quarterly_sales_trends': quarterly_sales_trends,
        'sales_by_temperature': sales_by_temperature,
        'sales_by_weather': sales_by_weather
    }
    for table_name, df in dataframes.items():
        connection = mysql.connector.connect(host=host, user=user, password=password, database=database)
        cursor = connection.cursor()
        # Create table with columns based on DataFrame columns
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ("
        create_table_query += ', '.join([f"{col} VARCHAR(255)" for col in df.columns])
        create_table_query += ")"
        cursor.execute(create_table_query)
        connection.commit()
        # Insert data
        for row in df.itertuples(index=False):
            insert_query = f"INSERT INTO {table_name} VALUES {tuple(row)}"
            cursor.execute(insert_query)
            connection.commit()
        connection.close()
        return(sales_per_customer, avg_order_qty_per_product, top_5_selling_products, top_5_selling_customers, top_selling_combined, monthly_sales_trends, quarterly_sales_trends, sales_by_temperature, sales_by_weather)

def display_agg_data():
    sales_per_customer, avg_order_qty_per_product, top_5_selling_products, top_5_selling_customers,top_selling_combined, monthly_sales_trends, quarterly_sales_trends, sales_by_temperature, sales_by_weather=calculate_agg_df()
    # 1. Sales amount per customer
    sales_per_customer.plot(kind='bar', x='customer_name', y='price', title='Sales Amount per Customer')
    plt.xlabel('Customer Name')
    plt.ylabel('Sales Amount')
    plt.show()

    # 2. Average order quantity per product
    avg_order_qty_per_product.plot(kind='bar', x='product_id', y='quantity', title='Average Order Quantity per Product')
    plt.xlabel('Product ID')
    plt.ylabel('Average Order Quantity')
    plt.show()

    # 3. Top-selling products and customers
    top_selling_combined.plot(kind='bar', x='value', y='category', title='Top Selling Products and Customers')
    plt.xlabel('Value')
    plt.ylabel('Category')
    plt.show()

    # 4. Monthly sales trends
    monthly_sales_trends.plot(kind='line', x='month', y='total_sales', title='Monthly Sales Trends')
    plt.xlabel('Month')
    plt.ylabel('Total Sales')
    plt.show()

    # 5. Quarterly sales trends
    quarterly_sales_trends.plot(kind='bar', x='quarter', y='total_sales', title='Quarterly Sales Trends')
    plt.xlabel('Quarter')
    plt.ylabel('Total Sales')
    plt.show()

    # 6. Sales by temperature
    sales_by_temperature.plot(kind='bar', x='temperature', y='price', title='Sales by Temperature')
    plt.xlabel('Temperature')
    plt.ylabel('Total Sales')
    plt.show()
if __name__ == "__main__":
    calculate_agg_df()
    display_agg_data()

