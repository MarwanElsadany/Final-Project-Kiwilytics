from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import timedelta
import pandas as pd
import matplotlib.pyplot as plt


PG_CONN_ID = 'postgres_conn'

default_args = {
    'owner' : 'kiwilytics',
    'retries' : 1,
    'retry_delay' : timedelta(minutes=2),
}

# Fetch Sales
def Fetch_Order_Data():
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    conn = hook.get_conn()
    query = """
        select 
            o.orderdate::date as sale_date ,
            od.productid ,
            p.productname ,
            od.quantity ,
            p.price 
            from orders o 
            join order_details od on o.orderid = od.orderid 
            join products p on od.productid = p.productid 
    """
    
    df = pd.read_sql(query, conn)
    df.to_csv("/home/kiwilytics/Marwan_out_airflow/fetch_sales_data.csv", index=False)

# total_revenue
def proc_dialily_revenue():
    df=pd.read_csv("/home/kiwilytics/Marwan_out_airflow/fetch_sales_data.csv")
    df['total_revenue'] = df['price'] * df['quantity']

    revenue_per_day = df.groupby('sale_date')['total_revenue'].sum().reset_index()
    revenue_per_day.to_csv("/home/kiwilytics/Marwan_out_airflow/dialily_revenue.csv", index=False)


# Visualization_of_total_revenue
def Visualisation_total_revenue():
    df = pd.read_csv("/home/kiwilytics/Marwan_out_airflow/dialily_revenue.csv")
    df['sale_date'] = pd.to_datetime(df['sale_date'])

    plt.figure(figsize=(12,6))
    plt.plot(df['sale_date'], df['total_revenue'], marker='o', linestyle='-')
    plt.title("Dialily Total Sales Revenue")
    plt.xlabel("Date")
    plt.ylabel("Total Revenue")
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.tight_layout()

    out_path = '/home/kiwilytics/Marwan_out_airflow/Total_Revenue_Visualization.png'
    plt.savefig(out_path)
    plt.close()
    print(f"Revenue Saved {out_path}")



# calculate top 5 products by revenue
def calculate_top_products():
    df=pd.read_csv("/home/kiwilytics/Marwan_out_airflow/fetch_sales_data.csv")
    df['total_revenue']=df['price']*df['quantity']

    product_revenue=df.groupby('productname')['total_revenue'].sum().reset_index()
    top5=product_revenue.sort_values('total_revenue',ascending=False).head(5)
    top5.to_csv("/home/kiwilytics/Marwan_out_airflow/top5_products.csv",index=False)

# visualize top 5 products
def visualize_top_products():
    df=pd.read_csv("/home/kiwilytics/Marwan_out_airflow/top5_products.csv")

    plt.figure(figsize=(12,6))
    plt.bar(df['productname'],df['total_revenue'],color='skyblue')
    plt.title('Top 5 Products by Total Revenue')
    plt.xlabel('Product')
    plt.ylabel('Total Revenue')
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.tight_layout()

    outputpath="/home/kiwilytics/Marwan_out_airflow/top5_products.png"
    plt.savefig(outputpath)
    plt.close()
    print(f"Top 5 Product Saved {outputpath}")


# DAG
with DAG(
    dag_id= 'dialily_total_revenue_analysis',
    default_args= default_args ,
    start_date= days_ago(1) ,
    schedule_interval="@weekly" ,
    catchup= False ,
    description='DAG extracts sales data from Postgres, calculates dialily revenue and top 5 products'
) as dag:
    
    task_fetch_data = PythonOperator(
        task_id='Fetch_Order_Data',
        python_callable= Fetch_Order_Data
    )

    task_process_revenue = PythonOperator(
        task_id='proc_dialily_revenue',
        python_callable=proc_dialily_revenue
    )

    task_Visualization_revenue = PythonOperator(
        task_id='Visualisation_total_revenue',
        python_callable=Visualisation_total_revenue
    )

    task_calculate_top_products=PythonOperator(
        task_id='calculate_top_products',
        python_callable=calculate_top_products
    )

    task_visualize_top_products=PythonOperator(
        task_id='visualize_top_products',
        python_callable=visualize_top_products
    )

    task_fetch_data >> task_process_revenue >> task_Visualization_revenue

    task_fetch_data >> task_calculate_top_products >> task_visualize_top_products



