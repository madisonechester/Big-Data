from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.email_operator import EmailOperator

import pandas as pd
from pymongo import MongoClient 
from ucimlrepo import fetch_ucirepo 
from datetime import datetime, timedelta

def download_dataset(**kwargs):
    # fetch dataset 
    online_retail = fetch_ucirepo(id=352) 
    df = online_retail.data.original
    print(df.shape)

    # save dataframe to a csv file
    file_path = '/tmp/online_retail.csv'
    df.to_csv(file_path, index=False)

    # push the file path to xcom
    kwargs['ti'].xcom_push(key='file_path', value=file_path)

def data_cleaning(**kwargs):
    # pull the file path from xcom
    ti = kwargs['ti']
    file_path = ti.xcom_pull(key='file_path', task_ids='download_dataset')
    
    # read the dataframe from the csv file
    df = pd.read_csv(file_path)
    print(df.shape)

    # clean data
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
    df.dropna(inplace = True)
    df.drop_duplicates(inplace=True)
    df.reset_index(inplace=True, drop=True)

    # save the processed dataframe back to the csv file
    file_path = '/tmp/online_retail.csv'
    df.to_csv(file_path, index=False)
    
    # push the processed file path back to xcom
    ti.xcom_push(key='file_path', value=file_path)

def data_transformation(**kwargs):
    # pull the file path from xcom
    ti = kwargs['ti']
    file_path = ti.xcom_pull(key='file_path', task_ids='data_cleaning')
    
    # read the dataframe from the csv file
    df = pd.read_csv(file_path)
    print(df.shape)

    # create the new column
    df['TotalPrice'] = df['Quantity'] * df['UnitPrice']

    # save the transformed dataframe back to the csv file
    file_path = '/tmp/online_retail.csv'
    df.to_csv(file_path, index=False)
    
    # push the transformed file path back to XCom
    ti.xcom_push(key='file_path', value=file_path)

def data_to_nosql(**kwargs):
    # pull the file path from XCom
    ti = kwargs['ti']
    file_path = ti.xcom_pull(key='file_path', task_ids='data_transformation')

    # read the dataframe from the csv file
    df = pd.read_csv(file_path)
    print(df.shape)
    retail_data = df.to_dict(orient='records')
    print(retail_data)
    client = MongoClient('mongodb://mymongo:27017')

    # specify the database to use
    db = client['mydatabase']

    # specify the collection to use
    collection = db['retail']

    # insert data into the collection
    result = collection.insert_many(retail_data)
    print(result)

# fetch the emails that will recieve the notification
emails = Variable.get("emails")
email_list = [value.strip() for value in emails.split(',')]

# defaults
default_args = {
    'owner': 'Madison',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 25),
    'email': email_list,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG('Assignment3', default_args=default_args, description="Big Data Assignment 3", schedule_interval=timedelta(days=1))

# tasks created using operators
t1 = PythonOperator(
    task_id='download_dataset',
    python_callable=download_dataset,
    dag=dag)

t2 = PythonOperator(
    task_id='data_cleaning',
    python_callable=data_cleaning,
    dag=dag)

t3 = PythonOperator(
    task_id='data_transformation',
    python_callable=data_transformation,
    dag=dag)

t4 = PythonOperator(
    task_id='data_to_nosql',
    python_callable=data_to_nosql,
    dag=dag)

# summary email task
summary_email = EmailOperator(
    task_id='send_summary_email',
    to=email_list,
    subject='DAG {{ dag.dag_id }}: Summary',
    html_content="""
    <h3>Summary for DAG {{ dag.dag_id }}</h3>
    <p>The DAG {{ dag.dag_id }} has completed its run.</p>
    <p>Run details:</p>
    <ul>
        <li>Execution Date: {{ ds }}</li>
        <li>Start Date: {{ execution_date }}</li>
        <li>End Date: {{ next_execution_date }}</li>
    </ul>
    """,
    dag=dag
)

#DAG
t1>>t2>>t3>>t4>>summary_email
