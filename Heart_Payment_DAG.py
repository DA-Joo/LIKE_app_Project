from airflow import DAG
from airflow.operators.python import PythonOperator
from google.oauth2 import service_account
from datetime import datetime, timedelta
import pandas as pd
import os
import pendulum

default_args = dict(
    owner='flowerdong',
    email=['flowerdong@airflow.com'],
    email_on_failure=False,
    retries=3
)

# 경로 및 변수 설정
JSON_KEY_PATH = "/opt/airflow/config/final-project-03-454404-c3db50af204b.json"
DW_BUCKET = "votes_bucket"
DM_BUCKET = "dm_dashboard_bucket"
PROJECT_ID = "final-project-03-454404"
LOCATION = "asia-northeast3"


def dw_to_dm_payment(**kwargs):
    df = pd.read_csv(
        f"gs://{DW_BUCKET}/accounts_paymenthistory.csv",
        storage_options={'token': JSON_KEY_PATH}
    )

    df['created_at'] = pd.to_datetime(df['created_at'])
    df.dropna(subset=['user_id', 'productId', 'created_at'], inplace=True)
    df['date'] = df['created_at'].dt.date

    # heart 종류만 필터링
    heart_types = ['heart.777', 'heart.200', 'heart.4000', 'heart.1000']
    df = df[df['productId'].isin(heart_types)]

    # heart 숫자 추출
    df['heart_count'] = df['productId'].str.extract(r'heart\.(\d+)').astype(int)

    # 일별 총 매출
    revenue = df.groupby('date')['heart_count'].sum().reset_index(name='daily_revenue')

    # 일별 heart 종류별 개수
    heart_counts = (
        df.groupby(['date', 'productId'])
        .size()
        .reset_index(name='count')
        .pivot(index='date', columns='productId', values='count')
        .fillna(0)
        .astype(int)
        .reset_index()
    )

    # merge
    daily_df = pd.merge(revenue, heart_counts, on='date')
    daily_df = daily_df[['date', 'daily_revenue'] + heart_types]

    daily_df.rename(columns={'heart.777' : 'heart_777', 'heart.200' : 'heart_200', 'heart.4000' : 'heart_4000', 'heart.1000' : 'heart_1000'}, inplace=True)


    # 저장
    daily_df.to_csv(
        f"gs://{DM_BUCKET}/daily_heart_metrics.csv",
        index=False,
        storage_options={'token': JSON_KEY_PATH}
    )


def dm_to_bigquery_payment(**kwargs):
    credentials = service_account.Credentials.from_service_account_file(JSON_KEY_PATH)
    PROJECT_ID = "final-project-03-454404"
    DATASET_ID = "looker_dashboard"
    TABLE_ID = "daily_heart_metrics"

    df_daily = pd.read_csv(
        f"gs://{DM_BUCKET}/daily_heart_metrics.csv",
        storage_options={'token': JSON_KEY_PATH}
    )
    df_daily.to_gbq(
        destination_table=f"{DATASET_ID}.{TABLE_ID}",
        project_id=PROJECT_ID,
        location=LOCATION,
        if_exists='replace',
        credentials=credentials
    )


with DAG(
    dag_id="heart_payment_dag",
    start_date=pendulum.datetime(2023, 5, 13, tz="Asia/Seoul"),
    schedule="0 8 * * *",
    catchup=False,
    tags=['heart', 'revenue'],
    default_args=default_args
) as dag:

    task_dw_to_dm_payment = PythonOperator(
        task_id='dw_to_dm_heart',
        python_callable=dw_to_dm_payment
    )

    task_dm_to_bq_payment = PythonOperator(
        task_id='dm_to_bigquery_heart',
        python_callable=dm_to_bigquery_payment
    )

    task_dw_to_dm_payment >> task_dm_to_bq_payment