import pendulum, ast
import pandas as pd
import numpy as np
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.oauth2 import service_account

JSON_KEY_PATH = "/opt/airflow/config/final-project-03-454404-c3db50af204b.json"
DW_BUCKET = "votes_bucket"
DM_BUCKET = "dm_dashboard_bucket"
PROJECT_ID = "final-project-03-454404"
LOCATION = 'asia-northeast3'
DATASET_ID = "final-project-03-454404.looker_dashboard"

default_args = dict(
    owner='haye',
    email=['hayechoi@gmail.com'],
    email_on_failure=False,
    retries=3
)

def preprocess_and_calculate_3au(**kwargs):
    # 1. 데이터 불러오기
    accounts_attendance = pd.read_csv(
        f"gs://{DW_BUCKET}/accounts_attendance.csv",
        storage_options={'token': JSON_KEY_PATH}
    )
    accounts_blockrecord = pd.read_csv(
        f"gs://{DW_BUCKET}/accounts_blockrecord.csv",
        storage_options={'token': JSON_KEY_PATH}
    )
    accounts_friendrequest = pd.read_csv(
        f"gs://{DW_BUCKET}/accounts_friendrequest.csv",
        storage_options={'token': JSON_KEY_PATH}
    )
    accounts_paymenthistory = pd.read_csv(
        f"gs://{DW_BUCKET}/accounts_paymenthistory.csv",
        storage_options={'token': JSON_KEY_PATH}
    )
    accounts_pointhistory = pd.read_csv(
        f"gs://{DW_BUCKET}/accounts_pointhistory.csv",
        storage_options={'token': JSON_KEY_PATH}
    )
    accounts_timelinereport = pd.read_csv(
        f"gs://{DW_BUCKET}/accounts_timelinereport.csv",
        storage_options={'token': JSON_KEY_PATH}
    )
    accounts_user = pd.read_csv(
        f"gs://{DW_BUCKET}/accounts_user.csv",
        storage_options={'token': JSON_KEY_PATH}
    )
    accounts_userquestionrecord = pd.read_csv(
        f"gs://{DW_BUCKET}/accounts_userquestionrecord.csv",
        storage_options={'token': JSON_KEY_PATH}
    )
    polls_questionreport = pd.read_csv(
        f"gs://{DW_BUCKET}/polls_questionreport.csv",
        storage_options={'token': JSON_KEY_PATH}
    )

    # 2. 전처리
    accounts_attendance["attendance_date_list"] = accounts_attendance["attendance_date_list"].apply(ast.literal_eval)
    accounts_attendance = accounts_attendance.explode('attendance_date_list')
    accounts_attendance.rename(columns={"attendance_date_list": "created_at"}, inplace=True)
    accounts_attendance["created_at"] = pd.to_datetime(accounts_attendance["created_at"])
    accounts_attendance.drop(columns='id', inplace=True)
    accounts_attendance.dropna(inplace=True)

    accounts_blockrecord["created_at"] = pd.to_datetime(accounts_blockrecord["created_at"])
    accounts_friendrequest["created_at"] = pd.to_datetime(accounts_friendrequest["created_at"])
    accounts_friendrequest["updated_at"] = pd.to_datetime(accounts_friendrequest["updated_at"])
    accounts_friendrequest.rename(columns={'send_user_id': 'user_id'}, inplace=True)
    accounts_friendrequest.drop(columns='id', inplace=True)
    accounts_friendrequest.drop_duplicates(inplace=True)
    accounts_friendrequest.dropna(inplace=True)

    accounts_paymenthistory["created_at"] = pd.to_datetime(accounts_paymenthistory["created_at"])
    accounts_paymenthistory.drop(columns='id', inplace=True)
    accounts_paymenthistory.drop_duplicates(inplace=True)

    accounts_pointhistory["created_at"] = pd.to_datetime(accounts_pointhistory["created_at"])
    accounts_pointhistory.drop(columns='id', inplace=True)
    accounts_pointhistory.drop_duplicates(inplace=True)

    accounts_timelinereport["created_at"] = pd.to_datetime(accounts_timelinereport["created_at"])
    accounts_timelinereport.drop(columns='id', inplace=True)
    accounts_timelinereport.drop_duplicates(inplace=True)

    accounts_user.rename(columns={'id': 'user_id'}, inplace=True)
    accounts_user["created_at"] = pd.to_datetime(accounts_user["created_at"])
    accounts_user.dropna(subset='gender', inplace=True)

    accounts_userquestionrecord["created_at"] = pd.to_datetime(accounts_userquestionrecord["created_at"])
    accounts_userquestionrecord.drop(columns='id', inplace=True)
    accounts_userquestionrecord.drop_duplicates(inplace=True)

    polls_questionreport["created_at"] = pd.to_datetime(polls_questionreport["created_at"])
    polls_questionreport.drop(columns='id', inplace=True)
    polls_questionreport.drop_duplicates(inplace=True)

    dfs = [
        accounts_attendance, accounts_blockrecord, accounts_friendrequest,
        accounts_paymenthistory, accounts_pointhistory, accounts_timelinereport,
        accounts_user, accounts_userquestionrecord, polls_questionreport
    ]

    df_combined = pd.concat([df[['user_id', 'created_at']] for df in dfs], ignore_index=True)
    df_combined['created_at'] = pd.to_datetime(df_combined['created_at'])

    # 3. DAU/WAU/MAU 계산
    df_combined['date'] = df_combined['created_at'].dt.date
    df_combined['week'] = df_combined['created_at'].dt.to_period('W').apply(lambda r: r.start_time.date())
    df_combined['month'] = df_combined['created_at'].dt.to_period('M').apply(lambda r: r.start_time.date())

    df_combined.groupby('date')['user_id'].nunique().reset_index(name='DAU') \
        .to_csv(f"gs://{DM_BUCKET}/dau.csv", index=False, storage_options={'token': JSON_KEY_PATH})
    df_combined.groupby('week')['user_id'].nunique().reset_index(name='WAU') \
        .to_csv(f"gs://{DM_BUCKET}/wau.csv", index=False, storage_options={'token': JSON_KEY_PATH})
    df_combined.groupby('month')['user_id'].nunique().reset_index(name='MAU') \
        .to_csv(f"gs://{DM_BUCKET}/mau.csv", index=False, storage_options={'token': JSON_KEY_PATH})


def upload_3au_to_bigquery(**kwargs):
    credentials = service_account.Credentials.from_service_account_file(JSON_KEY_PATH)
    for filename in ['dau', 'wau', 'mau']:
        df = pd.read_csv(
            f"gs://{DM_BUCKET}/{filename}.csv",
            storage_options={'token': JSON_KEY_PATH}
        )
        df.to_gbq(
            destination_table=f"{DATASET_ID}.{filename}",
            project_id=PROJECT_ID,
            location=LOCATION,
            if_exists='replace',
            credentials=credentials
        )

with DAG(
    dag_id="3AU_dag",
    start_date=pendulum.datetime(2024, 11, 10, tz="Asia/Seoul"),
    schedule="0 8 * * *",
    catchup=False,
    tags=['3AU', 'user_metrics'],
    default_args=default_args
) as dag:
    preprocess_and_calculate = PythonOperator(
        task_id="preprocess_and_calculate_3au",
        python_callable=preprocess_and_calculate_3au
    )

    upload_to_bq = PythonOperator(
        task_id="upload_3au_to_bigquery",
        python_callable=upload_3au_to_bigquery
    )

    preprocess_and_calculate >> upload_to_bq
