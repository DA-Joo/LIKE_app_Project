import pendulum, requests
from google.oauth2 import service_account
from google.cloud import bigquery
from airflow import DAG
import pandas as pd
import numpy as np
from airflow.operators.python import PythonOperator
import ast

default_args = dict(
    owner = 'joo',
    email = ['joohyeong@airflow.com'],
    email_on_failure = False,
    retries = 3
    )

JSON_KEY_PATH = "/opt/airflow/config/final-project-03-454404-c3db50af204b.json"
DW_BUCKET = "votes_bucket"
DM_BUCKET = "dm_dashboard_bucket"
PROJECT_ID = "final-project-03-454404"
LOCATION = "asia-northeast3"

def dw_to_dm(**kwargs):
    
    # GCS에 있는 파일 읽기
    accounts_attendance = pd.read_csv(
        f"gs://{DW_BUCKET}/accounts_attendance.csv",
        storage_options={'token' : JSON_KEY_PATH}
    )
    accounts_friendrequest = pd.read_csv(
        f"gs://{DW_BUCKET}/accounts_friendrequest.csv",
        storage_options={'token' : JSON_KEY_PATH}
    )
    accounts_userquestionrecord = pd.read_csv(
        f"gs://{DW_BUCKET}/accounts_userquestionrecord.csv",
        storage_options={'token' : JSON_KEY_PATH}
    )
    accounts_blockrecord = pd.read_csv(
        f"gs://{DW_BUCKET}/accounts_blockrecord.csv",
        storage_options={'token' : JSON_KEY_PATH}
    )
    accounts_timelinereport = pd.read_csv(
        f"gs://{DW_BUCKET}/accounts_timelinereport.csv",
        storage_options={'token' : JSON_KEY_PATH}
    )
    polls_questionreport = pd.read_csv(
        f"gs://{DW_BUCKET}/polls_questionreport.csv",
        storage_options={'token' : JSON_KEY_PATH}
    )
    accounts_paymenthistory = pd.read_csv(
        f"gs://{DW_BUCKET}/accounts_paymenthistory.csv",
        storage_options={'token' : JSON_KEY_PATH}
    )
    accounts_user = pd.read_csv(
        f"gs://{DW_BUCKET}/accounts_user.csv",
        storage_options={'token' : JSON_KEY_PATH}
    )
    accounts_pointhistory = pd.read_csv(
        f"gs://{DW_BUCKET}/accounts_pointhistory.csv",
        storage_options={'token' : JSON_KEY_PATH}
    )

    # 전처리
    ## accounts_attendance
    accounts_attendance
    # accounts_attendance 날짜 리스트 형식 풀기
    accounts_attendance["attendance_date_list"] = accounts_attendance["attendance_date_list"].apply(ast.literal_eval)
    accounts_attendance = accounts_attendance.explode('attendance_date_list')
    # 컬럼명 created_at으로 변경
    accounts_attendance.rename(columns = {"attendance_date_list" : "created_at"}, inplace = True)
    accounts_attendance = accounts_attendance.reset_index(drop=True)
    # 날짜 데이터 형식 변경
    accounts_attendance["created_at"] = pd.to_datetime(accounts_attendance["created_at"])
    # id 컬럼 제거
    accounts_attendance.drop(columns='id', inplace=True)
    # 날짜 결측치 제거(약 20000건)
    accounts_attendance.dropna(inplace=True)
    ## accounts_blockrecord
    # 날짜 데이터 형식 변경
    accounts_blockrecord["created_at"] = pd.to_datetime(accounts_blockrecord["created_at"])

    ## accounts_friendrequest
    accounts_friendrequest
    # 날짜 데이터 형식 변경
    accounts_friendrequest["created_at"] = pd.to_datetime(accounts_friendrequest["created_at"])
    accounts_friendrequest["updated_at"] = pd.to_datetime(accounts_friendrequest["updated_at"])
    # accounts_friendrequest 컬럼명 변경 
    accounts_friendrequest.rename(columns={'send_user_id': 'user_id'}, inplace=True)
    accounts_friendrequest.drop(columns='id', inplace=True)
    accounts_friendrequest.drop_duplicates(inplace=True)
    accounts_friendrequest.dropna(inplace=True)
    ## accounts_paymenthistory
    accounts_paymenthistory
    # 날짜 데이터 형식 변경
    accounts_paymenthistory["created_at"] = pd.to_datetime(accounts_paymenthistory["created_at"])
    # id 컬럼 제거
    accounts_paymenthistory.drop(columns='id', inplace=True)
    # 중복값 제거
    accounts_paymenthistory.drop_duplicates(inplace=True)
    ## accounts_pointhistory
    # 날짜 데이터 형식 변경
    accounts_pointhistory["created_at"] = pd.to_datetime(accounts_pointhistory["created_at"])
    # id 컬럼 제거
    accounts_pointhistory.drop(columns='id', inplace=True)
    # 중복값 제거
    accounts_pointhistory.drop_duplicates(inplace=True)
    ## accounts_timelinereport
    # 날짜 데이터 형식 변경
    accounts_timelinereport["created_at"] = pd.to_datetime(accounts_timelinereport["created_at"])
    # id 컬럼 제거
    accounts_timelinereport.drop(columns='id', inplace=True)
    # 중복값 제거
    accounts_timelinereport.drop_duplicates(inplace=True)
    ## accounts_user
    accounts_user.rename(columns={'id': 'user_id'}, inplace=True)
    # 날짜 데이터 형식 변경
    accounts_user["created_at"] = pd.to_datetime(accounts_user["created_at"])
    accounts_user.dropna(subset='gender', inplace=True)
    ## accounts_userquestionrecord
    # 날짜 데이터 형식 변경
    accounts_userquestionrecord["created_at"] = pd.to_datetime(accounts_userquestionrecord["created_at"])
    # id 컬럼 제거
    accounts_userquestionrecord.drop(columns='id', inplace=True)
    # 중복값 제거
    accounts_userquestionrecord.drop_duplicates(inplace=True)
    ## polls_questionreport
    polls_questionreport
    # 날짜 데이터 형식 변경
    polls_questionreport["created_at"] = pd.to_datetime(polls_questionreport["created_at"])
    # id 컬럼 제거
    polls_questionreport.drop(columns='id', inplace=True)
    # 중복값 제거
    polls_questionreport.drop_duplicates(inplace=True)
    ## 통합 기록 데이터프레임 생성
    dfs = [
        accounts_attendance, accounts_blockrecord, accounts_friendrequest,
        accounts_paymenthistory, accounts_pointhistory, accounts_timelinereport,
        accounts_user, accounts_userquestionrecord, polls_questionreport
    ]

    # 각 데이터프레임에서 'user_id'와 'created_at' 컬럼만 선택 후 결합
    df_combined = pd.concat([df[['user_id', 'created_at']] for df in dfs], ignore_index=True)
    # created_at 날짜형식 통일(%Y-%m-%d)
    df_combined['created_at'] = pd.to_datetime(df_combined['created_at'])
    df_combined['created_at'] = pd.to_datetime(df_combined['created_at'], format='mixed').dt.strftime('%Y-%m-%d')
    df_combined['created_at'] = pd.to_datetime(df_combined['created_at'])


    ### 중복값 확인 및 제거
    df_combined.drop_duplicates(inplace=True)
    ### 통합 데이터 날짜순 변경
    df_combined = df_combined.sort_values(by='created_at')

    # user_id 별 최소 created_at 값 찾기(코호트 생성)
    min_dates = df_combined.groupby('user_id')['created_at'].min().reset_index()
    min_dates['created_at'] = pd.to_datetime(min_dates['created_at'])
    min_dates['cohort_month'] = min_dates['created_at'].dt.to_period('M')

    df_combined = df_combined.merge(min_dates[['user_id', 'cohort_month']], on='user_id')
    df_combined['days_since_first_act'] = (df_combined['created_at'] - df_combined.groupby('user_id')['created_at'].transform('min')).dt.days

    target_days = [0, 3, 7, 14, 30, 90, 180]
    df_combined['days_since_first_act'] = df_combined['days_since_first_act'].apply(lambda x: min([d for d in target_days if d >= x], default=np.nan))

    df_combined = df_combined.dropna()

    cohort_data = df_combined.groupby(['cohort_month', 'days_since_first_act']).agg(n_customers=('user_id', 'nunique')).reset_index()

    # 코호트 피벗 테이블
    cohort_pivot = cohort_data.pivot_table(index='cohort_month', columns='days_since_first_act', values='n_customers')

    # 코호트 사이즈 산출
    cohort_size = cohort_pivot.iloc[:, 0]

    # 유지율 계산
    retention_matrix = np.round(cohort_pivot.divide(cohort_size, axis=0) * 100, 2)
    retention_matrix.iloc[:, 0] = cohort_size

    # 컬럼명 변경
    retention_chart = retention_matrix.rename(columns={
        0.0: 'volumn', 3.0: '3days', 7.0: '7days', 
        14.0: '14days', 30.0: '30days', 90.0: '90days', 180.0: '180days'
    }).reset_index()  # 인덱스를 컬럼으로 변환

    print(retention_chart)  # 변경 확인

    # 변환된 DF를 GCS에 저장
    retention_chart.to_csv(
        f"gs://{DM_BUCKET}/retention_chart.csv",
        index=False,  # 인덱스 제외
        storage_options={'token': JSON_KEY_PATH}
    )

    
    

        
def dm_to_bigquery(**kwargs):
    credentials = service_account.Credentials.from_service_account_file(JSON_KEY_PATH)
    PROJECT_ID = "final-project-03-454404"
    DATASET_ID = "looker_dashboard"
    TABLE_ID = "retention_chart"


    retention_metrix = pd.read_csv(
        f"gs://{DM_BUCKET}/retention_chart.csv",
        storage_options={'token': JSON_KEY_PATH}
    )
    print(retention_metrix)
    retention_metrix.to_gbq(
        destination_table=f"{DATASET_ID}.{TABLE_ID}",
        project_id=PROJECT_ID,
        location=LOCATION,
        if_exists='replace',
        credentials=credentials
    )
    print("BigQuery retention chart 업로드 완료")

with DAG(
    dag_id="retention_analysis_dag",
    start_date=pendulum.datetime(2024, 11, 10, tz='Asia/Seoul'),
    schedule="0 8 * * *", # cron 표현식
    tags = ['retention chart'],
    default_args = default_args,
    catchup=False
) as dag:
    
    dw_to_dm_preprocessing = PythonOperator(
        task_id = "dw_to_dm",
        python_callable=dw_to_dm,
    )

    dm_to_bqr = PythonOperator(
        task_id = "dm_to_bigquery",
        python_callable=dm_to_bigquery
    )

dw_to_dm_preprocessing >> dm_to_bqr