from datetime import timedelta, timezone, date
import datetime as dt
from distutils.command.clean import clean
from airflow.decorators import dag, task
from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator, ShortCircuitOperator, PythonOperator
from airflow.operators.bash import BashOperator
import logging
from airflow.sensors.python import PythonSensor
import pandas as pd
import snowflake.connector


default_args = {
    'owner': 'kaustubhmahesh',
    'depends_on_past': False,
    'start_date': dt.datetime(2023, 8, 2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def check():
    from datetime import datetime
    wds=[datetime(2024,1,15).date(), datetime(2024, 1, 9).date(), datetime(2024, 1, 10).date(), datetime(2023, 12, 23).date(), datetime(2023, 12, 24).date(), datetime(2023,12,17).date(), datetime(2023,12,18).date(),datetime(2023,12,19).date()]
    if(datetime.now().date() in wds):
        return True
    else:
        return False


def download_file_from_sftp():
    import paramiko
    from io import BytesIO
    import datetime as dt
    import snowflake.connector
    import pandas as pd 

    host = 'sftp.ebs.thomsonreuters.com'
    port = 22
    username = 'PSarchFGSFTP'
    password = 'geT4anKu'
    remote_path = '/Fieldglass'
    buffer = BytesIO()

    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(hostname=host, username=username, password=password,allow_agent=False)
    sftp_client = ssh_client.open_sftp()

    sftp_client.chdir(remote_path)
    for f in sorted(sftp_client.listdir_attr(), key=lambda k: k.st_mtime, reverse=True):
        print(f.filename)
        down = sftp_client.getfo(f.filename, buffer)
        print(down)
        print("File downloaded successfully!")
        break
    # Close the SFTP session and SSH connection
    sftp_client.close()
    ssh_client.close()


    buffer.seek(0)
    df = pd.read_csv(buffer,skiprows=1)
    df['Start Date'] = pd.to_datetime(df['Start Date'], errors='coerce').dt.strftime('%Y-%m-%d').replace('NaT','')
    df['Safe End Date'] = pd.to_datetime(df['Safe End Date'], errors='coerce').dt.strftime('%Y-%m-%d').replace('NaT','')
    df['Contract End Date'] = pd.to_datetime(df['Contract End Date'], errors='coerce').dt.strftime('%Y-%m-%d').replace('NaT','')

    df['Snapshot_Date'] = pd.Timestamp.today().strftime('%Y-%m-%d')
    df['Primary Cost Center Code'] = df['Primary Cost Center Code'].astype(str)

    def clean_numeric_column(column):
        return pd.to_numeric(column.replace(',', '', regex=True), errors='coerce', downcast='integer')


    columns_to_clean = ['Tenure','Tenure based upon Security ID', 'Projected Tenure', 'Profile Worker Bill Rate','Contingent/SOW Worker Bill Rate [ST/Hr]','Contingent/SOW Worker Bill Rate [Monthly Base Salary/MO]','Contingent/SOW Worker Bill Rate [PWD Standard/Day]']
    df[columns_to_clean] = df[columns_to_clean].apply(clean_numeric_column)
    df.columns = map(lambda x: str(x).upper(), df.columns)
    df.transform(lambda x: x.fillna('') if x.dtype == 'object' else x.fillna(0))


    def divide_chunks(l, n):
        for i in range(0, len(l), n):
            yield l[i:i + n]

    data = []
    time=dt.datetime.now()
    # if str.strip(row[12]) != '' else None
    for index, row in df.iterrows():
        row = tuple(row)+(time,)
        # row = [None if pd.isna(value) else value for value in row]  # Replace NaT with None
        row = [value if pd.notnull(value) and not pd.isnull(value) else None for value in row]

        data.append((row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10],
                    row[11], row[12] if str.strip(str(row[12])) != '' else None, row[13] if str.strip(str(row[13])) != '' else None, row[14] if str.strip(str(row[14])) != '' else None, row[15], row[16], row[17], row[18], row[19], row[20],
                    row[21], row[22], row[23], row[24], row[25], row[26], row[27], row[28], row[29], row[30],
                    row[31], row[32], row[33], row[34], row[35], row[36], row[37], row[38], row[39], row[40],
                    row[41], row[42], row[43], row[44], row[45], row[46], row[47], row[48], row[49], row[50],
                    row[51], row[52], row[53], row[54], row[55], row[56], row[57], row[58], row[59], row[60]))
        # return 
    conn = snowflake.connector.connect(
        user='a208043_finance_staging_dev_svc_user',
        host="a206448_prod.us-east-1.snowflakecomputing.com",
        account="a206448_prod.us-east-1",
        warehouse="A208043_FINANCE_STAGING_DEV_MDS_WH",
        database="MYDATASPACE",
        password="612NIxX0Df9kzaP1AcO8",
        schema="A208043_FINANCE_STAGING_DEV"
        )

    sfconnector = conn.cursor()
    chunked_rows_final = divide_chunks(
        data, 16384)
    chunked_rows_final = list(chunked_rows_final)
    for l in chunked_rows_final:
        try:
            sfconnector.executemany("INSERT INTO CONSOLIDATED_WORKER_HC_TBL_DRAFT_KAUSTUBH (MAIN_DOC_ID, WO_ID, WORKER_ID,SAFE_ID, SECURITY_ID, LAST_NAME, FIRST_NAME, EMAIL_ID, TR_EMAIL_ID, EMAIL_NET_ACCESS_FLG, SUPPLIER_ID, STATUS, START_DATE, SAFE_END_DATE, CONTRACT_END_DATE,WORKER_JOB_TITLE,JOB_CODE,WORKER_TYPE,FTE_DESC,ACCOUNT_TYPE,MGR_NAME,MGR_ID,SUPR_NAME,SUPR_ID,BUSINESS_UNIT_1,DIVISION,BUSINESS_UNIT_2,SUB_BUSINESS_UNIT,REGION,LOC_NAME,LOC_CODE,PRIMARY_COST_CENTER_NAME,PRIMARY_COST_CENTER_CODE,TR_SITE_FLG,FACILITIES_ONLY_FLG,ADDRESS1,ADDRESS2,CITY,STATE,ZIP_CODE,COUNTRY,PHONE_NO,OLD_SAFE_ID,CREATE_DATE,BUYER_REF,COMMENTS,SENSITIVE_ACCESS_FLG,TENURE,PRJ_TENURE,SECID_TENURE,CURRENCY,PROF_WRK_RATE_TYPE,PROF_WRK_BILL_RATE,ST_HR_BILL_RATE,DS_DAY_BILL_RATE,HS_HR_BILL_RATE,ST_MON_MON_MO_BILL_RATE,PWD_SB_ONCALL_HR_BILL_RATE,PWD_ST_DAY_BILL_RATE,STDAY_DAILY_DAY_BILL_RATE,SNAPSHOT_DATE) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", l)
            print("Data Loaded Successfully!")
        except Exception as e:
            print(e)


with DAG(
    'fieldglass_weekly_staging_dag',
    default_args=default_args,
    description='DAG to load file to Snowflake from SFTP',
    schedule_interval='7 9 * * *',
    catchup=False,
) as dag:
    WDcheck = ShortCircuitOperator(
        task_id = 'Workday_Check',
        python_callable = check
    )
    
    read_write_snow= PythonVirtualenvOperator(
        task_id='read_write_snow',
        requirements=["snowflake-connector-python","pandas"],
        python_callable=download_file_from_sftp,
        system_site_packages=True,
        provide_context=True
    )

    @task()
    def call_sp():
        import snowflake.connector
        try:
            conn= snowflake.connector.connect(
                    user='a208043_finance_staging_dev_svc_user',
                    host="a206448_prod.us-east-1.snowflakecomputing.com",
                    account="a206448_prod.us-east-1",
                    warehouse="A208043_FINANCE_STAGING_DEV_MDS_WH",
                    database="MYDATASPACE",
                    password="612NIxX0Df9kzaP1AcO8",
                    schema="A208043_FINANCE_STAGING_DEV"
            )
            sfconnector= conn.cursor()
        except Exception as e:
            print(e)
        print("Successfully Created Connection")
        try:
            sfconnector.execute("CALL FIELDGLASS_WEEKLY_STG()")
        except Exception as e:
            print(e)


    WDcheck>>read_write_snow
