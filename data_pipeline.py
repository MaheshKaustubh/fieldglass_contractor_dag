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
    wds=[datetime(2024,2,12).date(), datetime(2024, 2, 19).date(), datetime(2024, 2, 26).date(),
        datetime(2024, 5, 2).date(), datetime(2024, 6, 3).date(), datetime(2024, 7, 1).date(),
        datetime(2024, 8, 1).date(), datetime(2024, 9, 2).date(), datetime(2024, 10, 1).date(),
        datetime(2024, 4, 11).date(), datetime(2024, 12, 2).date()]
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
    import json
    import boto3

    #SFTP Crediantials 
    host = 'sftp.ebs.thomsonreuters.com'
    port = 22
    username = 'PSarchFGSFTP'
    password = 'geT4anKu'
    remote_path = '/Fieldglass'
    archive_path = '/Fieldglass_Archived'
    buffer = BytesIO()

    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(hostname=host, username=username, password=password, allow_agent=False)
    sftp_client = ssh_client.open_sftp()
    
    sftp_client.chdir(remote_path)

    
    #Loopin around to get latest file based on timestamp.  
    for f in sorted(sftp_client.listdir_attr(), key=lambda k: k.st_mtime, reverse=True):
        print(f.filename)
        down = sftp_client.getfo(f.filename, buffer)
        print(down)
        print("File downloaded successfully!")

        # Move the downloaded file to the "Archived" folder
        new_path = f"{archive_path}/{f.filename}"
        sftp_client.rename(f.filename, new_path)
        print(f"File moved to '{new_path}'")

        break  # Stop after processing the latest file

    # Function to remove file from archived folder.
    def archive_file_remove():
        from datetime import datetime
        wds=[datetime(2024,1,22).date(),datetime(2024, 3, 1).date(), datetime(2024, 4, 1).date(),
             datetime(2024, 5, 2).date(), datetime(2024, 6, 3).date(), datetime(2024, 7, 1).date(),
             datetime(2024, 8, 1).date(), datetime(2024, 9, 2).date(), datetime(2024, 10, 1).date(),
             datetime(2024, 4, 11).date(), datetime(2024, 12, 2).date()]
        if(datetime.now().date() in wds):
            host = 'sftp.ebs.thomsonreuters.com'
            port = 22
            username = 'PSarchFGSFTP'
            password = 'geT4anKu'
            archive_path = '/Fieldglass_Archived'

            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_client.connect(hostname=host, username=username, password=password, allow_agent=False)
            sftp_client = ssh_client.open_sftp()

            sftp_client.chdir(archive_path)

            files = sorted(sftp_client.listdir_attr(), key=lambda k: k.st_mtime, reverse=True)
            if len(files) >= 2:
                second_latest_file = files[1]
                print(f"Removing second latest file: {second_latest_file.filename}")
                sftp_client.remove(second_latest_file.filename)
                print(f"File '{second_latest_file.filename}' removed from the SFTP server.")

            sftp_client.close()
            ssh_client.close()
        else:
            return False
    
    func_call = archive_file_remove()


    # Close the SFTP session and SSH connection
    sftp_client.close()
    ssh_client.close()

    buffer.seek(0)
    df = pd.read_csv(buffer,skiprows=1)
    df['Start Date'] = pd.to_datetime(df['Start Date'], errors='coerce').dt.strftime('%Y-%m-%d').replace('NaT','')
    df['Safe End Date'] = pd.to_datetime(df['Safe End Date'], errors='coerce').dt.strftime('%Y-%m-%d').replace('NaT','')
    df['Contract End Date'] = pd.to_datetime(df['Contract End Date'], errors='coerce').dt.strftime('%Y-%m-%d').replace('NaT','')
    df = df[~df['Contingent/SOW Worker Bill Rate [ST/Day (Daily)/Day]'].apply(lambda x: pd.notna(x) and str(x).startswith('Macro Ran:'))]
    df['Primary Cost Center Code'] = df['Primary Cost Center Code'].fillna(0).astype(int)
    df['Primary Cost Center Code'] = df['Primary Cost Center Code'].astype(str)
    df['Snapshot_Date'] = pd.Timestamp.today().strftime('%Y-%m-%d')
    # df['Primary Cost Center Code'] = df['Primary Cost Center Code'].astype(str)

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
    session=boto3.Session()
    client = session.client(service_name='secretsmanager', region_name='us-east-1')
    response = client.get_secret_value(SecretId='arn:aws:secretsmanager:us-east-1:573491702041:secret:a206529-MDS-CONSREVENUE-5k48RX', VersionStage='AWSCURRENT')

    secrets=json.loads(response['SecretString'])
    # print(secrets)
    # Establish a connection to your Snowflake instance
    conn = snowflake.connector.connect(
        user=str(secrets["user"]),
        host=str(secrets["host"]),
        account=str(secrets["account"]),
        warehouse=str(secrets["warehouse"]),
        database=str(secrets["database"]),
        password=str(secrets["password"]),
        schema=str(secrets["schema"])
    )
    sfconnector = conn.cursor()
    chunked_rows_final = divide_chunks(
        data, 16384)
    chunked_rows_final = list(chunked_rows_final)
    for l in chunked_rows_final:
        try:
            sfconnector.executemany("INSERT INTO MYDATASPACE.A208043_FINANCE_STAGING.CONSOLIDATED_WORKER_HC_TBL (MAIN_DOC_ID, WO_ID, WORKER_ID,SAFE_ID, SECURITY_ID, LAST_NAME, FIRST_NAME, EMAIL_ID, TR_EMAIL_ID, EMAIL_NET_ACCESS_FLG, SUPPLIER_ID, STATUS, START_DATE, SAFE_END_DATE, CONTRACT_END_DATE,WORKER_JOB_TITLE,JOB_CODE,WORKER_TYPE,FTE,ACCOUNT_TYPE,MGR_NAME,MGR_ID,SUPR_NAME,SUPR_ID,BUSINESS_UNIT,DIVISON,BUSINESS_UNIT_1,SUB_BUSINESS_UNIT,REGION,LOC_NAME,LOC_CODE,COST_CENTER_NAME,COST_CENTER_CODE,TR_SITE_FLG,FACILITIES_ONLY_FLG,ADDRESS1,ADDRESS2,CITY,STATE,ZIP_CODE,COUNTRY,PHONE_NO,OLD_SAFE_ID,CREATE_DATE,BUYER_REF,COMMENTS,SENSITIVE_ACCESS_FLG,TENURE,PRJ_TENURE,SECID_TENURE,CURR_CODE,PROF_WRK_RATE_TYPE,PROF_WRK_BILL_RATE,ST_HR_BILL_RATE,DS_DAY_BILL_RATE,HS_HR_BILL_RATE,ST_MON_MON_MO_BILL_RATE,PWD_SB_ONCALL_HR_BILL_RATE,PWD_ST_DAY_BILL_RATE,STDAY_DAILY_DAY_BILL_RATE,SNAPSHOT_DATE) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", l)
            print("Data Loaded Successfully!")
        except Exception as e:
            print(e)


with DAG(
    'fieldglass_weekly_staging_dag',
    default_args=default_args,
    description='DAG to load file to Snowflake from SFTP',
    schedule_interval='20 6 * * *',
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
        import boto3
        import json
        import snowflake.connector
        
        session=boto3.Session()
        client = session.client(service_name='secretsmanager', region_name='us-east-1')
        response = client.get_secret_value(SecretId='arn:aws:secretsmanager:us-east-1:573491702041:secret:a206529-MDS-CONSREVENUE-5k48RX', VersionStage='AWSCURRENT')

        secrets=json.loads(response['SecretString'])
        # print(secrets)
        try:
            conn = snowflake.connector.connect(
                user=str(secrets["user"]),
                host=str(secrets["host"]),
                account=str(secrets["account"]),
                warehouse=str(secrets["warehouse"]),
                database=str(secrets["database"]),
                password=str(secrets["password"]),
                schema=str(secrets["schema"])
            )
            sfconnector= conn.cursor()
        except Exception as e:
            print(e)
        print("Successfully Created Connection")
        try:
            sfconnector.execute("CALL MYDATASPACE.A208043_FINANCE_STAGING.FIELDGLASS_WEEKLY_PBI_SP()")
        except Exception as e:
            print(e)


    WDcheck>>read_write_snow>>call_sp()
