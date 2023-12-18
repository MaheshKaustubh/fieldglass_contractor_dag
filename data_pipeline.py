from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator, BranchPythonOperator, ShortCircuitOperator
from pathlib import Path


default_args = {
    'owner': 'kaustubhmahesh',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 15),  
    'email': ['kaustubh.mahesh@thomsonreuters.com','nikhil.vaishnav@thomsonreuters.com', 'gautham.ranand@thomsonreuters.com', 'santoshkumar.banakar@thomsonreuters.com', 'aruna.tn@thomsonreuters.com', 'kirti.birla@thomsonreuters.com'],
    'email_on_retry': True,
    'email_on_success': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def check():
    from datetime import datetime
    wds=[datetime(2023, 8, 1).date(), datetime(2023, 9, 1).date(), datetime(2023, 10, 3).date(), datetime(2023, 11, 2).date(), datetime(2023, 12, 4).date(), datetime(2023,12,17).date(), datetime(2023,12,18).date(), datetime(2023,12,19).date(), datetime(2023,12,20).date(), datetime(2023,12,21).date(), datetime(2023,12,22).date()]
    if(datetime.now().date() in wds):
        return True
    else:
        return False



def load_to_snow():
    import snowflake.connector
    import os
    import paramiko
    host = 'sftp.ebs.thomsonreuters.com'
    port = 22
    username = 'PSarchFGSFTP'
    password = 'geT4anKu'
    remote_file_path = '/Fieldglass/CONSOLIDATED_WORKER_HEADCOUNT_TEST.csv'
    local_file_path = 'CONSOLIDATED_WORKER_HEADCOUNT_TEST.csv'

 

    try:
            transport = paramiko.Transport((host, port))
            transport.connect(username=username, password=password)
            sftp = paramiko.SFTPClient.from_transport(transport)

            down= sftp.get(remote_file_path, local_file_path)
            print(down)
            print("File downloaded successfully!")

            sftp.close()
            transport.close()

    except Exception as e:
            print(f"Error: {e}")

    # import boto3
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
        sfconnector.execute("create or replace stage DATA_LOAD_STAGE file_format=CSV_HEADER")
    except Exception as e:
        print(e)

    # file_path = 'C:/Users/6126176/AppData/Local/Packages/CanonicalGroupLimited.Ubuntu_79rhkp1fndgsc/LocalState/rootfs/home/kaustubhmahesh/airflow/CONSOLIDATED_WORKER_HEADCOUNT_TEST.csv'    
# C:/Users/6126176/AppData/Local/Packages/CanonicalGroupLimited.Ubuntu_79rhkp1fndgsc/LocalState/rootfs/home/kaustubhmahesh/airflow/CONSOLIDATED_WORKER_HEADCOUNT_TEST.csv
    file_path='/usr/local/airflow/CONSOLIDATED_WORKER_HEADCOUNT_TEST.csv'
    if os.path.exists(file_path):
        try: 
            sfconnector.execute(f"put file://{file_path} @DATA_LOAD_STAGE auto_compress=true")
        except Exception as e: 
            print(e)
    else: 
        print("File not Found")
    try: 
        sfconnector.execute("insert into CONSOLIDATED_WORKER_HC_TBL_DRAFT_KAUSTUBH select t.$1,t.$2,t.$3,t.$4,t.$5,t.$6,t.$7,t.$8,t.$9,t.$10,t.$11,t.$12,t.$13,t.$14,t.$15,t.$16,t.$17,t.$18,t.$19,t.$20,t.$21,t.$22,t.$23,t.$24,t.$25,t.$26,t.$27,t.$28,t.$29,t.$30,t.$31,t.$32,t.$33,t.$34,t.$35,t.$36,t.$37,t.$38,t.$39,t.$40,t.$41,t.$42,t.$43,t.$44,t.$45,t.$46,t.$47,t.$48,t.$49,t.$50,t.$51,t.$52,t.$53,t.$54,t.$55,t.$56,t.$57,t.$58,t.$59,t.$60, current_timestamp from @data_load_stage/CONSOLIDATED_WORKER_HEADCOUNT_TEST.csv.gz as t")
    except Exception as e:
        print(e)

    # import os 
    os.remove('CONSOLIDATED_WORKER_HEADCOUNT_TEST.csv')


with DAG(
    'FIELDGLASS_CONTRACTOR_DAG',
    default_args=default_args,
    description='DAG to download file from SFTP',
    schedule_interval='15 17 * * *',
    catchup=False
) as dag:
    WDCheck=ShortCircuitOperator(
        task_id='Workday_Check',
        python_callable= check
    )

    # @task()
    # def download_file_from_sftp():
    #     import paramiko
    #     host = 'sftp.ebs.thomsonreuters.com'
    #     port = 22
    #     username = 'PSarchFGSFTP'
    #     password = 'geT4anKu'
    #     remote_file_path = '/Fieldglass/CONSOLIDATED_WORKER_HEADCOUNT_TEST.csv'
    #     local_file_path = 'CONSOLIDATED_WORKER_HEADCOUNT_TEST.csv'

 

    #     try:
    #         transport = paramiko.Transport((host, port))
    #         transport.connect(username=username, password=password)
    #         sftp = paramiko.SFTPClient.from_transport(transport)

    #         down= sftp.get(remote_file_path, local_file_path)
    #         print(down)
    #         print("File downloaded successfully!")

    #         sftp.close()
    #         transport.close()

    #     except Exception as e:
    #         print(f"Error: {e}")


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
            sfconnector.execute("CALL STORED_PROCEDURES_CALL()")
        except Exception as e:
            print(e)

 

    load_to_snowflake= PythonVirtualenvOperator(
            task_id='load_to_snowflake',
            requirements=["snowflake-connector-python"],
            python_callable=load_to_snow
        )

    WDCheck>>load_to_snowflake

    # WDCheck>>download_file_from_sftp()>>load_to_snowflake>>call_sp()
