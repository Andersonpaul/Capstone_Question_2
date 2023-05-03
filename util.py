import psycopg2
import os
from dotenv import dotenv_values
import datetime

config = dict (dotenv_values(".env"))

def get_database_conn():
    # Get database credentials from environment variable
    config = dict (dotenv_values(".env"))

    IAM_ROLE = config.get('IAM_ROLE')
    db_user_name = config.get('DB_USER_NAME')
    db_password = config.get('DB_PASSWORD')
    host = config.get('HOST')
    port = config.get('PORT')
    db_name = config.get('DB_NAME')
    # Create and return a postgresql database connection object
    return psycopg2.connect(f'postgresql://{db_user_name}:{db_password}@{host}:{port}/{db_name}')



def load_data(table_name):
    s3_path = f's3://jobscapstone/transformed/output_{str(datetime.date.today())}.csv' 
    iam_role = config.get('IAM_ROLE')

    sql_stmt = f"""
    copy {table_name}
    from '{s3_path}'
    IAM_ROLE '{iam_role}'
    csv
    IGNOREHEADER 1;
    """
    conn = get_database_conn()
    cur = conn.cursor()
    cur.execute(sql_stmt)
    conn.commit()
    cur.close() # Close cursor
    conn.close() # Close connection

    print('Data successfully written to PostgreSQL database')