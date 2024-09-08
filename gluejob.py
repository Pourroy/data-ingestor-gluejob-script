from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import sys
import pandas as pd
import boto3
from io import BytesIO
import pandas as pd
from datetime import datetime, timedelta
import json
import pymssql
import psycopg2

def convert_boolean_to_string(value):
    value_dict = {
        False: 'false',
        True: 'true',
        'False': 'false',
        'True': 'true',
        'f': 'false',
        't': 'true'
    }
    if value not in value_dict:
        return value
    else:
        return value_dict[value]

def table_with_booleans_to_convert(table):
    tables_with_booleans = ['retail_order_migrations', 'retail_subscription_readjustments']
    return table in tables_with_booleans

def boolean_columns_by_table(table):
    tables = {
        'retail_order_migrations': ['pre_paid', 'main'],
        'retail_subscription_readjustments': ['suspended']
    }
    if table_with_booleans_to_convert(table):
        return tables[table]
    else:
        return []

def converting_boolean_values_to_string(data_frame, table):
    values_list = boolean_columns_by_table(table)
    for value in values_list:
        data_frame[value] = data_frame[value].apply(convert_boolean_to_string)
    return data_frame

def get_param(param):
    try:
        response = ssm_client.get_parameter(Name=param, WithDecryption=True)
        return response
    except Exception as e:
        logger.info(f"JobErrror::get_param: {e}")
        return e

def get_credentials():
    response = get_param('/aws/reference/secretsmanager/op-buc/assigned-products-credentials')
    try:
        result_string = response['Parameter']['Value'].replace("\\", "")
        result_json = json.loads(result_string)
        return result_json
    except Exception as e:
        logger.info(f"JobErrror::get_credentials: {e}")
        return e

def get_parameters():
    response = get_param('/op-buc/assigned-products-parameters')
    try:
        result_string = response['Parameter']['Value'].replace("\\", "")
        result_json = json.loads(result_string)
        return result_json
    except Exception as e:
        logger.info(f"JobErrror::get_parameters: {e}")
        return e

def str_current_midnight_date():
    current_datetime = datetime.now()
    current_datetime_midnight = current_datetime.replace(hour=0, minute=0, second=0, microsecond=0)
    str_current_datetime_midnight = current_datetime_midnight.strftime('%Y-%m-%d %H:%M:%S')

    return str_current_datetime_midnight

def default_last_execution_dict():
    last_execution_date = datetime.strptime(str_current_midnight_date(),  '%Y-%m-%d %H:%M:%S') - timedelta(days=1)
    str_last_execution_date = last_execution_date.strftime('%Y-%m-%d %H:%M:%S')

    tables_last_execution_date_dict = {
        "retail_orders": str_last_execution_date,
        "retail_subscriptions": str_last_execution_date,
        "retail_plans": str_last_execution_date,
        "retail_items": str_last_execution_date,
        "retail_provisionings": str_last_execution_date,
        "retail_order_migrations": str_last_execution_date,
        "retail_migrations": str_last_execution_date,
        "retail_subscription_readjustments": str_last_execution_date,
        "checkout_orders": str_last_execution_date
    }

    return tables_last_execution_date_dict

def parameter_store_get_date():
    response = ssm_client.get_parameter(Name='/op-buc/assigned-products-schedule-date')

    all_tables_last_execution = default_last_execution_dict()

    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        result_string = response['Parameter']['Value'].replace("\\", "")
        result_string = result_string.replace("'", '"')
        result_json = json.loads(result_string)
        all_tables_last_execution = result_json

        logger.info(f"OP::BUC::AssignedProduct::JOB: Current value of parameter - '/op-buc/assigned-products-schedule-date': {all_tables_last_execution}")
    else:
        logger.error('OP::BUC::AssignedProduct::JOB: Error getting Parameter to CronJob')

    return all_tables_last_execution

def parameter_store_update_date():
    logger.info('OP::BUC::AssignedProduct::JOB: Start Putting Parameter to ParameterStore')
    srt_current_date = str(date_object)

    response = ssm_client.put_parameter(
        Name='/op-buc/assigned-products-schedule-date',
        Value=srt_current_date,
        Overwrite=True,
        Type='String'
    )

    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        logger.info(f"OP::BUC::AssignedProduct::JOB: Parameter '/op-buc/assigned-product-schedule-date' new value is: {srt_current_date}")
    else:
        logger.error('OP::BUC::AssignedProduct::JOB: Error Putting Parameter to ParameterStore')

def update_table_execution_datetime(table):
    datetime_value = today_datetime_sqlserver if table == 'checkout_orders' else today_datetime_pqsl
    logger.info(f"OP::BUC::AssignedProduct::JOB: Updating last execution date of table {table} to {datetime_value}")
    date_object[table] = datetime_value

def default_behavior():
    logger.info(f"OP::BUC::AssignedProduct::JOB: Combination of JobTriggerOrigin => {env['JobTriggerOrigin']} and JobMode => {env['JobMode']} not mapped. Nothing is gonna happen.")

def select_flow():
    flows = {
        'OnDemand': data_frame_from_s3_files,
        'Scheduled': data_frame_from_databases,
        'Default': default_behavior
    }
    if env['JobTriggerOrigin'] in flows:
        selected_flow = flows[env['JobTriggerOrigin']]
    else:
        selected_flow = default_behavior

    return selected_flow()

def data_frame_from_databases():
    for table in tables_list():
        data_frame = data_frame_from_database_query_search(table)
        posting_parquets_in_s3(table, data_frame)
        update_table_execution_datetime(table)

def data_frame_from_s3_files():
    for table in tables_list():
        data_frame = data_frame_from_s3_file(table)

        if data_frame is None:
            logger.error(f"OP::BUC::AssignedProduct::JOB: Empty file or file not exists for table {table}")
            continue

        posting_parquets_in_s3(table, data_frame)

def posting_parquets_in_s3(table, data_frame):
    data_column = 'DT_CreatedAt' if table == 'checkout_orders' else 'created_at'
    df_size = len(data_frame)
    logger.info(f'OP::BUC::AssignedProduct::JOB: Dataframe table {table} size is {str(df_size)}')
    if df_size <= 0:
        logger.info(f'OP::BUC::AssignedProduct::JOB: Stopping posting_parquets_in_s3 - Cause: The table {table} returns {str(df_size)} rows')
        return None

    data_frame = converting_boolean_values_to_string(data_frame, table)
    data_frame[data_column] = pd.to_datetime(data_frame[data_column]) # converting to create partition index
    grouped_df = data_frame.groupby([data_frame[data_column].dt.year, data_frame[data_column].dt.month])

    for (year, month), group in grouped_df:
        group[data_column] = group[data_column].dt.strftime('%Y-%m-%d %H:%M:%S.%f') # converting back to string
        s3_path = f"raw/locaweb/{table}/company=Locaweb/{table}_year={int(year)}/{table}_month={int(month)}/{table}.parquet"
        s3_parquet_file_put_and_update(group, s3_path, table)

def s3_parquet_file_put_and_update(grouped_df, s3_path, table):
    id_column = 'ID_Order' if table == 'checkout_orders' else 'id'
    logger.info('OP::BUC::AssignedProduct::JOB: Starting post final Dataframe on S3')
    final_df  = grouped_df
    previous_df = read_or_create_df(s3_path, options={})

    if previous_df is not None:
        df_concatenated_itens = pd.concat([final_df, previous_df])
        final_df = df_concatenated_itens.drop_duplicates(subset=[id_column], keep='first')

    buffer = BytesIO()
    final_df.to_parquet(buffer, compression='snappy', index=False)
    buffer.seek(0)
    s3.put_object(Body=buffer.getvalue(), Bucket=f"op-buc-lwsa-s3-{env['Environment']}", Key= s3_path)
    buffer.close
    logger.info(f'OP::BUC::AssignedProduct::JOB: Successfully upsert file on S3PATH: {s3_path}')

def read_or_create_df(s3_path, options={}):
    logger.info(f'OP::BUC::AssignedProduct::JOB: Initializing read_or_create_df:: {s3_path}')
    try:
        logger.info(f'OP::BUC::AssignedProduct::JOB: Trying to read {s3_path}')
        response = s3.get_object(Bucket=f"op-buc-lwsa-s3-{env['Environment']}", Key=s3_path)
        body = response['Body'].read()

        buffer = BytesIO(body)
        df = pd.read_parquet(buffer, **options)
        buffer.close()

        logger.info(f'OP::BUC::AssignedProduct::JOB: Trying to read finished')
    except s3.exceptions.NoSuchKey:
        logger.info(f'OP::BUC::AssignedProduct::JOB: Create a empty dataframe')
        df = None

    logger.info(f'OP::BUC::AssignedProduct::JOB: Finishing read_or_create_df')
    return df

def data_frame_from_s3_file(table):
    database = 'checkout' if table == 'checkout_orders' else 'corleone'
    s3_path = f"load/locaweb/assigned-product/{database}/{table}.csv"
    logger.info(f"OP::BUC::AssignedProduct::JOB: S3 path of load data {s3_path}")
    try:
        response = s3.get_object(Bucket=f"op-buc-lwsa-s3-{env['Environment']}", Key=s3_path)
        csv_data = response['Body'].read()
        table_schema = select_data_frame_schema(table)
        columns_list = list(table_schema)
        separator = ',' if table == 'checkout_orders' else ';'
        data_frame = pd.read_csv(BytesIO(csv_data), sep=separator, usecols=columns_list).convert_dtypes().astype(table_schema)
        logger.info(f'OP::BUC::AssignedProduct::JOB: Dataframe from table {table} successfully created')
    except s3.exceptions.NoSuchKey:
        logger.error(f'OP::BUC::AssignedProduct::JOB: File ERROR {table}.csv NOT FOUND')
        data_frame = None

    return data_frame

def create_postgresql_connection():
    conn = psycopg2.connect(
        dbname=job_params['corleone_database'],
        user=job_credentials['corleone_user'],
        password=job_credentials['corleone_password'],
        host=job_params['corleone_host'],
        port=job_params['corleone_port']
    )
    return conn

def create_msqlserver_connection():
    conn = pymssql.connect(
        server=job_params['checkout_host'],
        user=job_credentials['checkout_user'],
        port=job_params['checkout_port'],
        password=job_credentials['checkout_password'],
        database=job_params['checkout_database']
    )
    return conn

def data_frame_from_database_query_search(table):
    logger.info('OP::BUC::AssignedProduct::JOB: Create DB Connection')
    sql_query = select_query(table)
    conn = create_msqlserver_connection() if table == 'checkout_orders' else create_postgresql_connection()
    cursor = conn.cursor()
    logger.info(f"OP::BUC::AssignedProduct::JOB: QUERY: {sql_query}")
    cursor.execute(sql_query)
    query_result = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    desired_columns = list(select_data_frame_schema(table))
    cursor.close()
    conn.close()
    logger.info('OP::BUC::AssignedProduct::JOB: Close DB Connection')
    query_result_schema = select_data_frame_schema(table)
    df = pd.DataFrame(query_result, columns=column_names).convert_dtypes().astype(query_result_schema)

    return df[desired_columns]

def time_query_options(database_type):
    database_options_dict = {
        'psql': {'connection': create_postgresql_connection, 'query': 'SELECT NOW()'},
        'sqlserver': {'connection': create_msqlserver_connection, 'query': 'SELECT GETDATE()'}
    }
    return database_options_dict[database_type]

def time_query(database_type):
    logger.info(f'OP::BUC::AssignedProduct::JOB: Create DB Connection For Time query type {database_type}')
    database_options = time_query_options(database_type)
    conn = database_options['connection']()
    cursor = conn.cursor()
    check_date_query = database_options['query']
    logger.info(f'OP::BUC::AssignedProduct::JOB: Query executed: {check_date_query}')
    cursor.execute(check_date_query)
    query_result = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    logger.info(f'OP::BUC::AssignedProduct::JOB: DB Time Result: {str(query_result)}')
    str_date_time = query_result.strftime('%Y-%m-%d %H:%M:%S')


    return str_date_time

def tables_list():
    tables = [
        'retail_orders',
        'retail_subscriptions',
        'retail_plans',
        'retail_items',
        'retail_provisionings',
        'retail_order_migrations',
        'retail_migrations',
        'retail_subscription_readjustments'#,
        #'checkout_orders'
    ]

    if env['JobMode'] == 'allTables':
        returned_tables = tables
    elif env['JobMode'] in tables:
        returned_tables = [env['JobMode']]
    else:
        default_behavior()
        returned_tables = []

    return returned_tables

def select_query(table):
    queries = {
        'retail_orders': retail_orders_query(),
        'retail_subscriptions': retail_subscriptions_query(),
        'retail_plans': retail_plans_query(),
        'retail_items': retail_items_query(),
        'retail_provisionings': retail_provisionings_query(),
        'retail_order_migrations': retail_order_migrations_query(),
        'retail_migrations': retail_migrations_query(),
        'retail_subscription_readjustments': retail_subscription_readjustments_query(),
        'checkout_orders': checkout_orders_query()
    }

    return queries[table]

def select_data_frame_schema(table):
    schema = {
        'retail_orders': retail_orders_schema(),
        'retail_subscriptions': retail_subscriptions_schema(),
        'retail_plans': retail_plans_schema(),
        'retail_items': retail_items_schema(),
        'retail_provisionings': retail_provisionings_schema(),
        'retail_order_migrations': retail_order_migrations_schema(),
        'retail_migrations': retail_migrations_schema(),
        'retail_subscription_readjustments': retail_subscription_readjustments_schema(),
        'checkout_orders': checkout_orders_schema()
    }

    return schema[table]

def retail_subscription_readjustments_schema():
    schema = {
        'id': 'string[python]',
        'index_type_id': 'string[python]',
        'index_name': 'string[python]',
        'monthly_percentage': 'string[python]',
        'annually_percentage': 'string[python]',
        'month_year': 'string[python]',
        'applied_at': 'string[python]',
        'retail_subscription_id': 'string[python]',
        'created_at': 'string[python]',
        'updated_at': 'string[python]',
        'suspended': 'string[python]'
    }
    return schema

def retail_orders_schema():
    schema = {
        'id': 'string[python]',
        'customer_id': 'string[python]',
        'agreement': 'string[python]',
        'status': 'string[python]',
        'checkout_order_id': 'string[python]',
        'charge_id': 'string[python]',
        'number': 'string[python]',
        'checkout_order_xml': 'string[python]',
        'created_at': 'string[python]',
        'updated_at': 'string[python]',
        'generic_attributes': 'string[python]',
        'antifraud_id': 'string[python]'
    }
    return schema

def retail_subscriptions_schema():
    schema = {
        'id': 'string[python]',
        'status': 'string[python]',
        'number': 'string[python]',
        'checkout_order_id': 'string[python]',
        'priced_at': 'string[python]',
        'feature': 'string[python]',
        'periodicity': 'string[python]',
        'price_list_id': 'string[python]',
        'retail_order_id': 'string[python]',
        'created_at': 'string[python]',
        'updated_at': 'string[python]',
        'period': 'string[python]',
        'billing_started_at': 'string[python]',
        'closing_days': 'string[python]',
        'issue_company': 'string[python]',
        'billing_info_id': 'string[python]',
        'billing_date': 'string[python]',
        'customer_id': 'string[python]',
        'readjustment_index_type': 'string[python]',
        'readjustment_index_date': 'string[python]'
    }
    return schema

def retail_plans_schema():
    schema = {
        'id': 'string[python]',
        'plan_version_id': 'string[python]',
        'status': 'string[python]',
        'retail_subscription_id': 'string[python]',
        'created_at': 'string[python]',
        'updated_at': 'string[python]',
        'discount': 'string[python]',
        'price_list_id': 'string[python]',
        'readjustment_at': 'string[python]',
        'priced_at': 'string[python]'
    }
    return schema

def retail_items_schema():
    schema = {
        'id': 'string[python]',
        'number': 'string[python]',
        'parent': 'string[python]',
        'quantity': 'string[python]',
        'service_id': 'string[python]',
        'discount': 'string[python]',
        'retail_plan_id': 'string[python]',
        'created_at': 'string[python]',
        'updated_at': 'string[python]',
        'prefix': 'string[python]',
        'service_key': 'string[python]',
        'price_list_id': 'string[python]',
        'readjustment_at': 'string[python]',
        'priced_at':'string[python]'
    }
    return schema

def retail_provisionings_schema():
    schema = {
        'id': 'string[python]',
        'parent': 'string[python]',
        'status_code': 'string[python]',
        'retail_item_id': 'string[python]',
        'installed_at': 'string[python]',
        'deactivated_at': 'string[python]',
        'reactivated_at': 'string[python]',
        'cancelled_at': 'string[python]',
        'created_at': 'string[python]',
        'updated_at': 'string[python]',
        'status': 'string[python]'
    }
    return schema

def retail_order_migrations_schema():
    schema = {
        'id': 'string[python]',
        'retail_subscription_id': 'string[python]',
        'new_retail_subscription_id': 'string[python]',
        'customer_id': 'string[python]',
        'prefix': 'string[python]',
        'published_at': 'string[python]',
        'status': 'string[python]',
        'created_at': 'string[python]',
        'updated_at': 'string[python]',
        'external_id': 'string[python]',
        'pre_paid': 'string[python]',
        'main': 'string[python]'
    }
    return schema

def retail_migrations_schema():
    schema = {
        'id': 'string[python]',
        'retail_order_migration_id': 'string[python]',
        'retail_item_id': 'string[python]',
        'retail_provisioning_id': 'string[python]',
        'service_key': 'string[python]',
        'quantity': 'string[python]',
        'number': 'string[python]',
        'parent': 'string[python]',
        'status': 'string[python]',
        'type': 'string[python]',
        'created_at': 'string[python]',
        'updated_at': 'string[python]',
        'retail_migration_id': 'string[python]',
        'provisioning_status': 'string[python]',
        'parent_provisioning_id': 'string[python]',
        'old_quantity': 'string[python]'
    }
    return schema

def checkout_orders_schema():
    schema = {
        'ID_Order': 'string[python]',
        'NM_Indicated_Login': 'string[python]',
        'DS_Order': 'string[python]',
        'DT_CreatedAt': 'string[python]',
        'DT_UpdatedAt': 'string[python]',
        'dt_finalization': 'string[python]',
        'status': 'string[python]',
        'total_amount': 'string[python]',
        'use_anti_fraude': 'string[python]',
        'Login': 'string[python]',
        'customerEmail': 'string[python]'
    }
    return schema

def checkout_orders_query():
    query =  f"""
    SELECT
        *
    FROM [TB_Checkout_Orders]
    WHERE DT_UpdatedAt >= '{date_object['checkout_orders']}'
    """
    return query

def retail_subscription_readjustments_query():
    query = f"""
    SELECT
        retail_subscription_readjustments.*
    FROM retail_subscription_readjustments
    JOIN retail_subscriptions ON retail_subscriptions.id = retail_subscription_readjustments.retail_subscription_id
    WHERE retail_subscriptions.updated_at >= '{date_object['retail_subscription_readjustments']}'
    """
    return query

def retail_orders_query():
    query = f"SELECT * FROM retail_orders WHERE updated_at >= '{date_object['retail_orders']}'"
    return query

def retail_subscriptions_query():
    query = f"SELECT * FROM retail_subscriptions WHERE updated_at >= '{date_object['retail_subscriptions']}'"
    return query

def retail_plans_query():
    query = f"""
    SELECT
        retail_plans.*
    FROM retail_plans
    JOIN retail_subscriptions ON retail_subscriptions.id = retail_plans.retail_subscription_id
    WHERE retail_subscriptions.updated_at >= '{date_object['retail_plans']}'
    """
    return query

def retail_items_query():
    query = f"""
    SELECT
        retail_items.*
    FROM retail_items
    JOIN retail_plans ON retail_plans.id = retail_items.retail_plan_id
    JOIN retail_subscriptions ON retail_subscriptions.id = retail_plans.retail_subscription_id
    WHERE retail_subscriptions.updated_at >= '{date_object['retail_items']}'
    """
    return query

def retail_provisionings_query():
    query = f"""
    SELECT
        retail_provisionings.*
    FROM retail_provisionings
    JOIN retail_items ON retail_items.id = retail_provisionings.retail_item_id
    JOIN retail_plans ON retail_plans.id = retail_items.retail_plan_id
    JOIN retail_subscriptions ON retail_subscriptions.id = retail_plans.retail_subscription_id
    WHERE retail_subscriptions.updated_at >= '{date_object['retail_provisionings']}'
    """
    return query

def retail_order_migrations_query():
    query = f"""
    SELECT
        retail_order_migrations.*
    FROM retail_order_migrations
    JOIN retail_subscriptions ON retail_subscriptions.id = retail_order_migrations.retail_subscription_id
    WHERE retail_subscriptions.updated_at >= '{date_object['retail_order_migrations']}'
    """
    return query

def retail_migrations_query():
    query = f"""
    SELECT
        retail_migrations.*
    FROM retail_migrations
    JOIN retail_order_migrations ON retail_order_migrations.id = retail_migrations.retail_order_migration_id
    JOIN retail_subscriptions ON retail_subscriptions.id = retail_order_migrations.retail_subscription_id
    WHERE retail_subscriptions.updated_at >= '{date_object['retail_migrations']}'
    """
    return query

## @params: [JOB_NAME]
env = getResolvedOptions(sys.argv, ['JOB_NAME', 'Environment', 'JobTriggerOrigin', 'JobMode'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(env['JOB_NAME'], env)

#Global Scope Itens
logger = glueContext.get_logger()
s3 = boto3.client('s3')
ssm_client = boto3.client('ssm')
job_params = get_parameters()
job_credentials = get_credentials()
today_datetime_pqsl = time_query('psql')
today_datetime_sqlserver = '' # time_query('sqlserver')

date_object = parameter_store_get_date()

select_flow()

parameter_store_update_date()

logger.info("OP::BUC::AssignedProduct::JOB: SUCCESFULLY FINISHED!")
job.commit()
