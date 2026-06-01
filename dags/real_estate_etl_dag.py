import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook

POSTGRES_CONN_ID = 'postgres_real_estate'
CLEANED_DATA_PATH = '/mnt/e/Data-eng-vscode/data/cleaned'

default_args = {
    'owner': 'anas',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def truncate_all_tables():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    hook.run('TRUNCATE TABLE transactions, listings, property_features, agents, properties, buyers, locations, agencies CASCADE')


def load_csv_to_postgres(csv_file, table_name):
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()
    df = pd.read_csv(f'{CLEANED_DATA_PATH}/{csv_file}')
    df.to_sql(table_name, engine, if_exists='append', index=False)
    print(f'{table_name}: {len(df)} rows loaded')


def load_agencies():
    load_csv_to_postgres('agencies_cleaned.csv', 'agencies')


def load_locations():
    load_csv_to_postgres('locations_cleaned.csv', 'locations')


def load_buyers():
    load_csv_to_postgres('buyers_cleaned.csv', 'buyers')


def load_agents():
    load_csv_to_postgres('agents_cleaned.csv', 'agents')


def load_properties():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()
    df_props = pd.read_csv(f'{CLEANED_DATA_PATH}/properties_cleaned.csv')
    df_locs = pd.read_csv(f'{CLEANED_DATA_PATH}/locations_cleaned.csv')
    valid_loc_ids = set(df_locs['location_id'])
    dropped = len(df_props) - df_props['location_id'].isin(valid_loc_ids).sum()
    df_props = df_props[df_props['location_id'].isin(valid_loc_ids)]
    df_props.to_sql('properties', engine, if_exists='append', index=False)
    print(f'properties: {len(df_props)} rows loaded, {dropped} dropped (orphan location_id)')


def load_property_features():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()
    df = pd.read_csv(f'{CLEANED_DATA_PATH}/property_features_cleaned.csv')
    df_props = pd.read_csv(f'{CLEANED_DATA_PATH}/properties_cleaned.csv')
    df_locs = pd.read_csv(f'{CLEANED_DATA_PATH}/locations_cleaned.csv')
    valid_loc_ids = set(df_locs['location_id'])
    valid_prop_ids = set(df_props[df_props['location_id'].isin(valid_loc_ids)]['property_id'])
    dropped = len(df) - df['property_id'].isin(valid_prop_ids).sum()
    df = df[df['property_id'].isin(valid_prop_ids)]
    df.to_sql('property_features', engine, if_exists='append', index=False)
    print(f'property_features: {len(df)} rows loaded, {dropped} dropped (orphan property_id)')


def load_listings():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()
    df = pd.read_csv(f'{CLEANED_DATA_PATH}/listings_cleaned.csv')
    df_props = pd.read_csv(f'{CLEANED_DATA_PATH}/properties_cleaned.csv')
    df_locs = pd.read_csv(f'{CLEANED_DATA_PATH}/locations_cleaned.csv')
    df_agents = pd.read_csv(f'{CLEANED_DATA_PATH}/agents_cleaned.csv')
    valid_loc_ids = set(df_locs['location_id'])
    valid_prop_ids = set(df_props[df_props['location_id'].isin(valid_loc_ids)]['property_id'])
    valid_agent_ids = set(df_agents['agent_id'])
    dropped = len(df) - (df['property_id'].isin(valid_prop_ids) & df['agent_id'].isin(valid_agent_ids)).sum()
    df = df[df['property_id'].isin(valid_prop_ids) & df['agent_id'].isin(valid_agent_ids)]
    df.to_sql('listings', engine, if_exists='append', index=False)
    print(f'listings: {len(df)} rows loaded, {dropped} dropped (orphan FK)')


def load_transactions():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()
    df = pd.read_csv(f'{CLEANED_DATA_PATH}/transactions_cleaned.csv')
    df_listings = pd.read_csv(f'{CLEANED_DATA_PATH}/listings_cleaned.csv')
    df_buyers = pd.read_csv(f'{CLEANED_DATA_PATH}/buyers_cleaned.csv')
    df_agents = pd.read_csv(f'{CLEANED_DATA_PATH}/agents_cleaned.csv')
    df_props = pd.read_csv(f'{CLEANED_DATA_PATH}/properties_cleaned.csv')
    df_locs = pd.read_csv(f'{CLEANED_DATA_PATH}/locations_cleaned.csv')
    valid_loc_ids = set(df_locs['location_id'])
    valid_prop_ids = set(df_props[df_props['location_id'].isin(valid_loc_ids)]['property_id'])
    valid_agent_ids = set(df_agents['agent_id'])
    valid_listing_ids = set(df_listings[df_listings['property_id'].isin(valid_prop_ids) & df_listings['agent_id'].isin(valid_agent_ids)]['listing_id'])
    valid_buyer_ids = set(df_buyers['buyer_id'])
    mask = (df['listing_id'].isin(valid_listing_ids) & df['buyer_id'].isin(valid_buyer_ids) & df['agent_id'].isin(valid_agent_ids))
    dropped = len(df) - mask.sum()
    df = df[mask]
    df.to_sql('transactions', engine, if_exists='append', index=False)
    print(f'transactions: {len(df)} rows loaded, {dropped} dropped (orphan FK)')


with DAG(
    dag_id='real_estate_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for real estate data',
    schedule='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    t_truncate = PythonOperator(task_id='truncate_all_tables', python_callable=truncate_all_tables)

    t_agencies = PythonOperator(task_id='load_agencies', python_callable=load_agencies)
    t_locations = PythonOperator(task_id='load_locations', python_callable=load_locations)
    t_buyers = PythonOperator(task_id='load_buyers', python_callable=load_buyers)
    t_agents = PythonOperator(task_id='load_agents', python_callable=load_agents)
    t_properties = PythonOperator(task_id='load_properties', python_callable=load_properties)
    t_property_features = PythonOperator(task_id='load_property_features', python_callable=load_property_features)
    t_listings = PythonOperator(task_id='load_listings', python_callable=load_listings)
    t_transactions = PythonOperator(task_id='load_transactions', python_callable=load_transactions)

    # Truncate first, then Wave 1 (no FK) in parallel
    t_truncate >> [t_agencies, t_locations, t_buyers]

    # Wave 1 -> Wave 2
    [t_agencies, t_locations] >> t_agents
    [t_agencies, t_locations] >> t_properties

    # Wave 2 -> Wave 3
    [t_agents, t_properties] >> t_listings
    [t_agents, t_properties, t_buyers] >> t_property_features

    # Wave 3 -> Wave 4
    t_listings >> t_transactions
