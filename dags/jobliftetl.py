
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import logging
import requests
import pandas as pd
import glob
import os
from datetime import datetime, timedelta, date


URL = 'https://api.exchangeratesapi.io/'


PATH_CPC = '/usr/local/data'


FOLDER_PATH_DESTINATION_RATE = '/usr/local/results/exchange_rate_data.csv'

FOLDER_PATH_DESTINATION_CPC = '/usr/local/results/combined_cpc_files_data.csv'

FOLDER_PATH_DESTINATION_FINAL = '/usr/local/results/final_aggregated_data.csv'

START_DATE = date(2020, 1, 1)

END_DATE = date(2020, 1, 11)


def push_data(df, folder_destination):
    """
    Pushing data frames as csv file
    """
    logging.info('Push %s to the %s csv' % (df, folder_destination))
    df.to_csv(folder_destination, sep=';', encoding='utf-8', index=False)

def read_csv(folder):
    """
    Reading csv files as dataframes
    """
    return pd.read_csv(folder, sep=';',  header=0)


#My inital structure was to make it dynamic and run the dag for every day with it s specific clickout file but change it to run it once
def daterange(start_date, end_date):
    """ returning date range to process the ETL"""
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

#url, start_date, end_date, folder_dest_file
def get_exchange_rate(**kwargs):
    """
    Call api to get daily rates EUR base
    """
    logging.info('Starting loading exchange rate data')
    url_link = kwargs['url']
    all_list = []
    #calling api with range does not return rates in the weekend for instance
    for day in daterange(kwargs['start_date'], kwargs['end_date']):
        day.strftime("%Y-%m-%d")
        api_call_url = url_link + str(day)
        logging.info('trying to call following api %s' % api_call_url)
        response = requests.get(api_call_url).json()
        #storing the json response into a dataframe in a specific structure as for instance table
        for k, v in response['rates'].items():
            all_list.append([str(day), k, v])
        all_list.append([str(day), 'EUR', 1])
    df = pd.DataFrame.from_records(all_list, columns=['date', 'currency', 'rate'])
    push_data(df, kwargs['folder_dest_file'])


def change_to_date(data):
    """
    :param data: field
    :return:
    """
    d = datetime.strptime(data, "%Y-%m-%d %H:%M:%S.%f%z")
    return d.strftime('%Y-%m-%d')

#path_folder, dest_file
def read_files(**kwargs):
    """
    Read clickout csv files and push the data as csv file
    """
    logging.info("Starting loading cpc data from %s" % kwargs['path_folder'])
    all_files = glob.glob(os.path.join(kwargs['path_folder'], '*.csv'))
    logging.info('Processing files:  %s' % ('\n '.join(all_files)))
    df_for_each_File = (pd.read_csv(f) for f in all_files)
    grouped_df = pd.concat(df_for_each_File, ignore_index=True)
    grouped_df.rename(columns={'timestamp': 'date'}, inplace=True)
    grouped_df['date'] = grouped_df['date'].apply(lambda date: change_to_date(date))
    push_data(grouped_df, kwargs['dest_file'])


# get_exchange_rate(url)['rates'][df['date']][df['currency'].lower()
#dfp1, dfp2, dest_file
def transform_data(**kwargs):
    """
    Merge Two dataframes, apply transformation to it and then push it as csv file
    """
    logging.info('Loading data from csv files')
    df1 = read_csv(kwargs['dfp1'])
    df2 = read_csv(kwargs['dfp2'])

    merge_df = pd.merge(df1, df2, how='left', on=['date', 'currency'])
    #Making sure the column data types is numeric
    merge_df['cpc'] = pd.to_numeric(merge_df['cpc'])
    merge_df['rate'] = pd.to_numeric(merge_df['rate'])
    logging.info('Applying rate for every day')
    merge_df['cpc'] = merge_df['cpc'] * merge_df['rate']
    grouped_partition = merge_df.groupby(['date', 'country']).agg({'cpc': ['sum']})
    grouped_partition.columns = ['aggregated_revenue']
    grouped_partition.aggregated_revenue = grouped_partition.aggregated_revenue.round(2)
    grouped_partition = grouped_partition.reset_index()
    push_data(grouped_partition, kwargs['dest_file'])
    logging.info('ETL JOBLIFT SUCCESSFULLY RAN.')



dag = DAG('joblift_cpc_ETL',
            description='Simple  tutorial DAG',
            start_date=datetime.now() - timedelta(days=4),
            schedule_interval='0 0 * * *'
         )


load_cpc_data = PythonOperator(task_id='load_cpc_files_data',
                               python_callable=read_files,
                               op_kwargs={'path_folder': PATH_CPC, 'dest_file': FOLDER_PATH_DESTINATION_CPC},
                               dag= dag)

load_rate_data = PythonOperator(task_id= 'load_exchange_rate_api_data',
                               python_callable=get_exchange_rate,
                               op_kwargs={'url': URL, 'start_date': START_DATE, 'end_date': END_DATE, 'folder_dest_file': FOLDER_PATH_DESTINATION_RATE},
                               dag=dag)

Transform_push = PythonOperator(task_id='transform_data',
                               python_callable=transform_data,
                               op_kwargs={'dfp1': FOLDER_PATH_DESTINATION_CPC, 'dfp2': FOLDER_PATH_DESTINATION_RATE, 'dest_file': FOLDER_PATH_DESTINATION_FINAL},
                               dag=dag)


Transform_push.set_upstream([load_rate_data, load_cpc_data])
