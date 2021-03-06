import datetime
import time
import numpy as np
import os
import pandas as pd
import json
import random
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.options import Options

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

def api_test():
    path_to_driver = '/home/airflow_gcp/chromedriver'

    chrome_options = Options()
    chrome_options.add_argument("--headless")

    chrome_options.add_experimental_option("prefs", {
      "download.default_directory": "/home/ingest/api_test/",
      "download.prompt_for_download": False,
      "download.directory_upgrade": True,
      "safebrowsing.enabled": True
    })

    driver = webdriver.Chrome(path_to_driver, options=chrome_options)
    url = 'https://postgreapi.nsmgendpoints.co.uk/'
    # go to the page
    print('Getting page.')
    driver.get(url)
    print('Waiting for the page to load.')
    # locate the initial button and click it
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((
            By.XPATH, '//*[@id="operations-postgreapi-get_main_class"]/div[2]/div/div[1]/div[1]/div[2]/button'))
    )
    print('Click Try It Out button.')
    driver.find_element_by_xpath(
        '//*[@id="operations-postgreapi-get_main_class"]/div[2]/div/div[1]/div[1]/div[2]/button').click()
    # insert the parameters for the API call
    # handle token
    print('Fill parameters fields.')
    token = 'gEUwEuUbiEFI1b8bXh4NSFlEkTk'
    token_bar = driver.find_element_by_xpath(
        '//*[@id="operations-postgreapi-get_main_class"]/div[2]/div/div[1]/div[2]/table/tbody/tr[1]/td[2]/input').send_keys(token)
    # handle DatasetList
    DatasetList = ''
    DatasetList_bar = driver.find_element_by_xpath(
        '//*[@id="operations-postgreapi-get_main_class"]/div[2]/div/div[1]/div[2]/table/tbody/tr[2]/td[2]/input').send_keys(
        DatasetList)
    # TableOrDataset
    TableOrDataset = 'Table'
    TableOrDataset_bar = driver.find_element_by_xpath(
        '//*[@id="operations-postgreapi-get_main_class"]/div[2]/div/div[1]/div[2]/table/tbody/tr[3]/td[2]/input').send_keys(
        TableOrDataset)

    # Dataset
    Dataset = 'city'
    Dataset_bar = driver.find_element_by_xpath(
        '//*[@id="operations-postgreapi-get_main_class"]/div[2]/div/div[1]/div[2]/table/tbody/tr[4]/td[2]/input').send_keys(
        Dataset)

    # Table
    Table = 'CityEconomicsDetails'
    Table_bar = driver.find_element_by_xpath(
        '//*[@id="operations-postgreapi-get_main_class"]/div[2]/div/div[1]/div[2]/table/tbody/tr[5]/td[2]/input').send_keys(
        Table)
    print('Click Execute button.')
    # click 'execute'
    driver.find_element_by_xpath(
        '//*[@id="operations-postgreapi-get_main_class"]/div[2]/div/div[2]/button[1]').click()
    
    #start_time
    start_time = datetime.datetime.now()
    
    # wait for search results to be fetched
    WebDriverWait(driver, 200).until(
        EC.presence_of_element_located((
            By.XPATH, '//*[@id="operations-postgreapi-get_main_class"]/div[2]/div/div[3]/div[2]/div/div/table/tbody/tr/td[2]/div[1]/div/a'))
    )
    print('Download the file.')
    # click the button to download the data
    driver.find_element_by_xpath(
            '//*[@id="operations-postgreapi-get_main_class"]/div[2]/div/div[3]/div[2]/div/div/table/tbody/tr/td[2]/div[1]/div/a'
        ).click()
    # end time
    end_time = datetime.datetime.now()
    time.sleep(10)
    time_exec = end_time - start_time
    print('Check size and shape of the downloaded file.')
    # locate the downloaded file locally
    test_file =  os.listdir('/home/ingest/api_test/')[0]
    file_size = os.path.getsize('/home/ingest/api_test/' + test_file)/1_000_000
    # open the file and load the dict
    data = pd.read_csv('/home/ingest/api_test/' + test_file, engine='python')

    # check if test was successfull
    if len(data.shape) >= 2:
        message = 'API test executed in {}'.format(time_exec) + ' successfully. File size: ' + str(round(file_size,2)) + 'MB.\n'
    else:
        message = 'API test executed in {}'.format(time_exec) + ' unsuccessfully. File size: ' + str(round(file_size,2)) + 'MB.\n'
    # delete the file
    os.remove('/home/ingest/api_test/' + test_file)
    print('Write to log file.')
    with open('/home/ingest/logs/api_test_log.csv','a') as f:
        curr_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        f.write(str(curr_time) + '|' + Dataset + '.' + Table + '|' + message)
       
    return True

default_args = {
    'owner': 'test_data_hub_api',
    'start_date': datetime.datetime(2020, 9, 23, 0, 1, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('test_data_hub_api',
     catchup=False,
     default_args=default_args,
     schedule_interval='0 0 * * *',
     ) as dag:
    
    
    test_api = PythonOperator(task_id='api_test',
                               python_callable=api_test)
    

test_api