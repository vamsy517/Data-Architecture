from google.oauth2 import service_account
from google.cloud import bigquery
import pandas_gbq
from requests import get
from pandas import read_csv, DataFrame, concat
import os
import json, sys
from io import StringIO
from time import sleep
from numpy import isnan
import datetime
import pytz

from code_base import logpr
from code_base import get_request, gbq_credentials, gbq_upload
from datahub_updater import Updater
import json
import requests


# read the api token from file
with open('/home/airflow_gcp/airflow/dags/code_base/credentials.json') as json_file:
    data = json.load(json_file)
    token = data['token']

class CommonTradeAPI():
    def __init__(self, save_path, gbq_creds_path):
        self.save_path = save_path
        self.gbq_creds_path = os.path.join(gbq_creds_path,'gbq_credentials.json')
        return None

    def tables_gbq_check(self):
        '''
        Method to check for all existing tables in GBQ
        '''
        self.gbq_creds = gbq_credentials(filename=self.gbq_creds_path)
        query = """SELECT * FROM ComTrade.INFORMATION_SCHEMA.TABLES"""
        table_list = (self.gbq_creds['bqclient'].query(query).result().to_dataframe(bqstorage_client=self.gbq_creds['bqstorageclient']))

        table_list = list(table_list['table_name'])

        table_checks = {}
        for table in ['CountryCodes','TradeRegimes','CommoditiesAnnual','CommoditiesMonthly','ServicesAnnual','ServicesMonthly',
        'CommoditiesMapping','ServicesMapping']:
            if table not in table_list:
                table_checks[table] = False
            else:
                table_checks[table] = True

        return table_checks

    def tables_create_countrycodes(self, table_checks):
        if table_checks['CountryCodes'] == False:
            logpr('Table: {} does not exist in GBQ. Uploading...'.format('CountryCodes'))
            # if table does not exist in the database create an empty file
            country_codes = DataFrame(data={
                'rtCode':[999999],
                'rtTitle':['Test']
                }
                )
            # save it inside the ingest/input folder
            country_codes.to_csv(os.path.join(self.save_path,'ComTrade__CountryCodes_{}.csv'.format((datetime.datetime.now()).strftime("%Y%m%d%H%M"))),
                                 index=False)

            sleep(400)
        headers = requests.utils.default_headers()
        headers['User-Agent'] = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36'
        countrycodes = get_request(url='https://comtrade.un.org/Data/cache/reporterAreas.json', headers=headers).json()['results']
        countrycodes = DataFrame(countrycodes)
        countrycodes.columns = ['rtCode','rtTitle']
        countrycodes['rtTitle'] = countrycodes['rtTitle'].str.replace('(','')
        countrycodes['rtTitle'] = countrycodes['rtTitle'].str.replace(')','')
        countrycodes = countrycodes[countrycodes['rtCode']!='all']
        countrycodes['rtCode'] = countrycodes['rtCode'].astype('int')

        updater = Updater(
                                    'project-pulldata', 
                                    'ComTrade', 'CountryCodes', countrycodes,
                                    'rtCode', ['rtCode','rtTitle'],
                                    list(countrycodes.rtCode),
                                    #self.gbq_creds_path
                                    )
        updater.update_data()

    def tables_create_traderegimes(self, table_checks):
        if table_checks['TradeRegimes'] == False:
            logpr('Table: {} does not exist in GBQ. Uploading...'.format('TradeRegimes'))
            # if table does not exist in the database create an empty file
            trade_regimes = DataFrame(data={
                'rgCode':[999999],
                'rgDesc':['Test']
                }
                )
            # save it inside the ingest/input folder
            trade_regimes.to_csv(os.path.join(self.save_path,'ComTrade__TradeRegimes_{}.csv'.format((datetime.datetime.now()).strftime("%Y%m%d%H%M"))),index=False)

            sleep(400)
        headers = requests.utils.default_headers()
        headers['User-Agent'] = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36'
        trade_regimes = get_request(url='https://comtrade.un.org/Data/cache/tradeRegimes.json',headers=headers).json()['results']
        trade_regimes = DataFrame(trade_regimes)
        trade_regimes.columns = ['rgCode','rgDesc'] 
        trade_regimes['rgDesc'] = trade_regimes['rgDesc'].str.replace('(','')
        trade_regimes['rgDesc'] = trade_regimes['rgDesc'].str.replace(')','')
        trade_regimes = trade_regimes[trade_regimes['rgCode']!='all']
        trade_regimes['rgCode'] = trade_regimes['rgCode'].astype('int')

        updater = Updater(
                                'project-pulldata', 
                                'ComTrade', 'TradeRegimes', trade_regimes,
                                'rgCode', ['rgCode','rgDesc'],
                                list(trade_regimes.rgCode),
                                #self.gbq_creds_path
                                )
        updater.update_data()

    def tables_create_commoditiesmapping(self, table_checks):
        if table_checks['CommoditiesMapping'] == False:
            logpr('Table: {} does not exist in GBQ. Uploading...'.format('CommoditiesMapping'))
            # if table does not exist in the database create an empty file
            comminfo = DataFrame(data={
                'cmdCode':[999999],
                'cmdDescE':['Test']
                }
                )
            # save it inside the ingest/input folder
            comminfo.to_csv(os.path.join(self.save_path,'ComTrade__CommoditiesMapping_{}.csv'.format((datetime.datetime.now()).strftime("%Y%m%d%H%M"))),index=False)

    def tables_create_servicesmapping(self, table_checks):
        if table_checks['ServicesMapping'] == False:
            logpr('Table: {} does not exist in GBQ. Uploading...'.format('ServicesMapping'))
            # if table does not exist in the database create an empty file
            servinfo = DataFrame(data={
                'cmdCode':[9999999],
                'cmdDescE':['Test']
                }
                )
            # save it inside the ingest/input folder
            servinfo.to_csv(os.path.join(self.save_path,'ComTrade__ServicesMapping_{}.csv'.format((datetime.datetime.now()).strftime("%Y%m%d%H%M"))),index=False)

    def tables_create_commoditiesdata(self, table_checks):
        if table_checks['CommoditiesMonthly'] == False:
            logpr('Table: {} does not exist in GBQ. Uploading...'.format('CommoditiesMonthly'))
            # if table does not exist in the database create an empty file
            comminfo = DataFrame(data={
                'pfCode':[999999],
                'yr':[1000],
                'period':[100001],
                'rgCode':[999999],
                'rtCode':[999999],
                'ptCode':[999999],
                'cmdCode':[999999],
                'TradeValue':[0]
                }
                )
            # save it inside the ingest/input folder
            comminfo.to_csv(os.path.join(self.save_path,'ComTrade__CommoditiesMonthly_{}.csv'.format((datetime.datetime.now()).strftime("%Y%m%d%H%M"))),index=False)

        if table_checks['CommoditiesAnnual'] == False:
            logpr('Table: {} does not exist in GBQ. Uploading...'.format('CommoditiesAnnual'))
            # if table does not exist in the database create an empty file
            comminfo = DataFrame(data={
                'pfCode':[999999],
                'yr':[1000],
                'period':[1000],
                'rgCode':[999999],
                'rtCode':[999999],
                'ptCode':[999999],
                'cmdCode':[999999],
                'TradeValue':[0]
                }
                )
            # save it inside the ingest/input folder
            comminfo.to_csv(os.path.join(self.save_path,'ComTrade__CommoditiesAnnual_{}.csv'.format((datetime.datetime.now()).strftime("%Y%m%d%H%M"))),index=False)

    def tables_create_servicesdata(self, table_checks):
        if table_checks['ServicesMonthly'] == False:
            logpr('Table: {} does not exist in GBQ. Uploading...'.format('ServicesMonthly'))
            # if table does not exist in the database create an empty file
            comminfo = DataFrame(data={
                'pfCode':[999999],
                'yr':[1000],
                'period':[100001],
                'rgCode':[999999],
                'rtCode':[999999],
                'ptCode':[999999],
                'cmdCode':[999999],
                'TradeValue':[0]
                }
                )
            # save it inside the ingest/input folder
            comminfo.to_csv(os.path.join(self.save_path,'ComTrade__ServicesMonthly_{}.csv'.format((datetime.datetime.now()).strftime("%Y%m%d%H%M"))),index=False)

        if table_checks['ServicesAnnual'] == False:
            logpr('Table: {} does not exist in GBQ. Uploading...'.format('ServicesAnnual'))
            # if table does not exist in the database create an empty file
            comminfo = DataFrame(data={
                'pfCode':[999999],
                'yr':[1000],
                'period':[1000],
                'rgCode':[999999],
                'rtCode':[999999],
                'ptCode':[999999],
                'cmdCode':[999999],
                'TradeValue':[0]
                }
                )
            # save it inside the ingest/input folder
            comminfo.to_csv(os.path.join(self.save_path,'ComTrade__ServicesAnnual_{}.csv'.format((datetime.datetime.now()).strftime("%Y%m%d%H%M"))),index=False)

    def get_data_availability(self):
        '''
        '''
        headers = requests.utils.default_headers()
        headers['User-Agent'] = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36'
        self.data_availability = []
        for type_ in ['C','S']:
            for freq in ['A','M']:
                get_data_params = {
                    'type':     type_,
                    'freq':     freq,
                    'token': token
                }
                try:
                    data_availability = get_request(url='http://comtrade.un.org/api//refs/da/view?', params = get_data_params, headers= headers).json()
                    data_availability = DataFrame.from_records(data_availability)
                    if len(data_availability)>0:
                        data_availability = data_availability[data_availability['px'].isin(['H5','EB02','HS'])]
                        data_availability['ps'] = data_availability['ps'].astype('int64')
                        data_availability = data_availability[data_availability['ps']>2010] 
                        data_availability.sort_values(by='ps', ascending=False, inplace=True)
                        data_availability.reset_index(drop=True, inplace=True)
                        self.data_availability.append(data_availability)
                        logpr('Number of raw requests {}{}: {}'.format(type_, freq, len(new)))
                        logpr('Total records in df: {}'.format(str(data_availability.TotalRecords.sum())))
                except OSError as e:
                    logpr(e)
                    continue
                except:
                    logpr('Error occurred')
                    continue
                sleep(1)

    def get_comtrade_data(self):
        '''
        '''
        # FOR EACH ROW IN THE DATA AVAILABILITY
        # 1. CHECK IF IT'S COMMODITY OR SERVICE AND ANNUAL OR MONTHLY
        # 2. CONSTRUCT A POST REQUEST FOR EACH TRADE REGIME FOR EACH ROW
        # 3. APPEND EACH RESULT_DF TO A LIST AND CONCATENATE THE ENTIRE LIST AFTER
        for df in self.data_availability:
            reporter_data = []
            for index,row in df.iterrows():
                if row.type.lower() == 'commodities':
                    type_ = 'C'
                else:
                    type_ = 'S'
                if row.freq.lower() == 'annual':
                    freq_ = 'A'
                else:
                    freq_ = 'M'

                for traderegime in ['1','2','3','4']:
                    sleep(1)
                    get_data_params={
                                    'url':'http://comtrade.un.org/api/get?',
                                    'params': {
                                                'fmt':'json',
                                                'freq':freq_,
                                                'ps':str(int(row.ps)),
                                                'px':row.px,
                                                'r':str(int(row.r)),
                                                'type':type_,
                                                'max':'10000', 
                                                'head':'M', 
                                                'rg':traderegime,
                                                'token': token}
                                            }
                    try:
                        result_df = get_request(**get_data_params).json()['dataset']
                    except OSError as e:
                        logpr(e)
                        continue
                    except:
                        logpr('Error occurred')
                        continue

                    if len(result_df)>0:
                        result_df = DataFrame.from_dict(result_df)
                        reporter_data.append(result_df)

            if len(reporter_data)>0:
                filename1 = ''
                filename2 = ''

                try:
                    result = concat(reporter_data, axis=0, ignore_index=True)
                except:
                    continue

                if len(result)>0 and isinstance(result, DataFrame):
                    result['rtTitle'] = result['rtTitle'].str.replace('(','')
                    result['rgDesc'] = result['rgDesc'].str.replace('(','')
                    result['rtTitle'] = result['rtTitle'].str.replace(')','')
                    result['rgDesc'] = result['rgDesc'].str.replace(')','')

                    result1 = result[['pfCode','yr','period','rgCode','rtCode','ptCode','cmdCode','TradeValue']].copy()
                    result2 = result[['cmdCode','cmdDescE']].copy()
                    result1[['yr','period','rgCode','rtCode','ptCode','cmdCode','TradeValue']] = result[['yr','period','rgCode','rtCode','ptCode','cmdCode','TradeValue']].astype('int')
                    result2['cmdCode'] = result2['cmdCode'].astype('int')
                    result2 = result2.drop_duplicates(subset=['cmdCode'],keep='last').reset_index(drop=True)

                    if type_ == 'C':
                        filename1 += 'Commodities'
                        filename2 += 'CommoditiesMapping'
                    else:
                        filename1 += 'Services'
                        filename2 += 'ServicesMapping'

                    if freq_ == 'A':
                        filename1 += 'Annual'
                    else:
                        filename1 += 'Monthly'
                    
                    try:
                        logpr('Uploading')
                        updater = Updater(
                            'project-pulldata', 
                            'ComTrade', filename1, result1,
                            'rtCode', ['pfCode','yr','period','rgCode','rtCode','ptCode','cmdCode','TradeValue'],
                            list(result1.rtCode)
                            )
                        updater.update_data()
                    
                        updater1 = Updater(
                            'project-pulldata', 
                            'ComTrade', filename2, result2,
                            'cmdCode', ['cmdCode'],
                            list(result2.cmdCode)
                            )
                        updater1.update_data()
                    except OSError as e:
                        logpr(e)
                        continue
                    except:
                        logpr('Error occurred')
                        continue

    def run(self):
        '''
        Method to encompass the entire process
        '''
        # Get data availability from the API
        self.get_data_availability()
        # Download
        # self.get_comtrade_data()

 
