#-*- coding: utf-8 -*-
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas_gbq
import json, sys
import numpy as np
from datetime import datetime
from pandas import DataFrame, concat, isnull
import pandas as pd
import random
import string,re 

from code_base import logpr 
from code_base import gbq_credentials, gbq_upload
from code_base import post_request, get_request
import sql_queries

class CarmenTemplates():
    def __init__(self):
        '''
            This class will be used to pull data from project-pulldata.Country in GBQ
            and construct a text template containing information about a selected country.
        '''
        self.auth_ = ('carmen', 'q8n!(UUIYP)o8o*sFTYrx#z^')
        #self.branch = 'https://test-newstatesman-b2b.pantheonsite.io/investmentmonitor'
        self.branch_ = 'https://investmentmonitor.ai'

        return None

    def version__(self):
        version = '2.07'
        return version

    ############################################################
    ######## HELPER FUNCTIONS
    def country_indicators_map(self):
        '''
            Create a new table with encoded values for each indicator in the Country.MacroYearData
            data frame in GBQ.
        '''
        gbq_creds = gbq_credentials()

        query = 'Select distinct Indicator as Indicator from project-pulldata.Country.MacroYearData'
        df = (gbq_creds['bqclient'].query(query).result().to_dataframe(bqstorage_client=gbq_creds['bqstorageclient']))
        df.reset_index(drop=True, inplace=True)

        df1 = DataFrame(df['Indicator'].str.split(' in ', expand=True, n=1))
        df1.columns = ['short1','type1']
        df1 = df1[df1['type1'].isna()==False]

        df2 = df['Indicator'].str.split(':', expand=True, n=1)
        df2.columns = ['short2','type2']
        df2 = df2[df2['type2'].isna()==False]

        df = concat([df,df1,df2],axis=1)

        df['short'] = df['short1'].combine_first(df['short2'])
        df['short'] = df['short'].combine_first(df['Indicator'])
        df['type'] = df['type1'].combine_first(df['type2'])
        df['type'] = df['type'].combine_first(df['Indicator'])
        df = df[['Indicator','short','type']]

        def get_random_alphanumeric_string(length,df):
            letters_and_digits = string.ascii_letters + string.digits
            result_list = [''.join((random.choice(letters_and_digits) for i in range(length))) for el in range(len(df))]
            return result_list

        df['code'] = get_random_alphanumeric_string(20,df)

        gbq_upload(df, params={'destination_table':'Country.IndicatorMap', 'project_id':'project-pulldata', 'if_exists':'replace'})

    def format_big_numbers(self,number):
        import math

        number = float(number)

        if len(str(int(number)))<7:
            number_formatted = '{number:,}'.format(number=int(number))

        else:
            millnames = ['',' thousand',' million','bn','trn']

            millidx = max(0,min(len(millnames)-1,int(math.floor(0 if number == 0 else math.log10(abs(number))/3))))

            number_formatted = '{:.1f}{}'.format((number / 10**(3 * millidx - 1))/10, millnames[millidx])

        return number_formatted

    def format_style(self):
        self.iter_text = self.iter_text.replace(" - ", " – ")

        indicators = re.findall(r"{.+?}",self.iter_text)
        for indicator in indicators:
            indicator_old = indicator
            for i in ['{','}']:
                indicator = indicator.replace(i,'')
            indicator_ = indicator.split('|')
            self.run_params[indicator_[0]] = indicator_[1]
            self.iter_text = self.iter_text.replace(indicator_old,'')

        for repstring in ['. ', '\n']:
            splits = self.iter_text.split(repstring)
            for old in splits:
                new = old
                if len(new)>0:
                    new = new[0].upper() + new[1:]
                self.iter_text = self.iter_text.replace(old,new)

        blocks = re.findall(r"\$.+? million",self.iter_text)
        for ind in range(0,len(blocks)):
            old = blocks[ind]
            new = blocks[ind]
            new = new.replace(' million', 'm')
            self.iter_text = self.iter_text.replace(old,new)

    def add_to_results(self):

        perc_missing = len(self.missing_values) / len(self.indicators_databased)
        perc_found = 1 - perc_missing

        if perc_found > 0.5:

            self.results[self.run_params['country']] = {

                    'meta':{'publish':'yes','created_at': str(datetime.now()),'info_part': perc_found},

                    'post':{
                    '_display_author_name': '1',
                    'description': self.run_params['description'],
                    'title': self.run_params['title'],
                    'author': '13255',
                    'status': 'draft',
                    'content': self.iter_text,
                    'template': 'single-auto.php',
                    #'tags': '74',
                    #'categories': '21',
                    }
                    }

            try:
                if self.run_params['country'] == 'United Kingdom':
                    region = str(self.regions_dict['UK'])
                elif self.run_params['country'] == 'United States':
                    region = str(self.regions_dict['US'])
                else:
                    region = str(self.regions_dict[self.run_params['country']])
                self.results[self.run_params['country']]['post']['region'] = region
            except Exception as e:
                self.results[self.run_params['country']]['meta']['region'] = 'Region Not Available'
                logpr('Region not in WP: {}'.format(e))

        else:
            logpr('{}: {}'.format(self.run_params['country'], 'Not enough information to generate template'))
            self.results[self.run_params['country']] = {
                    'meta':{'publish':'no','created_at': str(datetime.now()),'info_part': perc},
                    'post':{
                    'content': 'Not enough information to generate template.',
                    }
                    }

    # NOT FINISHED
    def get_rank_params(self):
        df = self.iter_text_indicators_info.copy()
        for index, row in df.iterrows():
            if row['type'] in ['rank']:

                source = row['source']
                name = row['name']
                type_ = row['type']
                formatting = row['formatting']
                replace_with = row['replace_with']

                if source == 'Country.BusinessWorldBankReport':
                    tmp = self.datasets['rank'][source]
                    tmp = tmp[tmp['Country']==self.run_params['country']]
                    tmp = tmp[tmp['Indicator']==name]# | (tmp['code']==name)]

                    if len(tmp)>0:
                        tmp = tmp.sort_values(by='Year',ascending=False)
                        tmp.reset_index(drop=True,inplace=True)
                        if formatting == 'value':
                            value = int(tmp.loc[0,'Value'])
                        if formatting == 'year':
                            value = int(tmp.loc[0,'Year'])
                    else:
                        value = 'v??'
                        
                    string = re.findall(r"{.+?}",replace_with)
                    
                    if row['block'] == '{if}':
                        if value != 'v??':
                            replace_with = replace_with.replace(string[0],str(value))
                            df.loc[index,'replace_with'] = replace_with
                        else:
                            df.loc[index,'replace_with'] = ''
                    else:
                        replace_with = replace_with.replace(string[0],str(value))
                        df.loc[index,'replace_with'] = replace_with
                        
                if source == 'Country.MacroYearData':
                    tmp = self.datasets['rank'][source]
                    tmp = tmp[(tmp['Indicator']==name) | (tmp['code']==name)]
                    tmp = tmp.sort_values(by=['Year','Value'],ascending=[False,False]).reset_index(drop=True)
                    country_index = tmp.index[tmp['Country']==self.run_params['country']]

                    if formatting == 'value':
                        value = country_index[0]+1
                    elif formatting == 'year':
                        value = tmp.loc[country_index[0],'Year']
                    else:
                        value = 'v??'
                    
                    string = re.findall(r"{.+?}",replace_with)
                    
                    if row['block'] == '{if}':
                        if value != 'v??':
                            replace_with = replace_with.replace(string[0],str(value))
                            df.loc[index,'replace_with'] = replace_with
                        else:
                            df.loc[index,'replace_with'] = ''
                    else:
                        replace_with = replace_with.replace(string[0],str(value))
                        df.loc[index,'replace_with'] = replace_with

        return df

    ######## END
    ############################################################

    ############################################################
    ######## STEP 0
    def get_params(self, country_list='United Kingdom', create_wp='no', sortby='Population', dataset='country',
    text=''):
        '''
            Create a dictionary with key parameters and their values that will be used in the next steps of the templating.
            Parameters:
                country_list - a string of country name or integer
                create_wp - a string yes/no whether to create the post of WordPress
                sortby - if country list is a digit, choose an indicator to sort by
        '''
        self.run_params = {}
        # TEXT
        self.run_params['text'] = text

        # YEAR
        self.run_params['year'] = int(datetime.now().year)

        # DATE
        self.run_params['date'] = str(datetime.now().date())

        # DATASET
        self.run_params['dataset'] = dataset

        # CREATE POST ON WP
        if create_wp == None:
            self.run_params['create_wp'] = 'no' 
        else:
            self.run_params['create_wp'] = create_wp

        # SORTBY
        if sortby != None:
            self.run_params['sortby'] = sortby
        else:
            self.run_params['sortby'] = None

        self.run_params['country_list'] = country_list
            
        logpr('Run parameters: \n{}'.format(self.run_params))

    def get_data(self):
        if self.run_params['dataset'] == 'country':
            query = sql_queries.country_params
            logpr(query)
            gbq_creds = gbq_credentials()
            # get data
            self.df_main = (gbq_creds['bqclient'].query(query).result().to_dataframe(bqstorage_client=gbq_creds['bqstorageclient']))
        else:
            logpr('No Valid Dataset Selected')
            sys.exit()

    def get_dataframes(self):
        if self.run_params['dataset'] == 'country' and self.run_params['country_list'] == 'G20':
            country_list = ['Argentina','Australia','Brazil','Canada','China','France','Germany','India','Indonesia','Italy','Japan',
            'South Korea','Mexico','Russia','Saudi Arabia','South Africa','Turkey','United Kingdom','United States','European Union']
            self.df_main = self.df_main[self.df_main['Country'].isin(country_list)]
            self.df_rank = self.df_main.copy()
        elif self.run_params['dataset'] == 'country' and self.run_params['country_list'] != 'G20':
            self.df_rank = self.df_main.copy()
            self.df_main = self.df_main[self.df_main['Country'].isin(self.run_params['country_list'])]
        else:
            logpr('No valid parameters selected')


    ############################################################
    ######## STEP 0
    def get_functions_from_text(self):
        self.functions = re.findall(r"{.+?}",self.run_params['text'])

        logpr('Found Number Of Sections: {}'.format(len(self.functions)))
        logpr('Found Sections: {}'.format(self.functions))

    def get_values_from_functions(self):
        self.function_value_dict = {}
        for function in self.functions:
            function_whole = function
            for el in ['{','}']:
                function = function.replace(el,'',1)
            function_info = function.split('|')

            if function_info[0] == 'value':
                indicator_value = self.get_raw_value(indicator=function_info[1],year=function_info[2])
            elif function_info[0] == 'year':
                indicator_value = self.get_raw_year(indicator=function_info[1],year=function_info[2])




            if function_whole not in self.function_value_dict.keys():
                self.function_value_dict[function_whole] = indicator_value

    def get_raw_value(self,indicator,year):
        if year == '':
                year == self.run_params['year']

        if self.run_params['dataset'] == 'country':
            df_tmp = self.df_main.copy()
            df_tmp = df_tmp[df_tmp['Country']==self.run_params['country'] and df_tmp['Indicator']==indicator and df_tmp['Year']==year]            

            if len(df_tmp)>0:
                df_tmp.sort_values(by='Year', inplace=True, ascending=False)
                df_tmp.reset_index(drop=True, inplace=True)
                new = df_tmp.loc[0, 'Indicator']
            else:
                df_tmp = self.df_main.copy()
                df_tmp = df_tmp[df_tmp['Country']==self.run_params['country'] and df_tmp['Indicator']==indicator and df_tmp['Year']<=self.run_params['year']]
                if len(df_tmp)>0:
                    df_tmp.sort_values(by='Year', inplace=True, ascending=False)
                    df_tmp.reset_index(drop=True, inplace=True)
                    new = df_tmp.loc[0, 'Indicator']
                else:
                    new = 'v??'

        return str(new)

    def get_raw_year(self,indicator,year):
        if year == '':
                year == self.run_params['year']

        if self.run_params['dataset'] == 'country':
            df_tmp = self.df_main.copy()
            df_tmp = df_tmp[df_tmp['Country']==self.run_params['country'] and df_tmp['Indicator']==indicator and df_tmp['Year']==year]            

            if len(df_tmp)>0:
                df_tmp.sort_values(by='Year', inplace=True, ascending=False)
                df_tmp.reset_index(drop=True, inplace=True)
                new = df_tmp.loc[0, 'Year']
            else:
                df_tmp = self.df_main.copy()
                df_tmp = df_tmp[df_tmp['Country']==self.run_params['country'] and df_tmp['Indicator']==indicator and df_tmp['Year']<=self.run_params['year']]
                if len(df_tmp)>0:
                    df_tmp.sort_values(by='Year', inplace=True, ascending=False)
                    df_tmp.reset_index(drop=True, inplace=True)
                    new = df_tmp.loc[0, 'Year']
                else:
                    new = 'v??'

        return str(new)






    ############################################################
    ######## STEP 1
    def text_parse_tags(self,text):
        '''
            A method to extract the indicator names and key information about them
            Parameters:
                text - the text template with tags inside
        '''
        # SAVE TEXT
        self.text = text
        # FIND INDICATORS
        indicators = re.findall(r"{.+?}",self.text)
        #indicators = list(map(lambda ele: ele.split(r'{.+?'), self.text))
        logpr('Sections Found: {}'.format(len(indicators)))
        logpr('Sections: {}'.format(str(indicators)))

        self.indicators_general = []
        self.indicators_databased = []
        for indicator in indicators:
            for el in ['{','}']:
                indicator = indicator.replace(el,'',1)
            indicator = indicator.split('|')
            if indicator[0] in ['title','description','year_','country_','city_','date_']:
                self.indicators_general.append(indicator)
            else:
                self.indicators_databased.append(indicator)

    ######## END
    ############################################################


    ############################################################
    ######## STEP 2
    def get_country_filters(self,country_list):
        # COUNTRY LIST
        if country_list == None:
            self.run_params['country_list'] = ['United Kingdom']
        else:
            self.run_params['country_list'] = list(country_list.split(','))
        
        # WHERE FILTERS
        try:
            # if number, pull the top n countries sorted by the selected indicator
            limit = int(self.run_params['country_list'][0])
            gbq_creds = gbq_credentials()
            query = sql_queries.numeric_country_list.format(indicator=self.run_params['sortby'],
                                                            limit=limit,
                                                            year=self.run_params['year'])
            countries = (gbq_creds['bqclient'].query(query).result().to_dataframe(bqstorage_client=gbq_creds['bqstorageclient']))
            self.run_params['country_list'] = list(countries['Country'])
            countries = str(tuple(list(countries['Country'])))
            
            self.run_params['country_where_filter'] = "where Country in {}".format(countries)
            logpr(self.run_params['country_where_filter'])
            logpr('Selected Number Parameter')
        except Exception as e:
            logpr(e)
            if self.run_params['country_list'][0]=='bulk':
                self.run_params['country_where_filter'] = '''Where Country != 'This is a random country' '''
            elif len(self.run_params['country_list'])>1:
                self.run_params['country_where_filter'] = "where Country in {}".format(str(tuple(self.run_params['country_list'])))
            else:
                self.run_params['country_where_filter'] = "where Country = '{}'".format(self.run_params['country_list'][0])
            logpr('Selected String Parameter')

        logpr('Getting CountryIDs')
        query = sql_queries.country_id.format(country_where_filter=self.run_params['country_where_filter'])
        logpr(query)
        gbq_creds = gbq_credentials()
        country_creds_df = (gbq_creds['bqclient'].query(query).result().to_dataframe(bqstorage_client=gbq_creds['bqstorageclient']))
        country_creds = country_creds_df.to_dict('list')
        
        if len(country_creds['CountryID'])>1:
            self.run_params['countryid_filter'] = "where CountryID in {}".format(str(tuple(country_creds['CountryID'])))
        else:
            self.run_params['countryid_filter'] = "where CountryID = {}".format(country_creds['CountryID'][0])

    

    def get_wp_regions(self):
        ########################## GET WP REGIONS
        regions_dict = {}
        pull = True
        i=1
        while pull == True:
            regions = get_request(params = {
                'url':self.branch_ + '/wp-json/wp/v2/region?per_page=100&page={}'.format(i),
                'auth':self.auth_
            }        
            )
            regions = regions.json()
            if len(regions)==0:
                pull = False
            else:
                for region in regions:
                    regions_dict[region['name']] = region['id']
                    
            i += 1

        self.regions_dict = regions_dict

    def get_data_gbq_limited(self):
        '''
            A method to extract indicators and other data from selected sources
        '''
        #############################
        ############################# EXTRACT INDICATORS
        indicators_to_pull = []
        for indicator in self.indicators_databased:
            if indicator[0] not in indicators_to_pull:
                indicators_to_pull.append(indicator[0])

        logpr('Indicators To Pull: {}'.format(indicators_to_pull))
        if self.run_params['dataset'] == 'country':
            if len(indicators_to_pull)>0:
                # construct indicators filter
                indicator_filter = indicators_to_pull
                if len(indicator_filter)==1:
                    indicator_filter = str(tuple(indicators_to_pull)).replace(',','')
                else:
                    indicator_filter = str(tuple(indicators_to_pull))
                # construct query
                query = sql_queries.country_params.format(
                    country_where_filter=self.run_params['country_where_filter'],
                    indicators=indicator_filter,
                    year=self.run_params['year'])
                logpr(query)
                gbq_creds = gbq_credentials()
                # get data
                self.df_main = (gbq_creds['bqclient'].query(query).result().to_dataframe(bqstorage_client=gbq_creds['bqstorageclient']))
            else:
                logpr('Not Data To Pull')
        else:
            logpr('No Valid Dataset Selected')
            sys.exit()

    ######## END
    ############################################################


    ############################################################
    ######## STEP 3
    def get_indicators_replace_value_year(self):
        result_dict = {}
        self.missing_values = []
        if self.run_params['dataset'] == 'country':
            indicator_list = [indicator for indicator in self.indicators_databased if indicator[2] in ['value','year']]
            for indicator in indicator_list:

                df_tmp = self.df_main.copy()
                df_tmp = df_tmp[df_tmp['Country']==self.run_params['country']]

                name = indicator[0]
                value_year = indicator[2].capitalize()
                # Check whether there is a year specified in the indicator
                # if it's empty then take the most recent year
                # if it's not, then take the specified year
                # if the specified year is not present, take the most recent year
                # if the most recent year is not present in any of the cases, then take an empty value of v??
                
                if indicator[1] == '':
                    # MOST RECENT
                    year = self.run_params['year']
                    df_tmp = df_tmp[df_tmp['Year']<int(year)]
                    df_tmp = df_tmp[(df_tmp['Indicator']==name) | (df_tmp['code']==name)]
                    
                    if len(df_tmp)>0:
                        df_tmp.sort_values(by='Year', inplace=True, ascending=False)
                        df_tmp.reset_index(drop=True, inplace=True)

                        new = df_tmp.loc[0, value_year]
                        # INSERT NUMBER FORMATTING HERE
                        if value_year == 'Value':
                            if 'proportion' in name.lower() or 'rate' in name.lower():
                                new = np.round(new,1)
                                new = '{}%'.format(new)
                            else:
                                new = self.format_big_numbers(new)
                                if 'USD' in name:
                                    new = '${}'.format(new)

                        old = '{' + str(indicator[0]) + '|' + str(indicator[1]) + '|' + str(indicator[2]) + '}'
                        self.iter_text = self.iter_text.replace(old,str(new))
                    else:
                        old = '{' + str(indicator[0]) + '|' + str(indicator[1]) + '|' + str(indicator[2]) + '}'
                        new = 'v??'
                        self.iter_text = self.iter_text.replace(old,str(new))
                        self.missing_values.append(str(new))
                else:
                    # SPECIFIC YEAR
                    year = indicator[1]
                    df_tmp = df_tmp[df_tmp['Year']==int(year)]
                    df_tmp = df_tmp[(df_tmp['Indicator']==name) | (df_tmp['code']==name)]
                    
                    if len(df_tmp)>0:
                        df_tmp.sort_values(by='Year', inplace=True, ascending=False)
                        df_tmp.reset_index(drop=True, inplace=True)

                        new = df_tmp.loc[0, value_year]
                        # INSERT NUMBER FORMATTING HERE
                        if value_year == 'Value':
                            if 'proportion' in name.lower() or 'rate' in name.lower():
                                new = np.round(new,1)
                                new = '{}%'.format(new)
                            else:
                                new = self.format_big_numbers(new)
                                if 'USD' in name:
                                    new = '${}'.format(new)

                        old = '{' + str(indicator[0]) + '|' + str(indicator[1]) + '|' + str(indicator[2]) + '}'
                        self.iter_text = self.iter_text.replace(old,str(new))
                    else:
                        year = self.run_params['year']
                        df_tmp = self.df_main.copy()
                        df_tmp = df_tmp[df_tmp['Country']==self.run_params['country']]
                        df_tmp = df_tmp[df_tmp['Year']==int(year)]
                        df_tmp = df_tmp[(df_tmp['Indicator']==name) | (df_tmp['code']==name)]

                        if len(df_tmp) > 0:
                            df_tmp.sort_values(by='Year', inplace=True, ascending=False)
                            df_tmp.reset_index(drop=True, inplace=True)
                            new = df_tmp.loc[0, value_year]
                            # INSERT NUMBER FORMATTING HERE
                            if value_year == 'Value':
                                if 'proportion' in name.lower() or 'rate' in name.lower():
                                    new = np.round(new,1)
                                    new = '{}%'.format(new)
                                else:
                                    new = self.format_big_numbers(new)
                                    if 'USD' in name:
                                        new = '${}'.format(new)
                                        new = new.replace(' million', 'm')

                            old = '{' + str(indicator[0]) + '|' + str(indicator[1]) + '|' + str(indicator[2]) + '}'
                            self.iter_text = self.iter_text.replace(old,str(new))
                        else:
                            old = '{' + str(indicator[0]) + '|' + str(indicator[1]) + '|' + str(indicator[2]) + '}'
                            new = 'v??'
                            self.iter_text = self.iter_text.replace(old,str(new))
                            self.missing_values.append(str(new))

    def get_indicators_replace_general(self):
        for i in 'year','country','city','date':
            try:
                if i == 'country':
                    tmp = self.df_main.loc[self.df_main['Country']==self.run_params['country'],'AutoName'].copy()
                    tmp = tmp[0]
                    self.iter_text = self.iter_text.replace('country_',tmp)
                else:
                    self.iter_text = self.iter_text.replace(i+'_',str(self.run_params[i]))
            except Exception as e:
                #logpr(e)
                None

    def text_loop(self, text):
        self.results = {}
        if self.run_params['dataset'] == 'country':
            for country in self.df_main['Country'].unique():
                logpr('Generating Template for: {}'.format(country))
                
                self.run_params['country'] = country
                self.get_values_from_functions()

                # PER COUNTRY STUFF
                #self.iter_text = self.text
                #self.run_params['country'] = country

                # DATA FUNCTIONS
                #self.get_indicators_replace_value_year()
                #self.get_indicators_replace_general()

                # CUSTOM FUNCTIONALITIES

                # STYLE FUNCTIONS
                #self.format_style()

                # ADD TEXT TO RESULTS DICTIONARY
                #self.add_to_results()

    ######## END
    ############################################################


    ############################################################
    ######## STEP 4
    def text_publish(self):
        if self.run_params['create_wp']=='yes':
            logpr('Publishing...')
            for key in self.results.keys():
                if self.results[key]['meta']['publish'] == 'yes':
                    self.b = post_request(params = {
                        #'auth': ('denis', 'test123123'),
                        'auth':('emilfilipov', 'P3g8gKrfI6R78ybPwFZ5bw7C'),
                        'url':'https://pr-disco-newstatesman-b2b.pantheonsite.io/investmentmonitor/wp-json/wp/v2/posts',
                        'data':self.results[key]['post'],
                        #headers=headers
                        }
                        )

    ######## END
    ############################################################


            













