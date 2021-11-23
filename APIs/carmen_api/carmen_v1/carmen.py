#-*- coding: utf-8 -*-
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas_gbq
import json, sys, re
import numpy as np
from datetime import datetime

from code_base import logpr 
from code_base import gbq_credentials
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
        self.branch = 'https://investmentmonitor.ai'
        return None

    def version__(self):
        version = '1.25'
        return version

    def formatting1(self, text):
        
        text_original = text
        text_tmp = text.split('\n')
        blocks = []

        for block in text_tmp:
            blocks.extend(block.split('. '))
            
        for block in range(0,len(blocks)):
            if 'the' in blocks[block][:3]:
                block_new = blocks[block][0].upper() + blocks[block][1:]
                text_original = text_original.replace(blocks[block], block_new)
                
        if " - " in text_original:
            text_original = text_original.replace(" - ", " – ")

        blocks = re.findall(r"\$.+? million",text_original)
        for ind in range(0,len(blocks)):
            old = blocks[ind]
            new = blocks[ind]
            new = new.replace(' million', 'm')
            text_original = text_original.replace(old,new)

        return text_original

    def ranking(self, num, largest=False):
        rank = ''
        num = str(num).strip()
        len2_ths = ['10','11','12','13','14','15','16','17','18','19','20']

        if len(num) == 1:
            if num == '1':
                if largest==True:
                    rank = '\b'
                else:
                    rank = num + 'st'
            elif num == '2':
                rank = num + 'nd'
            elif num == '3':
                rank = num + 'rd'
            else:
                rank = num + 'th'
        else:
            if num[-2:] in len2_ths:
                rank = num + 'th'
            else:
                if num[-1]=='1':
                    rank = num + 'st'
                elif num[-1]=='2':
                    rank = num + 'nd'
                elif num[-1]=='3':
                    rank = num + 'rd'
                else:
                    rank = num + 'th'

        if largest==True:
            style_dict = {'1':'','2':'second','3':'third','4':'forth','5':'fifth',
            '6':'sixth','7':'seventh','8':'eighth','9':'ninth','10':'tenth'}
        else:
            style_dict = {'1':'first','2':'second','3':'third','4':'forth','5':'fifth',
            '6':'sixth','7':'seventh','8':'eighth','9':'ninth','10':'tenth'}
        
        if num in ['1','2','3','4','5','6','7','8','9','10']:
            rank = num.replace(num,style_dict[num])

        return rank

    def format_big_numbers(self,number):
        import math

        number = np.round(float(number),1)

        if len(str(int(number)))<7:
            number_formatted = '{number:,}'.format(number=int(number))

        else:
            millnames = ['',' thousand',' million','bn','trn']
            millidx = max(0,min(len(millnames)-1,int(math.floor(0 if number == 0 else math.log10(abs(number))/3))))

            number_formatted = '{:.1f}{}'.format((number / 10**(3 * millidx - 1))/10, millnames[millidx])
            temp = number_formatted.split('.')
            if temp[1][0] == '0':
                number_formatted = number_formatted.replace('.0', '')
            
        return number_formatted

    def data_pull_gbq(self, country=['United Kingdom'], gdp_growth_years=[2010,2025]):
        '''
        Pull the necessary data from GBQ into a pd.DataFrame.
        Parameters:
            country - a list of countries for which to pull data
            gdp_growth_years - range for displaying a statistic
        Output:
            pd.DataFrame(s)
        '''
        self.gdp_growth_years = gdp_growth_years
        self.year = int(datetime.now().year) #- 1
        
        ########################## LOAD GBQ CREDENTIALS AND CREATE CLIENT
        gbq_creds = gbq_credentials()
        ##########################

        ########################## CONSTRUCT WHERE FILTER FOR COUNTRY
        top_10_list = ['Russia','Germany','China','United Kingdom','France','Spain','Italy','Austria','Hungary','United States']

        try:
            filter_param = int(country[0])
            country_where_filter = "where Country in {}".format(str(tuple(top_10_list[:filter_param])))
            country_where_mapping_filter = "where Monitors in {a} or GlobalData in {a}".format(a=str(tuple(top_10_list[:filter_param])))
            logpr('Selected Number Parameter')
        except Exception as e:
            logpr('Selected String Parameter')
            if country[0]=='bulk':
                country_where_filter = '''Where Country != 'This is a random country' '''
                country_where_mapping_filter = '''Where  Monitors != 'asdasdasd' or GlobalData != 'asdasdasd' '''
            elif country[0]=='test_multiple':
                country_where_filter = "where Country in ('France', 'United States', 'Bermuda', 'Ecuador', 'Lebanon', 'Morocco', 'Nigeria', 'India', 'Fiji', 'Marshall Islands', 'Tuvalu')"
                country_where_mapping_filter = "where Monitors in ('France', 'United States', 'Bermuda', 'Ecuador', 'Lebanon', 'Morocco', 'Nigeria', 'India', 'Fiji', 'Marshall Islands', 'Tuvalu') or GlobalData in ('France', 'United States', 'Bermuda', 'Ecuador', 'Lebanon', 'Morocco', 'Nigeria', 'India', 'Fiji', 'Marshall Islands', 'Tuvalu')"
            elif len(country)>1:
                country_where_filter = "where Country in {}".format(str(tuple(country)))
                country_where_mapping_filter = "where Monitors in {a} or GlobalData in {a}".format(a=str(tuple(country)))
            else:
                country_where_filter = "where Country = '{}'".format(country[0])
                country_where_mapping_filter = "where Monitors = '{a}' or GlobalData = '{a}'".format(a=country[0])
        ##########################

        ########################## COUNTRY BASIC INFO
        logpr('Getting CountryIDs')
        query = sql_queries.country_id.format(country_where_filter=country_where_filter)
        logpr(query)
        self.country_creds_df = (gbq_creds['bqclient'].query(query).result().to_dataframe(bqstorage_client=gbq_creds['bqstorageclient']))
        self.country_creds = self.country_creds_df.to_dict('list')
        ##########################

        ########################## CONSTRUCT WHERE FILTER FOR COUNTRYID INFO
        if len(self.country_creds['CountryID'])>1:
            countryid_filter = "where CountryID in {}".format(str(tuple(self.country_creds['CountryID'])))
        else:
            countryid_filter = "where CountryID = {}".format(self.country_creds['CountryID'][0])
        ##########################

        ########################## COUNTRY GENERAL INDICATORS
        logpr('Getting Country Indicators')
        query = sql_queries.country_params.format(
                                                countryid_filter = countryid_filter
                                                #years=str(tuple(self.year_main_anlz_tminus))
                                                )
        logpr(query)
        self.country_indicators = (gbq_creds['bqclient'].query(query).result().to_dataframe(bqstorage_client=gbq_creds['bqstorageclient']))
        ##########################

        ########################## COUNTRY POPULATION RANK
        logpr('Getting Country Population Rank')
        query = sql_queries.population_rank.format(year=self.year)
        logpr(query)
        self.population_rank = (gbq_creds['bqclient'].query(query).result().to_dataframe(bqstorage_client=gbq_creds['bqstorageclient']))
        ##########################

        ########################## COUNTRY GDP RANK
        logpr('Getting Country GDP Rank')
        query = sql_queries.gdp_rank.format(year=self.year)
        logpr(query)
        self.gdp_rank = (gbq_creds['bqclient'].query(query).result().to_dataframe(bqstorage_client=gbq_creds['bqstorageclient']))
        ##########################

        ########################## COUNTRY CITY POPULATION RANK
        logpr('Getting Country City Population')
        query = sql_queries.cities.format(country_where_filter=country_where_filter,
                                          year=self.year)
        logpr(query)
        self.cities = (gbq_creds['bqclient'].query(query).result().to_dataframe(bqstorage_client=gbq_creds['bqstorageclient']))
        ##########################

        ########################## WORLD BANK REPORT
        logpr('Getting World Bank Report')
        query = sql_queries.world_bank_report.format(country_where_filter=country_where_filter)
        logpr(query)
        self.doing_business = (gbq_creds['bqclient'].query(query).result().to_dataframe(bqstorage_client=gbq_creds['bqstorageclient']))
        ##########################

        ########################## GET WP REGIONS
        regions_dict = {}
        pull = True
        i=1
        while pull == True:
            regions = get_request(params = {
                'url':self.branch + '/wp-json/wp/v2/region?per_page=100&page={}'.format(i),
                #'auth':('emilfilipov', 'P3g8gKrfI6R78ybPwFZ5bw7C'),
                'auth':self.auth_
            }        
            )
            regions = regions.json()
            if len(regions)==0:
                print('asd')
                pull = False
            else:
                for region in regions:
                    regions_dict[region['name']] = region['id'] #.lower()
            i += 1
        self.regions_dict = regions_dict

        ########################## GET WP CATEGORIES
        #categories_dict = {}
        #pull = True
        #i=1
        #while pull == True:
        #    categories = get_request(params = {
        #    'url':'https://pr-disco-newstatesman-b2b.pantheonsite.io/investmentmonitor/wp-json/wp/v2/categories?per_page=100&page={}'.format(i),
        #    'auth':('emilfilipov', 'P3g8gKrfI6R78ybPwFZ5bw7C')
        #    }        
        #    )
        #    categories = categories.json()
        #    if len(categories)==0:
        #        pull = False
        #    else:
        #        for category in categories:
        #            categories_dict[category['name']] = category['id'] #.lower()       
        #    i += 1
        #self.categories_dict = categories_dict

        ########################## COUNTRY NAME MAPPING
        logpr('Getting Country Names')
        query = sql_queries.country_text_names.format(country_where_filter=country_where_mapping_filter)
        logpr(query)
        self.country_name_mapping = (gbq_creds['bqclient'].query(query).result().to_dataframe(bqstorage_client=gbq_creds['bqstorageclient']))

    def extract_indicators(self, var_list, country, type_):
        results = {}
        
        for var in var_list:
            df1 = self.country_indicators.loc[self.country_indicators['CountryID']==country]
            df1 = df1.loc[df1['Indicator']==var]
            df1 = df1.loc[df1['Year']<=self.year]
            df1.sort_values(by='Year',ascending=False,inplace=True)
            df1.reset_index(drop=True,inplace=True)
            if len(df1)>0:
                if type_ == '':
                    v = df1.loc[0,'Value']
                else:
                    v = type_(df1.loc[0,'Value'])
                if type_ == float:
                    v = np.round(v,1)
                    if str(v)[-1] == '0':
                        v = int(v)
                results[var] = [v, df1.loc[0,'Year']]
            else:
                results[var] = ['v??', 'y??']

        return results

    def extract_growth(self, var, country, ystart, yend):
        '''
        Helper function to calculate the % growth of GDP between 2 years
        '''
        df_gdp = self.country_indicators.loc[(self.country_indicators['Indicator']==var) & (self.country_indicators['CountryID']==country)].copy()
        df_gdp.sort_values(by='Year', ascending=False, inplace=True)
        df_gdp.reset_index(drop=True, inplace=True)

        # self.year, self.gdp_growth_years[0], self.gdp_growth_years[1]

        df_gdp_start = df_gdp[df_gdp['Year']==ystart].copy()
        df_gdp_start.reset_index(drop=True, inplace=True)
        if len(df_gdp_start)>0:
            df_gdp_start = int(df_gdp_start.loc[0,'Value'])
        else:
            df_gdp_start = 'v??'

        df_gdp_end = df_gdp[df_gdp['Year']==yend].copy()
        df_gdp_end.reset_index(drop=True, inplace=True)
        if len(df_gdp_end)>0:
            df_gdp_end = int(df_gdp_end.loc[0,'Value'])
        else:
            df_gdp_end = 'v??'

        if (df_gdp_start!='v??') and (df_gdp_end!='v??'):
            gdp_growth = int(((df_gdp_end - df_gdp_start) / df_gdp_start)*100)
        else:
            gdp_growth = 'v??'

        return gdp_growth

    def extract_city_pop(self, country):
        
        df = self.cities[self.cities['Country']==country].copy()
        df.reset_index(drop=True, inplace=True)

        if len(df)>=3:
            pop1 = self.format_big_numbers(df.loc[0,'Value'])
            pop2 = self.format_big_numbers(df.loc[1,'Value'])
            pop3 = self.format_big_numbers(df.loc[2,'Value'])
            cities = '{city1} ({pop1} people), {city2} ({pop2}) and {city3} ({pop3})'.format(
                city1 = df.loc[0,'City'],
                pop1 = pop1,
                city2 = df.loc[1,'City'],
                pop2 = pop2,
                city3 = df.loc[2,'City'],
                pop3 = pop3,
            )
        else:
            cities = 'v??'

        cities = cities.replace('-','/')

        return cities

    def extract_world_bank_ranking(self, country):
        '''
        Helper function to extract a single value for the analyzed year.
        '''

        # doing business
        doing_bus = self.doing_business[(self.doing_business['Ranking']=='Doing Business Guide') & (self.doing_business['Country']==country)].copy()
        doing_bus.sort_values(by='ReportYear', ascending=False, inplace=True)
        doing_bus.reset_index(drop=True,inplace=True)
        # global competitiveness
        compet_re = self.doing_business[(self.doing_business['Ranking']=='Global Competitiveness Index') & (self.doing_business['Country']==country)].copy()
        compet_re.sort_values(by='ReportYear', ascending=False, inplace=True)
        compet_re.reset_index(drop=True,inplace=True)


        if len(doing_bus)>=2:
            doing_bus_cur_rank = [doing_bus.loc[0,'Rank'], doing_bus.loc[0,'ReportYear']]
            doing_bus_pre_rank = [doing_bus.loc[1,'Rank'], doing_bus.loc[1,'ReportYear']]
            doing_bus_increase = doing_bus_cur_rank[0] - doing_bus_pre_rank[0]
        elif len(doing_bus)==1:
            doing_bus_cur_rank = [doing_bus.loc[0,'Rank'], doing_bus.loc[0,'ReportYear']]
            doing_bus_pre_rank = ['v??','y??']
            doing_bus_increase = 'v??'
        else:
            doing_bus_cur_rank = ['v??', 'y??']
            doing_bus_pre_rank = ['v??', 'y??']
            doing_bus_increase = 'v??'

        if len(compet_re)>=2:
            compet_re_cur_rank = [compet_re.loc[0,'Rank'], compet_re.loc[0,'ReportYear']]
            compet_re_pre_rank = [compet_re.loc[1,'Rank'], compet_re.loc[1,'ReportYear']]
            compet_re_increase = compet_re_cur_rank[0] - compet_re_pre_rank[0]
        elif len(compet_re)==1:
            compet_re_cur_rank = [compet_re.loc[0,'Rank'], compet_re.loc[0,'ReportYear']]
            compet_re_pre_rank = ['v??','y??']
            compet_re_increase = 'v??'
        else:
            compet_re_cur_rank = ['v??', 'y??']
            compet_re_pre_rank = ['v??','y??']
            compet_re_increase = 'v??'

        result_string = '''\n\n'''

        if doing_bus_cur_rank[0]!='v??':
            rank = self.ranking(doing_bus_cur_rank[0],largest=False)
            #num = str(int(doing_bus_cur_rank[0]))
            #if num[-1]=='1':
            #    tmp = ' ' + num + 'st '
            #    logpr('doing_bus_cur_rank: {}'.format(tmp))
            #elif num in ['10','11','12','13','14','15','16','17','18','19','20']:
            #    tmp = ' ' + num + 'th '
            #    logpr('doing_bus_cur_rank: {}'.format(tmp))
            #elif num[-1]=='2':
            #    tmp = ' ' + num + 'nd '
            #    logpr('doing_bus_cur_rank: {}'.format(tmp))
            #elif num[-1]=='3':
            #    tmp = ' ' + num + 'rd '
            #    logpr('doing_bus_cur_rank: {}'.format(tmp))
            #else:
            #    tmp = ' ' + num + 'th '
            #    logpr('doing_bus_cur_rank: {}'.format(tmp))

            result_string += '''{country} ranked {rank} in the World Bank's Doing Business Report ({year}).'''.format(
                country=self.text_country_name, rank=rank, year=doing_bus_cur_rank[1])

            if doing_bus_pre_rank[0]!='v??':
                rank = self.ranking(doing_bus_pre_rank[0],largest=False)
                #num = str(int(doing_bus_pre_rank[0]))
                #if num[-1]=='1':
                #    tmp = ' ' + num + 'st'
                #elif num in ['10','11','12','13','14','15','16','17','18','19','20']:
                #    tmp = ' ' + num + 'th '
                #elif num[-1]=='2':
                #    tmp = ' ' + num + 'nd'
                #elif num[-1]=='3':
                #    tmp = ' ' + num + 'rd'
                #else:
                #    tmp = ' ' + num + 'th'    

                result_string += ''' In the {year} report it finished {rank}.'''.format(
                        rank=rank,
                        year=doing_bus_pre_rank[1]
                    )

        if compet_re_cur_rank[0]!='v??':
            rank = self.ranking(compet_re_cur_rank[0],largest=False)
            #num = str(int(compet_re_cur_rank[0]))
            #if num[-1]=='1':
            #    tmp = ' ' + num + 'st '
            #elif num in ['10','11','12','13','14','15','16','17','18','19','20']:
            #    tmp = ' ' + num + 'th '
            #elif num[-1]=='2':
            #    tmp = ' ' + num + 'nd'
            #elif num[-1]=='3':
            #    tmp = ' ' + num + 'rd'
            #else:
            #    tmp = ' ' + num + 'th'

            result_string += ''' In the World Economic Forum’s Global Competitiveness Report ({year_cur}), {country} finished {rank_cur}'''.format(
                year_cur = compet_re_cur_rank[1],
                country = self.text_country_name,
                rank_cur = rank,
            )
            
            if compet_re_pre_rank[0]!='v??':
                rank = self.ranking(compet_re_cur_rank[0])
                #num = str(int(compet_re_pre_rank[0]))
                #if num[-1]=='1':
                #    tmp = ' ' + num + 'st '
                #elif num in ['10','11','12','13','14','15','16','17','18','19','20']:
                #    tmp = ' ' + num + 'th '
                #elif num[-1]=='2':
                #    tmp = ' ' + num + 'nd '
                #elif num[-1]=='3':
                #    tmp = ' ' + num + 'rd '
                #else:
                #    tmp = ' ' + num + 'th '

                result_string += ", after ranking {rank_pre} in {year_pre}.".format(
                    rank_pre = rank,
                    year_pre = compet_re_pre_rank[1]
                )
            else:
                result_string += '.'

        return result_string

    def text_generate(self):
        '''
        This method will be used to generate the text template and fill it with the necessary numbers.
        '''
        self.results = {}
        for index,row in self.country_creds_df.iterrows():

            self.text_country_name = self.country_name_mapping[(self.country_name_mapping['Monitors']==row.Country) | (self.country_name_mapping['GlobalData']==row.Country)].copy()
            if len(self.text_country_name)>0:
                self.text_country_name = list(self.text_country_name['AutoName'])[0]
            else:
                self.text_country_name = row.Country

            logpr('Generating Template for {}'.format(row.Country))

            ########### INDICATORS & TEXT #########################
            indicators1 = self.extract_indicators(type_=int, country=row.CountryID,
            var_list=['Population','Proportion of male population','Proportion of female population','Total employment','Total unemployment',
            'Nominal GDP per capita (USD)','Number of students in tertiary education','Number of students in secondary education',
            'Number of students in primary education','Nominal GDP (USD)',
            'Private healthcare expenditure (USD)','Public healthcare expenditure (USD)'])

            indicators2 = self.extract_indicators(type_=float, country=row.CountryID,
            var_list=['Proportion of male population','Proportion of female population',
            'Public sector finances: Proportion of government education expenditure in GDP'])

            indicators3 = self.extract_indicators(type_='', country=row.CountryID,
            var_list=['Employment rate','Unemployment rate'])

            # GDP GROWTH
            gdp_growth1 = self.extract_growth(ystart=self.gdp_growth_years[0],yend=self.year, country=row.CountryID,
            var='Nominal GDP (USD)')
            gdp_growth2 = self.extract_growth(ystart=self.year,yend=self.gdp_growth_years[1], country=row.CountryID,
            var='Nominal GDP (USD)')

            # CITY POPULATION
            cities = self.extract_city_pop(country=row.Country)

            # COUNTRY POPULATION RANK
            country_population_rank = self.population_rank.copy()
            country_population_rank = country_population_rank.sort_values(by=['Year','Value'],ascending=[False,False]).reset_index(drop=True)
            rank_ = country_population_rank.index[country_population_rank['CountryID']==row.CountryID]
            if len(rank_)>0:
                country_population_rank = [rank_[0] + 1, country_population_rank.loc[rank_,'Year'].values[0] ]
            else:
                country_population_rank = ['v??','y??']

            # COUNTRY GDP 
            country_economy_rank = self.gdp_rank.copy()
            country_economy_rank = country_economy_rank.sort_values(by=['Year','Value'],ascending=[False,False]).reset_index(drop=True)
            rank_ = country_economy_rank.index[country_economy_rank['CountryID']==row.CountryID]
            if len(rank_)>0:
                country_economy_rank = [rank_[0] + 1, country_economy_rank.loc[rank_,'Year'].values[0] ]
            else:
                country_economy_rank = ['v??','y??']

            # WB DOING BUSINESS REPORT
            wb_string = self.extract_world_bank_ranking(row.Country)

            # CALCULATE AVAILABLE INFORMATION
            self.indicators = len(indicators1) + len(indicators2) + len(indicators3) + 5
            values = []
            values.extend(indicators1.values())
            values.extend(indicators2.values())
            values.extend(indicators3.values())
            values.append(gdp_growth1)
            values.append(gdp_growth2)
            values.append(cities)
            values.append(country_population_rank[0])
            values.append(country_economy_rank[0])

            self.values = len([x for x in values if 'v??' not in str(x)])

            info_part = self.values/self.indicators

            if info_part > 0.5:
                final_string = ''''''

                # POPULATION PARAGRAPH - DONE
                if indicators1['Population'][0]!='v??':
                    population = self.format_big_numbers(indicators1['Population'][0])
                    tmp_string = 'As of {year}, {country} had a population of {population}'.format(
                        year = indicators1['Population'][1],
                        country = self.text_country_name,
                        population = population
                    )
                    if indicators1['Population'][1] == self.year:
                        tmp_string = tmp_string.replace('had','has')
                        tmp_string = tmp_string.replace('was','is')
                        tmp_string = tmp_string.replace('were','are')
                    else:
                        tmp_string = tmp_string.replace('has','had')
                        tmp_string = tmp_string.replace('is','was')
                        tmp_string = tmp_string.replace('are','were')
                    
                    final_string += tmp_string

                    if country_population_rank[0]!='v??':
                        rank = self.ranking(country_population_rank[0],largest=False)
                        #num = str(int(country_population_rank[0]))
                        #logpr(num)
                        #tmp=num
                        #if num=='1':
                        #    tmp = ' '
                        #elif num in ['10','11','12','13','14','15','16','17','18','19','20']:
                        #    tmp = ' ' + num + 'th '
                        #elif len(num)>1:
                        #    if num[-2:] in ['10','11','12','13','14','15','16','17','18','19','20']:
                        #        tmp = ' ' + num + 'th '
                        #elif num[-1]=='1':
                        #    tmp = ' ' + num + 'st '
                        #elif num[-1]=='2':
                        #    tmp = ' ' + num + 'nd '
                        #elif num[-1]=='3':
                        #    tmp = ' ' + num + 'rd '
                        #else:
                        #    tmp = ' ' + num + 'th '

                        tmp_string = ', making it the {pop_rank} largest country in the world by this measure. '.format(
                        pop_rank=rank 
                        )

                        if country_population_rank[1] == self.year:
                            tmp_string = tmp_string.replace('had','has')
                            tmp_string = tmp_string.replace('was','is')
                            tmp_string = tmp_string.replace('were','are')
                        else:
                            tmp_string = tmp_string.replace('has','had')
                            tmp_string = tmp_string.replace('is','was')
                            tmp_string = tmp_string.replace('are','were')
                        
                        final_string += tmp_string
                    else:
                        final_string += '.'

                    if (indicators2['Proportion of male population'][0]!='v??') and (indicators2['Proportion of female population'][0]!='v??'):
                        tmp_string = "There are {population_male} males and {population_female} females.".format(
                            population_male= '{}%'.format(indicators2['Proportion of male population'][0]),
                            population_female= '{}%'.format(indicators2['Proportion of female population'][0]),
                            )

                        if (indicators1['Proportion of female population'][1] == self.year) and  indicators1['Proportion of male population'][1] == self.year:
                            tmp_string = tmp_string.replace('had','has')
                            tmp_string = tmp_string.replace('was','is')
                            tmp_string = tmp_string.replace('were','are')
                        else:
                            tmp_string = tmp_string.replace('has','had')
                            tmp_string = tmp_string.replace('is','was')
                            tmp_string = tmp_string.replace('are','were')
                        
                        final_string += tmp_string

                # EMPLOYMENT PARAGRAPH
                if indicators1['Total employment'][0]!='v??':
                    total_employment = self.format_big_numbers(indicators1['Total employment'][0])
                    tmp_string = "\n\nThere were {total_employment} people employed in {country}".format(
                        country = self.text_country_name,
                        total_employment = total_employment,
                        year = indicators1['Total employment'][1],
                        )

                    if indicators1['Total employment'][1] == self.year:
                        tmp_string = tmp_string.replace('had','has')
                        tmp_string = tmp_string.replace('was','is')
                        tmp_string = tmp_string.replace('were','are')
                    else:
                        tmp_string = tmp_string.replace('has','had')
                        tmp_string = tmp_string.replace('is','was')
                        tmp_string = tmp_string.replace('are','were')
                    
                    final_string += tmp_string

                    if indicators3['Unemployment rate'][0]!='v??':
                        final_string += ", with an unemployment rate of {unemp_rate}%".format(
                            unemp_rate = np.round(indicators3['Unemployment rate'][0],1),
                            )
                        if indicators1['Total unemployment'][0]!='v??':
                            total_employment = self.format_big_numbers(indicators1['Total unemployment'][0])
                            final_string += " - {total_unemployment} people.".format(
                                total_unemployment=str(total_employment))
                        else:
                            final_string += '.'
                    else:
                        final_string += '.'

                    unique_country_id = self.text_country_name.lower()
                    for thing in [',',':',';','\'','\"',' ']:
                        unique_country_id = unique_country_id.replace(thing, '')

                    html_string = """\n\n
                    <div id=\"{unique_country_id}-unemployment\" style=\"min-width: 288px; max-width: 900px ;\"></div>
                    <script type=\"text/javascript\" src=\"https://pym.nprapps.org/pym.v1.min.js\"></script>
                    <script>var pymParent = new pym.Parent(\"{unique_country_id}-unemployment\", \"https://nsmg-projects-public.s3.eu-west-2.amazonaws.com/test/nsmg-051/index.html?c={country_list}\", {asd});</script>
                    <style>#{unique_country_id}-unemployment iframe {width}</style>""".format(
                        country_list=self.text_country_name,
                        unique_country_id = unique_country_id,
                        asd ='',
                        width = '{ width: 100%}'
                    )

                    final_string += html_string

                # ECONOMY RANK AND GDP PARAGRAPH
                if country_economy_rank[0]!='v??':
                    rank = self.ranking(country_economy_rank[0],largest=False)
                    #num = str(int(country_economy_rank[0]))
                    #logpr(num)
                    #tmp=num
                    #if num=='1':
                    #    tmp = ' '
                    #elif num in ['10','11','12','13','14','15','16','17','18','19','20']:
                    #    tmp = ' ' + num + 'th '
                    #elif len(num)>1:
                    #    if num[-2:] in ['10','11','12','13','14','15','16','17','18','19','20']:
                    #        tmp = ' ' + num + 'th '
                    #elif num[-1]=='1':
                    #    tmp = ' ' + num + 'st '
                    #elif num[-1]=='2':
                    #    tmp = ' ' + num + 'nd '
                    #elif num[-1]=='3':
                    #    tmp = ' ' + num + 'rd '
                    #else:
                    #    tmp = ' ' + num + 'th '

                    tmp_string = "\n\n{country} is the {economy_rank} largest economy in the world".format(
                        country=self.text_country_name, 
                        economy_rank=rank
                        )

                    #if country_economy_rank[1] == self.year:
                    #    tmp_string = tmp_string.replace('had','has')
                    #    tmp_string = tmp_string.replace('was','is')
                    #    tmp_string = tmp_string.replace('were','are')
                    #else:
                    #    tmp_string = tmp_string.replace('has','had')
                    #    tmp_string = tmp_string.replace('is','was')
                    #    tmp_string = tmp_string.replace('are','were')
                    
                    final_string += tmp_string
                    
                    if indicators1['Nominal GDP (USD)'][0]!='v??':
                        gdp_ = self.format_big_numbers(indicators1['Nominal GDP (USD)'][0])
                        final_string += " with a GDP of ${gdp}. ".format(gdp=gdp_)
                    else:
                        final_string += '.'

                    if (gdp_growth1!='v??') & (gdp_growth2!='v??'):
                        final_string += "{country}'s GDP has grown by {gdp_growth1}% between {ystart1} and {yend1} and is expected to grow by {gdp_growth2}% by {yend2}. ".format(
                            gdp_growth1=gdp_growth1,
                            gdp_growth2=gdp_growth2,
                            ystart1=self.gdp_growth_years[0],
                            yend1=self.year,
                            yend2=self.gdp_growth_years[1],
                            country=self.text_country_name
                        )
                    
                    if indicators1['Nominal GDP per capita (USD)'][0]!='v??':
                        final_string == "Its GDP per capita is ${gdp_per_cap:,}. ".format(
                            gdp_per_cap=indicators1['Nominal GDP per capita (USD)'][0]
                            )

                # EDUCATION PARAGRAPH
                if indicators1['Number of students in tertiary education'][0]!='v??':
                    tertiary_ed = self.format_big_numbers(indicators1['Number of students in tertiary education'][0])

                    tmp_string = "\n\nAs of {year} there were {tertiary_ed} students in tertiary education in {country}".format(
                        year=indicators1['Number of students in tertiary education'][1],
                        tertiary_ed=tertiary_ed,
                        country=self.text_country_name
                    )

                    if indicators1['Number of students in tertiary education'][1] == self.year:
                        tmp_string = tmp_string.replace('had','has')
                        tmp_string = tmp_string.replace('was','is')
                        tmp_string = tmp_string.replace('were','are')
                    else:
                        tmp_string = tmp_string.replace('has','had')
                        tmp_string = tmp_string.replace('is','was')
                        tmp_string = tmp_string.replace('are','were')
                    
                    final_string += tmp_string

                    
                    if (indicators1['Number of students in secondary education'][0]!='v??') and (indicators1['Number of students in primary education'][0]!='v??'):
                        secondary_ed = self.format_big_numbers(indicators1['Number of students in secondary education'][0])
                        primary_ed = self.format_big_numbers(indicators1['Number of students in primary education'][0])
                        
                        final_string += ", with {secondary_ed} students in secondary and {primary_ed} in primary education.".format(
                            secondary_ed=secondary_ed,
                            primary_ed=primary_ed
                        )
                    else:
                        final_string += '.'
                
                    if indicators2['Public sector finances: Proportion of government education expenditure in GDP'][0]!='v??':
                        tmp_string = " {country} spent {ed_spending}% of its GDP on education in {year}.".format(
                            country=self.text_country_name,
                            ed_spending=indicators2['Public sector finances: Proportion of government education expenditure in GDP'][0],
                            year=indicators2['Public sector finances: Proportion of government education expenditure in GDP'][1]
                        )

                        if indicators1['Number of students in tertiary education'][1] == self.year:
                            tmp_string = tmp_string.replace('had','has')
                            tmp_string = tmp_string.replace('was','is')
                            tmp_string = tmp_string.replace('were','are')
                        else:
                            tmp_string = tmp_string.replace('has','had')
                            tmp_string = tmp_string.replace('is','was')
                            tmp_string = tmp_string.replace('are','were')
                    
                        final_string += tmp_string

                # WORLD BANK REPORTS PARAGRAPH
                final_string += wb_string

                # HEALTHCARE PARAGRAPH
                if indicators1['Public healthcare expenditure (USD)'][0]!='v??':
                    public_hc = self.format_big_numbers(indicators1['Public healthcare expenditure (USD)'][0]*1000000)
                    final_string += "\n\nPublic healthcare expenditure totalled ${public_hc} ".format(
                        public_hc=public_hc,
                    )
                    if indicators1['Private healthcare expenditure (USD)'][0]!='v??':
                        private_hc = self.format_big_numbers(indicators1['Private healthcare expenditure (USD)'][0]*1000000)
                        final_string += "and private healthcare expenditure equalled ${private_hc} in {year}.".format(
                            private_hc=private_hc,
                            year=indicators1['Private healthcare expenditure (USD)'][1]
                        )
                    else:
                        final_string += "."
                
                if cities!='v??':
                    final_string += "\n\nThe largest urban areas in {country} are {cities}. \n\n".format(
                        country=self.text_country_name,
                        cities=cities,
                    )
                

                ############################################################
                ############################################################
                ############################################################
                
                #iframe_ = """<iframe width =\"900\" height = \"450\" src=\"https://app.powerbi.com/view?r=eyJrIjoiN2RmODEzMGMtYmQ1Yy00ZDhlLWFjMmMtMGNkNjhjZTk4MWU4IiwidCI6ImZhMDU0NTk3LTQ1NDUtNGE0ZS1iZGY2LTFkZmFmMjZjYzBjOCJ9\" frameborder=\"0\" allowFullScreen=\"true\"></iframe>"""

                #iframe2_ = """<iframe width=\"1140\" height=\"541.25\" src=\"https://app.powerbi.com/reportEmbed?reportId=933f6ddb-164d-44a5-af59-55fbc6ef8d5a&appId=547fcfcd-2a7e-4ff2-b58d-2aaa18ad76a8&autoAuth=true&ctid=fa054597-4545-4a4e-bdf6-1dfaf26cc0c8&config=eyJjbHVzdGVyVXJsIjoiaHR0cHM6Ly93YWJpLXVrLXNvdXRoLWItcHJpbWFyeS1yZWRpcmVjdC5hbmFseXNpcy53aW5kb3dzLm5ldC8ifQ%3D%3D&Filter={country}\" frameborder=\"0\" allowFullScreen=\"true'"></iframe>""".format(
                #    country = row.Country
                #)

                #html_string = html_string.replace('unique_country_id',unique_country_id)

                #final_string += iframe2_

                #final_string += iframe_

                #final_string += """\n\n"""

                #final_string += html_string

                ############################################################
                ############################################################
                ############################################################

                final_string = self.formatting1(final_string)

                description_text = "All of the key data and information needed when considering investing in {country}.".format(
                    country = self.text_country_name
                )

                self.results[row.Country] = {

                'meta':{'publish':'yes','created_at': str(datetime.now())},

                'post':{
                '_display_author_name': '1',
                'description': description_text,
                'author': '15286',
                'title': 'Investing in {country}: what you need to know'.format(country=self.text_country_name),
                'status': 'draft',
                'content': final_string,
                'template': 'single-auto.php',
                #'tags': '74',
                #'categories': '21',
                
                }
                }

                try:
                    if row.Country == 'United Kingdom':
                        region = str(self.regions_dict['UK'])
                    elif row.Country == 'United States':
                        region = str(self.regions_dict['US'])
                    else:
                        region = str(self.regions_dict[row.Country])
                    self.results[row.Country]['post']['region'] = region
                except Exception as e:
                    logpr('Region not in WP: {}'.format(e))                
                
            else:
                logpr('{}: {}'.format(row.Country, 'Not enough information to generate template'))
                self.results[row.Country] = {

                    'meta':{'publish':'no','created_at': str(datetime.now())},
                    'post':{
                    'content': 'Not enough information to generate template.',
                    'info_part': info_part}
                    }

    def text_publish(self):

        for key in self.results.keys():
            if self.results[key]['meta']['publish'] == 'yes':
                self.b = post_request(params = {
                    #'auth': ('denis', 'test123123'),
                    #'auth':('emilfilipov', 'P3g8gKrfI6R78ybPwFZ5bw7C'),
                    'auth':self.auth_,
                    'url':self.branch + '/wp-json/wp/v2/posts',
                    'data':self.results[key]['post'],
                    #headers=headers
                    }
                    )

