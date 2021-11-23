country_id = '''
    select CountryID, Country, Region
    from project-pulldata.Country.MacroeconomicsListing
    {country_where_filter}
    '''

country_params = '''
    select CountryID,Indicator,Year,Value
    from project-pulldata.Country.MacroYearData
    {countryid_filter}
    and 
    Indicator in ('Population','Female population','Male population','Proportion of male population','Proportion of female population',
    'Total employment','Total unemployment','Employment rate','Unemployment rate','Nominal GDP per capita (USD)',
    'Number of students in tertiary education','Number of students in primary education','Number of students in secondary education',
    'Public sector finances: Proportion of government education expenditure in GDP','Private healthcare expenditure (USD)',
    'Public healthcare expenditure (USD)','Nominal GDP (USD)')
    '''

country_text_names = '''
    SELECT * FROM project-pulldata.Country.CountryNameMapping 
    {country_where_filter}
    '''

cities = '''
    SELECT *
    FROM project-pulldata.City.CityYearData AS yeardata 
    left join project-pulldata.City.CityEconomicsListing as listing using (CityID)
    {country_where_filter} and
    yeardata.Indicator = 'Total population' and
    yeardata.Year = {year}
    order by Year DESC,Value DESC
    '''

world_bank_report = '''
    select Country, CountryID, Ranking, Rank, ReportYear
    from project-pulldata.Country.BusinessWorldBankReport
    {country_where_filter}
    '''

population_rank = '''
    select CountryID,Indicator,Year,Value
    from project-pulldata.Country.MacroYearData
    where Indicator in ('Population') and Year = {year}
    '''

gdp_rank = '''
    select CountryID,Indicator,Year,Value
    from project-pulldata.Country.MacroYearData
    where Indicator in ('GDP by income (USD): Gross national income') and Year <= {year}
    '''

# 'GDP by income (USD): Gross national income'

