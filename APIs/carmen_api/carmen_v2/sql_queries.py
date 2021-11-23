numeric_country_list = '''
    select Country,CountryID,Indicator,Year,Value
    from project-pulldata.Country.MacroYearData a
    left join project-pulldata.Country.MacroeconomicsListing b using (CountryID)
    where Indicator = '{indicator}' and Year <= {year}
    order by Year desc, Value desc
    limit {limit}
    '''

country_id = '''
    select CountryID, Country
    from project-pulldata.Country.MacroeconomicsListing
    {country_where_filter}
    '''

country_params = '''
    select Country,AutoName,CountryID,Indicator,code,Year,Value
    from project-pulldata.Country.MacroYearData a
    left join project-pulldata.Country.IndicatorMap b using(Indicator)
    left join project-pulldata.Country.MacroeconomicsListing c using (CountryID)
    left join project-pulldata.Country.CountryNameMapping d on Country = GlobalData
    where Year >= 2000
    --{country_where_filter}
    --and Year <= {year} and
    --(Indicator in {indicators} or code in {indicators})
    '''

#city_params = '''
#    SELECT *
#    FROM project-pulldata.City.CityYearData AS yeardata 
#    left join project-pulldata.City.CityEconomicsListing as listing using (CityID)
#    {country_where_filter} and
#    yeardata.Indicator in {indicators} and
#    yeardata.Year <= {year}
#    order by Year DESC,Value DESC
#    '''

#country_rank = '''
#    select Country,CountryID,code,Indicator,Year,Value
#    from project-pulldata.Country.MacroYearData a
#    left join project-pulldata.Country.IndicatorMap b using(Indicator)
#    left join project-pulldata.Country.MacroeconomicsListing c using (CountryID)
#    where (Indicator in {indicators} or code in {indicators}) and Year <= {year}
#    '''

#world_bank_report = '''
#    select Country, CountryID, Ranking as Indicator, ReportYear as Year, Rank as Value
#    from project-pulldata.Country.BusinessWorldBankReport
#    {country_where_filter} and
#    Ranking in {ranking}
#    '''
