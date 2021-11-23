from covid19_data_update.utils import get_apple_last_date_json

COVID19_DATASET = 'covid19data'
APPLE_LAST_DATE = get_apple_last_date_json()
# dict with table names for keys and link for download for values
DATA_SOURCE = {
    'jhu_daily_covid_deaths_timeseries_global': 'https://github.com/CSSEGISandData/COVID-19/raw/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv',
    'jhu_daily_covid_deaths_timeseries_us': 'https://github.com/CSSEGISandData/COVID-19/blob/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv?raw=true',
    'jhu_daily_covid_cases_timeseries_global': 'https://github.com/CSSEGISandData/COVID-19/raw/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv',
    'jhu_daily_covid_cases_timeseries_us': 'https://github.com/CSSEGISandData/COVID-19/blob/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv?raw=true',
    'our_world_in_data_testing_data_global': 'https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/testing/covid-testing-all-observations.csv',
    'google_covid19_mobility_data': 'https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv',
    'oxford_covid19_government_response_tracker_oxcgrt': 'https://raw.githubusercontent.com/OxCGRT/covid-policy-tracker/master/data/OxCGRT_latest.csv',
    'oxford_covid19_government_response_tracker_oxcgrt_with_notes':'https://github.com/OxCGRT/covid-policy-tracker/raw/master/data/OxCGRT_latest_withnotes.csv',
    'apple_covid19_mobility_data': f'https://covid19-static.cdn-apple.com/covid19-mobility-data/current/v3/en-us/applemobilitytrends-{APPLE_LAST_DATE}.csv',
    'worldwide_governance_indicators': 'https://govdata360-backend.worldbank.org/api/v1/datasets/51/dump.csv',
    'excess_mortality_during_the_covid19_pandemic_monthly': 'https://raw.githubusercontent.com/Financial-Times/coronavirus-excess-mortality-data/main/data/ft_excess_deaths.csv'
}
