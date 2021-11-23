import pandas as pd
import numpy as np
from pytrends.request import TrendReq
import time


def download_trends(keywords_df, timeperiod, file_name):
    pytrends = TrendReq(hl='en-US', tz=360, retries=4,  timeout=None)

    lst_regions = ["GB", "US"]
    for column in keywords_df.columns:
        df_results = pd.DataFrame()
        keywords = keywords_df[column].dropna()
        for region in lst_regions:
            for item in keywords:
                print('Wait for 10 seconds between requests...')
                time.sleep(10)
                print("Downloading: " + column + " : " + region + " : " + item)
                if timeperiod == 'yearly':
                    timeframe = 'today 5-y'
                else:
                    if column == "new_statesman":
                        timeframe = 'now 1-d'
                    else:
                        timeframe = 'now 7-d'
                try:
                    pytrends.build_payload(kw_list=[item], timeframe=timeframe, geo=region)
                    related_queries_dict = pytrends.related_queries()
                except:
                    print(f'Timeout wait 30 sec...')
                    time.sleep(30)
                    try:
                        pytrends.build_payload(kw_list=[item], timeframe=timeframe, geo=region)
                        related_queries_dict = pytrends.related_queries()
                    except:
                        print('Timeout again. Skipping this item')
                        continue
                if related_queries_dict[item]["rising"] is not None:
                    df_rising = related_queries_dict[item]["rising"].head(20)
                    df_rising["keyword"] = item
                    df_rising["region"] = region
                    df_rising["breakout"] = np.where(df_rising['value'] >= 5000, 'breakout', 'rising')
                    df_results = df_results.append(df_rising)
                else:
                    print(f'{item}: No rising data')
        if df_results.empty:
            print(f' Domain {column} for all regions: No rising data')
        else:
            df_results = df_results.sort_values(by=["value"], ascending=False)
            # save result as excel file
            df_results.to_excel(f"/home/ingest/trends/{timeperiod}/{column}{file_name}.xlsx", index=False)
    return True
