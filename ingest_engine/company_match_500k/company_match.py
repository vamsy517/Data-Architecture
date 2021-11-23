from company_match_500k.utils import download_file
from company_match_500k.utils import clean_name
from company_match_500k.utils import get_closest_names
from google.cloud import storage
import pandas as pd
import re


class CompanyMatch:
    def __init__(self):
        self.clearbit_all, self.fixednames = self.load_clearbit_names()

    @staticmethod
    def load_clearbit_names():
        """
        Download excel files and create lists witn cearbit names
        :return:
        clearblit_all: list with clearbit names
        fixednames: list with the cleaned names
        """
        # client = storage.Client.from_service_account_json('company_match/pulldata-vvalchev.json')
        client = storage.Client(project='project-pulldata')
        clearbit_all = download_file('clearbit_all.p', client)
        fixednames = download_file('fixednames.p', client)
        return clearbit_all, fixednames

    def get_distance(self, df_names, n_closest, threshold):
        """
            Input:
                df_names: dataframe with companies that we will find matches
                n_closest: numer of returned matches
                threshold: prefferred threshold for Levenshtein distance
                clearbit_all: load in memory list with all companies that we use for matching
            :return:
                copy_wishlist_df: dataframe with all matched companie names
        """

        # create column, containing the closest matches as list by aplling on every df row the get_closest_names
        # function
        df_complex_original = df_names[df_names.name.str.contains(r'\([^)]*\)', regex=True, na=False)]
        df_simple = df_names[~df_names.name.str.contains(r'\([^)]*\)', regex=True, na=False)]
        df_complex_original['original_name'] = df_complex_original.name
        df_complex_original_abbreviation = df_complex_original.copy()
        df_complex_original_full_name = df_complex_original.copy()
        df_complex_original_abbreviation.name = df_complex_original_abbreviation.name.apply(
            lambda x: re.search(r'\([^)]*\)', x).group(0)[1:-1])
        df_complex_original_full_name.name = df_complex_original_full_name.name.apply(
            lambda x: re.sub(r'\([^)]*\)', '', x))
        df_complex_original = df_complex_original.append(df_complex_original_abbreviation).append(
            df_complex_original_full_name)
        df_complex_original = df_complex_original.sort_values('original_name')
        df_simple['original_name'] = df_simple.name
        df_all = df_complex_original.append(df_simple)
        df_all = df_all.reset_index(drop=True)
        df_name_fix = df_all['name'].to_frame()
        df_names = df_all['original_name'].to_frame()
        df_names.columns = ['name']

        df_name_fix['name'] = df_name_fix['name'].apply(clean_name)
        df_names['closest_companies'] = df_name_fix.apply(lambda x: get_closest_names(
            x['name'], self.clearbit_all, self.fixednames, n_closest, threshold), axis=1)
        # copy the dataframe
        copy_wishlist_df = df_names.copy()
        # create separate column for every element in the list and remove closest_companies column
        closest_strings = [f'closest_str_{i + 1}' for i in range(len(copy_wishlist_df.iloc[0, -1]))]
        closest_score = [f'closest_score_{i + 1}' for i in range(len(copy_wishlist_df.iloc[0, -1]))]
        copy_wishlist_df['len'] = copy_wishlist_df.apply(lambda x: len(x['closest_companies']), axis=1)
        assert copy_wishlist_df['len'].unique()[0] == n_closest
        copy_wishlist_df[closest_strings] = pd.DataFrame(copy_wishlist_df.closest_companies.apply(lambda x: [i[0] for i in x]).to_list(),
                                                         index=copy_wishlist_df.index)
        copy_wishlist_df[closest_score] = pd.DataFrame(copy_wishlist_df.closest_companies.apply(lambda x: [i[1] for i in x]).to_list(),
                                                         index=copy_wishlist_df.index)
        copy_wishlist_df = copy_wishlist_df.drop(['closest_companies', 'len'], axis=1)
        cols = copy_wishlist_df.columns.tolist()[1:]
        cols.sort(key=lambda x: int(x[-1]))
        copy_wishlist_df = copy_wishlist_df[['name'] + cols]
        return copy_wishlist_df