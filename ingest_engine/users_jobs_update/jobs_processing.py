import pandas as pd
from nltk.corpus import stopwords
import os
import re
from Levenshtein import distance as levenshtein_distance
from difflib import SequenceMatcher


# Get stopwords
stop_words = stopwords.words("english")

# Constants
unknowns = ["terte", "contact", "other"]

manual_mappings = {
    "professional and technical staff eg lawyer engineer architect": "professional"
}

manual_additions_to_master_list = [
    "government",
    "professional",
    "journalist",
    "retired",
]
manual_removals_to_master_list = ["senior"]


# Load master jobs lists and other data -------------------------------------------------
files = ["role_words", "job_titles"]
master_jobs_list = []
folder_path = os.path.dirname(__file__) + "/mapping_files/"
for file in files:
    open_file = open(folder_path + file + ".txt", "r")
    job_titles_list = open_file.readlines()
    job_titles_list = [line.rstrip("\n").lower() for line in job_titles_list]
    master_jobs_list = master_jobs_list + job_titles_list

master_jobs_list = [i.replace("\\", "") for i in master_jobs_list]

# Load abbreviations file
abbreviations_df = pd.read_csv(folder_path + "abbreviations_jobtitles.csv")

# Add abbreviations to jobs master list and deduplicate
master_jobs_list = list(
    set(
        master_jobs_list
        + abbreviations_df.abbreviation.tolist()
        + abbreviations_df.title.tolist()
    )
)

# Add additions and remove exclusions
master_jobs_list = [
    i for i in master_jobs_list if i not in manual_removals_to_master_list
]
master_jobs_list = master_jobs_list + manual_additions_to_master_list

# Load mapping table
seniority_table = pd.read_csv(folder_path + "seniority_grouping.csv")
# Load levels table
levels = pd.read_excel(folder_path + 'levels_mapping.xlsx')
levels.rename(columns={'Seniority level': 'seniority_level'},inplace=True)
levels.rename(columns={'Seniority Group': 'seniority_group'},inplace=True)


def clean_string(string=None, stop_words_list=None, remove_stop_words=False):
    """
    Cleans raw string

    Args:
        string: raw string to client
        stop_words_list: stop words list to exclude
        remove_stop_words: are we removing stop words from the string?

     Returns:
        str: cleaned string

    """

    # Clean string
    string = string.lower().replace(".", "").replace("-", " ")
    string = re.sub(r"[^A-Za-z\s]+", "", string).strip()

    # Remove stop words
    if remove_stop_words:
        string = " ".join(
            [word for word in string.split() if word not in stop_words_list]
        )

    return string


def clean_jobs_strings(df=None, column="job_title", remove_stop_words=False):
    """
    Cleans raw job titles string

    Args:
        df: dataframe of job titles
        column: column in df to clean
        remove_stop_words: are we removing stop words from the string?

     Returns:
        pd.DataFrame: dataframe with extra column 'cleaned_text'

    """

    # Clean
    df["cleaned_text"] = [
        clean_string(i, stop_words, remove_stop_words=remove_stop_words)
        for i in df[column].values
    ]

    # remove single symbols job titles
    df["len"] = df["cleaned_text"].apply(lambda x: len(x))
    df = df[df["len"] >= 2]
    df.drop("len", axis=1, inplace=True)

    # Consolidate
    df_cleaned = df[df["cleaned_text"] != ""]

    # Manual substitutions
    df_cleaned.replace({"cleaned_text": manual_mappings}, inplace=True)

    print(f'---- dataframe shape after cleaning and consolidating: {df_cleaned.shape} ----')

    return df_cleaned


def string_found(string1, string2):
    """
    Checks if either string appears in totality in the other

     Returns:
        bool: whether exists

    """

    if re.search(r"\b" + re.escape(string1) + r"\b", string2):
        return True
    if re.search(r"\b" + re.escape(string2) + r"\b", string1):
        return True
    return False


def string_similarity(master_value=None, job_string=None):
    """
     Gets similarity score between two strings

     Args:
         master_value: master job title string
         job_string: raw job string

      Returns:
         float: similarity measure

     """

    # Check if seniority is in master value but not in job string
    seniority_list = ["head", "chief", "president", "vp"]
    is_job_senior = any(
        [
            string_found(i, master_value) and not string_found(i, job_string)
            for i in seniority_list
        ]
    )
    if is_job_senior:
        return 0

    return SequenceMatcher(None, master_value, job_string).ratio()


def map_and_score_job_string(job_string=None, master_list=None):
    """
     Gets the highest scoring job match

     Args:
         master_list: master list of job strings
         job_string: raw job string

      Returns:
         tuple: matching job title and match score

     """

    # Exact match
    exact_matches = [i for i in master_list if i == job_string]

    # Exact matches get a score of 1
    if exact_matches:
        return exact_matches[0], 1

    # -------

    # Get string similarity
    str_similarity = [(i, string_similarity(i, job_string)) for i in master_list]

    # Select max similarity
    max_s_dist = max(str_similarity, key=lambda t: t[1])

    # If similarity above threshold, stop and assign and score
    if max_s_dist[1] > 0.7:
        return max_s_dist[0], 0.9

    # -------

    # String in job title
    str_in_title = [i for i in master_list if string_found(i, job_string)]

    # If there's a single value in str_in_title score that job 0.8 and return
    if len(str_in_title) == 1:
        return str_in_title[0], 0.8

    # -------

    # levenshtein_distance between title and string
    l_dist = [(i, levenshtein_distance(i, job_string)) for i in master_list]

    # Select min distance
    min_l_dist = min(l_dist, key=lambda t: t[1])

    # -------

    # If there's a title with LD < 3 and the title is in str_in_title or title_in_str score = 0.8
    if (min_l_dist[1] < 3) & (min_l_dist[0] in str_in_title):
        return min_l_dist[0], 0.7

    # -------

    # If there's a title with LD < 3 and the title is not in str_in_title or title_in_str score = 0.6
    if (min_l_dist[1] < 3) & (min_l_dist[0] not in str_in_title):
        return min_l_dist[0], 0.6

    # -------
    # Else we create a list of candidate jobs with scores and choose max
    # Candidate jobs list
    candidate_jobs = []

    # If len(str_in_title + title_in_str) > 1 then each title gets 1 / len(str_in_title + title_in_str)
    if (len(str_in_title) > 1) & (min_l_dist[0] not in str_in_title):
        for c in str_in_title:
            candidate_jobs.append((c, 1 / len(str_in_title)))

    # -------

    # Else score = 1 / LD
    if min_l_dist[1] >= 3:
        candidate_jobs.append((min_l_dist[0], 1 / min_l_dist[1]))

    return max(candidate_jobs, key=lambda t: t[1])


def remove_abbreviations(df=None, abbreviations_tbl=None, match_col=None):
    """
     Replaces abbreviations with actual words

     Args:
         df: input dataframe
         abbreviations_tbl: abbreviation - words mapping
         match_col: column in df to replace

      Returns:
         pd.DataFrame: dataframe with column updated

     """
    df = pd.merge(
        df, abbreviations_tbl, right_on="abbreviation", left_on=match_col, how="left"
    )

    df[match_col] = df.apply(
        lambda x: x["title"] if not pd.isnull(x["title"]) else x[match_col], axis=1
    )

    return df


def map_master_jobs_list(df=None):
    """
     Map raw job title to master list

     Args:
         df: input dataframe

      Returns:
         pd.DataFrame: dataframe with matches joined

     """

    # Map
    dict_of_matches = [
        {
            "cleaned_text": i,
            "assigned_job": map_and_score_job_string(i, master_list=master_jobs_list),
        }
        for i in df["cleaned_text"].values
    ]

    df_assigned = pd.DataFrame(dict_of_matches)
    df_assigned[["assigned_job", "score"]] = pd.DataFrame(
        df_assigned["assigned_job"].tolist(), index=df_assigned.index
    )

    # Remove abbreviations
    df_assigned = remove_abbreviations(
        df_assigned, abbreviations_df, match_col="assigned_job"
    )
    df_assigned = (
        df_assigned.groupby(["cleaned_text", "assigned_job"])["score"]
        .max()
        .reset_index()
    )

    # Add manual assignments
    df_assigned.loc[
        df_assigned["cleaned_text"].isin(unknowns), "assigned_job"
    ] = "other"
    df_assigned.loc[df_assigned["cleaned_text"].isin(unknowns), "score"] = 1.0

    return df_assigned


def adding_seniority_grouping(df):
    """
     Add seniority grouping to the table

     Args:
         df: input dataframe

      Returns:
         pd.DataFrame: dataframe with seniority grouping joined

     """

    # Clean
    seniority_table.columns = [
        i.lower().replace(" ", "_") for i in seniority_table.columns
    ]
    seniority_table["key_words"] = [
        clean_string(i) for i in seniority_table["key_words"].values
    ]

    # Map a keyword to each job in master list
    keyword_master_mapping = [
        {
            "master_job_title": i,
            "keyword_match": map_and_score_job_string(
                i, master_list=seniority_table["key_words"].tolist()
            ),
        }
        for i in master_jobs_list
    ]

    keyword_master_mapping = pd.DataFrame(keyword_master_mapping)
    keyword_master_mapping[["keyword_match", "keyword_score"]] = pd.DataFrame(
        keyword_master_mapping["keyword_match"].tolist(),
        index=keyword_master_mapping.index,
    )

    # Sort
    keyword_master_mapping.sort_values("keyword_score", ascending=False, inplace=True)

    # Remove abbreviations
    keyword_master_mapping = remove_abbreviations(
        keyword_master_mapping, abbreviations_df, match_col="keyword_match"
    )
    keyword_master_mapping = (
        keyword_master_mapping.groupby(["master_job_title", "keyword_match"])[
            "keyword_score"
        ]
        .max()
        .reset_index()
    )

    # Filter
    keyword_master_mapping = keyword_master_mapping[
        keyword_master_mapping["keyword_score"] > 0.25
    ]

    # Join to mapped raw jobs
    df = pd.merge(
        df,
        keyword_master_mapping,
        left_on="assigned_job",
        right_on="master_job_title",
        how="left",
    )
    df = df[["cleaned_text", "assigned_job", "score", "keyword_match", "keyword_score"]]

    # Finally join seniority table
    df = pd.merge(
        df, seniority_table, left_on="keyword_match", right_on="key_words", how="left"
    )
    df.drop("keyword_match", axis=1, inplace=True)

    # Filter
    df = df[(df["score"] > 0.25)]
    # Join to mapped raw jobs
    df = pd.merge(df, keyword_master_mapping, left_on='assigned_job', right_on='master_job_title', how='left')
    df = df[['cleaned_text', 'assigned_job', 'score', 'keyword_match', 'score']]
    # Finally join seniority table
    df = pd.merge(df, seniority_table, left_on='keyword_match', right_on='key_words', how='left')
    df.drop('keyword_match', axis=1, inplace=True)
    return df


def run_jobs_processing(df_users: pd.DataFrame) -> pd.DataFrame:
    """

    :param df_users: DataFrame with the new jobs data
    :return: Mapped DataFrame with JobTitle and Seniority
    """

    # Clean jobs strings
    df_cleaned = clean_jobs_strings(df_users)

    # Map cleaned jobs string to master jobs list
    df_assigned = map_master_jobs_list(df_cleaned)

    # Merge
    df_cleaned = pd.merge(df_cleaned, df_assigned, right_on='cleaned_text', left_on='cleaned_text', how='left')

    # Adding seniority grouping
    df_cleaned = adding_seniority_grouping(df_cleaned)
    
    # Join original job string
    df = pd.merge(df_users, df_cleaned, left_on='cleaned_text', right_on='cleaned_text', how='left')
    final_df = df[['job_title', 'assigned_job', 'seniority_group']]
    final_df['seniority_group'] = ['Member of Staff/ Other'
                                   if pd.isnull(i) else i for i in final_df['seniority_group'].values]
    final_df_levels = pd.merge(final_df, levels[['seniority_group', 'seniority_level']],on='seniority_group')
    final_df_levels.drop_duplicates(inplace=True)
    final_df_levels.columns = ['job_title_original', 'job_title_4k', 'job_title_11', 'job_title_seniority']
    
    users_job_titles = pd.merge(df_users, final_df_levels, left_on='job_title', right_on='job_title_original')
    users_job_titles.drop(['job_title', 'cleaned_text', 'len'], inplace=True, axis=1)
    
    return users_job_titles
