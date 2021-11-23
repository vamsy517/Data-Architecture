import pandas as pd
import numpy as np
import segmentation_report_engine.utils.constants as constants


def calculate_cosine_similarity(vector_1: list, vector_2: list) -> float:
    """
    Calculate cosine similarity between two vectors
    :param vector_1: first list of values
    :param vector_2: second list of values
    :return: cosine similarity between the two lists
    """
    similarity = np.dot(vector_1, vector_2)/(np.linalg.norm(vector_1)*np.linalg.norm(vector_2))
    return similarity


def localize_hour(utc_hour: int, continent: str) -> int:
    """
    Calculate time in user local time
    :param utc_hour: hour in utc
    :param continent: continent as string
    :return: hour in user local time
    """
    if continent == "North America":
        local_hour = utc_hour + constants.NA_HOUR_DIFF
    elif continent == "South America":
        local_hour = utc_hour + constants.SA_HOUR_DIFF
    elif continent == "Europe":
        local_hour = utc_hour + constants.EU_HOUR_DIFF
    elif continent == "Africa":
        local_hour = utc_hour + constants.AF_HOUR_DFF
    elif continent == "Asia":
        local_hour = utc_hour + constants.AS_HOUR_DIFF
    else:
        local_hour = utc_hour
    # make sure there's no negatives in the localized hour
    if local_hour < 0:
        local_hour = local_hour + 24
    # lastly, make sure 24 is 0
    if local_hour == 24:
        local_hour = 0
    return local_hour


def extract_referrer_group(referrer: str, domain: str) -> str:
    """
    Extract referrer group based on predefined lists in constants.py
    :param referrer: referrer field from pageview_events table, which will be classified in a group
    :param domain: specific domain
    :return:
    """
    if any(wildcard in referrer for wildcard in constants.SOCIAL_LIST):
        referrer_group = 'social'
    elif any(wildcard in referrer for wildcard in constants.SEARCH_LIST):
        referrer_group = 'search'
    elif any(wildcard in referrer for wildcard in constants.NSMG_NETWORK):
        referrer_group = 'NSMG'
    elif any(wildcard in referrer for wildcard in [domain]):
        referrer_group = 'own_website'
    elif any(wildcard in referrer for wildcard in constants.EMAIL_LIST):
        referrer_group = 'email'
    else:
        referrer_group = 'other_referrer'
    return referrer_group


def extract_os_group(user_agent: str) -> str:
    """
    Extract operation system
    :param user_agent: user agent field from pageview_events, from which os will be extracted
    :return: extracted from user agent operation system
    """
    if constants.LINUX_AGENT_STR in user_agent:
        os_group = "linux"
    elif constants.ANDROID_AGENT_STR in user_agent:
        os_group = "android"
    elif constants.WIN10_AGENT_STR in user_agent:
        os_group = "windows10"
    elif (
        constants.WIN_AGENT_STR in user_agent
        and constants.WIN10_AGENT_STR not in user_agent
    ):
        os_group = "windows_older"
    elif constants.IPHONE_AGENT_STR in user_agent:
        os_group = "iphone"
    elif constants.MAC_AGENT_STR in user_agent:
        os_group = "macintosh"
    elif constants.APPLE_AGENT_STR in user_agent:
        os_group = "apple_other"
    else:
        os_group = "other_os"
    return os_group


def get_similarity(df_base: pd.DataFrame, df_new: pd.DataFrame, dim_for_measuring: list, group_cols: list,
                   n_clusters: int, model_type: str) -> float:
    """
    Calculate mean of similarity
    :param df_base: dataframe, containing data from Segmentation_{domain}.{table} between 2021-01-24 and 2021-01-31
    :param df_new: dataframe, containing yesterday data from Segmentation_{domain}.{table}
    :param dim_for_measuring: DIM_FOR_MEASURING_BEHAVIOUR or DIM_FOR_MEASURING_INDUSTRY
    :param group_cols: identification columns for the column aggregation
    :param n_clusters: number of clusters
    :param model_type: Industry, Jobs or Seniority
    :return: mean of similarity
    """
    df_base = df_base.groupby(group_cols, as_index=False).mean()
    # create dataframe with unknown industry and 0 for all other columns for that clusters that not appear in database
    df_unknown = pd.DataFrame(columns=df_base.columns)
    df_unknown[model_type] = ['unknown' for x in range(n_clusters)]
    df_unknown['cluster'] = [x for x in range(n_clusters)]
    df_unknown = df_unknown.fillna(value=0)
    # append dataframe to base and new dataframes
    df_base = df_base.append(df_unknown).reset_index(drop=True)
    old_combinations = [(col, cluster) for col, cluster in zip(df_base[model_type], df_base['cluster'])]
    new_combinations = [(col, cluster) for col, cluster in zip(df_new[model_type], df_new['cluster'])]
    in_old_not_in_new = set(old_combinations) - set(new_combinations)
    # for combination in in_old_not_in_new
    # add combination to new dataframe with 0
    for col, cluster in in_old_not_in_new:
        df_to_append = pd.DataFrame(columns=df_new.columns)
        df_to_append[model_type] = [col]
        df_to_append['cluster'] = [cluster]
        df_to_append = df_to_append.fillna(value=0)
        df_new = df_new.append(df_to_append).reset_index(drop=True)
    # for combination in in_new_not_in_old
    # move this combinations to unknown (sum)
    in_new_not_in_old = set(new_combinations) - set(old_combinations)
    for col, cluster in in_new_not_in_old:
        df_new[model_type][(df_new[model_type] == col) & (df_new['cluster'] == cluster)] = 'unknown'
    df_new = df_new.groupby(group_cols, as_index=False).sum()
    # assert shapes are same
    print(f'Asserting {model_type} dfs are in same shape')
    assert df_new.shape == df_base.shape
    df_base['vector_base'] = df_base[dim_for_measuring].values.tolist()
    df_similarity_base = df_base['vector_base']
    df_new['vector_new'] = df_new[dim_for_measuring].values.tolist()
    df_similarity_new = df_new['vector_new']
    df_similarity = pd.concat([df_similarity_base, df_similarity_new], axis=1)
    similarity = df_similarity.apply(lambda x: calculate_cosine_similarity(x['vector_base'],
                                                                                    x['vector_new']), axis=1)
    return similarity.mean()
