import pandas as pd
from segmentation.downloader.probability_model_downloader import ProbDownloader
from segmentation.config.load_config import load_config
import numpy as np
from pandas.core.common import flatten

# Config
config = load_config()


def clean_prob_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans probability column

    Returns:
        pd.DataFrame: The return value dataframe

    """

    # Add knowns column for users in clearbit
    df_knowns = df[df["known"] == 1]

    # Create normalised probability index
    df_unknowns = df[~df["user_id"].isin(df_knowns["user_id"].unique())]
    df_unknowns["min_prob"] = df_unknowns.groupby(["domain", "attribute"])[
        "probability"
    ].transform("min")
    df_unknowns["max_prob"] = df_unknowns.groupby(["domain", "attribute"])[
        "probability"
    ].transform("max")
    df_unknowns["prob_index"] = (
        df_unknowns["probability"] - df_unknowns["min_prob"]
    ) / (df_unknowns["max_prob"] - df_unknowns["min_prob"])

    return pd.concat([df_unknowns, df_knowns])


def get_data() -> pd.DataFrame:
    """
    Retrieves user - attribute probability table from BiqQuery

    Returns:
        pd.DataFrame: The return value dataframe

    """

    # Init downloader
    prob_downloader = ProbDownloader(model_type="prob_data", data_version=1)

    # Get data
    df_industry = prob_downloader.get_user_probability_table_from_bq(
        attribute="industry", max_only=True
    )
    df_job_title = prob_downloader.get_user_probability_table_from_bq(
        attribute="job_title", max_only=True
    )
    # let us beautify the industry and job title
    df_industry["attribute"] = df_industry.apply(
        lambda x: x["attribute"].replace("_", " ").title(), axis=1
    )
    df_job_title["attribute"] = df_job_title.apply(
        lambda x: x["attribute"].replace("_", " ").title(), axis=1
    )

    df_company = prob_downloader.get_user_probability_table_from_bq(attribute="company")
    print(df_company.columns)
    # Add unknown to estimatedAnnualRevenue, employeesRange, type, city and state
    df_company["estimatedAnnualRevenue"] = [
        i if i is not None and i != np.nan else "Unknown"
        for i in df_company["estimatedAnnualRevenue"]
    ]
    df_company["employeesRange"] = [
        i if i is not None and i != np.nan else "Unknown"
        for i in df_company["employeesRange"]
    ]
    df_company["type"] = [i if i is not None else "Unknown" for i in df_company["type"]]
    df_company["company_city"] = [
        i if i is not None and i != np.nan else "Unknown"
        for i in df_company["company_city"]
    ]
    df_company["company_state"] = [
        i if i is not None and i != np.nan else "Unknown"
        for i in df_company["company_state"]
    ]
    df_company["company_country"] = [
        i if i is not None and i != np.nan else "Unknown"
        for i in df_company["company_country"]
    ]
    df_company["techCategories"] = [
        i if i is not None and i != [] and isinstance(i, np.ndarray) else ["Unknown"]
        for i in df_company["techCategories"]
    ]
    df_company["tech"] = [
        i if i is not None and i != [] and isinstance(i, np.ndarray) else ["Unknown"]
        for i in df_company["tech"]
    ]
    df_company["tags"] = [
        i if i is not None and i != [] and isinstance(i, np.ndarray) else ["Unknown"]
        for i in df_company["tags"]
    ]
    # Add unknown to countries
    df_industry["country"] = [
        i if i is not None and i != np.nan else "Unknown"
        for i in df_industry["country"]
    ]
    df_industry["continent"] = [
        i if i is not None and i != np.nan else "Unknown"
        for i in df_industry["continent"]
    ]
    df_industry["province"] = [
        i if i is not None and i != np.nan else "Unknown"
        for i in df_industry["province"]
    ]
    df_industry["city"] = [
        i if i is not None and i != np.nan else "Unknown" for i in df_industry["city"]
    ]
    print(df_industry.columns)
    # Clean probability column
    df_industry = clean_prob_data(df_industry)

    # Clean job title data
    df_job_title["attribute"] = [
        i if i is not None else "Unknown" for i in df_job_title["attribute"]
    ]
    df_job_title = clean_prob_data(df_job_title)
    df_job_title = df_job_title[["user_id", "attribute", "prob_index", "known"]]
    df_job_title.columns = [
        "user_id",
        "job_title",
        "prob_index_job_title",
        "known_job_title",
    ]

    # Merge
    df = pd.merge(df_industry, df_job_title, on="user_id", how="left")
    df = pd.merge(df, df_company, on="user_id", how="left")
    return df


def get_dropdowns(df: pd.DataFrame) -> dict:

    # Industry names
    industry_names = df["attribute"].unique().tolist()
    industry_dropdown = [{"label": "All", "value": "all"}]
    industry_dropdown = industry_dropdown + [
        {"label": i, "value": i} for i in industry_names
    ]

    # Seniority list
    seniority_names = [
        i for i in df["job_title"].unique().tolist() if isinstance(i, str)
    ]
    seniority_dropdown = [{"label": "All", "value": "all"}]
    seniority_dropdown = seniority_dropdown + [
        {"label": i, "value": i} for i in seniority_names
    ]

    # Continent names
    continent_names = [i for i in df["continent"].unique() if i]
    continent_names.sort()
    all_continents = [{"label": "All", "value": "all"}]
    continent_dropdown = all_continents + [
        {"label": i, "value": i} for i in continent_names
    ]

    # Dictionary giving countries in a given continent
    continent_countries_options = {
        i: df[df["continent"] == i]["country"].unique().tolist()
        for i in df["continent"].unique()
        if i is not np.nan
    }
    continent_countries_options["all"] = [
        i for i in df["country"].unique().tolist() if i is not np.nan and i != ""
    ]

    # Dictionary giving province in a given countries
    countries_province_options = {
        i: [
            x
            for x in df[df["country"] == i]["province"].unique().tolist()
            if x is not np.nan
        ]
        for i in df["country"].unique()
        if i is not np.nan
    }
    countries_province_options["all"] = [
        i for i in df["province"].unique().tolist() if i is not np.nan and i != ""
    ]

    # Dictionary giving city in a given province
    province_cities_options = {
        i: [
            x
            for x in df[df["province"] == i]["city"].unique().tolist()
            if x is not np.nan
        ]
        for i in df["province"].unique()
        if i is not np.nan
    }
    province_cities_options["all"] = [
        i for i in df["city"].unique().tolist() if i is not np.nan and i != ""
    ]

    # Company countries names
    company_countries_names = [
        i for i in df["company_country"].unique() if i is not np.nan
    ]
    company_countries_names.sort()
    all_company_countries = [{"label": "All", "value": "all"}]
    company_countries_dropdown = all_company_countries + [
        {"label": i, "value": i} for i in company_countries_names
    ]

    # Dictionary giving company state in a given countries
    company_countries_state_options = {
        i: [
            x
            for x in df[df["company_country"] == i]["company_state"].unique().tolist()
            if x is not np.nan
        ]
        for i in df["company_country"].unique()
        if i is not np.nan
    }
    company_countries_state_options["all"] = [
        i for i in df["company_state"].unique().tolist() if i is not np.nan and i != ""
    ]

    # Dictionary giving company city in a given province
    company_state_cities_options = {
        i: [
            x
            for x in df[df["company_state"] == i]["company_city"].unique().tolist()
            if x is not np.nan
        ]
        for i in df["company_state"].unique()
        if i is not np.nan
    }
    company_state_cities_options["all"] = [
        i for i in df["company_city"].unique().tolist() if i is not np.nan and i != ""
    ]

    # Companytype names
    companytype_names = [i for i in df["type"].unique() if i is not np.nan]
    companytype_names.sort()
    all_companytypes = [{"label": "All", "value": "all"}]
    companytype_dropdown = all_companytypes + [
        {"label": i, "value": i} for i in companytype_names
    ]
    # Dictionary giving company in a given companytype
    companytype_company_options = {
        i: df[df["type"] == i]["company_name"].unique().tolist()
        for i in df["type"].unique()
        if i is not np.nan
    }
    companytype_company_options["all"] = [
        i for i in df["company_name"].unique().tolist() if i is not np.nan and i != ""
    ]
    # EmployeesRange
    employeesrange_names = [
        i for i in df["employeesRange"].unique().tolist() if isinstance(i, str)
    ]
    employeesrange_names.sort()
    employeesrange_dropdown = [{"label": "All", "value": "all"}]
    employeesrange_dropdown = employeesrange_dropdown + [
        {"label": i, "value": i} for i in employeesrange_names
    ]

    # EstimatedAnnualEevenue
    estimatedannualrevenue_names = [
        i for i in df["estimatedAnnualRevenue"].unique().tolist() if isinstance(i, str)
    ]
    estimatedannualrevenue_names.sort()
    estimatedannualrevenue_dropdown = [{"label": "All", "value": "all"}]
    estimatedannualrevenue_dropdown = estimatedannualrevenue_dropdown + [
        {"label": i, "value": i} for i in estimatedannualrevenue_names
    ]

    # Tech Categories
    tech_categories_names = [
        i for i in list(set(flatten(df.techCategories.values))) if isinstance(i, str)
    ]
    tech_categories_names.sort()
    tech_categories_dropdown = [{"label": "All", "value": "all"}]
    tech_categories_dropdown = tech_categories_dropdown + [
        {"label": i, "value": i} for i in tech_categories_names
    ]

    # Tech
    tech_names = [i for i in list(set(flatten(df.tech.values))) if isinstance(i, str)]
    tech_names.sort()
    tech_dropdown = [{"label": "All", "value": "all"}]
    tech_dropdown = tech_dropdown + [{"label": i, "value": i} for i in tech_names]

    # Tags
    tags_names = [i for i in list(set(flatten(df.tags.values))) if isinstance(i, str)]
    tags_names.sort()
    tags_dropdown = [{"label": "All", "value": "all"}]
    tags_dropdown = tags_dropdown + [{"label": i, "value": i} for i in tags_names]

    # Domains
    domain_names = df["domain"].unique().tolist()
    domains_dropdown = [{"label": "All", "value": "all"}]
    domains_dropdown = domains_dropdown + [
        {"label": i, "value": i} for i in domain_names
    ]

    return {
        "industry": industry_dropdown,
        "seniority": seniority_dropdown,
        "continent": continent_dropdown,
        "continent_countries": continent_countries_options,
        "countries_province": countries_province_options,
        "province_cities": province_cities_options,
        "company_country": company_countries_dropdown,
        "company_countries_state": company_countries_state_options,
        "company_state_cities": company_state_cities_options,
        "companytype": companytype_dropdown,
        "companytype_company": companytype_company_options,
        "domains": domains_dropdown,
        "employeesrange": employeesrange_dropdown,
        "estimatedannualrevenue": estimatedannualrevenue_dropdown,
        "tech_categories": tech_categories_dropdown,
        "tech": tech_dropdown,
        "tags": tags_dropdown,
    }


# def create_company_selection_dict(df: pd.DataFrame) -> dict:
#     # create dataframe for Company Selection
#     # create dataframe with specific company columns
#     df_company_selection = df[['company_name', 'attribute', 'annualRevenue', 'employees', 'company_city',
#                                'company_state', 'company_country', 'techCategories', 'tech',
#                                'emailAddresses']].drop_duplicates().dropna(subset=['company_name'])
#     print('Saving company selection')
#     df_company_selection.to_csv(r"C:\Users\User\PycharmProjects\segmentation\company_selection.csv")
#     # create number of users per company
#     df_company_users = df[['company_name', 'user_id']].drop_duplicates().dropna(subset=['company_name']).groupby(
#         ['company_name']).count().reset_index()
#     print('Saving company users')
#     df_company_users.to_csv(r"C:\Users\User\PycharmProjects\segmentation\company_users.csv")
#     # create new dataframe with user counts and specific company columns
#     df_selected_companies = pd.merge(df_company_selection, df_company_users, on="company_name")
#     print('Saving selected companies')
#     df_selected_companies.to_csv(r"C:\Users\User\PycharmProjects\segmentation\selected_companies.csv")
#     return df_selected_companies.to_dict()
