# Run this with `python -m segmentation.app` and
# visit http://127.0.0.1:8050/ in your web browser.

from segmentation.app_utils import app_utils as app_utils
import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_table
from dash.dependencies import Input, Output, State
import plotly.express as px
import pandas as pd
import numpy as np
from datetime import datetime
from dash.exceptions import PreventUpdate
import json
from flask_caching import Cache
from segmentation.config.load_config import load_config
from pandas.core.common import flatten


# import os

# Config
config = load_config()

pd.options.display.max_columns = 30

# Get stylesheets
external_stylesheets = ["https://codepen.io/chriddyp/pen/bWLwgP.css"]

# Init app
# TODO workaround for loadbalancer routes
app = dash.Dash(
    __name__,
    external_stylesheets=external_stylesheets,
    requests_pathname_prefix="/scubed/",
)
cache = Cache(
    app.server,
    config={
        # try 'filesystem' if you don't want to setup redis
        # "CACHE_TYPE": "redis",
        # "CACHE_REDIS_URL": os.environ.get("REDIS_URL", ""),
        # "CACHE_REDIS_HOST": "redis",
        "CACHE_TYPE": "filesystem",
        "CACHE_DIR": "cache-directory",
    },
)
app.config.suppress_callback_exceptions = True

timeout = 900

# from https://dash.plotly.com/deployment
server = app.server

# Get data
data_df = app_utils.get_data()

# Generate dropdown menus
drop_downs = app_utils.get_dropdowns(data_df)
print("start Layout")
# App layout
app.layout = html.Div(
    [
        html.H1("Franklin App"),
        html.Div(
            [
                html.Div(
                    [
                        html.Div(
                            [
                                html.H5("Select domain", style={"color": "black"}),
                                dcc.Dropdown(
                                    id="domain-choice",
                                    options=drop_downs["domains"],
                                    value=drop_downs["domains"][0]["value"],
                                    multi=True,
                                ),
                                html.Br(),
                                html.Hr(),
                                html.H4("User Selection", style={"color": "black"}),
                                html.H5(
                                    "Select user location", style={"color": "black"}
                                ),
                                html.H6("Select continent", style={"color": "black"}),
                                dcc.Dropdown(
                                    id="continent-choice",
                                    options=drop_downs["continent"],
                                    value=drop_downs["continent"][0]["value"],
                                    multi=False,
                                ),
                                html.H6("Select country", style={"color": "black"}),
                                dcc.Dropdown(
                                    id="countries-dropdown",
                                    multi=True,
                                ),
                                html.H6("Select province", style={"color": "black"}),
                                dcc.Dropdown(
                                    id="province-dropdown",
                                    multi=True,
                                ),
                                html.H6("Select city", style={"color": "black"}),
                                dcc.Dropdown(
                                    id="city-dropdown",
                                    multi=True,
                                ),
                                html.Br(),
                                html.Hr(),
                                html.H5("Select seniority", style={"color": "black"}),
                                dcc.Dropdown(
                                    id="job-dropdown",
                                    options=drop_downs["seniority"],
                                    value=drop_downs["seniority"][0]["value"],
                                    multi=True,
                                ),
                                html.Br(),
                                html.I(
                                    "'Knowns' are users tagged with the seniority selected"
                                ),
                                html.Br(),
                                dcc.Checklist(
                                    id="job-knowns",
                                    options=[
                                        {
                                            "label": "Include knowns",
                                            "value": "Y",
                                        }
                                    ],
                                    value=["Y"],
                                ),
                                html.Br(),
                                html.Hr(),
                                html.H5("Select industry", style={"color": "black"}),
                                dcc.Dropdown(
                                    id="industry-dropdown",
                                    options=drop_downs["industry"],
                                    value=drop_downs["industry"][0]["value"],
                                    multi=True,
                                ),
                                html.Br(),
                                html.H5("Certainty slider"),
                                "Choose audience size based on likelihood of user pool being of given industry",
                                html.Br(),
                                html.Br(),
                                html.Div(
                                    [
                                        html.Div(
                                            [html.P("Most likely")],
                                            style={
                                                "width": "50%",
                                                "float": "left",
                                                "display": "inline-block",
                                            },
                                        ),
                                        html.Div(
                                            [html.P("All")],
                                            style={
                                                "width": "50%",
                                                "float": "right",
                                                "text-align": "right",
                                                "display": "inline-block",
                                            },
                                        ),
                                        html.Br(),
                                        dcc.Slider(
                                            id="prob-slider",
                                            min=0,
                                            max=1,
                                            value=1,
                                            step=None,
                                            marks={
                                                round(i, 1): " "
                                                for i in np.arange(0, 1.2, 0.1)
                                            },
                                            # step=0.1,
                                            updatemode="drag",
                                        ),
                                        html.Br(),
                                        html.I(
                                            "'Knowns' are users tagged with the industry selected"
                                        ),
                                        html.Br(),
                                        dcc.Checklist(
                                            id="knowns",
                                            options=[
                                                {
                                                    "label": "Include knowns",
                                                    "value": "Y",
                                                }
                                            ],
                                            value=["Y"],
                                        ),
                                        html.Br(),
                                        html.Hr(),
                                        html.H4(
                                            "Company Selection",
                                            style={"color": "black"},
                                        ),
                                        html.H5(
                                            "Select company location",
                                            style={"color": "black"},
                                        ),
                                        html.H6(
                                            "Select country", style={"color": "black"}
                                        ),
                                        dcc.Dropdown(
                                            id="company-countries-dropdown",
                                            options=drop_downs["company_country"],
                                            value=drop_downs["company_country"][0][
                                                "value"
                                            ],
                                            multi=True,
                                        ),
                                        html.H6(
                                            "Select state", style={"color": "black"}
                                        ),
                                        dcc.Dropdown(
                                            id="company-state-dropdown",
                                            multi=True,
                                        ),
                                        html.H6(
                                            "Select city", style={"color": "black"}
                                        ),
                                        dcc.Dropdown(
                                            id="company-city-dropdown",
                                            multi=True,
                                        ),
                                        html.Br(),
                                        html.Hr(),
                                        html.H5(
                                            "Select company type",
                                            style={"color": "black"},
                                        ),
                                        dcc.Dropdown(
                                            id="companytype-dropdown",
                                            options=drop_downs["companytype"],
                                            value=drop_downs["companytype"][0]["value"],
                                            multi=False,
                                        ),
                                        html.H5(
                                            "Select company", style={"color": "black"}
                                        ),
                                        dcc.Dropdown(
                                            id="company-dropdown",
                                            multi=True,
                                        ),
                                        html.Br(),
                                        html.Hr(),
                                        html.H5(
                                            "Select employees range",
                                            style={"color": "black"},
                                        ),
                                        dcc.Dropdown(
                                            id="employeesrange_dropdown",
                                            options=drop_downs["employeesrange"],
                                            value=drop_downs["employeesrange"][0][
                                                "value"
                                            ],
                                            multi=True,
                                        ),
                                        html.Br(),
                                        html.Hr(),
                                        html.H5(
                                            "Select estimated annual revenue",
                                            style={"color": "black"},
                                        ),
                                        dcc.Dropdown(
                                            id="estimatedannualrevenue_dropdown",
                                            options=drop_downs[
                                                "estimatedannualrevenue"
                                            ],
                                            value=drop_downs["estimatedannualrevenue"][
                                                0
                                            ]["value"],
                                            multi=True,
                                        ),
                                        html.Br(),
                                        html.Hr(),
                                        html.H5(
                                            "Select tech categories",
                                            style={"color": "black"},
                                        ),
                                        dcc.Dropdown(
                                            id="tech-categories-dropdown",
                                            options=drop_downs["tech_categories"],
                                            value=drop_downs["tech_categories"][0][
                                                "value"
                                            ],
                                            multi=True,
                                        ),
                                        html.Br(),
                                        html.Hr(),
                                        html.H5(
                                            "Select tech",
                                            style={"color": "black"},
                                        ),
                                        dcc.Dropdown(
                                            id="tech-dropdown",
                                            options=drop_downs["tech"],
                                            value=drop_downs["tech"][0]["value"],
                                            multi=True,
                                        ),
                                        html.Br(),
                                        html.Hr(),
                                        html.H5(
                                            "Select tags",
                                            style={"color": "black"},
                                        ),
                                        dcc.Dropdown(
                                            id="tags-dropdown",
                                            options=drop_downs["tags"],
                                            value=drop_downs["tags"][0]["value"],
                                            multi=True,
                                        ),
                                    ]
                                ),
                                html.Hr(),
                                html.H5("Export audience"),
                                html.Button("Export", id="btn_txt"),
                                dcc.Download(id="download-users"),
                            ],
                            style={
                                "width": "75%",
                                "margin": 25,
                                "display": "inline-block",
                            },
                        ),
                    ],
                    style={
                        "width": "25%",
                        "display": "inline-block",
                        "background-color": "#f5f5f5",
                        "border-radius": "5px",
                        "box-shadow": "2px" "2px" "2px" "lightgrey",
                    },
                ),
                html.Div(
                    [
                        html.Div(
                            [
                                html.Div(
                                    [
                                        html.H5("Total Selected Audience"),
                                        dcc.Graph(id="total-plot"),
                                    ],
                                    style={
                                        "width": "50%",
                                        "float": "left",
                                        "display": "inline-block",
                                    },
                                ),
                                html.Div(
                                    [
                                        html.H5("Audience Info"),
                                        html.Div(id="metrics-table"),
                                    ],
                                    style={
                                        "width": "50%",
                                        "float": "right",
                                        "display": "inline-block",
                                    },
                                ),
                            ]
                        ),
                        html.Div(
                            [
                                html.Br(),
                                html.Hr(),
                                html.Br(),
                            ],
                            style={
                                "width": "100%",
                                "float": "left",
                                "display": "inline-block",
                            },
                        ),
                        html.Div(
                            [
                                html.Div(
                                    [
                                        html.H5("Industry Split"),
                                        dcc.Graph(id="industry-plot"),
                                    ],
                                    style={
                                        "width": "50%",
                                        "float": "left",
                                        "display": "inline-block",
                                    },
                                ),
                                html.Div(
                                    [
                                        html.H5("Seniority Split"),
                                        dcc.Graph(id="job-plot"),
                                    ],
                                    style={
                                        "width": "50%",
                                        "float": "right",
                                        "display": "inline-block",
                                    },
                                ),
                            ],
                            style={
                                "width": "100%",
                                "float": "left",
                                "display": "inline-block",
                            },
                        ),
                        html.Div(
                            [
                                html.Br(),
                                html.Hr(),
                                html.Br(),
                            ],
                            style={
                                "width": "100%",
                                "float": "left",
                                "display": "inline-block",
                            },
                        ),
                        html.Div(
                            [
                                html.Div(
                                    [
                                        html.H5("Top 10 Industries"),
                                        html.Div(id="industry-table"),
                                    ],
                                    # set height as well...
                                    style={
                                        "width": "40%",
                                        "float": "left",
                                        "display": "inline-block",
                                    },
                                ),
                                html.Div(
                                    [
                                        html.H5("Top 10 Job Titles"),
                                        html.Div(id="jobs-table"),
                                    ],
                                    style={
                                        "width": "40%",
                                        "float": "right",
                                        "display": "inline-block",
                                    },
                                ),
                            ]
                        ),
                        html.Div(
                            [
                                html.Br(),
                                html.Hr(),
                                html.Br(),
                            ],
                            style={
                                "width": "100%",
                                "float": "left",
                                "display": "inline-block",
                            },
                        ),
                        html.Div(
                            [
                                html.Div(
                                    [
                                        html.H5("Top 10 Companies"),
                                        html.Div(id="companies-table"),
                                    ],
                                    style={
                                        "width": "40%",
                                        "float": "left",
                                        "display": "inline-block",
                                    },
                                ),
                                html.Div(
                                    [
                                        html.H5("Top 10 Countries by User"),
                                        html.Div(id="countries-table"),
                                    ],
                                    style={
                                        "width": "40%",
                                        "float": "right",
                                        "display": "inline-block",
                                    },
                                ),
                            ]
                        ),
                        html.Div(
                            [
                                html.Br(),
                                html.Hr(),
                                html.Br(),
                            ],
                            style={
                                "width": "100%",
                                "float": "left",
                                "display": "inline-block",
                            },
                        ),
                        html.Div(
                            [
                                html.H5("Company Selection"),
                                html.Div(id="selected-companies-table"),
                            ],
                            style={
                                "width": "100%",
                                "float": "left",
                                "display": "inline-block",
                            },
                        ),
                        html.Div(
                            [
                                html.Br(),
                                html.Hr(),
                                html.Br(),
                            ],
                            style={
                                "width": "100%",
                                "float": "left",
                                "display": "inline-block",
                            },
                        ),
                    ],
                    style={
                        "width": "70%",
                        "float": "right",
                        "display": "inline-block",
                    },
                ),
            ]
        ),
        # dcc.Store inside the app that stores the intermediate value
        dcc.Store(id="data_dict"),
    ]
)


print("Finish")
# App callbacks


@app.callback(
    Output("countries-dropdown", "options"), Input("continent-choice", "value")
)
def set_countries_options(selected_continent):
    print("set_countries_options")
    all_countries = [{"label": "All", "value": "all"}]
    return all_countries + [
        {"label": i, "value": i}
        for i in drop_downs["continent_countries"][selected_continent]
    ]


@app.callback(
    Output("countries-dropdown", "value"), Input("countries-dropdown", "options")
)
def set_countries_value(available_options):
    print("set_countries_value")
    return available_options[0]["value"]


@app.callback(
    Output("province-dropdown", "options"), Input("countries-dropdown", "value")
)
def set_province_options(selected_province):
    print("set_province_options")
    all_province = [{"label": "All", "value": "all"}]
    if selected_province == "all":
        selected_province = ["all"]
    return all_province + [
        {"label": i, "value": i}
        for i in list(
            flatten(
                [drop_downs["countries_province"].get(key) for key in selected_province]
            )
        )
    ]


@app.callback(
    Output("province-dropdown", "value"), Input("province-dropdown", "options")
)
def set_province_value(available_options):
    print("set_province_value")
    return available_options[0]["value"]


@app.callback(Output("city-dropdown", "options"), Input("province-dropdown", "value"))
def set_city_options(selected_city):
    print("set_city_options")
    all_city = [{"label": "All", "value": "all"}]
    if selected_city == "all":
        selected_city = ["all"]
    return all_city + [
        {"label": i, "value": i}
        for i in list(
            flatten([drop_downs["province_cities"].get(key) for key in selected_city])
        )
    ]


@app.callback(Output("city-dropdown", "value"), Input("city-dropdown", "options"))
def set_city_value(available_options):
    print("set_city_value")
    return available_options[0]["value"]


@app.callback(
    Output("company-state-dropdown", "options"),
    Input("company-countries-dropdown", "value"),
)
def set_company_state_options(selected_state):
    print("set_company_state_options")
    all_state = [{"label": "All", "value": "all"}]
    if selected_state == "all":
        selected_state = ["all"]
    return all_state + [
        {"label": i, "value": i}
        for i in list(
            flatten(
                [
                    drop_downs["company_countries_state"].get(key)
                    for key in selected_state
                ]
            )
        )
    ]


@app.callback(
    Output("company-state-dropdown", "value"),
    Input("company-state-dropdown", "options"),
)
def set_company_state_value(available_options):
    print("set_company_state_value")
    return available_options[0]["value"]


@app.callback(
    Output("company-city-dropdown", "options"), Input("company-state-dropdown", "value")
)
def set_company_city_options(selected_city):

    print("set_company_city_options")
    all_city = [{"label": "All", "value": "all"}]
    if selected_city == "all":
        selected_city = ["all"]
    return all_city + [
        {"label": i, "value": i}
        for i in list(
            flatten(
                [drop_downs["company_state_cities"].get(key) for key in selected_city]
            )
        )
    ]


@app.callback(
    Output("company-city-dropdown", "value"), Input("company-city-dropdown", "options")
)
def set_company_city_value(available_options):
    print("set_company_city_value")
    return available_options[0]["value"]


@app.callback(
    Output("company-dropdown", "options"), Input("companytype-dropdown", "value")
)
def set_company_options(selected_companytype):
    print("set_company_options")
    all_companies = [{"label": "All", "value": "all"}]
    return all_companies + [
        {"label": i, "value": i}
        for i in drop_downs["companytype_company"][selected_companytype]
    ]


@app.callback(Output("company-dropdown", "value"), Input("company-dropdown", "options"))
def set_company_value(available_options):
    print("set_company_value")
    return available_options[0]["value"]


@app.callback(
    Output("data_dict", "data"),
    Input("domain-choice", "value"),
    Input("countries-dropdown", "value"),
    Input("continent-choice", "value"),
    Input("province-dropdown", "value"),
    Input("city-dropdown", "value"),
    Input("industry-dropdown", "value"),
    Input("job-dropdown", "value"),
    Input("prob-slider", "value"),
    Input("knowns", "value"),
    Input("job-knowns", "value"),
    Input("company-countries-dropdown", "value"),
    Input("company-state-dropdown", "value"),
    Input("company-city-dropdown", "value"),
    Input("companytype-dropdown", "value"),
    Input("company-dropdown", "value"),
    Input("employeesrange_dropdown", "value"),
    Input("estimatedannualrevenue_dropdown", "value"),
    Input("tech-categories-dropdown", "value"),
    Input("tech-dropdown", "value"),
    Input("tags-dropdown", "value"),
)
@cache.memoize(timeout=timeout)  # in seconds
def update_df(
    domain,
    country,
    continent,
    province,
    city,
    industry,
    job,
    prob,
    ind_knowns,
    job_knowns,
    company_country,
    company_state,
    company_city,
    companytype,
    company,
    employeesrange,
    estimatedannualrevenue,
    tech_categories,
    tech,
    tags,
):
    print("update_df")
    # Domain
    if "all" not in domain:
        cond = data_df["domain"].isin(domain)
    else:
        cond = data_df["domain"].isin(data_df["domain"].unique().tolist())

    # Industry
    if "all" not in industry:
        cond = cond & (data_df["attribute"].isin(industry))

    # Job
    if "all" not in job:
        cond = cond & (data_df["job_title"].isin(job))

    # Company
    if "all" not in company:
        cond = cond & (data_df["company_name"].isin(company))
    else:
        if companytype != "all":
            cond = cond & (data_df["type"] == companytype)

    # Employeesrange
    if "all" not in employeesrange:
        cond = cond & (data_df["employeesRange"].isin(employeesrange))

    # Estimatedannualrevenue
    if "all" not in estimatedannualrevenue:
        cond = cond & (data_df["estimatedAnnualRevenue"].isin(estimatedannualrevenue))

    # Tech categories
    if "all" not in tech_categories:
        cond = cond & (
            data_df["techCategories"].apply(
                lambda x: set(tech_categories).issubset(set(x))
                if x is not np.nan
                else False
            )
        )

    # Tech
    if "all" not in tech:
        cond = cond & (
            data_df["tech"].apply(
                lambda x: set(tech).issubset(set(x)) if x is not np.nan else False
            )
        )

    # Tags
    if "all" not in tags:
        cond = cond & (
            data_df["tags"].apply(
                lambda x: set(tags).issubset(set(x)) if x is not np.nan else False
            )
        )

    # Country
    if "all" not in city:
        cond = cond & (data_df["city"].isin(city))
    else:
        if "all" not in province:
            cond = cond & (data_df["province"].isin(province))
        else:
            if "all" not in country:
                cond = cond & (data_df["country"].isin(country))
            else:
                if continent != "all":
                    cond = cond & (data_df["continent"] == continent)

    # Company Country
    if "all" not in company_city:
        cond = cond & (data_df["company_city"].isin(company_city))
    else:
        if "all" not in company_state:
            cond = cond & (data_df["company_state"].isin(company_state))
        else:
            if "all" not in company_country:
                cond = cond & (data_df["company_country"].isin(company_country))

    # Get total lookalikes count
    total_ind_predicted = data_df[cond][data_df[cond]["known"] == 0][
        "user_id"
    ].nunique()
    total_job_predicted = data_df[cond][data_df[cond]["known_job_title"] == 0][
        "user_id"
    ].nunique()

    # Get total knowns count
    total_ind_knowns = data_df[cond][data_df[cond]["known"] != 0]["user_id"].nunique()
    total_job_knowns = data_df[cond][data_df[cond]["known_job_title"] != 0][
        "user_id"
    ].nunique()

    # Filter
    df_cond = data_df[cond]
    # Predicted above industry certainty threshold or knowns
    df_count = df_cond[(df_cond["prob_index"] >= (1 - prob)) | (df_cond["known"] == 1)]
    # Filter knowns
    if not ind_knowns:
        df_count = df_count[df_count["known"] != 1]

    if not job_knowns:
        df_count = df_count[df_count["known_job_title"] != 1]
    # Get totals
    n_total = df_count["user_id"].nunique()
    n_ind_knowns = df_count[df_count["known"] == 1]["user_id"].nunique()
    n_job_knowns = df_count[df_count["known_job_title"] == 1]["user_id"].nunique()
    n_knowns_both = df_count[
        (df_count["known_job_title"] == 1) & (df_count["known"] == 1)
    ]["user_id"].nunique()

    n_ind_pred = df_count[df_count["known"] != 1]["user_id"].nunique()
    n_job_pred = df_count[df_count["known_job_title"] != 1]["user_id"].nunique()
    n_pred_both = df_count[
        (df_count["known_job_title"] != 1) & (df_count["known"] != 1)
    ]["user_id"].nunique()
    # Max prob industry
    df_count.loc[
        df_count.known == 1, "prob_index"
    ] = 10  # arbitrary number > 1 for knowns
    max_prob_industry_df = df_count[["user_id", "attribute", "prob_index"]].dropna()
    max_prob_industry_df = max_prob_industry_df.loc[
        max_prob_industry_df.groupby(["user_id"])["prob_index"].idxmax()
    ]
    # create dataframe for Company Selection
    # create dataframe with specific company columns
    df_company_selection_raw = df_count[
        [
            "company_name",
            "attribute",
            "annualRevenue",
            "employees",
            "company_city",
            "company_state",
            "company_country",
            "techCategories",
            "tech",
            "emailAddresses",
        ]
    ]
    print("df_company_selection_raw")
    print(df_company_selection_raw.shape)
    print(df_company_selection_raw.columns)
    # convert arrays to strings
    df_company_selection_raw["techCategories"] = df_company_selection_raw[
        "techCategories"
    ].apply(lambda x: ", ".join(x) if isinstance(x, np.ndarray) else x)
    df_company_selection_raw["tech"] = df_company_selection_raw["tech"].apply(
        lambda x: ", ".join(x) if isinstance(x, np.ndarray) else x
    )
    df_company_selection_raw["emailAddresses"] = df_company_selection_raw[
        "emailAddresses"
    ].apply(lambda x: ", ".join(x) if isinstance(x, np.ndarray) else x)
    df_company_selection_wout_na = df_company_selection_raw.dropna(
        subset=["company_name"]
    )
    print("AFTER DROP NA")
    print(df_company_selection_wout_na.shape)
    print(df_company_selection_wout_na.columns)
    df_company_selection_wout_na_str = df_company_selection_wout_na.astype("str")
    df_company_selection = df_company_selection_wout_na_str.drop_duplicates()
    print("Company Selection DF ready")
    print(df_company_selection.shape)
    print(df_company_selection.columns)
    # create number of users per company
    df_company_users = (
        df_count[["company_name", "user_id"]]
        .dropna(subset=["company_name"])
        .drop_duplicates()
        .groupby(["company_name"])
        .count()
        .reset_index()
    )
    print("COMPANY USERS READY")
    print(df_company_users.shape)
    print(df_company_users.columns)
    # create new dataframe with user counts and specific company columns
    df_selected_companies = pd.merge(
        df_company_selection, df_company_users, on="company_name"
    )
    print("MERGED DATAFRAMES")
    print(df_selected_companies.shape)
    print(df_selected_companies.columns)

    # add a table scross depth param
    table_scroll_depth = 100

    data_dict = {
        "total_ind_predicted": total_ind_predicted,
        "total_job_predicted": total_job_predicted,
        "total_ind_knowns": total_ind_knowns,
        "total_job_knowns": total_job_knowns,
        "n_total": n_total,
        "n_ind_knowns": n_ind_knowns,
        "n_job_knowns": n_job_knowns,
        "n_knowns_both": n_knowns_both,
        "n_ind_pred": n_ind_pred,
        "n_job_pred": n_job_pred,
        "n_pred_both": n_pred_both,
        "top_n_industries": pd.Series(
            max_prob_industry_df[["attribute", "user_id"]].drop_duplicates()[
                "attribute"
            ]
        )
        .value_counts()[:table_scroll_depth]
        .to_dict(),
        "top_n_job_titles": pd.Series(
            df_count[["job_title", "user_id"]].drop_duplicates()["job_title"]
        )
        .value_counts()[:table_scroll_depth]
        .to_dict(),
        "top_n_companies": pd.Series(
            df_count[["company_name", "user_id"]].drop_duplicates()["company_name"]
        )
        .value_counts()[:table_scroll_depth]
        .to_dict(),
        "top_n_countries": pd.Series(
            df_count[["country", "user_id"]].drop_duplicates()["country"]
        )
        .value_counts()[:table_scroll_depth]
        .to_dict(),
        "selected_companies": df_selected_companies.to_dict(),
    }
    print("Data DICT ALMOST READY")
    # let us fix the tables to have 'others'
    # int otherwise serialization breaks..
    data_dict["top_n_industries"]["Others"] = int(
        pd.Series(df_count[["attribute", "user_id"]].drop_duplicates()["attribute"])
        .value_counts()[table_scroll_depth:]
        .sum()
    )
    data_dict["top_n_job_titles"]["Others"] = int(
        pd.Series(df_count[["job_title", "user_id"]].drop_duplicates()["job_title"])
        .value_counts()[table_scroll_depth:]
        .sum()
    )
    data_dict["top_n_companies"]["Others"] = int(
        pd.Series(
            df_count[["company_name", "user_id"]].drop_duplicates()["company_name"]
        )
        .value_counts()[table_scroll_depth:]
        .sum()
    )
    data_dict["top_n_countries"]["Others"] = int(
        pd.Series(df_count[["country", "user_id"]].drop_duplicates()["country"])
        .value_counts()[table_scroll_depth:]
        .sum()
    )
    return json.dumps(data_dict)


@app.callback(
    Output("total-plot", "figure"),
    Input("data_dict", "data"),
)
def update_total_plot(jsonified_cleaned_data):
    print("update_total_plot")
    data_dict = json.loads(jsonified_cleaned_data)

    fig = px.bar(
        pd.DataFrame({"label": [" "], "value": [data_dict["n_total"]]}),
        y="label",
        x="value",
        # title="Audience size",
        orientation="h",
        color_discrete_sequence=["#00A6A6"],
        template="none",
        height=160,
    )
    # fig.update_xaxes(range=(0, data_dict["total_ind_predicted"]))
    fig.update_layout(
        transition_duration=500,
        xaxis_title="Audience Size",
        yaxis_title=" ",
        font_family="HelveticaNeue",
        font_color="black",
    )

    return fig


@app.callback(
    Output("industry-plot", "figure"),
    Input("data_dict", "data"),
)
def update_industry_plot(jsonified_cleaned_data):
    print("update_industry_plot")
    data_dict = json.loads(jsonified_cleaned_data)
    # add thousand separator
    # TODO: returns a string currently? Doesn't break anything and export is read fine in Excel...
    plt_df = pd.DataFrame(
        {
            "label": ["Lookalike", "Known"],
            "value": [f'{data_dict["n_ind_pred"]:,}', f'{data_dict["n_ind_knowns"]:,}'],
        }
    )
    fig = px.bar(
        plt_df,
        x="label",
        y="value",
        color="label",
        color_discrete_sequence=["#00A6A6", "#F3A83B"],
        template="none",
        text="value",
        width=600,
        title="Industry Totals",
    )
    fig.update_layout(
        showlegend=False,
        transition_duration=500,
        xaxis_title=" ",
        yaxis_title="Count",
        font_family="HelveticaNeue",
        font_color="black",
    )

    return fig


@app.callback(
    Output("job-plot", "figure"),
    Input("data_dict", "data"),
)
def update_jobs_plot(jsonified_cleaned_data):
    print("update_jobs_plot")
    data_dict = json.loads(jsonified_cleaned_data)
    # add a thousand separator
    plt_df = pd.DataFrame(
        {
            "label": ["Lookalike", "Known"],
            "value": [f'{data_dict["n_job_pred"]:,}', f'{data_dict["n_job_knowns"]:,}'],
        }
    )
    fig = px.bar(
        plt_df,
        x="label",
        y="value",
        color="label",
        color_discrete_sequence=["#00A6A6", "#F3A83B"],
        template="none",
        text="value",
        width=600,
        title="Job Totals",
    )
    fig.update_layout(
        showlegend=False,
        transition_duration=500,
        xaxis_title=" ",
        yaxis_title="Count",
        font_family="HelveticaNeue",
        font_color="black",
    )

    return fig


@app.callback(
    Output("metrics-table", "children"),
    Input("data_dict", "data"),
)
def update_table(jsonified_cleaned_data):
    print("update_table")
    data_dict = json.loads(jsonified_cleaned_data)
    table_df = pd.DataFrame(
        {
            " ": ["Job", "Industry", "Both"],
            "Known": [
                data_dict["n_job_knowns"],
                data_dict["n_ind_knowns"],
                data_dict["n_knowns_both"],
            ],
            "Lookalike": [
                data_dict["n_job_pred"],
                data_dict["n_ind_pred"],
                data_dict["n_pred_both"],
            ],
            "Total": [
                data_dict["n_job_knowns"] + data_dict["n_job_pred"],
                data_dict["n_ind_knowns"] + data_dict["n_ind_pred"],
                data_dict["n_total"],
            ],
        }
    )
    # add thousand separators...
    table_df.Known = table_df.Known.apply(lambda x: f"{x:,}")
    table_df.Lookalike = table_df.Lookalike.apply(lambda x: f"{x:,}")
    table_df.Total = table_df.Total.apply(lambda x: f"{x:,}")

    # table_df = table_df.set_index('index')

    return dash_table.DataTable(
        data=table_df.to_dict("records"),
        columns=[{"id": x, "name": x} for x in table_df.columns],
        export_format="csv",
    )


@app.callback(
    Output("industry-table", "children"),
    Input("data_dict", "data"),
)
def update_top_industries(jsonified_cleaned_data):
    print("update_top_industries")
    data_dict = json.loads(jsonified_cleaned_data)

    industry_dict = {i: [v] for i, v in data_dict["top_n_industries"].items()}

    table_df = pd.DataFrame.from_dict(industry_dict, orient="index").reset_index()
    table_df.columns = ["Industry", "Count"]
    # add thousands separator
    table_df.Count = table_df.Count.apply(lambda x: f"{x:,}")

    # table_df = table_df.set_index('index')

    return dash_table.DataTable(
        data=table_df.to_dict("records"),
        columns=[{"id": x, "name": x} for x in table_df.columns],
        export_format="csv",
        style_table={"overflowY": "scroll", "max-height": 300},
    )


@app.callback(
    Output("jobs-table", "children"),
    Input("data_dict", "data"),
)
def update_top_jobs(jsonified_cleaned_data):
    print("update_top_jobs")
    data_dict = json.loads(jsonified_cleaned_data)

    industry_dict = {i: [v] for i, v in data_dict["top_n_job_titles"].items()}

    table_df = pd.DataFrame.from_dict(industry_dict, orient="index").reset_index()
    table_df.columns = ["Job Title", "Count"]
    # add thousands separator
    table_df.Count = table_df.Count.apply(lambda x: f"{x:,}")

    # table_df = table_df.set_index('index')

    return dash_table.DataTable(
        data=table_df.to_dict("records"),
        columns=[{"id": x, "name": x} for x in table_df.columns],
        export_format="csv",
        style_table={"overflowY": "scroll", "max-height": 300},
    )


@app.callback(
    Output("companies-table", "children"),
    Input("data_dict", "data"),
)
def update_top_companies(jsonified_cleaned_data):
    print("update_top_companies")
    data_dict = json.loads(jsonified_cleaned_data)

    industry_dict = {i: [v] for i, v in data_dict["top_n_companies"].items()}

    table_df = pd.DataFrame.from_dict(industry_dict, orient="index").reset_index()
    table_df.columns = ["Company", "Count"]
    # add thousands separator
    table_df.Count = table_df.Count.apply(lambda x: f"{x:,}")

    # table_df = table_df.set_index('index')

    return dash_table.DataTable(
        data=table_df.to_dict("records"),
        columns=[{"id": x, "name": x} for x in table_df.columns],
        export_format="csv",
        style_table={"overflowY": "scroll", "max-height": 300},
    )


@app.callback(
    Output("countries-table", "children"),
    Input("data_dict", "data"),
)
def update_top_countries(jsonified_cleaned_data):
    print("update_top_countries")
    data_dict = json.loads(jsonified_cleaned_data)

    industry_dict = {i: [v] for i, v in data_dict["top_n_countries"].items()}

    table_df = pd.DataFrame.from_dict(industry_dict, orient="index").reset_index()
    table_df.columns = ["Country", "Count"]
    # add thousands separator
    table_df.Count = table_df.Count.apply(lambda x: f"{x:,}")

    # table_df = table_df.set_index('index')

    return dash_table.DataTable(
        data=table_df.to_dict("records"),
        columns=[{"id": x, "name": x} for x in table_df.columns],
        export_format="csv",
        style_table={"overflowY": "scroll", "max-height": 300},
    )


# selected companies table
@app.callback(
    Output("selected-companies-table", "children"),
    Input("data_dict", "data"),
)
def update_selected_companies_table(jsonified_cleaned_data):
    print("update_selected_companies_table")
    data_dict = json.loads(jsonified_cleaned_data)
    table_df = pd.DataFrame.from_dict(
        data_dict["selected_companies"], orient="columns"
    ).reset_index(drop=True)
    table_df.columns = [
        "Company Name",
        "Industry",
        "Estimated Revenue",
        "Employees",
        "City",
        "State",
        "Country",
        "Tech Categories",
        "Tech Stack",
        "Emails",
        "Count",
    ]
    # add thousands separator
    table_df.Count = table_df.Count.apply(lambda x: f"{int(x):,}")
    return dash_table.DataTable(
        data=table_df.to_dict("records"),
        columns=[{"id": x, "name": x} for x in table_df.columns],
        export_format="csv",
        style_table={"overflowY": "scroll", "max-height": 300},
    )


@app.callback(
    Output("download-users", "data"),
    Input("btn_txt", "n_clicks"),
    State("domain-choice", "value"),
    State("countries-dropdown", "value"),
    State("continent-choice", "value"),
    State("province-dropdown", "value"),
    State("city-dropdown", "value"),
    State("industry-dropdown", "value"),
    State("job-dropdown", "value"),
    State("prob-slider", "value"),
    State("knowns", "value"),
    State("job-knowns", "value"),
    State("company-countries-dropdown", "value"),
    State("company-state-dropdown", "value"),
    State("company-city-dropdown", "value"),
    State("companytype-dropdown", "value"),
    State("company-dropdown", "value"),
    State("employeesrange_dropdown", "value"),
    State("estimatedannualrevenue_dropdown", "value"),
    State("tech-categories-dropdown", "value"),
    State("tech-dropdown", "value"),
    State("tags-dropdown", "value"),
    prevent_initial_call=True,
)
def download_csv(
    n_clicks,
    domain,
    country,
    continent,
    province,
    city,
    industry,
    job,
    prob,
    ind_knowns,
    job_knowns,
    company_country,
    company_state,
    company_city,
    companytype,
    company,
    employeesrange,
    estimatedannualrevenue,
    tech_categories,
    tech,
    tags,
):
    print("download_csv")
    # Domain
    if "all" not in domain:
        cond = data_df["domain"].isin(domain)
    else:
        cond = data_df["domain"].isin(data_df["domain"].unique().tolist())

    # Industry
    if industry != "all":
        cond = cond & (data_df["attribute"].isin(industry))

    # Job
    if "all" not in job:
        cond = cond & (data_df["job_title"].isin(job))

    # Company
    if "all" not in company:
        cond = cond & (data_df["company_name"].isin(company))
    else:
        if companytype != "all":
            cond = cond & (data_df["type"] == companytype)

    # Employeesrange
    if "all" not in employeesrange:
        cond = cond & (data_df["employeesRange"].isin(employeesrange))

    # Estimatedannualrevenue
    if "all" not in estimatedannualrevenue:
        cond = cond & (data_df["estimatedAnnualRevenue"].isin(estimatedannualrevenue))

    # Tech categories
    if "all" not in tech_categories:
        cond = cond & (
            data_df["techCategories"].apply(
                lambda x: set(tech_categories).issubset(set(x))
                if x is not np.nan
                else False
            )
        )

    # Tech
    if "all" not in tech:
        cond = cond & (
            data_df["tech"].apply(
                lambda x: set(tech).issubset(set(x)) if x is not np.nan else False
            )
        )

    # Tags
    if "all" not in tags:
        cond = cond & (
            data_df["tags"].apply(
                lambda x: set(tags).issubset(set(x)) if x is not np.nan else False
            )
        )

    # Country
    if "all" not in city:
        cond = cond & (data_df["city"].isin(city))
    else:
        if "all" not in province:
            cond = cond & (data_df["province"].isin(province))
        else:
            if "all" not in country:
                cond = cond & (data_df["country"].isin(country))
            else:
                if continent != "all":
                    cond = cond & (data_df["continent"] == continent)

    # Company Country
    if "all" not in company_city:
        cond = cond & (data_df["company_city"].isin(company_city))
    else:
        if "all" not in company_state:
            cond = cond & (data_df["company_state"].isin(company_state))
        else:
            if "all" not in company_country:
                cond = cond & (data_df["company_country"].isin(company_country))

    # Filter
    df_cond = data_df[cond]

    # Predicted above certainty threshold or knowns
    df_cond = df_cond[(df_cond["prob_index"] >= (1 - prob)) | (df_cond["known"] == 1)]

    # Filter knowns
    if not ind_knowns:
        df_cond = df_cond[df_cond["known"] != 1]

    if not job_knowns:
        df_cond = df_cond[df_cond["known_job_title"] != 1]

    df_cond = df_cond[["user_id"]].drop_duplicates(ignore_index=True)

    if n_clicks is None:
        raise PreventUpdate
    else:
        return dcc.send_data_frame(
            df_cond.to_csv, f"user_list_{datetime.now()}.csv", index=False
        )


if __name__ == "__main__":
    app.run_server(debug=False)
