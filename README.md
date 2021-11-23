# This is the NSMG Data Architecture Repository

> The repository is used by the NSMG Data team for housing data ingestion, housekeeping and curation tools.

> The repository conforms to the following naming conventions:

1. GitHub artefacts:
   - Branches:
     - Template: \<issue tracker (Jira) task ID\>-\<description-separated-by-hyphens\>
     - Example: DSCOE-76-documenting-naming-conventions
     - Description: Having the task ID at the start makes it easier to track which branches address which needs, as defined in the issue tracker. Descriptions should be 2-4 words describing the essence of the code changes.
   - Commit summaries:
     - Template: \<issue tracker (Jira) task ID\> \<description separated by spaces\>
     - Example: Example: DSCOE-76 add conventions doc
     - Description: Adding the task ID makes it possible to apply plugins for task tracking at some point. Summary should be able to complete the sentence: When we push this commit it will ...
2. Code files:
   - .py or .ipynb (including directories that contain .py or .ipynb files):
     - Template: \<lowercase_with_underscores\>.py or .ipynb
     - Example: ingest_engine_github.py / ingest_economic_listing_api_to_csv.ipynb
     - Description: Description: This conforms to PEP-8 convention. Name should capture the core function of the program or module.
   - .sql:
     - Template: \<CapitalizedWordsWithoutDelimiters\>.sql
     - Example: UpdateTechMonitorViewArticleDate.sql
     - Description: Description: Easily distinguishable from the python files.

**The repo contains the following projects**

- Ingest Engine - a set of Airflow dags for pipelines that ingest data to NSMG's GCP database
- APIs - a set APIs, developed by the Data Team to address the data needs of the wider team


## Support

Reach out to us:

- Yavor Vrachev (Head of Data Architecture) - yavor.vrachev@ns-mediagroup.com
- Veselin Valchev (Lead of Data Science Team) - veselin.valchev@ns-mediagroup.com
- Nina Nikolaeva - nina.nikolaeva@ns-mediagroup.com
- Petyo Karatov - petyo.karatov@ns-mediagroup.com
- Dzhumle Mehmedova - dzhumle.mehmedova@ns-mediagroup.com
