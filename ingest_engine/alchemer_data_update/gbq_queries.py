QUERY_MAX_INTEGRATION_TIME = '''select max(date_submitted) as max_date,
                               from {project_id}.{dataset}.{table}'''