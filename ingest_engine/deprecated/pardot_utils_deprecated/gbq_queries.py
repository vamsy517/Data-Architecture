QUERY_MAX_MIN = '''select max(created_at) as max_created_at,
                   max(updated_at) as max_updated_at,
                   min(id) as minid,
                   max(id) as maxid
                   from {project_id}.{dataset}.{table}'''
QUERY_MAX_MIN_EMAIL = '''select max(created_at) as max_created_at,
                   min(id) as minid,
                   max(id) as maxid
                   from {project_id}.{dataset}.{table}'''