# query, which gets specific table from specific dataset
QUERY_WHOLE_TABLE = 'SELECT * FROM {project_id}.{dataset}.{table}'
# query, which get the Information Schema from GBQ
QUERY_INFORMATION_SCHEMA = 'SELECT * FROM INFORMATION_SCHEMA.SCHEMATA'
# query, which gets the Information Schema for specific dataset
QUERY_DATASET_INFORMATION_SCHEMA = 'SELECT * FROM {dataset}.INFORMATION_SCHEMA.TABLES'
# query, which gets the column types for specific table
QUERY_COLUMN_TYPES = '''SELECT column_name, data_type
    FROM `{project_id}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = '{table}' and column_name != 'last_updated_date'
    '''
