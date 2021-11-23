QUERY_APPLE_TABLE = '''
    SELECT column_name
    FROM `project-pulldata`.covid19data.INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = 'apple_covid19_mobility_data'
    ORDER BY ORDINAL_POSITION DESC 
    LIMIT 1;
    '''