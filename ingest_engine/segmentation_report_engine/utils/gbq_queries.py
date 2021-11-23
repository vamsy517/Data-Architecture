from ingest_utils.database_gbq import get_gbq_table
from ingest_utils.constants import PULLDATA_PROJECT_ID
import pandas as pd


class GBQQueries:
    """
    Class, which upon received list of predefined queries, returns the corresponding table from GBQ
    """
    
    def __init__(self, query_list: list, project_id: str = PULLDATA_PROJECT_ID):
        """
        Class initialization
        :param query_list: List of queries, used to get a specific table from GBQ
        :param project_id: project id of the project in GBQ
        """
        self.project_id = project_id
        self.query_list = query_list
        self.requested_table = self.get_requested_table()
        
    def get_requested_table(self) -> pd.DataFrame:
        """
        Construct the query, used to generate a specific table and get the table from GBQ
        :return: dataframe, containing the generated table
        """
        resulting_query = 'with ' + ','.join(self.query_list[:-1]) + " " + self.query_list[-1]
        return get_gbq_table(self.project_id, resulting_query)

