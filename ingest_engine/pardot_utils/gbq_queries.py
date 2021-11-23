QUERY_MAX_MIN = '''select max(created_at) as max_created_at,
                   max(updated_at) as max_updated_at,
                   min(id) as minid,
                   max(id) as maxid
                   from {project_id}.{dataset}.{table}'''
QUERY_MAX_MIN_EMAIL = '''select max(created_at) as max_created_at,
                   min(id) as minid,
                   max(id) as maxid
                   from {project_id}.{dataset}.{table}'''

QUERY_NSMG_CAMPAIGNS = "SELECT id FROM `project-pulldata.pardot_GDM.PardotCampaigns` where `group` = 'NSMG' or `group` = 'PMI'"

QUERY_NSMG_PROSPECTS = "SELECT distinct id FROM `project-pulldata.Pardot_NSMG.Prospects`"
QUERY_VERDICT_PROSPECTS = "SELECT distinct id FROM `project-pulldata.Pardot_Verdict.Prospects`"
QUERY_NSMG_LIST_MEMBERSHIP = "SELECT distinct list_id FROM `project-pulldata.Pardot_NSMG.ListMemberships`"
QUERY_VERDICT_LIST_MEMBERSHIP = "SELECT distinct list_id FROM `project-pulldata.Pardot_NSMG.ListMemberships`"
