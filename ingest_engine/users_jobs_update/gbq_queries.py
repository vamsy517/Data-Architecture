from ingest_utils.constants import PULLDATA_PROJECT_ID, PERMUTIVE_PROJECT_ID, PERMUTIVE_DATASET
from alchemer_data_update.constants import NS_DATASET, GD_DATASET
from pardot_utils.constants import B2B_DATASET, B2C_DATASET, PROSPECTS_TABLE

QUERY_GET_ALL_JOBS = f'''
                        with cte as (SELECT job_title,
                        created_at,
                        updated_at,
                        CASE
                            WHEN prospect.permutive_id is null THEN match.permutive_id
                            WHEN prospect.permutive_id = '0' THEN match.permutive_id
                            ELSE prospect.permutive_id
                        END as permutive_id
                        FROM `{PULLDATA_PROJECT_ID}.{B2B_DATASET}.{PROSPECTS_TABLE}` as prospect
                        LEFT JOIN `{PULLDATA_PROJECT_ID}.{B2B_DATASET}.PermutivePardotMatch` as match ON prospect.id=match.prospect_id
                        WHERE job_title is not null
                        and ((prospect.permutive_id is not null and prospect.permutive_id != '0')
                        or match.permutive_id is not null)
                        UNION ALL
                        SELECT job_title,
                        created_at,
                        updated_at,
                        CASE
                            WHEN prospect.permutive_id is null THEN match.permutive_id
                            WHEN prospect.permutive_id = '0' THEN match.permutive_id
                            ELSE prospect.permutive_id
                        END as permutive_id
                        FROM `{PULLDATA_PROJECT_ID}.{B2B_DATASET}.Prospects_pre_2020` as prospect
                        LEFT JOIN `{PULLDATA_PROJECT_ID}.{B2B_DATASET}.PermutivePardotMatch` as match ON prospect.id=match.prospect_id
                        WHERE job_title is not null
                        and ((prospect.permutive_id is not null and prospect.permutive_id != '0')
                        or match.permutive_id is not null)
                        UNION ALL
                        SELECT job_title,
                        created_at,
                        updated_at,
                        match.permutive_id as permutive_id
                        FROM `{PULLDATA_PROJECT_ID}.{B2C_DATASET}.{PROSPECTS_TABLE}` as prospect
                        LEFT JOIN `{PULLDATA_PROJECT_ID}.{B2C_DATASET}.PermutivePardotMatch` as match ON prospect.id=match.prospect_id
                        WHERE job_title is not null
                        and match.permutive_id is not null
                        UNION ALL
                        SELECT job_title,
                        TIMESTAMP(created_at) as created_at,
                        TIMESTAMP(updated_at) as updated_at,
                        match.permutive_id as permutive_id
                        FROM `{PULLDATA_PROJECT_ID}.{B2C_DATASET}.Prospects_old` as prospect
                        LEFT JOIN `{PULLDATA_PROJECT_ID}.{B2C_DATASET}.PermutivePardotMatch` as match ON prospect.id=match.prospect_id
                        WHERE job_title is not null
                        and match.permutive_id is not null),
                        cte2 as (
                        select * from cte
                        FULL OUTER join (
                        select answer, permutive_id as alchemer_permutive_id,
                        date_submitted 
                        from `{PULLDATA_PROJECT_ID}.{GD_DATASET}.Responses` as r
                        join `{PULLDATA_PROJECT_ID}.{GD_DATASET}.ResponsesDescriptions` as rd
                        on r.surveyID = rd.surveyID
                        and r.responseID = rd.responseID
                        where (question  like "%position%"
                        or question like "%job title%")
                        and permutive_id is not null
                        and answer not in('nan', '')
                        ) as al_gd
                        on cte.permutive_id = al_gd.alchemer_permutive_id
                        ),
                        cte3 as (
                        select * from cte
                        FULL OUTER join (
                        select answer, permutive_id as alchemer_permutive_id,
                        date_submitted 
                        from `{PULLDATA_PROJECT_ID}.{NS_DATASET}.Responses` as r
                        join `{PULLDATA_PROJECT_ID}.{NS_DATASET}.ResponsesDescriptions` as rd
                        on r.surveyID = rd.surveyID
                        and r.responseID = rd.responseID
                        where (question  like "%position%"
                        or question like "%job title%")
                        and permutive_id is not null
                        and answer not in('nan', '')
                        ) as al_gd
                        on cte.permutive_id = al_gd.alchemer_permutive_id
                        ),
                        job_data as (
                        select * from cte
                        FULL OUTER join (
                          SELECT
                            time as time_at,
                            form.value,
                            user_id,
                          FROM
                            `{PERMUTIVE_PROJECT_ID}.{PERMUTIVE_DATASET}.formsubmission_events`,
                            unnest(properties.form.properties) as form
                          Where
                          # add filtering
                            UPPER(form.name) like UPPER('%job%')
                            and form.name is not null
                            and form.value is not null
                            and UPPER(form.value) not like UPPER('%test%')
                            and UPPER(form.name) not like UPPER('%test%')
                            and value is not null
                        ) as fs
                        on cte.permutive_id = fs.value),
                        final as (
                        select 
                        distinct
                        CASE
                            WHEN job_title is null THEN answer
                            ELSE job_title
                        END as job_title,
                        CASE
                            WHEN job_title is null THEN 'Alchemer'
                            ELSE 'Pardot'
                        END as source_table,
                        CASE
                            WHEN permutive_id is null THEN alchemer_permutive_id
                            ELSE permutive_id
                        END as permutive_id,
                        CASE
                            WHEN created_at is null THEN date_submitted
                            ELSE created_at
                        END as created_at,
                        updated_at 
                        from cte2
                        UNION DISTINCT
                        select 
                        distinct
                        CASE
                            WHEN job_title is null THEN answer
                            ELSE job_title
                        END as job_title,
                        CASE
                            WHEN job_title is null THEN 'Alchemer'
                            ELSE 'Pardot'
                        END as source_table,
                        CASE
                            WHEN permutive_id is null THEN alchemer_permutive_id
                            ELSE permutive_id
                        END as permutive_id,
                        CASE
                            WHEN created_at is null THEN date_submitted
                            ELSE created_at
                        END as created_at,
                        updated_at 
                        from cte3


                        UNION DISTINCT
                        select 
                        distinct
                        CASE
                            WHEN job_title is null THEN value
                            ELSE job_title
                        END as job_title,
                        CASE
                            WHEN job_title is null THEN 'Permutive'
                            ELSE 'Pardot'
                        END as source_table,                        
                        CASE
                            WHEN permutive_id is null THEN user_id
                            ELSE permutive_id
                        END as permutive_id,
                        CASE
                            WHEN created_at is null THEN time_at
                            ELSE created_at
                        END as created_at,
                        updated_at 
                        from job_data)
                        select job_title, source_table, permutive_id,created_at,
                        CASE
                            WHEN updated_at is null THEN created_at
                            ELSE updated_at
                        END as updated_at,
                        from final
                        '''
GET_PREV_DATA = 'SELECT * FROM `project-pulldata.UsersJobs.users_jobs`'
DELETE_ROWS_USER_JOBS = 'delete from `{dataset}.users_jobs` where permutive_id in ({list_of_ids})'
