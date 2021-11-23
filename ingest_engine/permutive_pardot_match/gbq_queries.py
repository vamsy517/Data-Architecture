QUERY_EMAILS = """SELECT distinct x.value, user_id, max(time) as time
                  FROM `permutive-1258.global_data.formsubmission_events`, 
                  unnest (properties.form.properties) as x
                  where x.value like '%@%' and time >= DATE_ADD((
                  SELECT max(time) from `permutive-1258.global_data.formsubmission_events`
                  ), INTERVAL - 1 DAY)
                  group by x.value, user_id"""