WITH industry as (
  SELECT user_id,
         sector,
         subindustry,
         industry,
         industrygroup
  FROM   (
            SELECT   user_id,
                     properties.company.category.sector,
                     properties.company.category.subindustry,
                     properties.company.category.industry,
                     properties.company.category.industrygroup,
                     ROW_NUMBER() OVER (partition BY user_id ORDER BY TIME DESC) AS rn
            from     `permutive-1258.global_data.clearbit_events`
            WHERE    date(_partitiontime) >= "2020-12-12"
            AND      date(_partitiontime) <= "2021-04-01"
            AND      properties.client.domain IN ('newstatesman.com',
                                                  'www.newstatesman.com')
            AND      properties.client.user_agent NOT LIKE '%bot%'
            AND      properties.client.user_agent NOT LIKE '%Google Web Preview%' )
  WHERE rn = 1 and industry is not null),

get_raw_data as (
  SELECT * from permutive-1258.global_data.pageview_events
  # select the date range that you want to fit on later down the line
  WHERE DATE(_PARTITIONTIME) >= "2021-01-31" and DATE(_PARTITIONTIME) <= "2021-04-01"
  # exclude bots
  and properties.client.user_agent not like '%bot%'
  and properties.client.user_agent not like '%Google Web Preview%'
  # leave only those events that are for the publications we work for
  AND properties.client.domain in ('newstatesman.com', 'www.newstatesman.com')),

# this cte gets the number of sessions and number of views per user
views_and_sessions as (
  SELECT user_id,
    # count the distinct sessions
    count(distinct session_id) as number_of_sessions,
    # count the distinct 'views (either view_id or event_id)
    count(distinct CASE WHEN properties.client.type = 'web' THEN view_id ELSE event_id END) as number_of_views
    # from the raw data
  FROM get_raw_data
  # and group by user
  group by user_id),
  
referrers as (
  select
  user_id,
  # social group
  sum(--Social
  CASE WHEN properties.client.referrer like '%facebook%' then 1 WHEN properties.client.referrer like '%linkedin%' then 1 WHEN properties.client.referrer like '%twitter%' then 1 WHEN properties.client.referrer like '%t.co/%' then 1 WHEN properties.client.referrer like '%youtube%' then 1 WHEN properties.client.referrer like '%instagram%' then 1 WHEN properties.client.referrer like '%reddit%' then 1 WHEN properties.client.referrer like '%pinterest%' then 1 else 0 end) as visits_from_social,
  sum(--Search
  CASE WHEN properties.client.referrer like '%ask%' then 1 WHEN properties.client.referrer like '%baidu%' then 1 WHEN properties.client.referrer like '%bing%' then 1 WHEN properties.client.referrer like '%duckduckgo%' then 1 WHEN properties.client.referrer like '%google%' then 1 WHEN properties.client.referrer like '%yahoo%' then 1 WHEN properties.client.referrer like '%yandex%' then 1 else 0 end) as visits_from_search,
  sum(--Email
  CASE WHEN properties.client.referrer like '%Pardot%' then 1 WHEN properties.client.referrer like '%pardot%' then 1 else 0 end) as visits_from_email,
  sum(--NSMG network
  CASE WHEN properties.client.referrer like '%energymonitor.ai%' then 1 WHEN properties.client.referrer like '%investmentmonitor.ai%' then 1 WHEN properties.client.referrer like '%citymonitor.ai%' then 1 WHEN properties.client.referrer like '%newstatesman.com%' then 1 WHEN properties.client.referrer like '%cbronline.com%' then 1 WHEN properties.client.referrer like '%techmonitor.ai%' then 1 WHEN properties.client.referrer like '%elitetraveler.com%' then 1 else 0 end) as visits_from_NSMG_network,
  sum(--own website > redirect / linkclicks
  CASE WHEN properties.client.referrer like '%newstatesman.com%' then 1 WHEN properties.client.referrer like '%www.newstatesman.com%' then 1 else 0 end) as visits_from_own_website,
  # when the referrer is null, this means the url was directly fed to the browser
  sum(-- Typed-in URL
  CASE WHEN properties.client.referrer is NULL then 1 else 0 end) as visit_from_bookmark_or_url
  FROM get_raw_data
  group by user_id
),

# this cte gets the number of visits from either mobile or web
amp_or_web as (
  select
  user_id,
  sum(--Web
  CASE WHEN properties.client.type = 'web' THEN 1 ELSE 0 END) as visits_from_web,
  sum(--Mobile
  CASE WHEN properties.client.type = 'amp' THEN 1 ELSE 0 END) as visits_from_mobile
  FROM get_raw_data
  group by user_id
),

# this cte returns a binary code with 1 if the user is a lead, and 0 otherwise
lead_or_not as(
  select
    user_id,
    # we join with an existing and daily-updated table to get the result
    # if the join with the 'leads' table is successful for the user, they will get an 'earliest_event' value
    # if that value is not null, then the user is a lead
    case when earliest_event is null then 0 else 1 end as lead_or_not
    from(
    select * from views_and_sessions as users
      left join (select user_id as user_id_orig, earliest_event from project-pulldata.Audience.Leads) as orig_leads
    on users.user_id = orig_leads.user_id_orig
  )
),

# this cte returns the average time of day during which the user opens a page
timezone_corrected_timestamp as (
  select
    user_id,
    avg(
    # we use the continent of the user as a proxy for their location
    # no other options were easily attainable
    extract(hour from time at TIME ZONE CASE WHEN properties.geo_info.continent in ('North America') then 'US/Central'
    WHEN properties.geo_info.continent in ('South America') then 'Brazil/West'
    WHEN properties.geo_info.continent in ('Europe', 'Africa') then 'Europe/Berlin'
    WHEN properties.geo_info.continent in ('Asia') then 'Asia/Karachi'
    ELSE 'Etc/UTC'
    END)
    ) as avg_timestamp_for_continent
    FROM get_raw_data
    group by user_id
  ),
  
# this cte returns the number of visits from the various continents - NOTE! Might want to exclude this re biases...
visits_from_continent as (
  select
    user_id,
    sum(CASE WHEN properties.geo_info.continent in ('North America') THEN 1 ELSE 0 END) as visits_from_northamerica,
    sum(CASE WHEN properties.geo_info.continent in ('South America') THEN 1 ELSE 0 END) as visits_from_southamerica,
    sum(CASE WHEN properties.geo_info.continent in ('Europe') THEN 1 ELSE 0 END) as visits_from_europe,
    sum(CASE WHEN properties.geo_info.continent in ('Africa') THEN 1 ELSE 0 END) as visits_from_africa,
    sum(CASE WHEN properties.geo_info.continent in ('Asia') THEN 1 ELSE 0 END) as visits_from_asia,
    sum(CASE WHEN properties.geo_info.continent not in ('North America', 'South America', 'Europe', 'Africa', 'Asia')
    THEN 1 ELSE 0 END) as visits_from_othercontinent
    FROM get_raw_data
    group by user_id
  ),

# this cte returns the number of visits from various OSs
visits_from_os as (
  select
    user_id,
    sum(CASE WHEN properties.client.user_agent like '%Linux%' THEN 1 ELSE 0 END) as visits_from_linux,
    sum(CASE WHEN properties.client.user_agent like '%Android%' THEN 1 ELSE 0 END) as visits_from_android,
    sum(CASE WHEN properties.client.user_agent like '%Windows NT 10.0%' THEN 1 ELSE 0 END) as visits_from_windows10,
    sum(CASE WHEN properties.client.user_agent like '%Windows%'
    and properties.client.user_agent not like '%Windows NT 10.0%' THEN 1 ELSE 0 END) as visits_from_windows_older,
    sum(CASE WHEN properties.client.user_agent like '%iPhone%' THEN 1 ELSE 0 END) as visits_from_iphone,
    sum(CASE WHEN properties.client.user_agent like '%Macintosh%' THEN 1 ELSE 0 END) as visits_from_mac,
    sum(CASE WHEN properties.client.user_agent like '%AppleWebKit%' THEN 1 ELSE 0 END) as visits_from_apple_other,
  FROM get_raw_data
  group by user_id),

dwell_and_completion as (
  select user_id,
    avg(properties.aggregations.PageviewEngagement.completion) as average_completion,
    avg(properties.aggregations.PageviewEngagement.engaged_time) as average_dwell
    from permutive-1258.global_data.pageviewcomplete_events
    WHERE DATE(_PARTITIONTIME) >= "2021-01-31" and DATE(_PARTITIONTIME) <= "2021-04-01"
    # exclude instances where both of the metrics are = 0 - some bug?
    and properties.aggregations.PageviewEngagement.completion != 0
    and properties.aggregations.PageviewEngagement.engaged_time != 0
    # exclude bots
    and properties.client.user_agent not like '%bot%'
    and properties.client.user_agent not like '%Google Web Preview%'
    # leave only those events that are for the publications we work for
    AND properties.client.domain in ('newstatesman.com', 'www.newstatesman.com')
    group by user_id
    ),

# this cte returns the average virality of articles read by the user
avearge_virality as (
  select user_id,
    avg(articles.virality) as average_virality
    from get_raw_data as users
    # we use a helper table, which contains each article in our domain, alongside its 'virality'
    # a page's virality is the ratio between 1) the total number of views for the page, and
    # 2) the average number of views for pages within the publication
    left join project-pulldata.Segmentation_v2.virality_temp_1618310441 as articles
    on users.properties.client.title = articles.title
    group by user_id
  )


SELECT 
  views.*, ind.*except(user_id), ref.*except(user_id), amp.*except(user_id), lead.*except(user_id), tz.*except(user_id), 
  cont.*except(user_id), os.*except(user_id), dwell.*except(user_id), vir.*except(user_id)
FROM views_and_sessions as views
LEFT JOIN referrers as ref
ON views.user_id = ref.user_id
LEFT JOIN amp_or_web as amp
ON views.user_id = amp.user_id
LEFT JOIN lead_or_not as lead
ON views.user_id = lead.user_id
LEFT JOIN timezone_corrected_timestamp as tz
ON views.user_id = tz.user_id
LEFT JOIN visits_from_continent as cont
ON views.user_id = cont.user_id
LEFT JOIN visits_from_os as os
ON views.user_id = os.user_id
LEFT JOIN dwell_and_completion as dwell
ON views.user_id = dwell.user_id
LEFT JOIN avearge_virality as vir
ON views.user_id = vir.user_id
LEFT JOIN industry as ind
ON views.user_id = ind.user_id