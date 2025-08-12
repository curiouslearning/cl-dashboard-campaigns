WITH ranked_events AS (
  SELECT
    user_pseudo_id,
    event_name,
    event_date,
    geo.country AS country,
    geo.city AS city,
    geo.region AS region,

    -- cr_user_id
    (SELECT ep.value.string_value
     FROM UNNEST(event_params) ep
     WHERE ep.key = 'cr_user_id') AS cr_user_id,

    -- campaign_id (supports int or string storage)
    (SELECT IFNULL(CAST(up.value.int_value AS STRING), up.value.string_value)
     FROM UNNEST(user_properties) up
     WHERE up.key = 'campaign_id') AS campaign_id,

    -- source_id
    (SELECT up.value.string_value
     FROM UNNEST(user_properties) up
     WHERE up.key = 'source') AS source_id,

    traffic_source.name   AS traffic_source_name,
    traffic_source.source AS traffic_source_source,

    -- app_language from web_app_url
    LOWER(REGEXP_EXTRACT(
      (SELECT ep.value.string_value
       FROM UNNEST(event_params) ep
       WHERE ep.key = 'web_app_url'),
      r'[?&]cr_lang=([^&]+)')
    ) AS app_language,

    CAST(DATE(TIMESTAMP_MICROS(user_first_touch_timestamp)) AS DATE) AS first_open,

    ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp ASC) AS event_rank

  FROM `ftm-b9d99.analytics_159643920.events_*`
  WHERE
    -- scan only needed tables by suffix
    _TABLE_SUFFIX BETWEEN '20241108' AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())
    AND app_info.id = 'org.curiouslearning.container'
    AND event_name = 'app_launch'

    -- require web_app_url and restrict to FTM container webapp
    AND (SELECT ep.value.string_value
         FROM UNNEST(event_params) ep
         WHERE ep.key = 'web_app_url')
         LIKE 'https://feedthemonster.curiouscontent.org%'

    -- cohort window by user_first_touch
    AND CAST(DATE(TIMESTAMP_MICROS(user_first_touch_timestamp)) AS DATE)
        BETWEEN '2024-11-08' AND CURRENT_DATE()

    -- **require both IDs to be present**
    AND (SELECT IFNULL(CAST(up.value.int_value AS STRING), up.value.string_value)
         FROM UNNEST(user_properties) up
         WHERE up.key = 'campaign_id') IS NOT NULL
    AND (SELECT up.value.string_value
         FROM UNNEST(user_properties) up
         WHERE up.key = 'source') IS NOT NULL
)

SELECT
  user_pseudo_id,
  event_name,
  event_date,
  cr_user_id,
  country,
  city,
  region,
  campaign_id,
  source_id,
  traffic_source_name,
  traffic_source_source,
  app_language,
  first_open
FROM ranked_events
WHERE event_rank = 1
ORDER BY event_date;
