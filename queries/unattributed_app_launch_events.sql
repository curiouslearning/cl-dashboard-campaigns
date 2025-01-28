WITH
  query1_cr_user_ids AS (
  SELECT
    DISTINCT cr_user_id
  FROM (
    SELECT
      cr_user_id_params.value.string_value AS cr_user_id,
      ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp ASC) AS event_rank
    FROM
      `ftm-b9d99.analytics_159643920.events_202*` AS A,
      UNNEST(event_params) AS cr_user_id_params,
      UNNEST(user_properties) AS campaign_props,
      UNNEST(user_properties) AS source_props
    WHERE
      app_info.id = 'org.curiouslearning.container'
      AND event_name = 'app_launch'
      AND cr_user_id_params.key = 'cr_user_id'
      AND campaign_props.key = 'campaign_id'
      AND campaign_props.value.string_value IS NOT NULL
      AND source_props.key = 'source'
      AND source_props.value.string_value IS NOT NULL
        AND CAST(DATE(TIMESTAMP_MICROS(user_first_touch_timestamp)) AS DATE) BETWEEN '2024-11-08' AND CURRENT_DATE()
 )
  WHERE
    event_rank = 1 ),
  query2_ranked_events AS (
  SELECT
    user_pseudo_id,
    event_date,
    (SELECT crid.value.string_value 
       FROM UNNEST(event_params) AS crid 
       WHERE crid.key = 'cr_user_id') AS cr_user_id,
    geo.country AS country,
    geo.city AS city,
    geo.region AS region,
    traffic_source.name AS traffic_source_name,
    traffic_source.source AS traffic_source_source,
    LOWER((

        SELECT LOWER(REGEXP_EXTRACT(lp.value.string_value, '[?&]cr_lang=([^&]+)'))
        FROM UNNEST(event_params) AS lp
        WHERE lp.key = 'web_app_url'
    )) AS app_language,
    CAST(DATE(TIMESTAMP_MICROS(user_first_touch_timestamp)) AS DATE) AS first_open,
    ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp ASC) AS event_rank
  FROM
    `ftm-b9d99.analytics_159643920.events_202*`
  WHERE
    app_info.id = 'org.curiouslearning.container'
    AND event_name = 'app_launch'
    AND JSON_EXTRACT(
          (SELECT up.value.string_value 
           FROM UNNEST(user_properties) AS up 
           WHERE up.key = 'campaign_id'),
          '$'
        ) IS  NULL
    AND CAST(DATE(TIMESTAMP_MICROS(user_first_touch_timestamp)) AS DATE) BETWEEN '2024-11-08' AND CURRENT_DATE()

    AND (SELECT lp.value.string_value 
         FROM UNNEST(event_params) AS lp 
         WHERE lp.key = 'web_app_url') LIKE 'https://feedthemonster.curiouscontent.org%' )

SELECT
  user_pseudo_id,
  event_date,
  cr_user_id,
  country,
  city,
  region,
  traffic_source_name,
  traffic_source_source,
  app_language,
  first_open
FROM
  query2_ranked_events
WHERE
  cr_user_id NOT IN (
  SELECT
    cr_user_id
  FROM
    query1_cr_user_ids )
  AND event_rank = 1
ORDER BY
  event_date;