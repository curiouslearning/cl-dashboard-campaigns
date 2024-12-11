WITH
  ranked_events AS (
  SELECT
    user_pseudo_id,
    event_name,
    event_date,
    cr_user_id_params.value.string_value AS cr_user_id,
    LOWER(REGEXP_EXTRACT(app_params.value.string_value, '[?&]cr_lang=([^&]+)')) AS app_language,
    geo.country AS country,
    geo.city AS city,
    geo.region AS region,
    CASE
      WHEN campaign_props.value.int_value IS NOT NULL THEN CAST(campaign_props.value.int_value AS STRING)
      ELSE campaign_props.value.string_value
  END
    AS campaign_id,
    source_props.value.string_value AS source_id,
    traffic_source.name AS traffic_source,
    CAST(DATE(TIMESTAMP_MICROS(user_first_touch_timestamp)) AS DATE) AS first_open,
    ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp ASC) AS event_rank
  FROM
    `ftm-b9d99.analytics_159643920.events_2024*` AS A,
    UNNEST(event_params) AS app_params,
    UNNEST(event_params) AS cr_user_id_params,
    UNNEST(user_properties) AS campaign_props,
    UNNEST(user_properties) AS source_props
  WHERE
    event_name IN ( 'download_completed',
      'tapped_start',
      'selected_level',
      'puzzle_completed',
      'level_completed')
    -- Filtering for campaign_id
    AND campaign_props.key = 'campaign_id'
    AND (campaign_props.value.int_value IS NOT NULL
      OR campaign_props.value.string_value IS NOT NULL)
    -- Filtering for source
    AND source_props.key = 'source'
    AND app_params.value.string_value LIKE '%https://feedthemonster.curiouscontent.org%'
    AND source_props.value.string_value IS NOT NULL
    AND cr_user_id_params.key = 'cr_user_id'
    AND CAST(DATE(TIMESTAMP_MICROS(user_first_touch_timestamp)) AS DATE) BETWEEN '2024-09-11'
    AND CURRENT_DATE() )
SELECT
  user_pseudo_id,
  event_name,
  event_date,
  cr_user_id,
  country,
  app_language,
  city,
  region,
  campaign_id,
  source_id,
  traffic_source,
  first_open
FROM
  ranked_events
WHERE
  event_rank = 1
ORDER BY
  event_date;