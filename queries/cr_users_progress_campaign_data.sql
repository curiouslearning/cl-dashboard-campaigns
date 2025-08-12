WITH all_events AS (
  SELECT
    user_pseudo_id,
    cr_user_id_params.value.string_value AS cr_user_id,
    CAST(DATE(TIMESTAMP_MICROS(user_first_touch_timestamp)) AS DATE) AS first_open,
    geo.country AS country,
    LOWER(REGEXP_EXTRACT(app_params.value.string_value, r'[?&]cr_lang=([^&]+)')) AS app_language,

    -- campaign_id may be stored as int or string in user_properties
    CASE
      WHEN campaign_props.value.int_value IS NOT NULL THEN CAST(campaign_props.value.int_value AS STRING)
      ELSE campaign_props.value.string_value
    END AS campaign_id,

    source_props.value.string_value AS source_id,

    b.max_level AS max_game_level,

    -- earliest event_date for this user (in this event set)
    MIN(PARSE_DATE('%Y%m%d', event_date)) AS event_date,

    -- first level-achievement date (when a success level_completed occurs)
    MIN(
      CASE
        WHEN event_name = 'level_completed'
         AND success_params.key = 'success_or_failure'
         AND success_params.value.string_value = 'success'
         AND level_params.key = 'level_number'
        THEN PARSE_DATE('%Y%m%d', event_date)
        ELSE NULL
      END
    ) AS la_date,

    -- max user level achieved (+1 after a successful level_completed)
    MAX(
      CASE
        WHEN event_name = 'level_completed'
         AND success_params.key = 'success_or_failure'
         AND success_params.value.string_value = 'success'
         AND level_params.key = 'level_number'
        THEN level_params.value.int_value + 1
        ELSE 0
      END
    ) AS max_user_level,

    -- event counts
    SUM(
      CASE
        WHEN event_name = 'level_completed'
         AND success_params.key = 'success_or_failure'
         AND success_params.value.string_value = 'success'
         AND level_params.key = 'level_number'
        THEN 1 ELSE 0
      END
    ) AS level_completed_count,

    SUM(CASE WHEN event_name = 'puzzle_completed'  THEN 1 ELSE 0 END) AS puzzle_completed_count,
    SUM(CASE WHEN event_name = 'selected_level'    THEN 1 ELSE 0 END) AS selected_level_count,
    SUM(CASE WHEN event_name IN ('tapped_start','user_clicked') THEN 1 ELSE 0 END) AS tapped_start_count,
    SUM(CASE WHEN event_name = 'download_completed' THEN 1 ELSE 0 END) AS download_completed_count

  FROM
    `ftm-b9d99.analytics_159643920.events_202*` AS a,
    UNNEST(event_params) AS app_params,
    UNNEST(event_params) AS success_params,
    UNNEST(event_params) AS level_params,
    UNNEST(event_params) AS cr_user_id_params,
    UNNEST(user_properties) AS campaign_props,
    UNNEST(user_properties) AS source_props
  LEFT JOIN (
    SELECT app_language, max_level
    FROM `dataexploration-193817.user_data.language_max_level`
    GROUP BY app_language, max_level
  ) b
  ON b.app_language = LOWER(REGEXP_EXTRACT(app_params.value.string_value, r'[?&]cr_lang=([^&]+)'))

  WHERE
    -- event set
    event_name IN ('download_completed','tapped_start','selected_level','puzzle_completed','level_completed')

    -- require the specific params/properties (this enforces both campaign_id and source_id)
    AND app_params.key = 'page_location'
    AND app_params.value.string_value LIKE '%https://feedthemonster.curiouscontent.org%'
    AND device.web_info.hostname LIKE 'feedthemonster.curiouscontent.org%'

    AND cr_user_id_params.key = 'cr_user_id'

    AND source_props.key = 'source'
    AND source_props.value.string_value IS NOT NULL

    AND campaign_props.key = 'campaign_id'
    AND (campaign_props.value.int_value IS NOT NULL OR campaign_props.value.string_value IS NOT NULL)

    -- user cohort window
    AND CAST(DATE(TIMESTAMP_MICROS(user_first_touch_timestamp)) AS DATE)
        BETWEEN '2024-11-08' AND CURRENT_DATE()

  GROUP BY
    user_pseudo_id,
    cr_user_id,
    campaign_id,
    source_id,
    first_open,
    country,
    app_language,
    b.max_level
)

SELECT
  user_pseudo_id,
  cr_user_id,
  first_open,
  event_date,
  country,
  app_language,
  campaign_id,
  source_id,
  max_user_level,
  max_game_level,
  la_date,
  CASE
    WHEN level_completed_count > 0 THEN 'level_completed'
    WHEN puzzle_completed_count > 0 THEN 'puzzle_completed'
    WHEN selected_level_count > 0 THEN 'selected_level'
    WHEN tapped_start_count > 0 THEN 'tapped_start'
    WHEN download_completed_count > 0 THEN 'download_completed'
    ELSE NULL
  END AS furthest_event,
  SAFE_DIVIDE(max_user_level, max_game_level) * 100 AS gpc
FROM all_events;
