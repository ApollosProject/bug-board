
        WITH version_events AS (
          __SELECTS__
        ),
        normalized_events AS (
          SELECT
            seen_at,
            COALESCE(NULLIF(church, ''), 'Unknown church') AS church,
            NULLIF(build_church, '') AS build_church,
            LOWER(COALESCE(
              NULLIF(apollos_platform, ''),
              IF(source_dataset = 'apollos_roku', 'roku', NULL),
              IF(source_dataset = 'apollos_tv', 'tv', NULL),
              'unknown'
            )) AS apollos_platform,
            COALESCE(
              NULLIF(application_name, ''),
              IF(source_dataset = 'apollos_roku', 'Roku', NULL),
              'Unknown app'
            ) AS application_name,
            COALESCE(
              NULLIF(bundle_id, ''),
              IF(source_dataset = 'apollos_roku', 'roku', NULL),
              'unknown'
            ) AS bundle_id,
            apollos_version,
            app_version,
            native_build,
            native_version,
            app_update_id,
            source_revision,
            source_version,
            deployment_track,
            source_dataset,
            source_table,
            version_source
          FROM version_events
          WHERE apollos_version IS NOT NULL OR build_church IS NOT NULL
        ),
        filtered_events AS (
          SELECT *
          FROM normalized_events
          WHERE
            source_dataset = 'apollos_roku'
            OR (
              source_dataset = 'apollos_tv'
              AND apollos_platform IN ('amazon', 'androidtv', 'tvos', 'tv')
            )
            OR (
              source_dataset = 'apollos'
              AND apollos_platform NOT IN ('amazon', 'androidtv', 'tvos', 'tv', 'roku')
            )
            OR source_dataset NOT IN ('apollos', 'apollos_tv', 'apollos_roku')
        ),
        app_identity_events AS (
          SELECT
            *,
            IF(
              LOWER(bundle_id) IN ('unknown', 'roku'),
              CONCAT(church, '|', apollos_platform, '|', LOWER(bundle_id)),
              CONCAT(apollos_platform, '|', LOWER(bundle_id))
            ) AS app_identity_key
          FROM filtered_events
        ),
        display_churches AS (
          SELECT
            app_identity_key,
            ARRAY_AGG(
              IF(apollos_version IS NOT NULL, church, NULL) IGNORE NULLS
              ORDER BY IF(church = 'Unknown church', 1, 0), church
              LIMIT 1
            )[SAFE_OFFSET(0)] AS church,
            ARRAY_AGG(build_church IGNORE NULLS ORDER BY seen_at DESC LIMIT 1)
              [SAFE_OFFSET(0)] AS build_church,
            IF(
              COUNT(DISTINCT build_church) > 0,
              COUNT(DISTINCT build_church),
              COUNT(DISTINCT IF(apollos_version IS NOT NULL, church, NULL))
            ) AS deploy_target_count
          FROM app_identity_events
          GROUP BY app_identity_key
        ),
        version_observations AS (
          SELECT
            events.app_identity_key,
            display_churches.church,
            display_churches.build_church,
            display_churches.deploy_target_count,
            events.apollos_platform,
            events.application_name,
            events.bundle_id,
            events.apollos_version,
            events.app_version,
            events.native_build,
            events.native_version,
            events.app_update_id,
            events.source_revision,
            events.source_version,
            events.deployment_track,
            events.source_dataset,
            events.source_table,
            events.version_source,
            MAX(events.seen_at) AS latest_seen_at
          FROM app_identity_events events
          JOIN display_churches
            USING (app_identity_key)
          WHERE events.apollos_version IS NOT NULL
          GROUP BY
            events.app_identity_key,
            display_churches.church,
            display_churches.build_church,
            display_churches.deploy_target_count,
            events.apollos_platform,
            events.application_name,
            events.bundle_id,
            events.apollos_version,
            events.app_version,
            events.native_build,
            events.native_version,
            events.app_update_id,
            events.source_revision,
            events.source_version,
            events.deployment_track,
            events.source_dataset,
            events.source_table,
            events.version_source
        )
        SELECT
          observation.church,
          observation.build_church,
          observation.deploy_target_count,
          observation.apollos_platform,
          observation.application_name,
          observation.bundle_id,
          observation.apollos_version,
          observation.app_version,
          observation.native_build,
          observation.native_version,
          observation.app_update_id,
          observation.source_revision,
          observation.source_version,
          observation.deployment_track,
          observation.source_dataset,
          observation.source_table,
          observation.version_source,
          observation.latest_seen_at
        FROM version_observations observation
        ORDER BY observation.latest_seen_at DESC