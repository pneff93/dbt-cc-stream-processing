SELECT
    `sensorId`,
    `type`,
    ROUND(AVG(`value`), 1) AS `valueAvg`,
    COUNT(`value`) AS `count`,
    LAST_VALUE(`unit`) AS `unit`,
    LAST_VALUE(`timestamp`) AS `timestamp`,
    window_start,
    window_end
  FROM TABLE(
   TUMBLE(
     DATA => TABLE {{ ref('transform') }},
     TIMECOL => DESCRIPTOR($rowtime),
     SIZE => INTERVAL '10' SECONDS))
  GROUP BY window_start, window_end, `sensorId`, `type`