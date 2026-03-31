SELECT
  `sensorId`,
  `value`.`type` AS `type`,
  CASE
    WHEN `value`.`unit` = 'Fahrenheit' AND `value`.`type`= 'temperature'
     THEN 'Celsius'
    ELSE `value`.`unit`
   END AS `unit`,
  CASE
    WHEN `value`.`unit` = 'Fahrenheit' AND `value`.`type`= 'temperature'
    THEN (`value`.`value` - 32) / 1.8
    ELSE `value`.`value`
  END AS `value`,
  `timestamp`
FROM {{ ref('flatten') }}
WHERE `sensorId` IS NOT NULL