SELECT `sensorId`,
	CAST(ROW(`flatten`.`type`, `flatten`.`unit`, `flatten`.`value`) AS ROW < `type` STRING, `unit` STRING, `value` DOUBLE >) AS `value`,
	`timestamp`
FROM `team-emea`.`pneff-dbt`.`pneff-dbt-test`,
	UNNEST(`team-emea`.`pneff-dbt`.`pneff-dbt-test`.`value`) AS `flatten`
	WHERE `sensorId` IS NOT NULL