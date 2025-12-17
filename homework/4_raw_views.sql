CREATE EXTERNAL TABLE IF NOT EXISTS `raw_views` (
  `title` string,
  `views` int,
  `rank` int,
  `date` string,
  `retrieved_at` string
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://luciana-wikidata/raw-views/';