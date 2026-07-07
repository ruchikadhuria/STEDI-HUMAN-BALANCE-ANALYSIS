CREATE EXTERNAL TABLE IF NOT EXISTS stedi.step_trainer_landing (
 sensorreadingtime bigint,
 serialnumber string,
 distancefromobject double
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://stedi-data-lake-ruchika/step_trainer/landing/';
