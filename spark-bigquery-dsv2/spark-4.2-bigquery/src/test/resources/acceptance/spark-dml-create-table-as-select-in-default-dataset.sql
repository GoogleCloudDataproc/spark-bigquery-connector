CREATE TABLE ${TABLE}_shakespeare OPTIONS (table='bigquery-public-data.samples.shakespeare');


CREATE TABLE ${TABLE} AS
  SELECT word, SUM(word_count) as word_count
  FROM ${TABLE}_shakespeare
  WHERE word='spark' GROUP BY word;
