CREATE TABLE default.${TABLE} OPTIONS (table='bigquery-public-data.samples.shakespeare');

SELECT word, SUM(word_count) FROM default.${TABLE} WHERE word='spark' GROUP BY word;

-- cleanup
DROP TABLE default.${TABLE};
