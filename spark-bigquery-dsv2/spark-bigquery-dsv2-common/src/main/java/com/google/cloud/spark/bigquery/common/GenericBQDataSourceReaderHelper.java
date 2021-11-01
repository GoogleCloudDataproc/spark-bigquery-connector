package com.google.cloud.spark.bigquery.common;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.connector.common.ReadSessionCreatorConfig;
import com.google.cloud.bigquery.connector.common.ReadSessionResponse;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.spark.bigquery.ReadRowsResponseToInternalRowIteratorConverter;
import com.google.cloud.spark.bigquery.SchemaConverters;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class GenericBQDataSourceReaderHelper implements Serializable {

  public boolean isBatchReadEnable() {
    return true;
  }
  // this method should move into the spark bigquery connector common
  public ReadRowsResponseToInternalRowIteratorConverter createConverter(
          ImmutableList<String> selectedFields,
          ReadSessionResponse readSessionResponse,
          Optional<StructType> userProvidedSchema, ReadSessionCreatorConfig readSessionCreatorConfig) {
    ReadRowsResponseToInternalRowIteratorConverter converter;
    DataFormat format = readSessionCreatorConfig.getReadDataFormat();
    if (format == DataFormat.AVRO) {
      Schema schema =
              SchemaConverters.getSchemaWithPseudoColumns(readSessionResponse.getReadTableInfo());
      if (selectedFields.isEmpty()) {
        // means select *
        selectedFields =
                schema.getFields().stream()
                        .map(Field::getName)
                        .collect(ImmutableList.toImmutableList());
      } else {
        Set<String> requiredColumnSet = ImmutableSet.copyOf(selectedFields);
        schema =
                Schema.of(
                        schema.getFields().stream()
                                .filter(field -> requiredColumnSet.contains(field.getName()))
                                .collect(Collectors.toList()));
      }
      return ReadRowsResponseToInternalRowIteratorConverter.avro(
              schema,
              selectedFields,
              readSessionResponse.getReadSession().getAvroSchema().getSchema(),
              userProvidedSchema);
    }
    throw new IllegalArgumentException(
            "No known converted for " + readSessionCreatorConfig.getReadDataFormat());
  }
}
