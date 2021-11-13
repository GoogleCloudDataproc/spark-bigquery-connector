package com.google.cloud.spark.bigquery.common;

import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.ReadSessionCreatorConfig;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.spark.bigquery.SchemaConverters;
import java.io.Serializable;
import java.util.Optional;
import org.apache.spark.sql.types.StructType;

public class GenericBigQuerySchemaHelper implements Serializable {

  public boolean isEmptySchema(Optional<StructType> schema) {
    return schema.map(StructType::isEmpty).orElse(false);
  }

  public boolean isEnableBatchRead(
      ReadSessionCreatorConfig readSessionCreatorConfig, Optional<StructType> schema) {
    return readSessionCreatorConfig.getReadDataFormat() == DataFormat.ARROW
        && !isEmptySchema(schema);
  }

  public StructType readSchema(Optional<StructType> schema, TableInfo table) {
    return schema.orElse(
        SchemaConverters.toSpark(SchemaConverters.getSchemaWithPseudoColumns(table)));
  }
}
