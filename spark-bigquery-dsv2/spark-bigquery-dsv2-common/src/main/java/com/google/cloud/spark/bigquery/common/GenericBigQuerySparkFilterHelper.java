package com.google.cloud.spark.bigquery.common;

import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.ReadSessionCreatorConfig;
import com.google.cloud.spark.bigquery.SchemaConverters;
import com.google.cloud.spark.bigquery.SparkFilterUtils;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConversions;

public class GenericBigQuerySparkFilterHelper implements Serializable {
  private Map<String, StructField> fields;
  private Filter[] pushedFilters = new Filter[] {};

  public GenericBigQuerySparkFilterHelper(TableInfo table) {
    StructType convertedSchema =
        SchemaConverters.toSpark(SchemaConverters.getSchemaWithPseudoColumns(table));
    this.fields = new LinkedHashMap<>();
    for (StructField field : JavaConversions.seqAsJavaList(convertedSchema)) {
      fields.put(field.name(), field);
    }
  }

  public Map<String, StructField> getFields() {
    return fields;
  }

  public Filter[] getPushedFilters() {
    return pushedFilters;
  }

  public Filter[] pushFilters(
      Filter[] filters,
      ReadSessionCreatorConfig readSessionCreatorConfig,
      Map<String, StructField> fields) {
    List<Filter> handledFilters = new ArrayList<>();
    List<Filter> unhandledFilters = new ArrayList<>();
    for (Filter filter : filters) {
      if (SparkFilterUtils.isTopLevelFieldHandled(
          readSessionCreatorConfig.getPushAllFilters(),
          filter,
          readSessionCreatorConfig.getReadDataFormat(),
          fields)) {
        handledFilters.add(filter);
      } else {
        unhandledFilters.add(filter);
      }
    }
    this.pushedFilters = handledFilters.stream().toArray(Filter[]::new);
    return unhandledFilters.stream().toArray(Filter[]::new);
  }

  public Filter[] getPushedFilters(List<Filter> handledFilters) {
    return handledFilters.stream().toArray(Filter[]::new);
  }
}
