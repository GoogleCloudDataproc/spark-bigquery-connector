package com.google.cloud.spark.bigquery.okera;

import com.okera.recordservice.mr.PlanUtil;
import com.okera.recordservice.mr.RecordServiceConfig.ConfVars;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SQLContext;

class RecordServiceConf {
  // Spark requires that configs start with "spark." to be read.
  private static final String SPARK_CONF_PREFIX = "spark.";

  public static Configuration fromSQLContext(SQLContext sc) {
    Configuration conf = new Configuration();

    for (ConfVars confVar : ConfVars.values()) {
      String sparkKey = SPARK_CONF_PREFIX + confVar.name;

      OptionalUtil.or(
              getConfigValue(sc, sparkKey), () -> getConfigValue(sc.sparkContext(), sparkKey))
          .ifPresent((String value) -> conf.set(confVar.name, value));
    }

    return conf;
  }

  public static String getAccessToken(Configuration conf) {
    return Optional.ofNullable(PlanUtil.getTokenString(conf))
        .orElseGet(PlanUtil::tryReadTokenFromHomeDir);
  }

  private static Optional<String> getConfigValue(SparkContext sc, String key) {
    return Optional.ofNullable(sc.getConf())
        .flatMap(conf -> Optional.ofNullable(conf.get(key, null)));
  }

  private static Optional<String> getConfigValue(SQLContext sc, String key) {
    return Optional.ofNullable(sc.getConf(key, null));
  }
}
