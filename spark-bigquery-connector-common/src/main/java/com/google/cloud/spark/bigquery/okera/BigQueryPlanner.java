package com.google.cloud.spark.bigquery.okera;

import com.google.cloud.bigquery.connector.common.okera.AuthorizeQuery;
import com.okera.recordservice.core.AuthorizeQueryResult;
import com.okera.recordservice.core.ClientEnum;
import com.okera.recordservice.core.ConnectionContext;
import com.okera.recordservice.core.NetworkAddress;
import com.okera.recordservice.core.RecordServiceException;
import com.okera.recordservice.core.RecordServicePlannerClient;
import com.okera.recordservice.core.Request;
import com.okera.recordservice.mr.PlanUtil;
import java.io.IOException;
import java.util.List;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQueryPlanner implements AuthorizeQuery {
  private static final Logger logger = LoggerFactory.getLogger(BigQueryPlanner.class);

  private final Configuration conf;
  private final ConnectionContext context;
  private final List<NetworkAddress> hostPorts;
  private final String kerberosPrincipal;
  private final String serviceName;
  private final String tokenString;

  public BigQueryPlanner(SQLContext sqlContext) {
    this.conf = RecordServiceConf.fromSQLContext(sqlContext);
    this.kerberosPrincipal = PlanUtil.getKerberosPrincipal(conf);
    this.tokenString = RecordServiceConf.getAccessToken(conf);
    this.serviceName = PlanUtil.getServiceName(conf);
    try {
      this.context = PlanUtil.getBuilder("spark-bq", conf);
      this.hostPorts = PlanUtil.getPlannerHostPorts(conf);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public String authorizeQuery(String sql) {
    Request request = Request.createSqlRequest(sql);
    AuthorizeQueryResult authQuery =
        withClient(
            client -> {
              try {
                return client.authorizeQuery(
                    null, ClientEnum.BIG_QUERY, request, true, null, false, true);
              } catch (IOException | RecordServiceException e) {
                throw new RuntimeException(e);
              }
            });
    logger.debug("Authorized Query: originalSql={}, authorizedSql={}", sql, authQuery.sql);
    return authQuery.sql;
  }

  private <T> T withClient(Function<RecordServicePlannerClient, T> fn) {
    try (RecordServicePlannerClient client = getClient()) {
      return fn.apply(client);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private RecordServicePlannerClient getClient() {
    try {
      return PlanUtil.getPlanner(
          conf, context, hostPorts, kerberosPrincipal, serviceName, tokenString, null);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
