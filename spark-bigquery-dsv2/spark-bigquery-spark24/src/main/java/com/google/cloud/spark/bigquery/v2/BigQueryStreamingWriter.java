package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.types.StructType;
import scala.NotImplementedError;

import java.util.Optional;

public class BigQueryStreamingWriter implements StreamWriter {

/** In Development
  private final BigQueryClient bigQueryClient;
  private final SparkBigQueryConfig config;
  private final Configuration hadoopConfiguration;
  private final StructType sparkSchema;
  private final String queryId;
  private final SaveMode saveMode;
  private final Path gcsPath;
  private final Optional<IntermediateDataCleaner> intermediateDataCleaner;

  BigQueryStreamingWriter(BigQueryClient bigQueryClient,
                          SparkBigQueryConfig config,
                          Configuration hadoopConfiguration,
                          StructType sparkSchema,
                          String queryId,
                          OutputMode outputmode,
                          Path gcsPath,
                          Optional<IntermediateDataCleaner> intermediateDataCleaner) {
      this.bigQueryClient = bigQueryClient;
    this.config = config;
    this.hadoopConfiguration = hadoopConfiguration;
    this.sparkSchema = sparkSchema;
    this.queryId = queryId;
    this.saveMode = getSaveMode(outputmode);
    this.gcsPath = gcsPath;
    this.intermediateDataCleaner = intermediateDataCleaner;
  }
 */

  BigQueryStreamingWriter() {
    System.out.println("In BigQueryStreaming Writer");
  }

  @Override
  public void commit(long epochId, WriterCommitMessage[] messages) {

  }

  @Override
  public void abort(long epochId, WriterCommitMessage[] messages) {}

  @Override
  public DataWriterFactory<InternalRow> createWriterFactory() {
    // In Development
    // Call BigQueryIndirectWriterFactory
    // Either BigqueryIndirectWriterFactory will be used or new class will be created implementing
    // DataWriterFactory
    // Based on above params will be sent CreateStreamWriter
    // From there BigQueryIndirect will be used

    // return new BigQueryIndirectDataWriterFactory(conf,gcsDirPath,sparkSchema,avrosSchemaJson)
    return null;
  }

  SaveMode getSaveMode(OutputMode outputMode){
    if (outputMode == OutputMode.Complete()) {
      return SaveMode.Overwrite;
    } else if (outputMode == OutputMode.Update()) {
      throw new NotImplementedError("Updates are not yet supported");
    } else {
      return SaveMode.Append;
    }
  }
}
