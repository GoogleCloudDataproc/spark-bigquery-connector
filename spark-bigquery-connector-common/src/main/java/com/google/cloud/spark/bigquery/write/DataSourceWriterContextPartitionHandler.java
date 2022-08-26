package com.google.cloud.spark.bigquery.write;

import com.google.cloud.spark.bigquery.util.ScalaUtils;
import com.google.cloud.spark.bigquery.write.context.DataWriterContext;
import com.google.cloud.spark.bigquery.write.context.DataWriterContextFactory;
import com.google.cloud.spark.bigquery.write.context.WriterCommitMessageContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import java.io.Serializable;
import java.util.Iterator;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataSourceWriterContextPartitionHandler
    implements Function2<Integer, Iterator<Row>, Iterator<WriterCommitMessageContext>>,
        Serializable {

  private static Logger logger =
      LoggerFactory.getLogger(DataSourceWriterContextPartitionHandler.class);

  private final DataWriterContextFactory dataWriterContextFactory;
  private long epoch;
  private long taskId;

  public DataSourceWriterContextPartitionHandler(
      DataWriterContextFactory dataWriterContextFactory, long epoch) {
    this.dataWriterContextFactory = dataWriterContextFactory;
    this.epoch = epoch;
    TaskContext tc = TaskContext.get();
    this.taskId = tc != null ? tc.taskAttemptId() : 0;
  }

  @Override
  public Iterator<WriterCommitMessageContext> call(Integer partitionId, Iterator<Row> rowIterator)
      throws Exception {
    DataWriterContext dataWriterContext =
        dataWriterContextFactory.createDataWriterContext(partitionId, taskId, epoch);
    try {
      while (rowIterator.hasNext()) {
        Row row = rowIterator.next();
        InternalRow internalRow = ScalaUtils.getInstance().rowToInternalRow(row);
        dataWriterContext.write(internalRow);
      }
      return Iterators.forArray(dataWriterContext.commit());
    } catch (Exception e) {
      logger.warn(
          "Encountered error writing partition {} in task id {} for epoch {}. Calling DataWriter.abort()",
          partitionId,
          taskId,
          epoch,
          e);
      dataWriterContext.abort();
      return ImmutableList.<WriterCommitMessageContext>of().iterator();
    }
  }
}
