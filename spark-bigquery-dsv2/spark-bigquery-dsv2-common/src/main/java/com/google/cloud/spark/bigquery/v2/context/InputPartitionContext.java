package com.google.cloud.spark.bigquery.v2.context;

import java.io.Serializable;

public interface InputPartitionContext<T> extends Serializable {

  InputPartitionReaderContext<T> createPartitionReaderContext();
}
