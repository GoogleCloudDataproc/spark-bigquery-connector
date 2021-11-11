package com.google.cloud.spark.bigquery.v2;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.types.StructType;

public class BigQueryStreamWriterModule implements Module{
    private final String queryId;
    private final StructType sparkSchema;
    private final OutputMode mode;


    BigQueryStreamWriterModule(String queryId, StructType sparkSchema, OutputMode mode) {
        this.queryId = queryId;
        this.sparkSchema = sparkSchema;
        this.mode = mode;
    }

    @Override
    public void configure(Binder binder) {

    }

    @Singleton
    @Provides
    public BigQueryStreamingWriter provideStreamingWriter(){
      return new BigQueryStreamingWriter();
    }
}
