package com.google.cloud.spark.bigquery.common;

public class GenericBigQueryIndirectDataWriterFactory {
    String gcsDirPath;
    String avroSchemaJson;

    public GenericBigQueryIndirectDataWriterFactory(String gcsDirPath, String avroSchemaJson){
        this.gcsDirPath=gcsDirPath;
        this.avroSchemaJson=avroSchemaJson;
    }

    public String getAvroSchemaJson(){
        return this.avroSchemaJson;
    }

    public String getGcsDirPath(){
        return this.gcsDirPath;
    }
}
