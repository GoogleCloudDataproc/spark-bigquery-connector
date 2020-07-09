package com.google.cloud.bigquery.connector.common;

import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1alpha2.ProtoBufProto;
import com.google.protobuf.Descriptors;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;

// Cannot be serialized due to schemaDescriptor.
public class WriteSessionConfig implements Serializable {
    final TableId tableId;
    final StructType sparkSchema;
    final Schema bigQuerySchema;
    final ProtoBufProto.ProtoSchema protoSchema;
    final SaveMode saveMode;
    final String writeUUID;

    public WriteSessionConfig(TableId tableId, StructType sparkSchema, Schema bigQuerySchema,
                              ProtoBufProto.ProtoSchema protoSchema, SaveMode saveMode, String writeUUID) {
        this.tableId = tableId;
        this.sparkSchema = sparkSchema;
        this.bigQuerySchema = bigQuerySchema;
        this.protoSchema = protoSchema;
        this.saveMode = saveMode;
        this.writeUUID = writeUUID;
    }


    public TableId getTableId() {
        return tableId;
    }

    public StructType getSparkSchema() {
        return sparkSchema;
    }

    public Schema getBigQuerySchema() {
        return bigQuerySchema;
    }

    public ProtoBufProto.ProtoSchema getProtoSchema() {
        return protoSchema;
    }

    public SaveMode getSaveMode() {
        return saveMode;
    }

    public String getWriteUUID() {
        return writeUUID;
    }
}
