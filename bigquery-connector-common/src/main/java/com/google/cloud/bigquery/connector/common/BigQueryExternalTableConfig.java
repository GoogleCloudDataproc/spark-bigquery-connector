package com.google.cloud.bigquery.connector.common;


import com.google.cloud.bigquery.*;

import java.util.List;

import static com.google.cloud.bigquery.connector.common.BigQueryErrorCode.UNSUPPORTED;
import static java.lang.String.format;

public class BigQueryExternalTableConfig {

    // Check if table type is EXTERNAL or not
    public boolean isInputTableAExternalTable(TableInfo table) {
        TableDefinition tableDefinition = table.getDefinition();
        TableDefinition.Type tableType = tableDefinition.getType();
        if (TableDefinition.Type.EXTERNAL == tableType) {
            if (isExternalTableFormatSupported(table)) {
                return true;
            } else {
                throw new BigQueryConnectorException(
                        UNSUPPORTED,
                        format(
                                "External Table format not supported, Only Google Cloud Storage and CSV are supported"));
            }
        }
        return false;

    }

    //Check the file type(CSV/JSON/Parquet) of the external table
    public boolean isExternalTableFormatSupported(TableInfo table) {
        ExternalTableDefinition tableDefinition = table.getDefinition();
        if (tableDefinition.getFormatOptions().getType() == "CSV") {
            getListOfURIs(table);
            return true;
        }
        return false;
    }

    //get List of URIs where actual data stored for external table in gcs bucket
    public List<String> getListOfURIs(TableInfo table) {
        ExternalTableDefinition tableDefinition = table.getDefinition();
        return tableDefinition.getSourceUris();
    }

    //get Field Delimiter For CSV file
    public String getFieldReader(TableInfo tableInfo){
        ExternalTableDefinition tableDefinition= tableInfo.getDefinition();
        CsvOptions csvOptions= tableDefinition.getFormatOptions();
        return csvOptions.getFieldDelimiter();
    }
}
