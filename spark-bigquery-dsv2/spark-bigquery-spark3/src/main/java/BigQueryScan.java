import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;

import java.util.Optional;

//identify the list of parameter that we need to pass at runtime.
public class BigQueryScan implements Scan {
    private Optional<StructType> schema;
    private Optional<StructType> userProvidedSchema;

    @Override
    public StructType readSchema() {
        return null;
    }

    @Override
    public String description() {
        return "bigquery_scan";
    }

    // Identify all the arguments that we need to pass
    @Override
    public Batch toBatch() {
        return new BigQueryBatch();
    }
}
