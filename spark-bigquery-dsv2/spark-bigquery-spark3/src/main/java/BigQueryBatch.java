import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

// identify the list of parameter that we need to pass at runtime.
public class BigQueryBatch implements Batch {

    @Override
    public InputPartition[] planInputPartitions() {
        return new InputPartition[0];
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return null;
    }
}
