import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;

// identify the list of parameter that we need to pass at runtime.
public class BigQueryScanBuilder implements ScanBuilder {
    @Override
    public Scan build() {
        return new BigQueryScan();
    }
}
