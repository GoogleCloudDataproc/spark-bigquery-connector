import com.google.cloud.bigquery.connector.common.BigQueryReadClientFactory;
import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.spark.bigquery.ReadRowsResponseToInternalRowIteratorConverter;
import org.apache.spark.sql.connector.read.InputPartition;

//identify the list of parameter that we need to pass at runtime.
public class BigQueryInputPartition implements  InputPartition {
    private final BigQueryReadClientFactory bigQueryReadClientFactory;
    private final String streamName;
    private final ReadRowsHelper.Options options;
    private final ReadRowsResponseToInternalRowIteratorConverter converter;

    public BigQueryInputPartition(BigQueryReadClientFactory bigQueryReadClientFactory,
                                  String streamName,
                                  ReadRowsHelper.Options options,
                                  ReadRowsResponseToInternalRowIteratorConverter converter) {
        this.bigQueryReadClientFactory = bigQueryReadClientFactory;
        this.streamName = streamName;
        this.options = options;
        this.converter = converter;
    }

    @Override
    public String[] preferredLocations() {
        return new String[0];

    }
}
