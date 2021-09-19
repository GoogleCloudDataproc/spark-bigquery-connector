import com.google.cloud.bigquery.connector.common.BigQueryReadClientFactory;
import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.spark.bigquery.ReadRowsResponseToInternalRowIteratorConverter;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

import java.util.Iterator;

public class BigqueryPartitionReaderFactory implements PartitionReaderFactory {
    private final BigQueryReadClientFactory bigQueryReadClientFactory;
    private final String streamName;
    private final ReadRowsHelper.Options options;
    private final ReadRowsResponseToInternalRowIteratorConverter converter;


     BigqueryPartitionReaderFactory(BigQueryReadClientFactory bigQueryReadClientFactory, String streamName, ReadRowsHelper.Options options,
                                   ReadRowsResponseToInternalRowIteratorConverter converter) {
        this.bigQueryReadClientFactory = bigQueryReadClientFactory;
        this.streamName = streamName;
        this.options = options;
        this.converter = converter;
    }

    // need to check flow:- how to pass InputPartition for which we want to create partition readers on executors
    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        ReadRowsRequest.Builder readRowsRequest =
                ReadRowsRequest.newBuilder().setReadStream(streamName);
        ReadRowsHelper readRowsHelper =
                new ReadRowsHelper(bigQueryReadClientFactory, readRowsRequest, options);
        Iterator<ReadRowsResponse> readRowsResponses = readRowsHelper.readRows();
        return new BigQueryInputPartitionReader(readRowsResponses,converter,readRowsHelper);
    }


    @Override
    public boolean supportColumnarReads(InputPartition partition) {
        return true;
    }
}
