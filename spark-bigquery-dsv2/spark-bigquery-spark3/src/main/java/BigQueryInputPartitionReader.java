import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.spark.bigquery.ReadRowsResponseToInternalRowIteratorConverter;
import com.google.common.collect.ImmutableList;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;

import java.io.IOException;
import java.util.Iterator;

public class BigQueryInputPartitionReader implements PartitionReader<InternalRow> {
    private Iterator<ReadRowsResponse> readRowsResponses;
    private ReadRowsResponseToInternalRowIteratorConverter converter;
    private ReadRowsHelper readRowsHelper;
    private Iterator<InternalRow> rows = ImmutableList.<InternalRow>of().iterator();
    private InternalRow currentRow;

    BigQueryInputPartitionReader(Iterator<ReadRowsResponse> readRowsResponses, ReadRowsResponseToInternalRowIteratorConverter converter,
                                 ReadRowsHelper readRowsHelper) {
        this.readRowsResponses = readRowsResponses;
        this.converter = converter;
        this.readRowsHelper = readRowsHelper;
    }

    @Override
    public boolean next() throws IOException {
        while (!rows.hasNext()) {
            if (!readRowsResponses.hasNext()) {
                return false;
            }
            ReadRowsResponse readRowsResponse = readRowsResponses.next();
            rows = converter.convert(readRowsResponse);
        }
        currentRow = rows.next();
        return true;
    }

    @Override
    public InternalRow get() {
        return currentRow;
    }

    @Override
    public void close() throws IOException {
        readRowsHelper.close();
    }
}
